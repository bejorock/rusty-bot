use super::*;
use rusty_bot_core::{DocumentFork, DocumentScope, TaskId};
use serde_json::json;

fn address(key: &str) -> DocumentAddress {
    DocumentAddress::new("partial", DocumentScope::Task(TaskId(1)), key)
}
fn put(key: &str, value: i64) -> DocumentWrite {
    DocumentWrite::PutLatest {
        address: address(key),
        value: json!(value),
    }
}

#[test]
fn coalescing_keeps_latest_value_and_original_deadline() {
    let mut buffer = DeferredBuffer::new();
    buffer.defer(put("a", 1), 100).unwrap();
    buffer.defer(put("a", 2), 199).unwrap();
    assert_eq!(buffer.len(), 1);
    assert!(buffer.contains(&address("a")));
    assert_eq!(buffer.write_for(&address("a")), Some(&put("a", 2)));
    assert!(buffer.take_due(199, 100).is_empty());
    assert_eq!(buffer.take_due(200, 100), vec![put("a", 2)]);
    assert!(buffer.is_empty());
}

#[test]
fn due_selection_preserves_pending_writes_and_insertion_order() {
    let mut buffer = DeferredBuffer::new();
    buffer.defer(put("a", 1), 100).unwrap();
    buffer.defer(put("b", 2), 150).unwrap();
    buffer.defer(put("c", 3), 100).unwrap();
    assert_eq!(buffer.take_due(200, 100), vec![put("a", 1), put("c", 3)]);
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.drain(), vec![put("b", 2)]);
    assert!(buffer.drain().is_empty());
    assert!(!buffer.contains(&address("b")));
    assert!(buffer.write_for(&address("b")).is_none());
}

#[test]
fn history_changing_writes_are_rejected_without_mutation() {
    let mut buffer = DeferredBuffer::new();
    buffer.defer(put("a", 1), 0).unwrap();
    for write in [
        DocumentWrite::Retire {
            address: address("a"),
        },
        DocumentWrite::PutRewindable {
            address: address("b"),
            fork: DocumentFork::Initial,
            value: json!(2),
        },
    ] {
        assert!(!is_deferred_eligible(&write));
        assert!(matches!(
            buffer.defer(write, 0),
            Err(TxError::IneligibleDeferredWrite)
        ));
        assert_eq!(buffer.len(), 1);
    }
    assert_eq!(buffer.drain(), vec![put("a", 1)]);
}

#[test]
fn delta_replaces_pending_write_but_different_scopes_do_not_coalesce() {
    let mut buffer = DeferredBuffer::new();
    buffer.defer(put("a", 1), 0).unwrap();
    let delta = DocumentWrite::Delta {
        address: address("a"),
        ops: json!({"set": 2}),
    };
    assert!(is_deferred_eligible(&delta));
    buffer.defer(delta.clone(), 1).unwrap();
    let other = DocumentWrite::PutLatest {
        address: DocumentAddress::new("partial", DocumentScope::Task(TaskId(2)), "a"),
        value: json!(3),
    };
    buffer.defer(other.clone(), 1).unwrap();
    assert_eq!(buffer.drain(), vec![delta, other]);
}

#[test]
fn backward_clock_does_not_expire_a_future_deadline() {
    let mut buffer = DeferredBuffer::new();
    buffer.defer(put("a", 1), 100).unwrap();
    assert!(buffer.take_due(99, 10).is_empty());
    assert_eq!(buffer.take_due(100, 0), vec![put("a", 1)]);
}
