use super::*;
use rusty_bot_core::*;
use serde_json::json;
fn address(key: &str) -> DocumentAddress {
    DocumentAddress::new("state", DocumentScope::Conversation(ConversationId(1)), key)
}
async fn write(storage: &InMemoryStorage, writes: Vec<Write>) -> CommitReceipt {
    storage
        .commit(
            vec![CommitBatch { writes }],
            &CommitContext {
                session_id: SessionId::new(),
                conversation_id: Some(ConversationId(1)),
                task_id: None,
            },
        )
        .await
        .unwrap()
}
async fn document(storage: &InMemoryStorage, write_doc: DocumentWrite) -> Seq {
    let receipt = write(storage, vec![Write::Document(write_doc)]).await;
    assert!(receipt.is_fully_committed());
    receipt.batches[0].seq
}
#[tokio::test]
async fn latest_document_revisions_are_queryable_at_each_commit() {
    let s = InMemoryStorage::new();
    let a = address("key");
    let first = document(
        &s,
        DocumentWrite::PutLatest {
            address: a.clone(),
            value: json!({"a":1}),
        },
    )
    .await;
    let second = document(
        &s,
        DocumentWrite::PutLatest {
            address: a.clone(),
            value: json!({"b":2}),
        },
    )
    .await;
    let old = s
        .get_document(&a, DocumentPoint::Seq(first))
        .await
        .unwrap()
        .unwrap();
    let new = s
        .get_document(&a, DocumentPoint::Seq(second))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.value, json!({"a":1}));
    assert_eq!(new.value, json!({"b":2}));
    assert_eq!(old.id, new.id);
    assert_eq!(new.version, 2);
    document(&s, DocumentWrite::Retire { address: a.clone() }).await;
    assert!(s
        .get_document(&a, DocumentPoint::Current)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        s.get_document(&a, DocumentPoint::Seq(first))
            .await
            .unwrap()
            .unwrap()
            .value,
        old.value
    );
}
#[tokio::test]
async fn family_replacement_retires_only_other_keys_in_same_scope() {
    let s = InMemoryStorage::new();
    let a = address("old").family();
    let b = address("new").family();
    let seq = document(
        &s,
        DocumentWrite::PutLatest {
            address: a.clone(),
            value: json!(1),
        },
    )
    .await;
    document(
        &s,
        DocumentWrite::PutLatest {
            address: b.clone(),
            value: json!(2),
        },
    )
    .await;
    assert!(s
        .get_document(&a, DocumentPoint::Current)
        .await
        .unwrap()
        .is_none());
    assert!(s
        .get_document(&a, DocumentPoint::Seq(seq))
        .await
        .unwrap()
        .is_some());
    let query = DocumentQuery {
        kind: Some("state".into()),
        scope: Some(a.scope.clone()),
        key: Some("new".into()),
        at: DocumentPoint::Current,
        limit: 0,
    };
    assert_eq!(s.scan_documents(query).await.unwrap()[0].value, json!(2));
}
#[tokio::test]
async fn deltas_create_update_merge_replace_and_rewindable_rows() {
    let s = InMemoryStorage::new();
    let a = address("key");
    document(
        &s,
        DocumentWrite::Delta {
            address: a.clone(),
            ops: json!({"merge":{"a":1}}),
        },
    )
    .await;
    document(
        &s,
        DocumentWrite::Delta {
            address: a.clone(),
            ops: json!({"merge":{"b":2}}),
        },
    )
    .await;
    assert_eq!(
        s.get_document(&a, DocumentPoint::Current)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!({"a":1,"b":2})
    );
    document(
        &s,
        DocumentWrite::Delta {
            address: a.clone(),
            ops: json!({"set":[1,2]}),
        },
    )
    .await;
    assert_eq!(
        s.get_document(&a, DocumentPoint::Current)
            .await
            .unwrap()
            .unwrap()
            .version,
        3
    );
    document(
        &s,
        DocumentWrite::PutRewindable {
            address: a.clone(),
            fork: DocumentFork::Current,
            value: json!("rewound"),
        },
    )
    .await;
    assert_eq!(
        s.get_document(&a, DocumentPoint::Current)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!("rewound")
    );
    assert_eq!(apply_delta(&json!({}), &json!(5)), json!(5));
    assert_eq!(apply_delta(&json!(null), &json!({"merge":[1]})), json!([1]));
    let all = s
        .scan_documents(DocumentQuery {
            kind: None,
            scope: None,
            key: None,
            at: DocumentPoint::Current,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
}
#[test]
fn document_scope_roundtrip_and_rejection_preserve_owner_identity() {
    for scope in [
        DocumentScope::Session(SessionId::new()),
        DocumentScope::Conversation(ConversationId(7)),
        DocumentScope::Task(TaskId(8)),
    ] {
        let value = serde_json::to_value(&scope).unwrap();
        assert_eq!(
            serde_json::from_value::<DocumentScope>(value).unwrap(),
            scope
        );
        let _ = scope.owner_id();
    }
    assert!(serde_json::from_value::<DocumentScope>(
        json!({"scope_kind":"session","id":"not-uuid"})
    )
    .is_err());
}
