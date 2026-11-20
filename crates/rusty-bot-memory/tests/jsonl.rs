use rusty_bot_core::{
    CommitBatch, CommitContext, ConversationId, ConversationRecord, EventCursor, LiveFrame,
    LiveKind, SessionId, Storage, Write,
};
use rusty_bot_memory::FileSessionAdapter;
use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rusty-jsonl-{}-{}.jsonl",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn session() -> SessionId {
    SessionId(serde_json::from_str("\"00000000-0000-0000-0000-000000000001\"").unwrap())
}
fn frame(n: u64) -> LiveFrame {
    LiveFrame {
        task_id: None,
        kind: LiveKind::TextDelta,
        data: serde_json::json!({"text": n.to_string()}),
    }
}

#[tokio::test]
async fn round_trip_preserves_ids_commits_timestamps_and_progress() {
    let path = Temp::new();
    let session = session();
    let conversation = ConversationId(2);
    let expected_events;
    {
        let adapter = FileSessionAdapter::open(&path.0).unwrap();
        assert_eq!(adapter.mint_id().await.unwrap(), 1);
        assert_eq!(adapter.mint_id().await.unwrap(), 2);
        let receipt = adapter
            .commit(
                vec![CommitBatch {
                    writes: vec![
                        Write::Conversation(ConversationRecord {
                            id: conversation,
                            parent: None,
                            owner: None,
                        }),
                        Write::Event(rusty_bot_core::SessionEventDraft {
                            kind: "test".into(),
                            payload: serde_json::json!({"ok": true}),
                        }),
                    ],
                }],
                &CommitContext::conversation(session, conversation),
            )
            .await
            .unwrap();
        assert!(receipt.is_fully_committed());
        for n in 0..150 {
            adapter.append_frame(session, frame(n)).unwrap();
        }
        adapter.append_frame(SessionId::new(), frame(999)).unwrap();
        expected_events = adapter
            .scan_events(session, EventCursor::start(), 100)
            .await
            .unwrap()
            .events;
    }
    let adapter = FileSessionAdapter::open(&path.0).unwrap();
    assert_eq!(adapter.mint_id().await.unwrap(), 3);
    assert_eq!(
        adapter
            .get_conversation(conversation)
            .await
            .unwrap()
            .unwrap()
            .record
            .id,
        conversation
    );
    assert_eq!(
        adapter
            .scan_events(session, EventCursor::start(), 100)
            .await
            .unwrap()
            .events,
        expected_events
    );
    let mut after = 0;
    let mut frames = Vec::new();
    loop {
        let page = adapter.read_frames(session, after, 17).await.unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().cursor;
        frames.extend(page);
    }
    assert_eq!(frames.len(), 150);
    for (n, stored) in frames.iter().enumerate() {
        assert_eq!(stored.frame, frame(n as u64));
    }
}

#[tokio::test]
async fn refused_batch_is_not_journaled_and_durable_prefix_survives() {
    let path = Temp::new();
    {
        let adapter = FileSessionAdapter::open(&path.0).unwrap();
        let receipt = adapter
            .commit(
                vec![
                    CommitBatch {
                        writes: vec![Write::Event(rusty_bot_core::SessionEventDraft {
                            kind: "kept".into(),
                            payload: serde_json::Value::Null,
                        })],
                    },
                    CommitBatch { writes: vec![] },
                    CommitBatch {
                        writes: vec![Write::Event(rusty_bot_core::SessionEventDraft {
                            kind: "not reached".into(),
                            payload: serde_json::Value::Null,
                        })],
                    },
                ],
                &CommitContext::session(session()),
            )
            .await
            .unwrap();
        assert_eq!(receipt.batches.len(), 1);
        assert_eq!(receipt.refused.unwrap().batch_index, 1);
    }
    let adapter = FileSessionAdapter::open(&path.0).unwrap();
    let events = adapter
        .scan_events(session(), EventCursor::start(), 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].kind, "kept");
}

#[tokio::test]
async fn exclusive_owner_and_torn_tail_recovery() {
    let path = Temp::new();
    {
        let adapter = FileSessionAdapter::open(&path.0).unwrap();
        assert!(FileSessionAdapter::open(&path.0).is_err());
        adapter.append_frame(session(), frame(1)).unwrap();
    }
    let length = fs::metadata(&path.0).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&path.0)
        .unwrap()
        .write_all(b"{\"type\":\"frame\"")
        .unwrap();
    {
        let adapter = FileSessionAdapter::open(&path.0).unwrap();
        assert_eq!(fs::metadata(&path.0).unwrap().len(), length);
        adapter.append_frame(session(), frame(2)).unwrap();
    }
    let adapter = FileSessionAdapter::open(&path.0).unwrap();
    assert_eq!(
        adapter.read_frames(session(), 0, 10).await.unwrap().len(),
        2
    );
}

#[test]
fn complete_corrupt_records_fail_without_modifying_file() {
    let path = Temp::new();
    fs::write(&path.0, b"not-json\n").unwrap();
    assert!(FileSessionAdapter::open(&path.0).is_err());
    assert_eq!(fs::read(&path.0).unwrap(), b"not-json\n");
}
