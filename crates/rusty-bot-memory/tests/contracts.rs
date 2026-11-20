use rusty_bot_core::*;
use rusty_bot_memory::InMemoryStorage;

fn event(label: &str) -> Write {
    Write::Event(SessionEventDraft {
        kind: label.into(),
        payload: serde_json::Value::Null,
    })
}

#[tokio::test]
async fn refused_batch_does_not_leak_partial_writes() {
    let storage = InMemoryStorage::new();
    let session = SessionId::new();
    let conversation = ConversationRecord {
        id: ConversationId(1),
        parent: None,
        owner: None,
    };
    let submission = |id| {
        Write::Submission(SubmissionRecord {
            id: SubmissionId(id),
            conversation_id: conversation.id,
            request_id: Some("same-request".into()),
            kind: SubmissionKind::Input,
            status: SubmissionStatus::Queued,
            payload: serde_json::Value::Null,
            entry_id: None,
            error: None,
        })
    };
    let receipt = storage
        .commit(
            vec![CommitBatch {
                writes: vec![
                    Write::Conversation(conversation.clone()),
                    submission(2),
                    submission(3),
                ],
            }],
            &CommitContext::session(session),
        )
        .await
        .unwrap();
    assert!(receipt.refused.is_some());
    assert!(storage
        .get_conversation(ConversationId(1))
        .await
        .unwrap()
        .is_none());
    assert!(storage
        .scan_events(session, EventCursor::start(), 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn event_pagination_keeps_same_commit_ordinals_and_session_isolation() {
    let storage = InMemoryStorage::new();
    let session = SessionId::new();
    storage
        .commit(
            vec![CommitBatch {
                writes: vec![event("one"), event("two"), event("three")],
            }],
            &CommitContext::session(session),
        )
        .await
        .unwrap();
    storage
        .commit(
            vec![CommitBatch {
                writes: vec![event("other-session")],
            }],
            &CommitContext::session(SessionId::new()),
        )
        .await
        .unwrap();
    let mut cursor = EventCursor::start();
    let mut kinds = Vec::new();
    loop {
        let page = storage.scan_events(session, cursor, 1).await.unwrap();
        kinds.extend(page.events.iter().map(|event| event.kind.clone()));
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(kinds, ["one", "two", "three"]);
}

#[tokio::test]
async fn entry_pagination_supports_both_directions_without_duplicates() {
    let storage = InMemoryStorage::new();
    let conversation = ConversationId(1);
    let mut writes = vec![Write::Conversation(ConversationRecord {
        id: conversation,
        parent: None,
        owner: None,
    })];
    for id in 2..7 {
        writes.push(Write::Entry(EntryRecord {
            id: EntryId(id),
            conversation_id: conversation,
            kind: entry_kind::USER.into(),
            model: vec![],
            data: serde_json::Value::Null,
            head: None,
            edits: vec![],
            by_task_id: None,
        }));
    }
    assert!(storage
        .commit(
            vec![CommitBatch { writes }],
            &CommitContext::conversation(SessionId::new(), conversation)
        )
        .await
        .unwrap()
        .is_fully_committed());
    for ascending in [true, false] {
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let page = storage
                .scan_entries(
                    EntryQuery {
                        conversation_id: conversation,
                        kind: None,
                        by_task_id: None,
                        ascending,
                    },
                    cursor,
                    2,
                )
                .await
                .unwrap();
            ids.extend(page.items.iter().map(|entry| entry.record.id.0));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(
            ids,
            if ascending {
                vec![2, 3, 4, 5, 6]
            } else {
                vec![6, 5, 4, 3, 2]
            }
        );
    }
}

#[tokio::test]
async fn progress_cursors_filter_sessions_without_skipping_records() {
    let storage = InMemoryStorage::new();
    let first = SessionId::new();
    let second = SessionId::new();
    let frame = || LiveFrame {
        task_id: None,
        kind: LiveKind::TextDelta,
        data: serde_json::json!({"text":"test"}),
    };
    storage.append_frame(first, frame()).unwrap();
    storage.append_frame(second, frame()).unwrap();
    storage.append_frame(first, frame()).unwrap();
    let page = storage.read_frames(first, 0, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    let next = storage
        .read_frames(first, page[0].cursor, 10)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(
        next[0].cursor,
        storage.last_frame_cursor(first).await.unwrap()
    );
    assert!(storage
        .read_frames(first, next[0].cursor, 1)
        .await
        .unwrap()
        .is_empty());
}
