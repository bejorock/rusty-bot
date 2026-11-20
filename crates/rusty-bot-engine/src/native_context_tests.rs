use super::*;
use rusty_bot_core::*;
use rusty_bot_memory::MemoryStorage;
use serde_json::json;
fn request() -> HistoryRequest {
    HistoryRequest {
        model: "model".into(),
        messages: vec![
            Message::System {
                content: String::new(),
                sections: Default::default(),
                tools_added: vec![],
                tools_removed: vec![],
            },
            Message::user_text("summary"),
        ],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        temperature: None,
        max_tokens: Some(100),
        metadata: json!({}),
    }
}
fn task() -> TaskRecord {
    TaskRecord {
        id: TaskId(2),
        conversation_id: ConversationId(1),
        kind: "turn".into(),
        version: 0,
        input: json!({}),
        owner: None,
        background: false,
        abort_requested: false,
        abort_reason: None,
        abandon_on_restart: false,
        started_at: None,
        ended_at: None,
        restarts: 0,
        state: TaskState::Pending {
            checkpoint: None,
            wake: None,
        },
        memos: Default::default(),
    }
}
fn entry(id: i64, text: &str) -> EntryRecord {
    EntryRecord {
        id: EntryId(id),
        conversation_id: ConversationId(1),
        kind: "user".into(),
        model: vec![Message::user_text(text)],
        data: json!({}),
        head: None,
        edits: vec![],
        by_task_id: Some(TaskId(2)),
    }
}
async fn commit(s: &MemoryStorage, writes: Vec<Write>) {
    assert!(s
        .commit(
            vec![CommitBatch { writes }],
            &CommitContext::conversation(SessionId::new(), ConversationId(1))
        )
        .await
        .unwrap()
        .is_fully_committed());
}
#[tokio::test]
async fn baseline_roundtrip_reconstructs_suffix_and_updates_frontier() {
    let s = MemoryStorage::new();
    let c = ConversationId(1);
    let r = request();
    assert!(load(&s, c, "provider", &r).await.unwrap().is_none());
    let t = task();
    commit(
        &s,
        vec![Write::Task(t.clone()), Write::Entry(entry(10, "first"))],
    )
    .await;
    let updated = save(&s, SessionId::new(), c, &t, "provider", &r, EntryId(10))
        .await
        .unwrap();
    assert_eq!(updated.version, 1);
    commit(&s, vec![Write::Entry(entry(11, "suffix"))]).await;
    let (checkpoint, effective) = load(&s, c, "provider", &r).await.unwrap().unwrap();
    assert_eq!(effective.messages.len(), 3);
    assert_eq!(
        effective.messages.last(),
        Some(&Message::user_text("suffix"))
    );
    let next = save(
        &s,
        SessionId::new(),
        c,
        &updated,
        "provider",
        &effective,
        EntryId(11),
    )
    .await
    .unwrap();
    let (second, _) = load(&s, c, "provider", &r).await.unwrap().unwrap();
    assert_eq!(second.epoch, checkpoint.epoch);
    assert_eq!(second.marker_id, checkpoint.marker_id);
    assert_eq!(next.version, 2);
    assert!(
        save(&s, SessionId::new(), c, &next, "provider", &r, EntryId(10))
            .await
            .is_err()
    );
    assert!(load(&s, c, "other", &r).await.is_err());
    let mut changed = r.clone();
    changed.temperature = Some(0.7);
    assert!(load(&s, c, "provider", &changed).await.is_err());
}
#[tokio::test]
async fn checkpoint_validation_refuses_invalid_inputs_or_unpaired_marker() {
    let s = MemoryStorage::new();
    let c = ConversationId(1);
    let t = task();
    let r = request();
    assert!(save(
        &s,
        SessionId::new(),
        ConversationId(9),
        &t,
        "p",
        &r,
        EntryId(1)
    )
    .await
    .is_err());
    assert!(save(&s, SessionId::new(), c, &t, "", &r, EntryId(1))
        .await
        .is_err());
    let mut no_system = r.clone();
    no_system.messages.clear();
    assert!(
        save(&s, SessionId::new(), c, &t, "p", &no_system, EntryId(1))
            .await
            .is_err()
    );
    assert!(save(&s, SessionId::new(), c, &t, "p", &r, EntryId(1))
        .await
        .is_err());
    commit(
        &s,
        vec![Write::Document(DocumentWrite::PutLatest {
            address: address(c),
            value: json!({}),
        })],
    )
    .await;
    assert!(load(&s, c, "p", &r).await.is_err());
}
#[tokio::test]
async fn corrupt_checkpoint_fails_closed_without_sensitive_content_in_error() {
    let s = MemoryStorage::new();
    let c = ConversationId(1);
    let t = task();
    let r = request();
    commit(
        &s,
        vec![Write::Task(t.clone()), Write::Entry(entry(10, "first"))],
    )
    .await;
    save(&s, SessionId::new(), c, &t, "p", &r, EntryId(10))
        .await
        .unwrap();
    let doc = s
        .get_document(&address(c), DocumentPoint::Current)
        .await
        .unwrap()
        .unwrap();
    for field in ["version", "provider_id", "epoch", "frontier", "marker_id"] {
        let mut v = doc.value.clone();
        v[field] = json!("private-invalid-value");
        commit(
            &s,
            vec![Write::Document(DocumentWrite::PutLatest {
                address: address(c),
                value: v,
            })],
        )
        .await;
        let err = load(&s, c, "p", &r).await.err().unwrap();
        assert_eq!(
            err.to_string(),
            "contract violation: invalid native context checkpoint"
        );
    }
}
#[test]
fn bounded_writer_and_identity_reject_invalid_baselines() {
    let mut writer = Bounded(vec![]);
    assert_eq!(writer.write(b"abc").unwrap(), 3);
    writer.flush().unwrap();
    assert_eq!(writer.0, b"abc");
    let r = request();
    assert!(identity_matches(&r, &r));
    let mut other = r.clone();
    other.messages.remove(0);
    assert!(!identity_matches(&r, &other));
    assert_eq!(bounded_json(&json!({"a":"é"})).unwrap(), json!({"a":"é"}));
}
