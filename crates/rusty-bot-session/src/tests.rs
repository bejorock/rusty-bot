use super::*;
fn session() -> StoredSession {
    StoredSession::new(
        SessionConfig {
            id: "session".into(),
            model: "model".into(),
            namespace: "default".into(),
            task_queue: "queue".into(),
            idle_timeout_ms: 1000,
            initial_history: Default::default(),
        },
        "workflow".into(),
    )
}
fn claim(run: &str) -> SessionChange {
    SessionChange::Claim { run_id: run.into() }
}
fn begin(request: &str, text: &str) -> SessionChange {
    SessionChange::Begin {
        run_id: "run".into(),
        request_id: request.into(),
        text: text.into(),
    }
}
#[tokio::test]
async fn concurrent_create_preserves_identity_and_rejects_config_mismatch() {
    let store = MemorySessionStorage::default();
    let (a, b) = tokio::join!(
        store.create(session()),
        store.create(StoredSession::new(session().config, "other".into()))
    );
    assert_eq!(a.unwrap().workflow_id, b.unwrap().workflow_id);
    let mut wrong = session();
    wrong.config.model = "different".into();
    assert!(store.create(wrong).await.is_err());
}
#[test]
fn request_dedup_conflicts_and_final_commit_are_atomic() {
    let mut s = session();
    s.apply(&claim("run")).unwrap();
    s.apply(&begin("request", "hello")).unwrap();
    s.apply(&begin("request", "hello")).unwrap();
    let before = s.clone();
    assert!(s.apply(&begin("request", "different")).is_err());
    assert_eq!(s, before);
    assert!(s.apply(&begin("another", "hello")).is_err());
    let finish = SessionChange::Finish {
        run_id: "run".into(),
        turn_id: 1,
        result: Ok("answer".into()),
    };
    s.apply(&finish).unwrap();
    s.apply(&finish).unwrap();
    assert_eq!(
        s.completed_context(),
        vec![
            ChatMessage::User("hello".into()),
            ChatMessage::Assistant("answer".into())
        ]
    );
    assert!(s
        .apply(&SessionChange::Finish {
            run_id: "run".into(),
            turn_id: 1,
            result: Err("error".into())
        })
        .is_err());
}
#[test]
fn drafts_are_ordered_and_never_replace_a_final_result() {
    let mut s = session();
    s.apply(&claim("run")).unwrap();
    s.apply(&begin("request", "hello")).unwrap();
    for (seq, text) in [(2, "new"), (1, "old"), (2, "duplicate")] {
        s.apply(&SessionChange::Draft {
            run_id: "run".into(),
            turn_id: 1,
            sequence: seq,
            text: text.into(),
        })
        .unwrap();
    }
    assert_eq!(s.turns[0].draft, "new");
    assert!(s.snapshot().generating);
    assert!(s.completed_context().is_empty());
    s.apply(&SessionChange::Finish {
        run_id: "run".into(),
        turn_id: 1,
        result: Ok("final".into()),
    })
    .unwrap();
    s.apply(&SessionChange::Draft {
        run_id: "run".into(),
        turn_id: 1,
        sequence: 999,
        text: "late".into(),
    })
    .unwrap();
    assert_eq!(
        s.snapshot().messages[1].message,
        ChatMessage::Assistant("final".into())
    );
}
#[test]
fn new_execution_fences_old_writers_and_keeps_interrupted_turn_out_of_context() {
    let mut s = session();
    s.apply(&claim("run")).unwrap();
    s.apply(&begin("request", "hello")).unwrap();
    s.apply(&claim("new")).unwrap();
    let before = s.clone();
    assert!(s.apply(&claim("run")).is_err());
    assert_eq!(s, before);
    assert!(s
        .apply(&SessionChange::Finish {
            run_id: "run".into(),
            turn_id: 1,
            result: Ok("late".into())
        })
        .is_err());
    assert!(!s.snapshot().generating);
    assert_eq!(s.snapshot().messages[1].status, MessageStatus::Failed);
    assert!(s.completed_context().is_empty());
}
#[test]
fn close_is_not_permanent_and_round_trip_preserves_dedup() {
    let mut s = session();
    s.apply(&claim("run")).unwrap();
    s.apply(&SessionChange::Close {
        run_id: "run".into(),
    })
    .unwrap();
    assert!(s.closed);
    s.apply(&claim("next")).unwrap();
    assert!(!s.closed);
    assert_eq!(
        serde_json::from_str::<StoredSession>(&serde_json::to_string(&s).unwrap()).unwrap(),
        s
    );
}
