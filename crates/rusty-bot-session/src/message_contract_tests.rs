use super::*;
use serde_json::json;

fn replay(required: bool) -> ReplayState {
    ReplayState {
        provider: "anthropic".into(),
        model: Some("model".into()),
        format: "thinking".into(),
        version: 1,
        payload: json!({"secret":"do-not-log"}),
        required_for_replay: required,
    }
}
#[tokio::test]
async fn canonical_response_storage_is_atomic_immutable_and_not_lossily_projected() {
    let storage = MemorySessionStorage::default();
    let session = StoredSession::new(
        SessionConfig {
            id: "session".into(),
            model: "model".into(),
            namespace: "default".into(),
            task_queue: "q".into(),
            idle_timeout_ms: 1000,
            initial_history: ChatHistory::default(),
        },
        "workflow".into(),
    );
    storage.create(session).await.unwrap();
    storage
        .change(
            "session",
            SessionChange::Claim {
                run_id: "run".into(),
            },
        )
        .await
        .unwrap();
    storage
        .change(
            "session",
            SessionChange::Begin {
                run_id: "run".into(),
                request_id: "request".into(),
                text: "question".into(),
            },
        )
        .await
        .unwrap();
    let mut response = HistoryEntry::completed(
        "turn:1:assistant",
        ConversationMessage::Assistant {
            content: vec![
                ContentBlock::Replay {
                    state: replay(true),
                },
                ContentBlock::text("answer"),
            ],
        },
    );
    response.turn_id = Some(1);
    response.generation = Some(GenerationMetadata {
        finish_reason: Some(FinishReason::Stop),
        ..Default::default()
    });
    let change = SessionChange::FinishResponse {
        run_id: "run".into(),
        turn_id: 1,
        response: Box::new(response.clone()),
    };
    let saved = storage.change("session", change.clone()).await.unwrap();
    assert_eq!(storage.change("session", change).await.unwrap(), saved);
    assert_eq!(saved.history().entries[1], response);
    assert!(saved.try_completed_context().is_err());
    assert!(saved.requires_history_translation());
    let restored: StoredSession =
        serde_json::from_str(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(restored, saved);
    response.artifact = Some(json!({"changed":true}));
    assert!(storage
        .change(
            "session",
            SessionChange::FinishResponse {
                run_id: "run".into(),
                turn_id: 1,
                response: Box::new(response)
            }
        )
        .await
        .is_err());
    assert!(storage
        .change(
            "session",
            SessionChange::Finish {
                run_id: "run".into(),
                turn_id: 1,
                result: Ok("overwrite".into())
            }
        )
        .await
        .is_err());
    assert_eq!(storage.read("session").await.unwrap(), saved);
}
