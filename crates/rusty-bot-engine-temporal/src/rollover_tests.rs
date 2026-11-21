use super::*;
use crate::tests::FakeModel;
use rusty_bot_session::MemorySessionStorage;

async fn host(label: &str, idle: Duration, turns: u64) -> (Engine, Arc<FakeModel>, String) {
    let server = test_support::TestServer::connect(label).await;
    let model = Arc::new(FakeModel::default());
    let engine = Engine::connect(
        EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: idle,
        },
        model.clone(),
        Arc::new(MemorySessionStorage::default()),
    )
    .await
    .unwrap()
    .with_rollover_policy(RolloverPolicy {
        max_turns: turns,
        max_history_events: 8_000,
    })
    .unwrap();
    (engine, model, server.workflow_id)
}

async fn successor(session: &Session, previous: &str) -> String {
    loop {
        let current = session.current_run_id().await.unwrap();
        if current != previous {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
#[ignore = "requires TEMPORAL_ADDRESS; exercises repeated history rollover with fake model"]
async fn rollover_preserves_context_dedup_and_old_handles_follow_only_their_chain() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (engine, model, id) = host("rollover-chain", Duration::from_secs(30), 2).await;
        engine
            .run_until(async {
                let first = engine
                    .start_session(id.clone(), "fake".into())
                    .await
                    .unwrap();
                let attached = engine.resume_session(&id).await.unwrap();
                assert_eq!(
                    first
                        .send_request("one".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to hello; 0 prior messages".into())
                );
                assert_eq!(
                    first
                        .send_request("two".into(), "again".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to again; 2 prior messages".into())
                );
                let next = successor(&first, first.run_id()).await;
                assert_eq!(attached.current_run_id().await.unwrap(), next);
                assert_eq!(first.workflow_id(), attached.workflow_id());
                assert_eq!(
                    first
                        .send_request("one".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to hello; 0 prior messages".into())
                );
                assert_eq!(model.requests.lock().unwrap().len(), 2);
                assert_eq!(
                    attached
                        .send_request("three".into(), "third".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to third; 4 prior messages".into())
                );
                let third = successor(&first, &next).await;
                let closed = first.close().await.unwrap();
                assert_eq!(closed.messages.len(), 6);
                assert_eq!(attached.wait_closed().await.unwrap(), closed);
                assert_eq!(
                    first.execution_status().await.unwrap(),
                    ExecutionStatus::Completed
                );
                let resumed = engine.resume_session(&id).await.unwrap();
                assert_ne!(resumed.run_id(), third);
                assert_eq!(first.close().await.unwrap(), closed);
                assert_eq!(
                    resumed.execution_status().await.unwrap(),
                    ExecutionStatus::Running
                );
                assert_eq!(
                    resumed
                        .send_request("three".into(), "third".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to third; 4 prior messages".into())
                );
                assert_eq!(model.requests.lock().unwrap().len(), 3);
                resumed.close().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires TEMPORAL_ADDRESS; checks idle closure through a successor run"]
async fn rollover_carries_idle_deadline_and_wait_closed_follows_successor() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (engine, model, id) = host("rollover-idle", Duration::from_millis(1_500), 1).await;
        engine
            .run_until(async {
                let session = engine.start_session(id, "fake".into()).await.unwrap();
                session
                    .send_request("one".into(), "hello".into())
                    .await
                    .unwrap();
                let next = successor(&session, session.run_id()).await;
                assert_ne!(next, session.run_id());
                let closed = session.wait_closed().await.unwrap();
                assert!(closed.closing);
                assert_eq!(closed.messages.len(), 2);
                assert_eq!(model.requests.lock().unwrap().len(), 1);
                assert_eq!(
                    session.execution_status().await.unwrap(),
                    ExecutionStatus::Completed
                );
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn old_inputs_decode_with_safe_rollover_defaults() {
    let input: SessionInput =
        serde_json::from_value(serde_json::json!({"session_id":"id", "idle_timeout_ms":5000}))
            .unwrap();
    assert_eq!(input.rollover.max_turns, 100);
    assert_eq!(input.rollover.max_history_events, 8_000);
    assert_eq!(input.idle_deadline_ms, None);
}
