use super::*;
use crate::tests::FakeModel;

async fn engine(label: &str, idle_timeout: Duration) -> (Engine, Arc<FakeModel>, String) {
    let server = test_support::TestServer::connect(label).await;
    let model = Arc::new(FakeModel::default());
    let engine = Engine::connect(
        EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout,
        },
        model.clone(),
        Arc::new(rusty_bot_session::MemorySessionStorage::default()),
    )
    .await
    .unwrap();
    (engine, model, server.workflow_id)
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn manual_close_is_repeatable_and_old_handles_cannot_close_resumed_run() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (engine, model, id) = engine("manual-lifecycle", Duration::from_secs(30)).await;
        engine
            .run_until(async {
                let first = engine
                    .start_session(id.clone(), "fake".into())
                    .await
                    .unwrap();
                let attached = engine.resume_session(&id).await.unwrap();
                assert_eq!(first.run_id(), attached.run_id());
                assert_eq!(
                    first.execution_status().await.unwrap(),
                    ExecutionStatus::Running
                );
                first
                    .send_request("request".into(), "hello".into())
                    .await
                    .unwrap();
                let (closed, also_closed) = tokio::join!(first.close(), attached.close());
                let closed = closed.unwrap();
                assert_eq!(also_closed.unwrap(), closed);
                assert!(closed.closing);
                assert_eq!(
                    first.execution_status().await.unwrap(),
                    ExecutionStatus::Completed
                );
                assert_eq!(first.close().await.unwrap(), closed);
                assert_eq!(first.wait_closed().await.unwrap(), closed);
                assert!(first.send_message("old handle".into()).await.is_err());

                let resumed = engine.resume_session(&id).await.unwrap();
                assert_eq!(resumed.id(), first.id());
                assert_eq!(resumed.workflow_id(), first.workflow_id());
                assert_ne!(resumed.run_id(), first.run_id());
                // Old handles remain pinned even after a fresh execution starts.
                assert_eq!(first.close().await.unwrap(), closed);
                assert_eq!(
                    resumed.execution_status().await.unwrap(),
                    ExecutionStatus::Running
                );
                assert!(matches!(
                    resumed
                        .send_request("request".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed(_)
                ));
                assert_eq!(model.requests.lock().unwrap().len(), 1);
                let before = resumed.snapshot().await.unwrap();
                assert!(
                    resumed
                        .send_request("request".into(), "different".into())
                        .await
                        .is_err()
                );
                assert_eq!(resumed.snapshot().await.unwrap(), before);
                assert_eq!(
                    resumed.send_message("again".into()).await.unwrap(),
                    TurnResult::Completed("reply to again; 2 prior messages".into())
                );
                resumed.close().await.unwrap();
                // Closing an empty, not-yet-initialized execution must preserve the Signal.
                let empty = engine
                    .start_session(format!("{id}-empty"), "fake".into())
                    .await
                    .unwrap();
                let closed = empty.close().await.unwrap();
                assert!(closed.messages.is_empty());
                assert!(closed.closing);
                assert_eq!(empty.close().await.unwrap(), closed);
                assert_eq!(model.requests.lock().unwrap().len(), 2);
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn idle_completion_is_observable_without_polling_transcript_and_can_resume() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (engine, model, id) = engine("idle-lifecycle", Duration::from_millis(500)).await;
        engine
            .run_until(async {
                let first = engine
                    .start_session(id.clone(), "fake".into())
                    .await
                    .unwrap();
                first
                    .send_request("request".into(), "hello".into())
                    .await
                    .unwrap();
                let closed = first.wait_closed().await.unwrap();
                assert!(closed.closing);
                assert_eq!(
                    first.execution_status().await.unwrap(),
                    ExecutionStatus::Completed
                );
                assert_eq!(first.close().await.unwrap(), closed);
                assert_eq!(first.snapshot().await.unwrap(), closed);
                let resumed = engine.resume_session(&id).await.unwrap();
                assert_ne!(resumed.run_id(), first.run_id());
                assert!(matches!(
                    resumed
                        .send_request("request".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed(_)
                ));
                assert_eq!(model.requests.lock().unwrap().len(), 1);
                resumed.close().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
