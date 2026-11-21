use super::*;
use crate::{streaming_tests::PausedModel, tests::FakeModel};
use rusty_bot_session::MemorySessionStorage;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; drops only its own Update caller"]
async fn lost_response_is_recovered_by_same_request_without_repeating_generation() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("lost-response").await;
        let storage = Arc::new(MemorySessionStorage::default());
        let model = Arc::new(PausedModel { advance: tokio::sync::Semaphore::new(0) });
        let engine = Engine::connect(EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue, idle_timeout: Duration::from_secs(60),
        }, model.clone(), storage).await.unwrap();
        engine.run_until(async {
            let session = engine.start_session(server.workflow_id, "fake".into()).await.unwrap();
            // Scope ends while the server-side Update is still generating. Dropping
            // its caller does not cancel the accepted Update or its Activity.
            {
                let lost = session.send_request("request".into(), "hello".into());
                tokio::pin!(lost);
                tokio::select! {
                    result = &mut lost => panic!("provider has not been released: {result:?}"),
                    () = async {
                        while !session.snapshot().await.unwrap().messages.last().is_some_and(|m| m.message == ChatMessage::Assistant("Hello".into())) {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    } => {}
                }
            }
            model.advance.add_permits(2);
            // A repeated model call would block forever on exhausted permits.
            assert_eq!(session.send_request("request".into(), "hello".into()).await.unwrap(), TurnResult::Completed("Hello, world!".into()));
            assert_eq!(session.snapshot().await.unwrap().messages.len(), 2);
            assert_eq!(session.send_request("request".into(), "hello".into()).await.unwrap(), TurnResult::Completed("Hello, world!".into()));
            session.close().await.unwrap();
        }).await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; stops only its own worker during generation"]
async fn worker_shutdown_during_generation_settles_uncertain_turn_on_fresh_worker() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = test_support::TestServer::connect("generation-shutdown").await;
        let storage = Arc::new(MemorySessionStorage::default());
        let options = EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue, idle_timeout: Duration::from_secs(120),
        };
        let paused = Arc::new(PausedModel { advance: tokio::sync::Semaphore::new(0) });
        let engine = Engine::connect(options.clone(), paused, storage.clone()).await.unwrap();
        let session = engine.start_session(server.workflow_id, "fake".into()).await.unwrap();
        let request = session.send_request("request".into(), "hello".into());
        tokio::pin!(request);
        engine.run_until(async {
            tokio::select! {
                result = &mut request => panic!("generation must still be pending: {result:?}"),
                () = async {
                    while !session.snapshot().await.unwrap().messages.last().is_some_and(|m| m.message == ChatMessage::Assistant("Hello".into())) {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                } => {}
            }
        }).await.unwrap();
        let model = Arc::new(FakeModel::default());
        let fresh = Engine::connect(options, model.clone(), storage).await.unwrap();
        fresh.run_until(async {
            // Cancellation/timeout is an uncertain model outcome, not permission to
            // rerun this non-idempotent external operation automatically.
            assert!(request.await.is_err());
            let resumed = fresh.resume_session(session.id()).await.unwrap();
            assert_eq!(resumed.run_id(), session.run_id());
            assert!(matches!(resumed.send_request("request".into(), "hello".into()).await.unwrap(), TurnResult::Rejected(_)));
            assert_eq!(resumed.snapshot().await.unwrap().messages[1].status, MessageStatus::Failed);
            assert!(model.requests.lock().unwrap().is_empty());
            assert_eq!(resumed.send_message("again".into()).await.unwrap(), TurnResult::Completed("reply to again; 0 prior messages".into()));
            resumed.close().await.unwrap();
        }).await.unwrap();
    }).await.unwrap();
}
