use super::*;
use crate::{streaming_tests::PausedModel, tests::FakeModel};
use rusty_bot_session::MemorySessionStorage;

#[tokio::test]
#[ignore = "requires TEMPORAL_ADDRESS; fake providers, memory storage, fresh worker"]
async fn queued_updates_drain_then_rollover_and_new_worker_rebuilds_the_chain() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("rollover-boundary").await;
        let storage = Arc::new(MemorySessionStorage::default());
        let options = EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: Duration::from_secs(30),
        };
        let paused = Arc::new(PausedModel {
            advance: tokio::sync::Semaphore::new(0),
        });
        let engine = Engine::connect(options.clone(), paused.clone(), storage.clone())
            .await
            .unwrap()
            .with_rollover_policy(RolloverPolicy {
                max_turns: 1,
                max_history_events: 8_000,
            })
            .unwrap();
        let session = engine
            .start_session(server.workflow_id, "fake".into())
            .await
            .unwrap();
        engine
            .run_until(async {
                let turn = session.send_request("one".into(), "hello".into());
                tokio::pin!(turn);
                tokio::select! {
                    result = &mut turn => panic!("model not released: {result:?}"),
                    () = async {
                        while !session.snapshot().await.unwrap().generating {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    } => {}
                }
                // An unfinished accepted Update cannot be abandoned for rollover.
                assert_eq!(session.current_run_id().await.unwrap(), session.run_id());
                let queued = session.send_request("one".into(), "hello".into());
                let release = async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    paused.advance.add_permits(2);
                };
                let (first, duplicate, ()) = tokio::join!(turn, queued, release);
                assert_eq!(
                    first.unwrap(),
                    TurnResult::Completed("Hello, world!".into())
                );
                assert_eq!(
                    duplicate.unwrap(),
                    TurnResult::Completed("Hello, world!".into())
                );
                assert_eq!(session.snapshot().await.unwrap().messages.len(), 2);
                while session.current_run_id().await.unwrap() == session.run_id() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        let model = Arc::new(FakeModel::default());
        let fresh = Engine::connect(options, model.clone(), storage)
            .await
            .unwrap();
        fresh
            .run_until(async {
                // This handle still points to the original run; it follows only CAN links.
                assert_eq!(
                    session
                        .send_request("one".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("Hello, world!".into())
                );
                assert!(model.requests.lock().unwrap().is_empty());
                assert_eq!(
                    session
                        .send_request("two".into(), "again".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to again; 2 prior messages".into())
                );
                let (a, b) = tokio::join!(session.close(), session.close());
                assert_eq!(a.unwrap(), b.unwrap());
                assert_eq!(model.requests.lock().unwrap().len(), 1);
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires TEMPORAL_ADDRESS; checks history-event threshold independently of turn cap"]
async fn history_event_threshold_also_triggers_between_turns() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("rollover-events").await;
        let engine = Engine::connect(
            EngineOptions {
                address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
                namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
                task_queue: server.task_queue,
                idle_timeout: Duration::from_secs(30),
            },
            Arc::new(FakeModel::default()),
            Arc::new(MemorySessionStorage::default()),
        )
        .await
        .unwrap()
        .with_rollover_policy(RolloverPolicy {
            max_turns: u64::MAX,
            max_history_events: 1,
        })
        .unwrap();
        engine
            .run_until(async {
                let session = engine
                    .start_session(server.workflow_id, "fake".into())
                    .await
                    .unwrap();
                session.send_message("hello".into()).await.unwrap();
                while session.current_run_id().await.unwrap() == session.run_id() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert_eq!(session.close().await.unwrap().messages.len(), 2);
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
