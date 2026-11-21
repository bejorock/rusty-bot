use super::*;
use crate::tests::FakeModel;
use rusty_bot_postgres::PgSessionStorage;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires TEMPORAL_ADDRESS and dedicated RUSTY_TEST_DATABASE_URL; deletes only own row"]
async fn postgres_rollover_reloads_context_and_fences_predecessor_on_new_host() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("rollover-postgres").await;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&std::env::var("RUSTY_TEST_DATABASE_URL").unwrap())
            .await
            .unwrap();
        let storage = Arc::new(PgSessionStorage::new(pool.clone()));
        storage.migrate().await.unwrap();
        let options = EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: Duration::from_secs(30),
        };
        let model = Arc::new(FakeModel::default());
        let engine = Engine::connect(options.clone(), model.clone(), storage.clone())
            .await
            .unwrap()
            .with_rollover_policy(RolloverPolicy {
                max_turns: 1,
                max_history_events: 8_000,
            })
            .unwrap();
        let id = server.workflow_id;
        let session = engine
            .start_session(id.clone(), "fake".into())
            .await
            .unwrap();
        engine
            .run_until(async {
                session
                    .send_request("one".into(), "hello".into())
                    .await
                    .unwrap();
                loop {
                    let record = storage.read(&id).await.unwrap();
                    if record
                        .owner_run
                        .as_deref()
                        .is_some_and(|run| run != session.run_id())
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        let fresh_store = Arc::new(PgSessionStorage::new(pool.clone()));
        let fresh = Engine::connect(options, model.clone(), fresh_store.clone())
            .await
            .unwrap();
        fresh
            .run_until(async {
                assert_eq!(
                    session
                        .send_request("one".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to hello; 0 prior messages".into())
                );
                assert_eq!(model.requests.lock().unwrap().len(), 1);
                assert!(
                    fresh_store
                        .change(
                            &id,
                            SessionChange::Close {
                                run_id: session.run_id().into()
                            }
                        )
                        .await
                        .is_err()
                );
                assert_eq!(
                    session
                        .send_request("two".into(), "again".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to again; 2 prior messages".into())
                );
                session.close().await.unwrap();
                let stored = fresh_store.read(&id).await.unwrap();
                assert_eq!(stored.turns.len(), 2);
                assert!(stored.closed);
                assert!(stored.retired_runs.contains(&session.run_id().to_string()));
                assert_eq!(model.requests.lock().unwrap().len(), 2);
            })
            .await
            .unwrap();
        sqlx::query("DELETE FROM rusty_bot_sessions WHERE id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
