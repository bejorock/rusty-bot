use super::*;
use crate::tests::FakeModel;
use rusty_bot_postgres::PgSessionStorage;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS and dedicated RUSTY_TEST_DATABASE_URL"]
async fn postgres_idle_completion_restores_context_on_new_host() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = test_support::TestServer::connect("postgres-resume").await;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&std::env::var("RUSTY_TEST_DATABASE_URL").unwrap())
            .await
            .unwrap();
        let storage = Arc::new(PgSessionStorage::new(pool.clone()));
        storage.migrate().await.unwrap();
        let model = Arc::new(FakeModel::default());
        let options = EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: Duration::from_millis(750),
        };
        let id = server.workflow_id;
        let engine = Engine::connect(options.clone(), model.clone(), storage.clone())
            .await
            .unwrap();
        let mut workflow_id = String::new();
        engine
            .run_until(async {
                let session = engine
                    .start_session(id.clone(), "fake".into())
                    .await
                    .unwrap();
                workflow_id = session.workflow_id().into();
                assert!(matches!(
                    session
                        .send_request("request-1".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed(_)
                ));
                assert_eq!(session.snapshot().await.unwrap().messages.len(), 2);
                loop {
                    if session.snapshot().await.unwrap().closing {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                // Waiting for the result ensures the execution is Completed, not just
                // that the close Activity committed immediately before completion.
                server
                    .client
                    .get_workflow_handle::<crate::chat_session_workflow::Run>(workflow_id.clone())
                    .get_result(temporalio_client::WorkflowGetResultOptions::default())
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
        drop(engine);
        let fresh_store = Arc::new(PgSessionStorage::new(pool.clone()));
        let fresh = Engine::connect(options, model.clone(), fresh_store.clone())
            .await
            .unwrap();
        fresh
            .run_until(async {
                let session = fresh.resume_session(&id).await.unwrap();
                assert_eq!(session.workflow_id(), workflow_id);
                assert!(matches!(
                    session
                        .send_request("request-1".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed(_)
                ));
                assert_eq!(
                    model.requests.lock().unwrap().len(),
                    1,
                    "retry must not invoke model again"
                );
                assert_eq!(
                    session
                        .send_request("request-2".into(), "again".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to again; 2 prior messages".into())
                );
                session.close().await.unwrap();
                let stored = fresh_store.read(&id).await.unwrap();
                assert_eq!(stored.turns.len(), 2);
                assert_eq!(stored.retired_runs.len(), 1);
                assert!(stored.closed);
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
