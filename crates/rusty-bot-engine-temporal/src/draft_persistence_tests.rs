use super::*;
use crate::streaming_tests::PausedModel;
use rusty_bot_postgres::PgSessionStorage;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS and dedicated RUSTY_TEST_DATABASE_URL"]
async fn postgres_draft_visible_before_result_and_close_during_generation() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = test_support::TestServer::connect("postgres-draft").await;
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(5).connect(&std::env::var("RUSTY_TEST_DATABASE_URL").unwrap()).await.unwrap();
        let storage = Arc::new(PgSessionStorage::new(pool.clone())); storage.migrate().await.unwrap();
        let model = Arc::new(PausedModel { advance: tokio::sync::Semaphore::new(0) });
        let engine = Engine::connect(EngineOptions { address: std::env::var("TEMPORAL_ADDRESS").unwrap(), namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()), task_queue: server.task_queue, idle_timeout: Duration::from_secs(30) }, model.clone(), storage.clone()).await.unwrap();
        let id = server.workflow_id;
        engine.run_until(async {
            let session = engine.start_session(id.clone(), "paused".into()).await.unwrap();
            let update = session.send_request("draft-request".into(), "hello".into()); tokio::pin!(update);
            for expected in ["Hello", "Hello, world"] {
                let read = async {
                    loop {
                        let snapshot = storage.read(&id).await.unwrap().snapshot();
                        if snapshot.generating && snapshot.messages.last().is_some_and(|m| m.message == ChatMessage::Assistant(expected.into())) { break; }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                };
                tokio::select! { _ = read => {}, result = &mut update => panic!("model completed before persisted draft: {result:?}") }
                model.advance.add_permits(1);
            }
            // Explicit close while generation/final persistence is still settling must
            // not be overwritten by a storage snapshot that has closed=false.
            let close = session.close();
            let (reply, closed) = tokio::join!(update, close);
            assert_eq!(reply.unwrap(), TurnResult::Completed("Hello, world!".into()));
            assert!(closed.unwrap().closing);
            assert_eq!(storage.read(&id).await.unwrap().snapshot().messages.last().unwrap().message, ChatMessage::Assistant("Hello, world!".into()));
        }).await.unwrap();
        sqlx::query("DELETE FROM rusty_bot_sessions WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    }).await.unwrap();
}
