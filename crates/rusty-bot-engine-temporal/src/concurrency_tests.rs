//! Real Temporal admission tests; all model calls and transcripts are controlled fixtures.
use super::*;
use crate::tests::FakeModel;
use futures::future::join_all;
use rusty_bot_session::MemorySessionStorage;

async fn concurrent_hosts(
    label: &str,
    first_storage: Arc<dyn SessionStorage>,
    second_storage: Arc<dyn SessionStorage>,
) -> String {
    let server = test_support::TestServer::connect(label).await;
    let model = Arc::new(FakeModel::default());
    let options = EngineOptions {
        address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
        namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
        task_queue: server.task_queue,
        idle_timeout: Duration::from_secs(60),
    };
    let a = Engine::connect(options.clone(), model.clone(), first_storage.clone())
        .await
        .unwrap();
    let b = Engine::connect(options, model.clone(), second_storage.clone())
        .await
        .unwrap();
    let id = server.workflow_id;
    // No worker yet: all starts overlap at the client/storage/server admission boundary.
    // Each SDK host generates its own candidate Workflow ID; only one may win storage.
    let handles = join_all((0..24).map(|i| {
        let host = if i % 2 == 0 { &a } else { &b };
        host.start_session(id.clone(), "fake".into())
    }))
    .await
    .into_iter()
    .collect::<EngineResult<Vec<_>>>()
    .unwrap();
    let first = &handles[0];
    for handle in &handles {
        assert_eq!(handle.id(), id);
        assert_eq!(handle.workflow_id(), first.workflow_id());
        assert_eq!(handle.run_id(), first.run_id());
    }
    assert_eq!(
        first.execution_status().await.unwrap(),
        ExecutionStatus::Running
    );
    assert_eq!(
        first_storage.read(&id).await.unwrap().workflow_id,
        first.workflow_id()
    );
    assert_eq!(
        second_storage.read(&id).await.unwrap().workflow_id,
        first.workflow_id()
    );
    a.run_until(async {
        // Concurrent retries arriving through different handles must generate only once.
        for result in join_all(
            handles
                .iter()
                .take(8)
                .map(|s| s.send_request("request".into(), "hello".into())),
        )
        .await
        {
            assert!(matches!(result.unwrap(), TurnResult::Completed(_)));
        }
        assert_eq!(model.requests.lock().unwrap().len(), 1);
        let stored = first_storage.read(&id).await.unwrap();
        assert_eq!(stored.owner_run.as_deref(), Some(first.run_id()));
        assert!(stored.retired_runs.is_empty());
        assert_eq!(stored.turns.len(), 1);
        first.close().await.unwrap();

        // All resume callers race after completion. This must create exactly one new run.
        let resumed = join_all((0..24).map(|i| {
            let host = if i % 2 == 0 { &a } else { &b };
            host.resume_session(&id)
        }))
        .await
        .into_iter()
        .collect::<EngineResult<Vec<_>>>()
        .unwrap();
        let next = &resumed[0];
        assert_ne!(next.run_id(), first.run_id());
        for handle in &resumed {
            assert_eq!(handle.workflow_id(), first.workflow_id());
            assert_eq!(handle.run_id(), next.run_id());
        }
        first.close().await.unwrap();
        assert_eq!(
            next.execution_status().await.unwrap(),
            ExecutionStatus::Running
        );
        for result in join_all(
            resumed
                .iter()
                .take(8)
                .map(|s| s.send_request("request".into(), "hello".into())),
        )
        .await
        {
            assert!(matches!(result.unwrap(), TurnResult::Completed(_)));
        }
        assert_eq!(model.requests.lock().unwrap().len(), 1);
        let stored = second_storage.read(&id).await.unwrap();
        assert_eq!(stored.owner_run.as_deref(), Some(next.run_id()));
        assert_eq!(stored.retired_runs, vec![first.run_id().to_string()]);
        assert_eq!(stored.turns.len(), 1);
        next.close().await.unwrap();
    })
    .await
    .unwrap();
    id
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; shared in-memory adapter and fake model"]
async fn concurrent_create_and_resume_attach_to_one_run_across_sdk_hosts() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let storage = Arc::new(MemorySessionStorage::default());
        concurrent_hosts("single-run-memory", storage.clone(), storage).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS and dedicated RUSTY_TEST_DATABASE_URL"]
async fn postgres_independent_adapters_and_hosts_admit_only_one_active_run() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let url = std::env::var("RUSTY_TEST_DATABASE_URL").unwrap();
        let a = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .unwrap();
        let b = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .unwrap();
        let first = Arc::new(rusty_bot_postgres::PgSessionStorage::new(a.clone()));
        first.migrate().await.unwrap();
        let second = Arc::new(rusty_bot_postgres::PgSessionStorage::new(b.clone()));
        let id = concurrent_hosts("single-run-postgres", first, second).await;
        // Only this test's UUID-scoped logical session is removed.
        sqlx::query("DELETE FROM rusty_bot_sessions WHERE id = $1")
            .bind(id)
            .execute(&a)
            .await
            .unwrap();
        a.close().await;
        b.close().await;
    })
    .await
    .unwrap();
}
