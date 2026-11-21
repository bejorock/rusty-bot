//! Opt-in contract test; only use a dedicated database supplied by the caller.
use rusty_bot_postgres::PgSessionStorage;
use rusty_bot_session::*;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires RUSTY_TEST_DATABASE_URL pointing to a dedicated test database"]
async fn postgres_session_transactions_and_reopen() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("RUSTY_TEST_DATABASE_URL").expect("dedicated database URL"))
        .await
        .unwrap();
    let storage = PgSessionStorage::new(pool.clone());
    storage.migrate().await.unwrap();
    let id = format!("adapter-test-{}", uuid::Uuid::new_v4());
    let config = SessionConfig {
        id: id.clone(),
        model: "fake".into(),
        namespace: "default".into(),
        task_queue: "test".into(),
        idle_timeout_ms: 1000,
        initial_history: Default::default(),
    };
    let (a, b) = tokio::join!(
        storage.create(StoredSession::new(config.clone(), "first".into())),
        storage.create(StoredSession::new(config, "second".into()))
    );
    assert_eq!(a.unwrap().workflow_id, b.unwrap().workflow_id);
    storage
        .change(
            &id,
            SessionChange::Claim {
                run_id: "run".into(),
            },
        )
        .await
        .unwrap();
    let begin = SessionChange::Begin {
        run_id: "run".into(),
        request_id: "request".into(),
        text: "hello".into(),
    };
    let (a, b) = tokio::join!(
        storage.change(&id, begin.clone()),
        storage.change(&id, begin)
    );
    assert_eq!(a.unwrap().turns.len(), 1);
    assert_eq!(b.unwrap().turns.len(), 1);
    let draft = |sequence, text: &str| SessionChange::Draft {
        run_id: "run".into(),
        turn_id: 1,
        sequence,
        text: text.into(),
    };
    let (a, b) = tokio::join!(
        storage.change(&id, draft(2, "new")),
        storage.change(&id, draft(1, "old"))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(storage.read(&id).await.unwrap().turns[0].draft, "new");
    storage
        .change(
            &id,
            SessionChange::Finish {
                run_id: "run".into(),
                turn_id: 1,
                result: Ok("final".into()),
            },
        )
        .await
        .unwrap();
    storage
        .change(
            &id,
            SessionChange::Close {
                run_id: "run".into(),
            },
        )
        .await
        .unwrap();
    let reopened = PgSessionStorage::new(pool.clone());
    reopened
        .change(
            &id,
            SessionChange::Claim {
                run_id: "new".into(),
            },
        )
        .await
        .unwrap();
    assert!(storage
        .change(
            &id,
            SessionChange::Claim {
                run_id: "run".into()
            }
        )
        .await
        .is_err());
    assert!(storage.change(&id, draft(999, "stale")).await.is_err());
    let before = storage.read(&id).await.unwrap();
    assert_eq!(before.completed_context().len(), 2);
    assert!(storage
        .change(
            &id,
            SessionChange::Begin {
                run_id: "new".into(),
                request_id: "request".into(),
                text: "conflict".into()
            }
        )
        .await
        .is_err());
    assert_eq!(storage.read(&id).await.unwrap(), before);
    // Clean only the unique row this test created; retain the database and schema.
    sqlx::query("DELETE FROM rusty_bot_sessions WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
}
