//! Opt-in Redis contract test. Uses only one UUID-scoped key and deletes it afterward.
use redis::AsyncConnectionConfig;
use rusty_bot_redis::RedisSessionStorage;
use rusty_bot_session::*;
use std::time::Duration;

async fn storage(client: &redis::Client, prefix: String) -> RedisSessionStorage {
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    RedisSessionStorage::new(
        client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .unwrap(),
        prefix,
    )
    .unwrap()
}

async fn exercise(a: &dyn SessionStorage, b: &dyn SessionStorage, id: &str) {
    assert!(matches!(a.read(id).await, Err(StorageError::NotFound)));
    assert!(matches!(
        a.change(
            id,
            SessionChange::Claim {
                run_id: "run".into()
            }
        )
        .await,
        Err(StorageError::NotFound)
    ));
    let config = SessionConfig {
        id: id.into(),
        model: "fake".into(),
        namespace: "default".into(),
        task_queue: "test".into(),
        idle_timeout_ms: 1000,
        initial_history: Default::default(),
    };
    let (first, second) = tokio::join!(
        a.create(StoredSession::new(config.clone(), "first".into())),
        b.create(StoredSession::new(config.clone(), "second".into()))
    );
    assert_eq!(first.unwrap().workflow_id, second.unwrap().workflow_id);
    let mut other = config.clone();
    other.model = "different".into();
    assert!(matches!(
        a.create(StoredSession::new(other, "other".into())).await,
        Err(StorageError::Conflict(_))
    ));
    a.change(
        id,
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
    let (first, second) = tokio::join!(a.change(id, begin.clone()), b.change(id, begin));
    assert_eq!(first.unwrap().turns.len(), 1);
    assert_eq!(second.unwrap().turns.len(), 1);
    let draft = |sequence, text: &str| SessionChange::Draft {
        run_id: "run".into(),
        turn_id: 1,
        sequence,
        text: text.into(),
    };
    let (first, second) =
        tokio::join!(a.change(id, draft(2, "new")), b.change(id, draft(1, "old")));
    first.unwrap();
    second.unwrap();
    assert_eq!(a.read(id).await.unwrap().turns[0].draft, "new");
    let finish = SessionChange::Finish {
        run_id: "run".into(),
        turn_id: 1,
        result: Ok("final".into()),
    };
    let (first, second) = tokio::join!(a.change(id, finish.clone()), b.change(id, finish.clone()));
    first.unwrap();
    second.unwrap();
    let before = a.read(id).await.unwrap();
    assert!(matches!(
        a.change(
            id,
            SessionChange::Finish {
                run_id: "run".into(),
                turn_id: 1,
                result: Ok("different".into())
            }
        )
        .await,
        Err(StorageError::Conflict(_))
    ));
    assert_eq!(b.read(id).await.unwrap(), before);
    a.change(id, draft(999, "late")).await.unwrap();
    assert_eq!(b.read(id).await.unwrap(), before);
    a.change(
        id,
        SessionChange::Close {
            run_id: "run".into(),
        },
    )
    .await
    .unwrap();
    b.change(
        id,
        SessionChange::Claim {
            run_id: "new".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        a.change(id, draft(1000, "stale owner")).await,
        Err(StorageError::Conflict(_))
    ));
    assert!(matches!(
        a.change(
            id,
            SessionChange::Claim {
                run_id: "run".into()
            }
        )
        .await,
        Err(StorageError::Conflict(_))
    ));
    assert_eq!(
        b.read(id).await.unwrap().completed_context(),
        vec![
            ChatMessage::User("hello".into()),
            ChatMessage::Assistant("final".into())
        ]
    );
    b.change(
        id,
        SessionChange::Begin {
            run_id: "new".into(),
            request_id: "abandoned".into(),
            text: "next".into(),
        },
    )
    .await
    .unwrap();
    a.change(
        id,
        SessionChange::Claim {
            run_id: "third".into(),
        },
    )
    .await
    .unwrap();
    let state = b.read(id).await.unwrap();
    assert!(state.turns[1].result.as_ref().unwrap().is_err());
    assert_eq!(state.completed_context().len(), 2);
    // A u64 sequence beyond Lua's exact-number range must survive JSON byte-for-byte.
    a.change(
        id,
        SessionChange::Begin {
            run_id: "third".into(),
            request_id: "large-sequence".into(),
            text: "numeric".into(),
        },
    )
    .await
    .unwrap();
    a.change(
        id,
        SessionChange::Draft {
            run_id: "third".into(),
            turn_id: 3,
            sequence: u64::MAX,
            text: "exact".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(b.read(id).await.unwrap().turns[2].sequence, u64::MAX);
}

#[tokio::test]
async fn same_contract_runs_on_memory() {
    let memory = MemorySessionStorage::default();
    exercise(&memory, &memory.clone(), "memory-contract").await;
}

#[tokio::test]
#[ignore = "requires RUSTY_TEST_REDIS_URL; writes/deletes one unique test key"]
async fn redis_session_transactions_and_reconnect() {
    let url = std::env::var("RUSTY_TEST_REDIS_URL").expect("Redis test URL");
    // Do not print the URL or credential-bearing connection errors.
    let client = redis::Client::open(url).expect("valid Redis connection URL");
    let prefix = format!("rusty-bot-test:{}", uuid::Uuid::new_v4());
    let a = storage(&client, prefix.clone()).await;
    let b = storage(&client, prefix.clone()).await;
    let result = tokio::spawn(async move {
        exercise(&a, &b, "redis-contract").await;
    })
    .await;
    // Reconnect with a new client, even if a contract assertion failed; clean only our key.
    let reopened = storage(&client, prefix).await;
    let persisted = reopened.read("redis-contract").await;
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: usize = redis::cmd("DEL")
        .arg(reopened.session_key("redis-contract"))
        .query_async(&mut connection)
        .await
        .unwrap();
    result.unwrap();
    assert_eq!(persisted.unwrap().turns[0].result, Some(Ok("final".into())));
}
