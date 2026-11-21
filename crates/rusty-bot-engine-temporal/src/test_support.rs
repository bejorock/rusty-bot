//! Explicitly opted-in integration tests against an existing Temporal service.
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, Url};

pub(super) struct TestServer {
    pub client: Client,
    pub task_queue: String,
    pub workflow_id: String,
    pub storage: std::sync::Arc<rusty_bot_session::MemorySessionStorage>,
}

impl TestServer {
    pub async fn connect(label: &str) -> Self {
        let address = std::env::var("TEMPORAL_ADDRESS")
            .expect("set TEMPORAL_ADDRESS, for example http://100.88.152.92:7233");
        let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into());
        let connection = Connection::connect(
            ConnectionOptions::new(Url::parse(&address).expect("valid Temporal URL")).build(),
        )
        .await
        .expect("connect to existing Temporal service");
        let client = Client::new(connection, ClientOptions::new(namespace.clone()).build())
            .expect("create Temporal client");
        let id = format!("rusty-bot-{label}-{}", uuid::Uuid::new_v4());
        println!("Temporal: {address}; namespace: {namespace}; Workflow ID: {id}");
        use rusty_bot_session::{SessionConfig, SessionStorage, StoredSession};
        let storage = std::sync::Arc::new(rusty_bot_session::MemorySessionStorage::default());
        storage
            .create(StoredSession::new(
                SessionConfig {
                    id: id.clone(),
                    model: "fake-model".into(),
                    namespace,
                    task_queue: id.clone(),
                    idle_timeout_ms: 60_000,
                    initial_history: Default::default(),
                },
                id.clone(),
            ))
            .await
            .unwrap();
        Self {
            storage,
            client,
            task_queue: id.clone(),
            workflow_id: id,
        }
    }
}
