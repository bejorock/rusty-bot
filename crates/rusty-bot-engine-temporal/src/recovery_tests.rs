use super::*;
use crate::{streaming_tests::PausedModel, tests::FakeModel};
use rusty_bot_session::{MemorySessionStorage, StorageError, StorageResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use temporalio_client::WorkflowTerminateOptions;

/// Fail only final commits, either before writing or after committing an uncertain ack.
struct FaultStorage {
    inner: MemorySessionStorage,
    attempts: AtomicUsize,
    released: AtomicBool,
    after_write: bool,
}
#[async_trait::async_trait]
impl SessionStorage for FaultStorage {
    async fn create(&self, session: StoredSession) -> StorageResult<StoredSession> {
        self.inner.create(session).await
    }
    async fn read(&self, id: &str) -> StorageResult<StoredSession> {
        self.inner.read(id).await
    }
    async fn change(&self, id: &str, change: SessionChange) -> StorageResult<StoredSession> {
        if matches!(
            change,
            SessionChange::Finish { .. } | SessionChange::FinishResponse { .. }
        ) && !self.released.load(Ordering::SeqCst)
        {
            if self.after_write {
                self.inner.change(id, change).await?;
            }
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::Backend("controlled commit outage".into()));
        }
        self.inner.change(id, change).await
    }
}

#[tokio::test]
async fn fault_storage_intercepts_legacy_and_canonical_finishes_offline() {
    for after_write in [false, true] {
        for canonical in [false, true] {
            let storage = FaultStorage {
                inner: MemorySessionStorage::default(),
                attempts: AtomicUsize::new(0),
                released: AtomicBool::new(false),
                after_write,
            };
            let config = rusty_bot_session::SessionConfig {
                id: "session".into(),
                model: "fake".into(),
                namespace: "default".into(),
                task_queue: "queue".into(),
                idle_timeout_ms: 1_000,
                initial_history: Default::default(),
            };
            storage
                .create(StoredSession::new(config, "workflow".into()))
                .await
                .unwrap();
            storage
                .change(
                    "session",
                    SessionChange::Claim {
                        run_id: "run".into(),
                    },
                )
                .await
                .unwrap();
            storage
                .change(
                    "session",
                    SessionChange::Begin {
                        run_id: "run".into(),
                        request_id: "request".into(),
                        text: "hello".into(),
                    },
                )
                .await
                .unwrap();
            let finish = if canonical {
                let mut response = HistoryEntry::completed(
                    "turn:1:assistant",
                    ConversationMessage::assistant("answer"),
                );
                response.turn_id = Some(1);
                SessionChange::FinishResponse {
                    run_id: "run".into(),
                    turn_id: 1,
                    response: Box::new(response),
                }
            } else {
                SessionChange::Finish {
                    run_id: "run".into(),
                    turn_id: 1,
                    result: Ok("answer".into()),
                }
            };
            assert!(matches!(
                storage.change("session", finish.clone()).await,
                Err(StorageError::Backend(_))
            ));
            assert_eq!(storage.attempts.load(Ordering::SeqCst), 1);
            let stored = storage.read("session").await.unwrap();
            assert_eq!(stored.turns[0].result.is_some(), after_write);
            assert_eq!(stored.turns[0].response.is_some(), after_write && canonical);
            storage.released.store(true, Ordering::SeqCst);
            storage.change("session", finish.clone()).await.unwrap();
            storage.change("session", finish).await.unwrap();
            let stored = storage.read("session").await.unwrap();
            assert_eq!(stored.turns[0].result, Some(Ok("answer".into())));
            assert_eq!(stored.turns[0].response.is_some(), canonical);
        }
    }
}

async fn options(label: &str) -> (test_support::TestServer, EngineOptions) {
    let server = test_support::TestServer::connect(label).await;
    let options = EngineOptions {
        address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
        namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
        task_queue: server.task_queue.clone(),
        idle_timeout: Duration::from_secs(120),
    };
    (server, options)
}

async fn commit_outage(after_write: bool) {
    let (server, options) = options("commit-recovery").await;
    let storage = Arc::new(FaultStorage {
        inner: MemorySessionStorage::default(),
        attempts: AtomicUsize::new(0),
        released: AtomicBool::new(false),
        after_write,
    });
    let model = Arc::new(FakeModel::default());
    let engine = Engine::connect(options.clone(), model.clone(), storage.clone())
        .await
        .unwrap();
    let session = engine
        .start_session(server.workflow_id.clone(), "fake".into())
        .await
        .unwrap();
    let request = session.send_request("request".into(), "hello".into());
    tokio::pin!(request);
    engine.run_until(async {
        let observed = async {
            while storage.attempts.load(Ordering::SeqCst) < 6 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            result = &mut request => panic!("commit outage must not settle the request: {result:?}"),
            () = observed => {}
        }
        assert_eq!(model.requests.lock().unwrap().len(), 1);
        let stored = storage.read(session.id()).await.unwrap();
        assert_eq!(stored.turns[0].result.is_some(), after_write);
        assert_eq!(stored.turns[0].response.is_some(), after_write);
    }).await.unwrap();
    // Rebuild the worker with no Workflow cache while final persistence is pending.
    let fresh = Engine::connect(options, model.clone(), storage.clone())
        .await
        .unwrap();
    fresh
        .run_until(async {
            storage.released.store(true, Ordering::SeqCst);
            assert!(matches!(request.await.unwrap(), TurnResult::Completed(_)));
            let resumed = fresh.resume_session(session.id()).await.unwrap();
            assert_eq!(resumed.run_id(), session.run_id());
            assert!(matches!(
                resumed
                    .send_request("request".into(), "hello".into())
                    .await
                    .unwrap(),
                TurnResult::Completed(_)
            ));
            assert_eq!(model.requests.lock().unwrap().len(), 1);
            assert!(
                storage.read(session.id()).await.unwrap().turns[0]
                    .response
                    .is_some()
            );
            assert_eq!(resumed.snapshot().await.unwrap().messages.len(), 2);
            assert_eq!(
                resumed.send_message("again".into()).await.unwrap(),
                TurnResult::Completed("reply to again; 2 prior messages".into())
            );
            resumed.close().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; fake provider and controlled storage outage"]
async fn pending_commit_survives_more_than_five_failures_and_worker_restart() {
    tokio::time::timeout(Duration::from_secs(90), commit_outage(false))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; fake provider and uncertain storage acknowledgement"]
async fn uncertain_commit_ack_is_idempotent_after_worker_restart() {
    tokio::time::timeout(Duration::from_secs(90), commit_outage(true))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; terminates only its own UUID-scoped run"]
async fn terminated_generation_resumes_without_repeating_uncertain_request() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (server, options) = options("terminated-recovery").await;
        let storage = Arc::new(MemorySessionStorage::default());
        let paused = Arc::new(PausedModel {
            advance: tokio::sync::Semaphore::new(0),
        });
        let engine = Engine::connect(options.clone(), paused.clone(), storage.clone())
            .await
            .unwrap();
        let session = engine
            .start_session(server.workflow_id, "fake".into())
            .await
            .unwrap();
        engine
            .run_until(async {
                let request = session.send_request("uncertain".into(), "hello".into());
                let terminate = async {
                    loop {
                        let snapshot = session.snapshot().await.unwrap();
                        if snapshot.generating
                            && snapshot.messages.last().is_some_and(|m| {
                                m.message == ChatMessage::Assistant("Hello".into())
                            })
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    server
                        .client
                        .get_workflow_handle::<crate::chat_session_workflow::Run>(
                            session.workflow_id(),
                        )
                        .terminate(WorkflowTerminateOptions::default())
                        .await
                        .unwrap();
                    // A terminated execution cannot promptly notify an Activity which
                    // does not heartbeat; let this controlled provider settle its work.
                    paused.advance.add_permits(2);
                };
                let (result, ()) = tokio::join!(request, terminate);
                assert!(result.is_err());
                assert_eq!(
                    session.execution_status().await.unwrap(),
                    ExecutionStatus::Terminated
                );
                assert!(session.close().await.is_err());
            })
            .await
            .unwrap();
        let model = Arc::new(FakeModel::default());
        let fresh = Engine::connect(options, model.clone(), storage.clone())
            .await
            .unwrap();
        fresh
            .run_until(async {
                let resumed = fresh.resume_session(session.id()).await.unwrap();
                assert_ne!(resumed.run_id(), session.run_id());
                assert!(matches!(
                    resumed
                        .send_request("uncertain".into(), "hello".into())
                        .await
                        .unwrap(),
                    TurnResult::Rejected(_)
                ));
                assert!(model.requests.lock().unwrap().is_empty());
                assert_eq!(
                    resumed.snapshot().await.unwrap().messages[1].status,
                    MessageStatus::Failed
                );
                assert_eq!(
                    resumed
                        .send_request("new".into(), "again".into())
                        .await
                        .unwrap(),
                    TurnResult::Completed("reply to again; 0 prior messages".into())
                );
                resumed.close().await.unwrap();
                assert!(session.close().await.is_err());
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
