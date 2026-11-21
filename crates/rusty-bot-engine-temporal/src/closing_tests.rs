use super::*;
use crate::streaming_tests::PausedModel;
use temporalio_client::WorkflowQueryOptions;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow ID and paused fake model"]
async fn close_drains_active_turn_rejects_queued_turn_and_never_requires_database() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("closing-lifecycle").await;
        let model = Arc::new(PausedModel {
            advance: tokio::sync::Semaphore::new(0),
        });
        let engine = Engine::connect(
            EngineOptions {
                address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
                namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
                task_queue: server.task_queue,
                idle_timeout: Duration::from_secs(30),
            },
            model.clone(),
            Arc::new(rusty_bot_session::MemorySessionStorage::default()),
        )
        .await
        .unwrap();
        engine
            .run_until(async {
                let session = engine
                    .start_session(server.workflow_id, "paused".into())
                    .await
                    .unwrap();
                let execution = server
                    .client
                    .get_workflow_handle::<crate::chat_session_workflow::Run>(
                        session.workflow_id(),
                    );
                let send = session.send_request("active".into(), "hello".into());
                let close = async {
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
                    let closing = session.close();
                    tokio::pin!(closing);
                    let release = async {
                        loop {
                            if execution
                                .query(
                                    ChatSessionWorkflow::snapshot,
                                    (),
                                    WorkflowQueryOptions::default(),
                                )
                                .await
                                .unwrap()
                                .closing
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        assert_eq!(
                            session.execution_status().await.unwrap(),
                            ExecutionStatus::Running
                        );
                        // This request must not start a second model call after close was accepted.
                        let queued = session.send_request("queued".into(), "too late".into());
                        model.advance.add_permits(2);
                        assert!(matches!(queued.await.unwrap(), TurnResult::Rejected(_)));
                    };
                    let (closed, ()) = tokio::join!(&mut closing, release);
                    closed.unwrap()
                };
                let (reply, closed) = tokio::join!(send, close);
                assert_eq!(
                    reply.unwrap(),
                    TurnResult::Completed("Hello, world!".into())
                );
                assert!(closed.closing);
                assert_eq!(closed.messages.len(), 2);
                assert_eq!(closed.messages[1].status, MessageStatus::Completed);
                assert_eq!(session.close().await.unwrap(), closed);
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
