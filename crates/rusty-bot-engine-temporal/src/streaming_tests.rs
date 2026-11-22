use super::*;
use rusty_bot_providers::{DiscoveryRequest, GenerationRequest, ModelPage, Provider};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowGetResultOptions, WorkflowQueryOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
pub(super) struct PausedModel {
    pub(super) advance: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl Provider for PausedModel {
    fn operation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(45))
    }
    fn build_request(&self, _: &GenerationRequest) -> Result<serde_json::Value, ModelError> {
        Err(ModelError::unsupported("fake has no wire request"))
    }
    async fn list_models(
        &self,
        _: DiscoveryRequest,
        _: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        Err(ModelError::unsupported("fake has no discovery"))
    }
    async fn stream(
        &self,
        request: &GenerationRequest,
        events: tokio::sync::mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        use MessageStreamPayload as P;
        let id = &request.message_id;
        let operation = async {
            test_support::send_event(&events, id, 1, P::Start).await;
            test_support::send_event(
                &events,
                id,
                2,
                P::BlockStart {
                    index: 0,
                    block: BlockStart::Text,
                },
            )
            .await;
            test_support::send_event(
                &events,
                id,
                3,
                P::BlockDelta {
                    index: 0,
                    delta: "Hello".into(),
                },
            )
            .await;
            self.advance.acquire().await.unwrap().forget();
            test_support::send_event(
                &events,
                id,
                4,
                P::BlockDelta {
                    index: 0,
                    delta: ", world".into(),
                },
            )
            .await;
            self.advance.acquire().await.unwrap().forget();
            test_support::send_event(&events, id, 5, P::BlockEnd { index: 0 }).await;
            let response =
                HistoryEntry::completed(id, ConversationMessage::assistant("Hello, world!"));
            test_support::send_event(
                &events,
                id,
                6,
                P::Completed {
                    response: Box::new(response.clone()),
                },
            )
            .await;
            Ok(response)
        };
        tokio::select! {
            result = tokio::time::timeout(Duration::from_secs(45), operation) => result.unwrap_or_else(|_| Err(ModelError::new(rusty_bot_core::ModelErrorKind::Network, "fake timeout", false))),
            _ = cancel.cancelled() => Err(ModelError::new(rusty_bot_core::ModelErrorKind::Network, "fake cancelled", false)),
        }
    }
}

/// Only the interrupted-worker test needs a short, genuinely enforced budget.
pub(super) struct ShortPausedModel(pub(super) PausedModel);
#[async_trait::async_trait]
impl Provider for ShortPausedModel {
    fn operation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(2))
    }
    fn build_request(&self, request: &GenerationRequest) -> Result<serde_json::Value, ModelError> {
        self.0.build_request(request)
    }
    async fn list_models(
        &self,
        request: DiscoveryRequest,
        cancel: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        self.0.list_models(request, cancel).await
    }
    async fn stream(
        &self,
        request: &GenerationRequest,
        events: tokio::sync::mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.0.stream(request, events, cancel),
        )
        .await
        .unwrap_or_else(|_| {
            Err(ModelError::new(
                rusty_bot_core::ModelErrorKind::Network,
                "fake timeout",
                false,
            ))
        })
    }
}

#[tokio::test]
#[ignore = "uses an existing Temporal service; set TEMPORAL_ADDRESS"]
async fn temporal_streaming_is_visible_before_completion_and_final_reply_wins() {
    tokio::time::timeout(Duration::from_secs(90), streaming_roundtrip())
        .await
        .unwrap();
}

async fn streaming_roundtrip() {
    let server = super::test_support::TestServer::connect("stream").await;
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let model = Arc::new(PausedModel {
        advance: tokio::sync::Semaphore::new(0),
    });
    let worker = Worker::new(
        &runtime,
        server.client.clone(),
        WorkerOptions::new(server.task_queue.clone())
            .register_activities(LlmActivities::with_storage(
                model.clone(),
                server.storage.clone(),
            ))
            .register_activities(SessionActivities::new(server.storage.clone()))
            .register_workflow::<ChatSessionWorkflow>()
            .unwrap()
            .build(),
    )
    .unwrap();
    let handle = server
        .client
        .start_workflow(
            ChatSessionWorkflow::run,
            SessionInput {
                session_id: server.workflow_id.clone(),
                idle_timeout_ms: 60_000,
                rollover: RolloverPolicy::default(),
                idle_deadline_ms: None,
                activity_timeout_ms: 105_000,
            },
            WorkflowStartOptions::new(server.task_queue.clone(), server.workflow_id.clone())
                .execution_timeout(Duration::from_secs(120))
                .build(),
        )
        .await
        .unwrap();
    worker.run_until(async {
        let update = handle.execute_update(ChatSessionWorkflow::send_message, "hello".into(), WorkflowExecuteUpdateOptions::default());
        tokio::pin!(update);
        for expected in ["Hello", "Hello, world"] {
            let read_partial = async {
                loop {
                    let snapshot = handle.query(ChatSessionWorkflow::snapshot, (), WorkflowQueryOptions::default()).await.unwrap();
                    if let Some(last) = snapshot.messages.last()
                        && last.message == ChatMessage::Assistant(expected.into())
                    {
                        assert_eq!(snapshot.messages.len(), 2);
                        assert!(snapshot.generating);
                        assert_eq!(last.status, MessageStatus::Generating);
                        return last.turn_id;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let turn_id = tokio::select! {
                value = read_partial => value,
                result = &mut update => panic!("generation completed before partial could be read: {result:?}"),
            };
            assert_eq!(turn_id, 1);
            assert_eq!(server.storage.read(&server.workflow_id).await.unwrap().snapshot().messages.last().unwrap().message, ChatMessage::Assistant(expected.into()));
            println!("Observed before completion: {expected}");
            // A wrong turn and stale sequence must not replace the visible draft.
            handle.signal(ChatSessionWorkflow::partial_reply, PartialReply { turn_id: 999, sequence: 999, text: "wrong turn".into() }, WorkflowSignalOptions::default()).await.unwrap();
            handle.signal(ChatSessionWorkflow::partial_reply, PartialReply { turn_id, sequence: 1, text: "stale".into() }, WorkflowSignalOptions::default()).await.unwrap();
            let snapshot = handle.query(ChatSessionWorkflow::snapshot, (), WorkflowQueryOptions::default()).await.unwrap();
            assert_eq!(snapshot.messages.last().unwrap().message, ChatMessage::Assistant(expected.into()));
            model.advance.add_permits(1);
        }
        assert_eq!(update.await.unwrap(), TurnResult::Completed("Hello, world!".into()));
        let completed = handle.query(ChatSessionWorkflow::snapshot, (), WorkflowQueryOptions::default()).await.unwrap();
        assert!(!completed.generating);
        assert_eq!(completed.messages.last().unwrap().status, MessageStatus::Completed);
        assert_eq!(completed.messages.last().unwrap().message, ChatMessage::Assistant("Hello, world!".into()));
        handle.signal(ChatSessionWorkflow::partial_reply, PartialReply { turn_id: 1, sequence: 1000, text: "late".into() }, WorkflowSignalOptions::default()).await.unwrap();
        assert_eq!(handle.query(ChatSessionWorkflow::snapshot, (), WorkflowQueryOptions::default()).await.unwrap(), completed);
        handle.signal(ChatSessionWorkflow::close, (), WorkflowSignalOptions::default()).await.unwrap();
        assert_eq!(handle.get_result(WorkflowGetResultOptions::default()).await.unwrap().messages, completed.messages);
    }).await.unwrap();
}
