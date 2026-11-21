use super::*;
use rusty_bot_core::{AssistantTurn, ChatRequest, StreamEvent, Usage};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowGetResultOptions, WorkflowQueryOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

pub(super) struct PausedModel {
    pub(super) advance: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl ModelProvider for PausedModel {
    fn id(&self) -> &str {
        "paused"
    }
    async fn stream(
        &self,
        _request: ChatRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        _cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        events
            .send(StreamEvent::TextDelta("Hello".into()))
            .await
            .unwrap();
        self.advance.acquire().await.unwrap().forget();
        events
            .send(StreamEvent::TextDelta(", world".into()))
            .await
            .unwrap();
        self.advance.acquire().await.unwrap().forget();
        // Final output deliberately differs: Activity result, not partials, is authoritative.
        Ok(AssistantTurn {
            message: Message::Assistant {
                content: vec![ContentPart::Text {
                    text: "Hello, world!".into(),
                }],
                tool_calls: vec![],
                usage: None,
                stop: Some(StopReason::Stop),
            },
            tool_calls: vec![],
            usage: Usage::default(),
            stop: StopReason::Stop,
            provider_metadata: serde_json::Value::Null,
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
