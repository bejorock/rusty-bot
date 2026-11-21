use super::*;
use rusty_bot_core::{AssistantTurn, ModelErrorKind, StreamEvent, Usage};
use std::sync::Mutex;

#[derive(Default)]
pub(super) struct FakeModel {
    pub(super) requests: Mutex<Vec<ChatRequest>>,
}

#[async_trait::async_trait]
impl ModelProvider for FakeModel {
    fn id(&self) -> &str {
        "fake"
    }

    async fn stream(
        &self,
        request: ChatRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        _cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        let text = match request.messages.last().unwrap() {
            ProviderMessage::User { content } => content.clone(),
            _ => panic!("last message should be user"),
        };
        let prior = request.messages.len() - 1;
        self.requests.lock().unwrap().push(request);
        if text == "fail" {
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "fake refusal",
                false,
            ));
        }
        // More events than the Activity channel holds: must be drained, not deadlocked.
        for _ in 0..40 {
            events
                .send(StreamEvent::TextDelta("x".into()))
                .await
                .unwrap();
        }
        let reply = format!("reply to {text}; {prior} prior messages");
        Ok(AssistantTurn {
            message: Message::Assistant {
                content: vec![ContentPart::Text { text: reply }],
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
async fn activity_maps_full_context_and_drains_provisional_events() {
    let model = Arc::new(FakeModel::default());
    let activity = LlmActivities::new(model.clone());
    let messages = vec![
        ChatMessage::User("first".into()),
        ChatMessage::Assistant("answer".into()),
        ChatMessage::User("second".into()),
    ];
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        activity.generate_text(
            GenerateInput {
                model: "test-model".into(),
                messages,
            },
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(reply, "reply to second; 2 prior messages");
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests[0].model, "test-model");
    assert_eq!(requests[0].messages.len(), 3);
    assert_eq!(
        requests[0].messages[1],
        ProviderMessage::Assistant {
            content: Some("answer".into()),
            tool_calls: vec![]
        }
    );
    assert!(requests[0].tools.is_empty());
    assert_eq!(requests[0].tool_choice, ToolChoice::None);
}

#[tokio::test]
async fn model_errors_remain_serializable_activity_results() {
    let activity = LlmActivities::new(Arc::new(FakeModel::default()));
    let error = activity
        .generate_text(
            GenerateInput {
                model: "fake".into(),
                messages: vec![ChatMessage::User("fail".into())],
            },
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    let result = TurnResult::ModelFailed(error);
    let json = serde_json::to_string(&result).unwrap();
    assert_eq!(serde_json::from_str::<TurnResult>(&json).unwrap(), result);
}

#[test]
fn session_snapshot_roundtrips_without_runtime_handles() {
    let snapshot = SessionSnapshot {
        messages: vec![
            SessionMessage {
                turn_id: 1,
                message: ChatMessage::User("hello".into()),
                status: MessageStatus::Completed,
            },
            SessionMessage {
                turn_id: 1,
                message: ChatMessage::Assistant("hi".into()),
                status: MessageStatus::Completed,
            },
        ],
        generating: false,
        closing: false,
    };
    assert_eq!(
        serde_json::from_value::<SessionSnapshot>(serde_json::to_value(&snapshot).unwrap())
            .unwrap(),
        snapshot
    );
}

#[tokio::test]
#[ignore = "uses an existing Temporal service; set TEMPORAL_ADDRESS"]
async fn temporal_engine_host_runs_a_complete_session() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = super::test_support::TestServer::connect("engine-host").await;
        let engine = Engine::connect(
            EngineOptions {
                address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
                namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
                task_queue: server.task_queue,
                idle_timeout: Duration::from_secs(60),
            },
            Arc::new(FakeModel::default()),
            server.storage.clone(),
        )
        .await
        .unwrap();
        engine
            .run_until(async {
                let session = engine
                    .start_session(server.workflow_id, "fake-model".into())
                    .await
                    .unwrap();
                assert_eq!(
                    session.send_message("hello".into()).await.unwrap(),
                    TurnResult::Completed("reply to hello; 0 prior messages".into())
                );
                assert_eq!(session.snapshot().await.unwrap().messages.len(), 2);
                assert!(session.close().await.unwrap().closing);
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

// Real Temporal service, fake model: explicitly opt in; no server is started or stopped.
#[tokio::test]
#[ignore = "uses an existing Temporal service; set TEMPORAL_ADDRESS"]
async fn temporal_session_replays_on_a_fresh_worker_and_continues_chat() {
    tokio::time::timeout(Duration::from_secs(90), temporal_roundtrip())
        .await
        .unwrap();
}

async fn temporal_roundtrip() {
    use temporalio_client::{
        WorkflowExecuteUpdateOptions, WorkflowGetResultOptions, WorkflowQueryOptions,
        WorkflowSignalOptions, WorkflowStartOptions,
    };
    use temporalio_sdk::{Runtime, Worker, WorkerOptions};

    let server = super::test_support::TestServer::connect("replay").await;
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let model = Arc::new(FakeModel::default());
    let make_worker = || {
        Worker::new(
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
        .unwrap()
    };
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
    let mut before_restart = None;
    make_worker()
        .run_until(async {
            let result = handle
                .execute_update(
                    ChatSessionWorkflow::send_message,
                    "hello".to_string(),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                result,
                TurnResult::Completed("reply to hello; 0 prior messages".into())
            );
            before_restart = Some(
                handle
                    .query(
                        ChatSessionWorkflow::snapshot,
                        (),
                        WorkflowQueryOptions::default(),
                    )
                    .await
                    .unwrap(),
            );
        })
        .await
        .unwrap();
    // First worker has been shut down and dropped; the second has an empty Workflow cache.
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    make_worker()
        .run_until(async {
            let restored = handle
                .query(
                    ChatSessionWorkflow::snapshot,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(Some(&restored), before_restart.as_ref());
            assert_eq!(restored.messages.len(), 2);
            assert_eq!(
                model.requests.lock().unwrap().len(),
                1,
                "replay must not re-run completed LLM Activities"
            );
            let blank = handle
                .execute_update(
                    ChatSessionWorkflow::send_message,
                    "  ".to_string(),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
            assert!(matches!(blank, TurnResult::Rejected(_)));
            let failure = handle
                .execute_update(
                    ChatSessionWorkflow::send_message,
                    "fail".to_string(),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
            assert!(matches!(failure, TurnResult::ModelFailed(_)));
            let unchanged = handle
                .query(
                    ChatSessionWorkflow::snapshot,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(unchanged.messages[..2], restored.messages);
            assert_eq!(
                unchanged.messages.last().unwrap().status,
                MessageStatus::Failed
            );
            let second = handle
                .execute_update(
                    ChatSessionWorkflow::send_message,
                    "again".to_string(),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                second,
                TurnResult::Completed("reply to again; 2 prior messages".into())
            );
            // Two simultaneously submitted turns must see successive, complete contexts.
            let (a, b) = tokio::join!(
                handle.execute_update(
                    ChatSessionWorkflow::send_message,
                    "A".to_string(),
                    WorkflowExecuteUpdateOptions::default()
                ),
                handle.execute_update(
                    ChatSessionWorkflow::send_message,
                    "B".to_string(),
                    WorkflowExecuteUpdateOptions::default()
                )
            );
            assert!(matches!(a.unwrap(), TurnResult::Completed(_)));
            assert!(matches!(b.unwrap(), TurnResult::Completed(_)));
            let snapshot = handle
                .query(
                    ChatSessionWorkflow::snapshot,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(snapshot.messages.len(), 10);
            assert!(!snapshot.generating);
            handle
                .signal(
                    ChatSessionWorkflow::close,
                    (),
                    WorkflowSignalOptions::default(),
                )
                .await
                .unwrap();
            let final_state = handle
                .get_result(WorkflowGetResultOptions::default())
                .await
                .unwrap();
            assert_eq!(final_state.messages, snapshot.messages);
            assert!(final_state.closing);
        })
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.messages.len())
            .collect::<Vec<_>>(),
        [1, 3, 3, 5, 7]
    );
    assert_eq!(requests[2].messages[..2], requests[1].messages[..2]);
}
