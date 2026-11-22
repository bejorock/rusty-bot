use super::*;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn canonical_response_survives_resume_and_is_forwarded_losslessly() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("response-history").await;
        let model = Arc::new(tests::FakeModel::default());
        let storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let engine = Engine::connect(
            EngineOptions {
                address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
                namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
                task_queue: server.task_queue,
                idle_timeout: Duration::from_secs(30),
            },
            model.clone(),
            storage.clone(),
        )
        .await
        .unwrap();
        let session = engine
            .start_session(server.workflow_id, "fake".into())
            .await
            .unwrap();
        engine
            .run_until(async {
                session.close().await.unwrap();
            })
            .await
            .unwrap();
        // Seed a canonical response through the same storage transitions used by
        // model adapters. No provider is invoked by this test.
        storage
            .change(
                session.id(),
                SessionChange::Claim {
                    run_id: "seed".into(),
                },
            )
            .await
            .unwrap();
        storage
            .change(
                session.id(),
                SessionChange::Begin {
                    run_id: "seed".into(),
                    request_id: "seed-request".into(),
                    text: "question".into(),
                },
            )
            .await
            .unwrap();
        let mut response = HistoryEntry::completed(
            "turn:1:assistant",
            ConversationMessage::Assistant {
                content: vec![
                    ContentBlock::Replay {
                        state: ReplayState {
                            provider: "fake".into(),
                            model: Some("fake".into()),
                            format: "opaque".into(),
                            version: 1,
                            payload: serde_json::json!({"value":"preserve"}),
                            required_for_replay: true,
                        },
                    },
                    ContentBlock::text("answer"),
                ],
            },
        );
        response.turn_id = Some(1);
        response.generation = Some(GenerationMetadata {
            finish_reason: Some(FinishReason::Stop),
            ..Default::default()
        });
        storage
            .change(
                session.id(),
                SessionChange::FinishResponse {
                    run_id: "seed".into(),
                    turn_id: 1,
                    response: Box::new(response.clone()),
                },
            )
            .await
            .unwrap();
        storage
            .change(
                session.id(),
                SessionChange::Close {
                    run_id: "seed".into(),
                },
            )
            .await
            .unwrap();
        let resumed = engine.resume_session(session.id()).await.unwrap();
        assert_eq!(resumed.history().await.unwrap().entries[1], response);
        engine
            .run_until(async {
                assert_eq!(
                    resumed.send_message("next".into()).await.unwrap(),
                    TurnResult::Completed("reply to next; 2 prior messages".into())
                );
                assert_eq!(
                    model.requests.lock().unwrap()[0].history.entries[1],
                    response
                );
                assert_eq!(storage.read(session.id()).await.unwrap().turns.len(), 2);
                resumed.close().await.unwrap();
            })
            .await
            .unwrap();
        assert_eq!(resumed.history().await.unwrap().entries[1], response);
    })
    .await
    .unwrap();
}

#[derive(Default)]
struct RichModel {
    requests: std::sync::Mutex<Vec<rusty_bot_providers::GenerationRequest>>,
}

fn rich_response(id: &str) -> HistoryEntry {
    let mut response = HistoryEntry::completed(
        id,
        ConversationMessage::Assistant {
            content: vec![
                ContentBlock::Reasoning {
                    text: "private reasoning".into(),
                },
                ContentBlock::Replay {
                    state: ReplayState {
                        provider: "fake".into(),
                        model: Some("fake".into()),
                        format: "opaque".into(),
                        version: 1,
                        payload: serde_json::json!({"token":"state"}),
                        required_for_replay: true,
                    },
                },
                ContentBlock::text("authoritative answer"),
            ],
        },
    );
    response.generation = Some(GenerationMetadata {
        provider: Some("fake".into()),
        model: Some("fake".into()),
        response_id: Some("response-1".into()),
        finish_reason: Some(FinishReason::Stop),
        usage: Some(TokenUsage {
            input_tokens: Some(10),
            output_tokens: Some(4),
            ..Default::default()
        }),
        ..Default::default()
    });
    response
        .metadata
        .insert("trace".into(), serde_json::json!("preserved"));
    response
}

#[async_trait::async_trait]
impl rusty_bot_providers::Provider for RichModel {
    fn operation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(45))
    }
    fn build_request(
        &self,
        _: &rusty_bot_providers::GenerationRequest,
    ) -> Result<serde_json::Value, ModelError> {
        Err(ModelError::unsupported("no wire request"))
    }
    async fn list_models(
        &self,
        _: rusty_bot_providers::DiscoveryRequest,
        _: CancellationToken,
    ) -> Result<rusty_bot_providers::ModelPage, ModelError> {
        Err(ModelError::unsupported("no discovery"))
    }
    async fn stream(
        &self,
        request: &rusty_bot_providers::GenerationRequest,
        events: tokio::sync::mpsc::Sender<MessageStreamEvent>,
        _: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        self.requests.lock().unwrap().push(request.clone());
        let text = request
            .history
            .entries
            .last()
            .unwrap()
            .message
            .text_only()
            .map_err(|e| ModelError::new(ModelErrorKind::InvalidRequest, e.to_string(), false))?;
        if text == "fail partial" {
            use MessageStreamPayload as P;
            test_support::send_event(&events, &request.message_id, 1, P::Start).await;
            test_support::send_event(
                &events,
                &request.message_id,
                2,
                P::BlockStart {
                    index: 0,
                    block: BlockStart::Text,
                },
            )
            .await;
            test_support::send_event(
                &events,
                &request.message_id,
                3,
                P::BlockDelta {
                    index: 0,
                    delta: "discard me".into(),
                },
            )
            .await;
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "partial failure",
                false,
            ));
        }
        let response = rich_response(&request.message_id);
        test_support::emit_text(
            &events,
            &request.message_id,
            &response,
            &["provisional answer"],
        )
        .await;
        Ok(response)
    }
}

#[tokio::test]
async fn rich_activity_returns_authoritative_entry_not_preview() {
    let provider = Arc::new(RichModel::default());
    let activity = LlmActivities::new(provider);
    let entry = activity
        .generate_with_progress(
            GenerateInput {
                model: "fake".into(),
                message_id: "response".into(),
                history: ChatHistory {
                    schema_version: 1,
                    entries: vec![HistoryEntry::completed(
                        "user",
                        ConversationMessage::user("question"),
                    )],
                },
            },
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(entry, rich_response("response"));
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn rich_response_and_metadata_survive_worker_restart_and_failed_partial_is_excluded() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = test_support::TestServer::connect("rich-restart").await;
        let provider = Arc::new(RichModel::default());
        let storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let engine = Engine::connect(
            EngineOptions {
                address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
                namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
                task_queue: server.task_queue,
                idle_timeout: Duration::from_secs(60),
            },
            provider.clone(),
            storage,
        )
        .await
        .unwrap();
        let session = engine
            .start_session(server.workflow_id, "fake".into())
            .await
            .unwrap();
        engine
            .run_until(async {
                assert_eq!(
                    session.send_message("first".into()).await.unwrap(),
                    TurnResult::Completed("authoritative answer".into())
                );
                let mut expected = rich_response("turn:1:assistant");
                expected.turn_id = Some(1);
                assert_eq!(session.history().await.unwrap().entries[1], expected);
            })
            .await
            .unwrap();
        // run_until ends the worker. A fresh worker must replay and use persisted canonical context.
        engine
            .run_until(async {
                let resumed = engine.resume_session(session.id()).await.unwrap();
                let mut expected = rich_response("turn:1:assistant");
                expected.turn_id = Some(1);
                assert_eq!(resumed.history().await.unwrap().entries[1], expected);
                assert!(matches!(
                    resumed.send_message("fail partial".into()).await.unwrap(),
                    TurnResult::ModelFailed(_)
                ));
                assert_eq!(
                    resumed.send_message("next".into()).await.unwrap(),
                    TurnResult::Completed("authoritative answer".into())
                );
                let requests = provider.requests.lock().unwrap().clone();
                assert_eq!(requests.len(), 3);
                assert_eq!(requests[2].history.entries.len(), 3);
                assert_eq!(requests[2].history.entries[1], expected);
                assert_eq!(
                    requests[2].history.entries[2].message,
                    ConversationMessage::user("next")
                );
                assert!(
                    !serde_json::to_string(&requests[2].history)
                        .unwrap()
                        .contains("discard me")
                );
                resumed.close().await.unwrap();
            })
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
