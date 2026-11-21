use super::*;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn canonical_response_survives_resume_and_blocks_lossy_legacy_generation() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("response-history").await;
        let model = Arc::new(tests::FakeModel::default());
        let storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let engine = Engine::connect(EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: Duration::from_secs(30),
        }, model.clone(), storage.clone()).await.unwrap();
        let session = engine.start_session(server.workflow_id, "fake".into()).await.unwrap();
        engine.run_until(async { session.close().await.unwrap(); }).await.unwrap();
        // Seed a canonical response through the same storage transitions future model
        // adapters will use. No provider is invoked by this test.
        storage.change(session.id(), SessionChange::Claim { run_id: "seed".into() }).await.unwrap();
        storage.change(session.id(), SessionChange::Begin { run_id: "seed".into(), request_id: "seed-request".into(), text: "question".into() }).await.unwrap();
        let mut response = HistoryEntry::completed("turn:1:assistant", ConversationMessage::Assistant { content: vec![
            ContentBlock::Replay { state: ReplayState { provider: "fake".into(), model: Some("fake".into()), format: "opaque".into(), version: 1, payload: serde_json::json!({"value":"preserve"}), required_for_replay: true } },
            ContentBlock::text("answer"),
        ] });
        response.turn_id = Some(1);
        response.generation = Some(GenerationMetadata { finish_reason: Some(FinishReason::Stop), ..Default::default() });
        storage.change(session.id(), SessionChange::FinishResponse { run_id: "seed".into(), turn_id: 1, response: Box::new(response.clone()) }).await.unwrap();
        storage.change(session.id(), SessionChange::Close { run_id: "seed".into() }).await.unwrap();
        let resumed = engine.resume_session(session.id()).await.unwrap();
        assert_eq!(resumed.history().await.unwrap().entries[1], response);
        engine.run_until(async {
            assert!(matches!(resumed.send_message("next".into()).await.unwrap(), TurnResult::Rejected(message) if message.contains("provider translation")));
            assert!(model.requests.lock().unwrap().is_empty());
            assert_eq!(storage.read(session.id()).await.unwrap().turns.len(), 1);
            resumed.close().await.unwrap();
        }).await.unwrap();
        assert_eq!(resumed.history().await.unwrap().entries[1], response);
    }).await.unwrap();
}
