use super::*;

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; unique Workflow IDs and fake model only"]
async fn typed_history_is_stored_read_without_worker_and_never_silently_dropped() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let server = test_support::TestServer::connect("typed-history").await;
        let model = Arc::new(tests::FakeModel::default());
        let storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let engine = Engine::connect(EngineOptions {
            address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
            namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
            task_queue: server.task_queue,
            idle_timeout: Duration::from_secs(30),
        }, model.clone(), storage.clone()).await.unwrap();
        let initial = ChatHistory { schema_version: 1, entries: vec![
            HistoryEntry::completed("system", ConversationMessage::system("You are helpful.")),
            HistoryEntry::completed("example", ConversationMessage::User { content: vec![ContentBlock::Image { source: MediaSource::Reference { mime_type: "image/png".into(), key: "uploaded-example".into() } }] }),
        ] };
        let session = engine.start_session_with_history(server.workflow_id, "fake".into(), initial.clone()).await.unwrap();
        // No worker is running. Adapter reads must still work.
        assert_eq!(session.history().await.unwrap(), initial);
        engine.run_until(async {
            assert!(matches!(session.send_message("Describe it".into()).await.unwrap(), TurnResult::Rejected(message) if message.contains("provider translation")));
            assert!(model.requests.lock().unwrap().is_empty());
            assert_eq!(session.history().await.unwrap(), initial);
            assert!(storage.read(session.id()).await.unwrap().turns.is_empty());
            session.close().await.unwrap();
        }).await.unwrap();
        assert_eq!(session.history().await.unwrap(), initial);
    }).await.unwrap();
}
