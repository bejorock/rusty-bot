use rusty_bot::*;
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
struct Provider;
#[async_trait::async_trait]
impl ModelProvider for Provider {
    fn id(&self) -> &str {
        "test"
    }
    async fn stream(
        &self,
        _request: ChatRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        _cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        events
            .send(StreamEvent::TextDelta("hello".into()))
            .await
            .unwrap();
        Ok(AssistantTurn {
            message: Message::Assistant {
                content: vec![ContentPart::Text {
                    text: "hello".into(),
                }],
                tool_calls: vec![],
                usage: Some(Usage::default()),
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
async fn sdk_session_resumes_from_jsonl_after_adapter_is_dropped() {
    let path = Temp(std::env::temp_dir().join(format!("rusty-sdk-{}.jsonl", SessionId::new())));
    let root = std::env::current_dir().unwrap();
    let workspaces = LocalWorkspaceProvider::new(root.clone()).unwrap();
    let workspace = workspaces
        .open(WorkspaceSpec {
            key: WorkspaceKey::new("file-test"),
            root: Some(root),
            execution: ExecutionSelection::None,
        })
        .await
        .unwrap();
    let id;
    let first_entries;
    {
        let adapter = Arc::new(FileSessionAdapter::open(&path.0).unwrap());
        let bot = Bot::builder(Arc::new(Provider))
            .session_adapter(adapter)
            .build()
            .unwrap();
        let session = bot
            .create_session(
                workspace.clone(),
                SessionConfig {
                    model: ModelSelection {
                        provider: "test".into(),
                        model_id: "fake".into(),
                    },
                    tools: ToolFilter::Only(vec![]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        id = session.id();
        let mut run = session
            .prompt("first".into(), "hello".into())
            .await
            .unwrap();
        assert!(run.next_event().await.unwrap().is_some());
        run.wait().await.unwrap();
        first_entries = session.entries(None, 100).await.unwrap().items.len();
        session.shutdown().await.unwrap();
    }
    let adapter = Arc::new(FileSessionAdapter::open(&path.0).unwrap());
    let bot = Bot::builder(Arc::new(Provider))
        .session_adapter(adapter)
        .build()
        .unwrap();
    let session = bot.resume_session(id, workspace).await.unwrap();
    assert_eq!(
        session.entries(None, 100).await.unwrap().items.len(),
        first_entries
    );
    session
        .prompt("second".into(), "again".into())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(session.entries(None, 100).await.unwrap().items.len() > first_entries);
    session.shutdown().await.unwrap();
    workspaces
        .release(&WorkspaceKey::new("file-test"))
        .await
        .unwrap();
}
