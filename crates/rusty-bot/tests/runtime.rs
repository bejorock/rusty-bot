use rusty_bot::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Provider {
    calls: AtomicUsize,
    tools: bool,
}
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
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let tool_calls = if self.tools && first {
            (0..8)
                .map(|n| ToolCall {
                    id: format!("call-{n}"),
                    name: "barrier".into(),
                    arguments: serde_json::json!({}),
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if tool_calls.is_empty() {
            for n in 0..150 {
                events
                    .send(StreamEvent::TextDelta(n.to_string()))
                    .await
                    .unwrap();
            }
        }
        let stop = if tool_calls.is_empty() {
            StopReason::Stop
        } else {
            StopReason::ToolUse
        };
        Ok(AssistantTurn {
            message: Message::Assistant {
                content: vec![ContentPart::Text {
                    text: "done".into(),
                }],
                tool_calls: tool_calls.clone(),
                usage: Some(Usage::default()),
                stop: Some(stop),
            },
            tool_calls,
            usage: Usage::default(),
            stop,
            provider_metadata: serde_json::Value::Null,
        })
    }
}
struct BarrierTool {
    barrier: tokio::sync::Barrier,
    count: AtomicUsize,
}
#[async_trait::async_trait]
impl Tool for BarrierTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "barrier".into(),
            description: "Wait for all eight calls".into(),
            parameters: serde_json::json!({"type":"object", "properties":{}}),
        }
    }
    async fn execute(
        &self,
        _arguments: serde_json::Value,
        _ctx: &ToolContext,
    ) -> Result<ToolOutput, ToolError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.barrier.wait().await;
        Ok(ToolOutput::success("all eight started"))
    }
}
async fn workspace() -> (LocalWorkspaceProvider, Arc<dyn Workspace>) {
    let root = std::env::current_dir().unwrap();
    let provider = LocalWorkspaceProvider::new(root.clone()).unwrap();
    let workspace = provider
        .open(WorkspaceSpec {
            key: WorkspaceKey::new("test"),
            root: Some(root),
            execution: ExecutionSelection::None,
        })
        .await
        .unwrap();
    (provider, workspace)
}
fn config() -> SessionConfig {
    SessionConfig {
        model: ModelSelection {
            provider: "test".into(),
            model_id: "fake".into(),
        },
        tools: ToolFilter::Only(vec![]),
        progress: ProgressConfig {
            partial_interval_ms: 0,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn progress_is_not_dropped_when_reader_waits_until_run_finishes() {
    let adapter = Arc::new(MemoryStorage::new());
    let bot = Bot::builder(Arc::new(Provider {
        calls: AtomicUsize::new(0),
        tools: false,
    }))
    .session_adapter(adapter.clone())
    .build()
    .unwrap();
    let (workspaces, workspace) = workspace().await;
    let session = bot.create_session(workspace, config()).await.unwrap();
    let mut run = session.prompt("one".into(), "hello".into()).await.unwrap();
    // Do not read while the provider emits more than the removed capacity (64).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if adapter
                .read_frames(session.id(), 0, 1000)
                .await
                .unwrap()
                .iter()
                .filter(|f| f.frame.kind == LiveKind::TextDelta)
                .count()
                == 150
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // Start the next run before draining this handle: the older handle must
    // stop at its captured end cursor, not consume the new run's records.
    let mut second = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.prompt("two".into(), "again".into()).await {
                Ok(run) => break run,
                Err(SdkError::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
    let mut count = 0;
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(5), run.next_event())
        .await
        .unwrap()
        .unwrap()
    {
        if frame.kind == LiveKind::TextDelta {
            count += 1;
        }
    }
    assert_eq!(count, 150);
    assert_eq!(run.wait().await.unwrap().status, RunStatus::Idle);
    // A second run must not replay the first run's progress.
    let mut count = 0;
    while let Some(frame) = second.next_event().await.unwrap() {
        if frame.kind == LiveKind::TextDelta {
            count += 1;
        }
    }
    assert_eq!(count, 150);
    second.wait().await.unwrap();
    session.shutdown().await.unwrap();
    workspaces
        .release(&WorkspaceKey::new("test"))
        .await
        .unwrap();
}

#[tokio::test]
async fn all_eight_tool_calls_start_without_concurrency_limit_or_barrier() {
    let tool = Arc::new(BarrierTool {
        barrier: tokio::sync::Barrier::new(8),
        count: AtomicUsize::new(0),
    });
    let bot = Bot::builder(Arc::new(Provider {
        calls: AtomicUsize::new(0),
        tools: true,
    }))
    .session_adapter(Arc::new(MemoryStorage::new()))
    .tool(tool.clone())
    .build()
    .unwrap();
    let (workspaces, workspace) = workspace().await;
    let mut cfg = config();
    cfg.tools = ToolFilter::Only(vec!["barrier".into()]);
    let session = bot.create_session(workspace, cfg).await.unwrap();
    let run = session
        .prompt("parallel".into(), "use tools".into())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), run.wait())
        .await
        .expect("all calls must run concurrently")
        .unwrap();
    assert_eq!(result.status, RunStatus::Idle);
    assert_eq!(tool.count.load(Ordering::SeqCst), 8);
    session.shutdown().await.unwrap();
    workspaces
        .release(&WorkspaceKey::new("test"))
        .await
        .unwrap();
}

#[test]
fn builtin_registry_has_no_powershell_tool() {
    let names: Vec<_> = rusty_bot_engine::builtin_extension()
        .tools()
        .iter()
        .map(|t| t.spec().name)
        .collect();
    assert!(names.iter().any(|n| n == "bash"));
    assert!(!names.iter().any(|n| n == "powershell"));
}
