use super::*;
use crate::{MemoryStorage, ModelSelection, ProgressConfig, ToolFilter};
use rusty_bot_core::*;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

#[path = "engine_recovery_unit_tests.rs"]
mod engine_recovery;
#[path = "legacy_tools_unit_tests.rs"]
mod legacy_tools;
#[path = "orchestration_unit_tests.rs"]
mod orchestration;

struct Files;
#[async_trait::async_trait]
impl FileSystem for Files {
    fn id(&self) -> &str {
        "memory-files"
    }
    fn cwd(&self) -> &Path {
        Path::new("/unit")
    }
    async fn read_text_file(&self, _: &Path) -> Result<String, FileError> {
        Ok("hello\nworld".into())
    }
    async fn read_bytes(&self, _: &Path) -> Result<Vec<u8>, FileError> {
        Ok(b"hello\nworld".to_vec())
    }
    async fn write_file(&self, _: &Path, _: &[u8]) -> Result<(), FileError> {
        Ok(())
    }
    async fn append_file(&self, _: &Path, _: &[u8]) -> Result<(), FileError> {
        Ok(())
    }
    async fn truncate_file(&self, _: &Path, _: u64) -> Result<(), FileError> {
        Ok(())
    }
    async fn rename_file(&self, _: &Path, _: &Path) -> Result<(), FileError> {
        Ok(())
    }
    async fn list_dir(&self, _: &Path) -> Result<Vec<DirEntry>, FileError> {
        Ok(vec![])
    }
    async fn canonical_path(&self, p: &Path) -> Result<PathBuf, FileError> {
        Ok(p.to_path_buf())
    }
    async fn exists(&self, _: &Path) -> Result<bool, FileError> {
        Ok(true)
    }
    async fn create_dir(&self, _: &Path, _: bool) -> Result<(), FileError> {
        Ok(())
    }
    async fn remove_file(&self, _: &Path) -> Result<(), FileError> {
        Ok(())
    }
}
struct Space {
    key: WorkspaceKey,
}
impl Workspace for Space {
    fn key(&self) -> &WorkspaceKey {
        &self.key
    }
    fn root(&self) -> &Path {
        Path::new("/unit")
    }
    fn files(&self) -> Arc<dyn FileSystem> {
        Arc::new(Files)
    }
    fn execution(&self) -> Option<Arc<dyn Shell>> {
        None
    }
    fn execution_env_id(&self) -> Option<&str> {
        None
    }
    fn host_id(&self) -> &str {
        "unit-host"
    }
}
fn workspace() -> Arc<dyn Workspace> {
    Arc::new(Space {
        key: WorkspaceKey::new("unit"),
    })
}
struct Provider {
    steps: Mutex<VecDeque<Result<AssistantTurn, ModelError>>>,
    calls: AtomicUsize,
}
fn answer() -> AssistantTurn {
    AssistantTurn {
        message: Message::Assistant {
            content: vec![ContentPart::Text {
                text: "answer".into(),
            }],
            tool_calls: vec![],
            usage: Some(Usage::default()),
            stop: Some(StopReason::Stop),
        },
        tool_calls: vec![],
        usage: Usage::default(),
        stop: StopReason::Stop,
        provider_metadata: Value::Null,
    }
}
fn tool_turn(name: &str, args: Value) -> AssistantTurn {
    let calls = vec![ToolCall {
        id: "call-one".into(),
        name: name.into(),
        arguments: args,
    }];
    AssistantTurn {
        message: Message::Assistant {
            content: vec![],
            tool_calls: calls.clone(),
            usage: None,
            stop: Some(StopReason::ToolUse),
        },
        tool_calls: calls,
        stop: StopReason::ToolUse,
        ..answer()
    }
}
#[async_trait::async_trait]
impl ModelProvider for Provider {
    fn id(&self) -> &str {
        "unit"
    }
    async fn stream(
        &self,
        _: ChatRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let _ = events.send(StreamEvent::TextDelta("partial".into())).await;
        self.steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(answer()))
    }
}
struct Echo {
    name: String,
    fail: bool,
}
#[async_trait::async_trait]
impl Tool for Echo {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.clone(),
            description: "unit tool".into(),
            parameters: json!({"type":"object","required":["value"],"properties":{"value":{"type":"string"}},"additionalProperties":false}),
        }
    }
    async fn execute(&self, args: Value, _: &ToolContext) -> Result<ToolOutput, ToolError> {
        if self.fail {
            Ok(ToolOutput::error("refused", "test"))
        } else {
            Ok(ToolOutput::success(args["value"].as_str().unwrap_or("")))
        }
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        model: ModelSelection {
            provider: "unit".into(),
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
fn bot(steps: Vec<Result<AssistantTurn, ModelError>>) -> (Bot, Arc<Provider>) {
    let provider = Arc::new(Provider {
        steps: Mutex::new(steps.into()),
        calls: AtomicUsize::new(0),
    });
    let bot = Bot::builder(provider.clone())
        .session_adapter(Arc::new(MemoryStorage::new()))
        .build()
        .unwrap();
    (bot, provider)
}
async fn run(session: &Session, id: &str) -> Result<RunResult, SdkError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        session.prompt(id.into(), "hello".into()).await?.wait(),
    )
    .await
    .expect("bounded run")
}

#[tokio::test]
async fn text_run_persists_history_events_and_resumes_without_io() {
    let (bot, provider) = bot(vec![]);
    let session = bot.create_session(workspace(), config()).await.unwrap();
    let mut handle = session
        .prompt("first".into(), "hello".into())
        .await
        .unwrap();
    let mut frames = vec![];
    while let Some(frame) = handle.next_event().await.unwrap() {
        frames.push(frame);
    }
    assert!(!frames.is_empty());
    assert_eq!(handle.wait().await.unwrap().status, RunStatus::Idle);
    assert!(!session.entries(None, 100).await.unwrap().items.is_empty());
    assert!(!session
        .events(EventCursor::start(), 100)
        .await
        .unwrap()
        .events
        .is_empty());
    let resumed = bot.resume_session(session.id(), workspace()).await.unwrap();
    assert_eq!(resumed.conversation_id(), session.conversation_id());
    assert_eq!(
        run(&resumed, "second").await.unwrap().status,
        RunStatus::Idle
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_submission_does_not_call_provider_twice() {
    let (bot, p) = bot(vec![]);
    let session = bot.create_session(workspace(), config()).await.unwrap();
    run(&session, "same").await.unwrap();
    let result = run(&session, "same").await.unwrap();
    assert!(matches!(result.submission, SubmitOutcome::Duplicate { .. }));
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_input_rejection_does_not_generate() {
    let (bot, p) = bot(vec![]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let result = s
        .prompt("empty".into(), "  ".into())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(result.submission, SubmitOutcome::Rejected { .. }));
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn content_input_and_pagination_limits() {
    let (bot, _) = bot(vec![]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let run = s
        .prompt_content(
            "content".into(),
            vec![ContentPart::Text {
                text: "rich".into(),
            }],
        )
        .await
        .unwrap();
    run.wait().await.unwrap();
    assert!(s.entries(None, 0).await.is_err());
    assert!(s.events(EventCursor::start(), 0).await.is_err());
    let first = s.entries(None, 1).await.unwrap();
    assert_eq!(first.items.len(), 1);
    assert!(first.next.is_some());
    let second = s.entries(first.next, 100).await.unwrap();
    assert!(!second.items.is_empty());
}

#[tokio::test]
async fn model_failure_is_not_reported_as_success() {
    let (bot, _) = bot(vec![Err(ModelError::protocol("unit-failure"))]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    assert!(matches!(
        run(&s, "error").await,
        Err(SdkError::RunnerFailed(_))
    ));
    assert!(s
        .entries(None, 100)
        .await
        .unwrap()
        .items
        .iter()
        .any(|e| e.record.kind == entry_kind::ERROR));
}

#[tokio::test]
async fn custom_tool_success_is_settled_and_model_continues() {
    let p = Arc::new(Provider {
        steps: Mutex::new(
            vec![
                Ok(tool_turn("echo", json!({"value":"hello"}))),
                Ok(answer()),
            ]
            .into(),
        ),
        calls: AtomicUsize::new(0),
    });
    let bot = Bot::builder(p.clone())
        .session_adapter(Arc::new(MemoryStorage::new()))
        .tool(Arc::new(Echo {
            name: "echo".into(),
            fail: false,
        }))
        .build()
        .unwrap();
    let mut cfg = config();
    cfg.tools = ToolFilter::Only(vec!["echo".into()]);
    let s = bot.create_session(workspace(), cfg).await.unwrap();
    assert_eq!(run(&s, "tools").await.unwrap().status, RunStatus::Idle);
    assert_eq!(p.calls.load(Ordering::SeqCst), 2);
    let entries = s.entries(None, 100).await.unwrap().items;
    assert!(entries
        .iter()
        .any(|e| e.record.kind == entry_kind::TOOL_RESULT));
}

#[tokio::test]
async fn invalid_tool_arguments_produce_recorded_error_without_panicking() {
    let p = Arc::new(Provider {
        steps: Mutex::new(vec![Ok(tool_turn("echo", json!({"wrong":1}))), Ok(answer())].into()),
        calls: AtomicUsize::new(0),
    });
    let bot = Bot::builder(p)
        .session_adapter(Arc::new(MemoryStorage::new()))
        .tool(Arc::new(Echo {
            name: "echo".into(),
            fail: false,
        }))
        .build()
        .unwrap();
    let mut cfg = config();
    cfg.tools = ToolFilter::Only(vec!["echo".into()]);
    let s = bot.create_session(workspace(), cfg).await.unwrap();
    run(&s, "invalid-args").await.unwrap();
    assert!(s
        .entries(None, 100)
        .await
        .unwrap()
        .items
        .iter()
        .any(|e| e.record.kind == entry_kind::TOOL_RESULT
            && e.record.data.to_string().contains("invalid")));
}

#[tokio::test]
async fn unknown_or_disabled_tool_is_rejected() {
    let (bot, _) = bot(vec![Ok(tool_turn("unknown", json!({}))), Ok(answer())]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let outcome = run(&s, "unknown").await;
    assert!(
        outcome.is_err()
            || s.entries(None, 100)
                .await
                .unwrap()
                .items
                .iter()
                .any(|e| e.record.kind == entry_kind::TOOL_RESULT)
    );
}

#[tokio::test]
async fn cancelled_handle_releases_gate_and_allows_new_run() {
    let (bot, _) = bot(vec![]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let run = s.prompt("cancel".into(), "hello".into()).await.unwrap();
    run.cancel();
    assert_eq!(run.wait().await.unwrap().status, RunStatus::Cancelled);
    s.shutdown().await.unwrap();
    assert_eq!(
        self::run(&s, "later").await.unwrap().status,
        RunStatus::Idle
    );
}

#[tokio::test]
async fn drop_handle_requests_cancellation_and_shutdown_drains() {
    let (bot, _) = bot(vec![]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let handle = s.prompt("drop".into(), "hello".into()).await.unwrap();
    drop(handle);
    s.shutdown().await.unwrap();
    assert!(s.slot.state.lock().unwrap().active.is_none());
    assert_eq!(s.slot.state.lock().unwrap().shutdowns, 0);
}

#[tokio::test]
async fn busy_gate_and_missing_resume_have_typed_errors() {
    let (bot, _) = bot(vec![]);
    let s = bot.create_session(workspace(), config()).await.unwrap();
    let guard = s.slot.gate.lock().await;
    assert!(matches!(
        s.prompt("busy".into(), "x".into()).await,
        Err(SdkError::Busy)
    ));
    drop(guard);
    assert!(matches!(
        bot.resume_session(SessionId::new(), workspace()).await,
        Err(SdkError::NotFound)
    ));
}

#[tokio::test]
async fn configuration_and_workspace_mismatches_are_rejected() {
    let (bot, _) = bot(vec![]);
    let mut cfg = config();
    cfg.model.provider = "wrong".into();
    assert!(bot.create_session(workspace(), cfg).await.is_err());
    let s = bot.create_session(workspace(), config()).await.unwrap();
    assert!(bot
        .resume_session(
            s.id(),
            Arc::new(Space {
                key: WorkspaceKey::new("other")
            })
        )
        .await
        .is_err());
}

#[test]
fn builder_refuses_duplicate_names_and_invalid_policy() {
    let p = Arc::new(Provider {
        steps: Mutex::new(VecDeque::new()),
        calls: AtomicUsize::new(0),
    });
    assert!(Bot::builder(p.clone())
        .tool(Arc::new(Echo {
            name: "read".into(),
            fail: false
        }))
        .build()
        .is_err());
    let policy = HookJobPolicy {
        max_running: 0,
        ..Default::default()
    };
    assert!(Bot::builder(p).hook_job_policy(policy).build().is_err());
}
