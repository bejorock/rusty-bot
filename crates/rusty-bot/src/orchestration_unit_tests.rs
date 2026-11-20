use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Ext {
    mode: u8,
    yields: Arc<AtomicUsize>,
}
impl Extension for Ext {
    fn name(&self) -> &str {
        "unit-hooks"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![]
    }
    fn sections(&self) -> Vec<PromptSection> {
        vec![]
    }
    fn hooks(&self) -> Vec<Arc<dyn Hook>> {
        vec![Arc::new(Decisions {
            mode: self.mode,
            yields: self.yields.clone(),
        })]
    }
}
struct Decisions {
    mode: u8,
    yields: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Hook for Decisions {
    fn id(&self) -> &str {
        "decisions"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn points(&self) -> HookPoints {
        [
            HookPoint::BeforeRequest,
            HookPoint::BeforeTool,
            HookPoint::AfterTool,
            HookPoint::AfterTools,
            HookPoint::OnYield,
            HookPoint::AfterRun,
        ]
        .into()
    }
    async fn before_request(
        &self,
        messages: &[Message],
        ctx: &HookContext,
    ) -> Result<Option<Vec<Message>>, HookError> {
        assert!(!ctx.entries(None, 100).await.unwrap().items.is_empty());
        assert!(ctx.entries(None, 0).await.is_err());
        if self.mode == 7 {
            let mut result = messages.to_vec();
            result.push(Message::user_text("hook note"));
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
    async fn before_tool(&self, _: &ToolCall, _: &HookContext) -> Result<BeforeTool, HookError> {
        Ok(match self.mode {
            1 => BeforeTool::Block("blocked by hook".into()),
            2 => BeforeTool::Arguments(json!({"value":"rewritten"})),
            3 => BeforeTool::Arguments(json!({"value":3})),
            _ => BeforeTool::Continue,
        })
    }
    async fn after_tool(
        &self,
        _: &ToolCall,
        _: &ToolOutput,
        _: &HookContext,
    ) -> Result<Option<ToolOutput>, HookError> {
        Ok((self.mode == 4).then(|| ToolOutput::success("replacement")))
    }
    async fn after_tools(
        &self,
        _: EntryId,
        _: &[EntryId],
        _: &HookContext,
    ) -> Result<AfterTools, HookError> {
        Ok(if self.mode == 5 {
            AfterTools::Terminate
        } else {
            AfterTools::Continue
        })
    }
    async fn on_yield(&self, _: &HistoryTurn, _: &HookContext) -> Result<YieldDecision, HookError> {
        let count = self.yields.fetch_add(1, Ordering::SeqCst);
        Ok(if self.mode == 6 && count == 0 {
            YieldDecision::Continue(vec![ContentPart::Text {
                text: "please continue".into(),
            }])
        } else {
            YieldDecision::Stand
        })
    }
}
fn hook_bot(mode: u8, steps: Vec<Result<AssistantTurn, ModelError>>) -> (Bot, Arc<AtomicUsize>) {
    let p = Arc::new(Provider {
        steps: Mutex::new(steps.into()),
        calls: AtomicUsize::new(0),
    });
    let yields = Arc::new(AtomicUsize::new(0));
    let b = Bot::builder(p)
        .session_adapter(Arc::new(MemoryStorage::new()))
        .extension(Arc::new(Ext {
            mode,
            yields: yields.clone(),
        }))
        .tool(Arc::new(Echo {
            name: "echo".into(),
            fail: false,
        }))
        .build()
        .unwrap();
    (b, yields)
}
#[tokio::test]
async fn hook_decisions_block_rewrite_validate_replace_and_terminate_tools() {
    for (mode, expected) in [
        (1, "blocked_or_invalid_arguments"),
        (2, "rewritten"),
        (3, "argument"),
        (4, "replacement"),
        (5, "hello"),
    ] {
        let (b, _) = hook_bot(
            mode,
            vec![
                Ok(tool_turn("echo", json!({"value":"hello"}))),
                Ok(answer()),
            ],
        );
        let mut cfg = config();
        cfg.tools = ToolFilter::Only(vec!["echo".into()]);
        let session = b.create_session(workspace(), cfg).await.unwrap();
        let result = run(&session, "hook").await.unwrap();
        assert_eq!(result.status, RunStatus::Idle);
        let entries = session.entries(None, 100).await.unwrap();
        let text = entries
            .items
            .iter()
            .map(|e| serde_json::to_string(&e.record).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.to_lowercase().contains(expected),
            "mode {mode}: {text}"
        );
    }
}
#[tokio::test]
async fn yield_continuation_and_request_rewrite_are_committed_once() {
    for mode in [6, 7] {
        let (b, yields) = hook_bot(mode, vec![Ok(answer()), Ok(answer())]);
        let session = b.create_session(workspace(), config()).await.unwrap();
        run(&session, "yield").await.unwrap();
        assert!(yields.load(Ordering::SeqCst) >= 1);
        let entries = session.entries(None, 100).await.unwrap();
        let text = entries
            .items
            .iter()
            .map(|e| serde_json::to_string(&e.record).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        if mode == 6 {
            assert!(text.contains("please continue"));
            assert!(yields.load(Ordering::SeqCst) >= 2);
        }
    }
}
#[tokio::test]
async fn builtin_file_tools_execute_against_in_memory_files() {
    for (name, args) in [
        ("read", json!({"path":"x"})),
        ("read", json!({"path":"x","offset":2,"limit":1})),
        ("write", json!({"path":"x","content":"new"})),
        (
            "edit",
            json!({"path":"x","oldText":"hello","newText":"changed"}),
        ),
        (
            "edit",
            json!({"path":"x","oldText":"missing","newText":"changed"}),
        ),
        ("ls", json!({"path":"."})),
        ("grep", json!({"pattern":"hello"})),
        ("find", json!({"pattern":"*.rs"})),
        ("bash", json!({"command":"echo hi"})),
    ] {
        let (b, _) = bot(vec![Ok(tool_turn(name, args)), Ok(answer())]);
        let mut cfg = config();
        cfg.tools = ToolFilter::Only(vec![name.into()]);
        let s = b.create_session(workspace(), cfg).await.unwrap();
        let result = run(&s, "builtin").await.unwrap();
        assert_eq!(result.status, RunStatus::Idle);
        let entries = s.entries(None, 100).await.unwrap();
        assert!(
            entries
                .items
                .iter()
                .any(|e| e.record.kind == entry_kind::TOOL_RESULT),
            "missing {name} result"
        );
    }
}
#[tokio::test]
async fn round_limit_stops_repeated_tool_calls_without_hanging() {
    let (b, _) = hook_bot(
        0,
        vec![
            Ok(tool_turn("echo", json!({"value":"one"}))),
            Ok(tool_turn("echo", json!({"value":"two"}))),
            Ok(answer()),
        ],
    );
    let mut cfg = config();
    cfg.tools = ToolFilter::Only(vec!["echo".into()]);
    cfg.limits.max_tool_rounds = 1;
    let s = b.create_session(workspace(), cfg).await.unwrap();
    let result = run(&s, "limit").await;
    assert!(result.is_err() || matches!(result.unwrap().status, RunStatus::Idle));
}
