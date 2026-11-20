use super::*;
use rusty_bot_core::*;
use serde_json::json;
use std::sync::Mutex;

struct TestHook {
    mode: u8,
}
#[async_trait::async_trait]
impl Hook for TestHook {
    fn id(&self) -> &str {
        "test"
    }
    fn version(&self) -> &str {
        "1"
    }
    async fn before_tool(&self, _: &ToolCall, ctx: &HookContext) -> Result<BeforeTool, HookError> {
        assert_eq!(ctx.task_id(), ctx.run_id());
        assert_eq!(ctx.config(), &json!({}));
        assert!(ctx.entries(None, 0).await.is_err());
        assert!(ctx.entries(None, 100).await.is_ok());
        let _ = (ctx.session_id(), ctx.conversation_id(), ctx.cancel());
        match self.mode {
            1 => Ok(BeforeTool::Arguments(json!({"changed":true}))),
            2 => Ok(BeforeTool::Block("blocked".into())),
            3 => Err(HookError::new("secret code!", "secret message")),
            _ => Ok(BeforeTool::Continue),
        }
    }
    async fn after_tool(
        &self,
        _: &ToolCall,
        _: &ToolOutput,
        _: &HookContext,
    ) -> Result<Option<ToolOutput>, HookError> {
        Ok(Some(ToolOutput::success("replacement")))
    }
    async fn before_request(
        &self,
        m: &[Message],
        _: &HookContext,
    ) -> Result<Option<Vec<Message>>, HookError> {
        if self.mode == 2 {
            Ok(Some(vec![
                Message::System {
                    content: "bad".into(),
                    sections: Default::default(),
                    tools_added: vec![],
                    tools_removed: vec![],
                },
                Message::System {
                    content: "bad".into(),
                    sections: Default::default(),
                    tools_added: vec![],
                    tools_removed: vec![],
                },
            ]))
        } else {
            Ok(Some(m.to_vec()))
        }
    }
    async fn before_provider_request(
        &self,
        _: &str,
        _: &str,
        p: &serde_json::Value,
        _: &HookContext,
    ) -> Result<Option<serde_json::Value>, HookError> {
        let mut p = p.clone();
        p["temperature"] = json!(0.5);
        Ok(Some(p))
    }
    async fn after_tools_jobs(
        &self,
        _: EntryId,
        _: &[EntryId],
        ctx: &HookContext,
    ) -> Result<AfterToolsJobs, HookError> {
        Ok(AfterToolsJobs {
            direct: AfterTools::Continue,
            jobs: vec![ctx
                .spawn(async { Ok(AfterTools::Terminate) })
                .map_err(|_| HookError::new("spawn", "unavailable"))?],
        })
    }
    async fn on_yield_jobs(
        &self,
        _: &HistoryTurn,
        ctx: &HookContext,
    ) -> Result<YieldJobs, HookError> {
        if self.mode == 4 {
            Ok(YieldJobs::Jobs(vec![ctx
                .spawn(async {
                    Ok(YieldDecision::Continue(vec![ContentPart::Text {
                        text: "job continuation".into(),
                    }]))
                })
                .unwrap()]))
        } else {
            Ok(YieldJobs::Direct(YieldDecision::Continue(vec![
                ContentPart::Text {
                    text: if self.mode == 2 { " " } else { "continue" }.into(),
                },
            ])))
        }
    }
    async fn after_run_jobs(
        &self,
        _: &RunSummary,
        ctx: &HookContext,
    ) -> Result<Vec<HookJob<()>>, HookError> {
        Ok(vec![ctx.spawn(async { Ok(()) }).unwrap()])
    }
}
#[derive(Default)]
struct Reports(Mutex<Vec<HookReport>>);
impl HookReporter for Reports {
    fn report(&self, r: HookReport) {
        self.0.lock().unwrap().push(r)
    }
}
fn dispatcher(mode: u8) -> (Dispatcher, Arc<HookJobSupervisor>, Arc<Reports>) {
    let cancel = CancellationToken::new();
    let supervisor =
        Arc::new(HookJobSupervisor::new(HookJobPolicy::default(), cancel.clone()).unwrap());
    let reporter = Arc::new(Reports::default());
    let hook = ResolvedHook {
        descriptor: HookDescriptor {
            extension: "ext".into(),
            extension_version: "1".into(),
            hook: "test".into(),
            hook_version: "1".into(),
            points: [
                HookPoint::BeforeRequest,
                HookPoint::BeforeProviderRequest,
                HookPoint::AfterResponse,
                HookPoint::BeforeTool,
                HookPoint::AfterTool,
                HookPoint::AfterTools,
                HookPoint::OnYield,
                HookPoint::AfterRun,
            ]
            .into(),
            config: json!({}),
        },
        hook: Arc::new(TestHook { mode }),
    };
    (
        Dispatcher::new(
            vec![hook],
            SessionId::new(),
            ConversationId(1),
            Arc::new(rusty_bot_memory::InMemoryStorage::new()),
            Some(reporter.clone()),
            cancel,
            Some(supervisor.clone()),
        ),
        supervisor,
        reporter,
    )
}
fn turn() -> HistoryTurn {
    HistoryTurn {
        message: Message::Assistant {
            content: vec![ContentPart::Text {
                text: "answer".into(),
            }],
            tool_calls: vec![],
            usage: None,
            stop: Some(StopReason::Stop),
        },
        usage: Usage::default(),
        stop: StopReason::Stop,
        provider_metadata: json!({}),
    }
}
fn call() -> ToolCall {
    ToolCall {
        id: "id".into(),
        name: "tool".into(),
        arguments: json!({}),
    }
}
#[tokio::test]
async fn dispatches_mutations_replacements_and_successful_lifecycle() {
    let (d, s, _) = dispatcher(1);
    let task = TaskId(2);
    let mut call = call();
    assert_eq!(d.before_tool(task, &mut call).await.unwrap(), None);
    assert_eq!(call.arguments, json!({"changed":true}));
    let mut output = ToolOutput::success("original");
    d.after_tool(task, &call, &mut output).await.unwrap();
    assert_eq!(
        output.content,
        vec![ContentPart::Text {
            text: "replacement".into()
        }]
    );
    let mut messages = vec![Message::user_text("x")];
    d.before_request(task, &mut messages).await.unwrap();
    d.after_response(task, &turn()).await.unwrap();
    assert!(d.after_tools(task, EntryId(3), &[]).await.unwrap());
    assert!(d.on_yield(task, &turn(), None).await.unwrap().is_some());
    d.after_run(
        task,
        &RunSummary {
            reason: RunReason::Answer,
            first: EntryId(3),
            answer: EntryId(3),
            last: EntryId(3),
        },
    )
    .await
    .unwrap();
    s.seal_and_drain().await;
}
#[tokio::test]
async fn blocked_and_invalid_messages_yields_do_not_escape() {
    let (d, _s, r) = dispatcher(2);
    assert_eq!(
        d.before_tool(TaskId(2), &mut call()).await.unwrap(),
        Some("blocked".into())
    );
    let mut messages = vec![Message::user_text("original")];
    d.before_request(TaskId(2), &mut messages).await.unwrap();
    assert_eq!(messages, vec![Message::user_text("original")]);
    assert!(d
        .on_yield(TaskId(2), &turn(), None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(r.0.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn callback_errors_fail_closed_and_report_sanitized_code() {
    let (d, _s, r) = dispatcher(3);
    assert_eq!(
        d.before_tool(TaskId(2), &mut call()).await.unwrap(),
        Some("hook failed".into())
    );
    assert_eq!(r.0.lock().unwrap()[0].code, "hook_error");
}
#[tokio::test]
async fn yielded_jobs_are_settled_in_order() {
    let (d, _s, _) = dispatcher(4);
    assert_eq!(
        d.on_yield(TaskId(2), &turn(), None).await.unwrap(),
        Some(vec![ContentPart::Text {
            text: "job continuation".into()
        }])
    );
}
#[tokio::test]
async fn cancellation_preempts_callbacks_and_payloads() {
    let (d, _s, _) = dispatcher(0);
    d.cancel.cancel();
    assert!(matches!(
        d.before_tool(TaskId(2), &mut call()).await,
        Err(SessionError::Cancelled)
    ));
    let hook = d.payload_hook(TaskId(2)).unwrap();
    assert!(hook
        .apply(
            "openai.chat_completions.v1",
            "m",
            json!({"model":"m","messages":[]}),
            CancellationToken::new()
        )
        .await
        .is_err());
}
#[tokio::test]
async fn payload_hook_mutates_only_approved_fields() {
    let (d, _s, _) = dispatcher(0);
    let hook = d.payload_hook(TaskId(2)).unwrap();
    let result = hook
        .apply(
            "openai.chat_completions.v1",
            "m",
            json!({"model":"m","messages":[]}),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["temperature"], 0.5);
}
#[test]
fn replacement_validation_protects_system_and_tool_identity() {
    let mut messages = vec![
        Message::System {
            content: "system".into(),
            sections: Default::default(),
            tools_added: vec![],
            tools_removed: vec![],
        },
        Message::user_text("x"),
        Message::Assistant {
            content: vec![],
            tool_calls: vec![call()],
            usage: None,
            stop: None,
        },
        Message::ToolResult {
            tool_call_id: "id".into(),
            tool_name: "tool".into(),
            content: vec![],
            is_error: false,
            details: None,
        },
        Message::Assistant {
            content: vec![],
            tool_calls: vec![],
            usage: None,
            stop: Some(StopReason::Stop),
        },
    ];
    assert!(valid_replacement(&messages, &messages));
    if let Message::ToolResult { tool_name, .. } = &mut messages[3] {
        *tool_name = "wrong".into();
    }
    assert!(!valid_replacement(&[], &messages));
    assert!(!valid_replacement(
        &[],
        &[Message::ToolResult {
            tool_call_id: "missing".into(),
            tool_name: "tool".into(),
            content: vec![],
            is_error: false,
            details: None
        }]
    ));
}
