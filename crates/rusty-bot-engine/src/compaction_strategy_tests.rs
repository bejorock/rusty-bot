use super::*;
use rusty_bot_core::*;
use serde_json::json;

struct SummaryProvider {
    mode: u8,
}
#[async_trait]
impl ModelProvider for SummaryProvider {
    fn id(&self) -> &str {
        "summary"
    }
    async fn stream(
        &self,
        _: ChatRequest,
        _: tokio::sync::mpsc::Sender<StreamEvent>,
        _: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        unreachable!("native history method is used")
    }
    async fn stream_history(
        &self,
        request: HistoryRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        _: CancellationToken,
    ) -> Result<HistoryTurn, ModelError> {
        assert_eq!(request.tool_choice, ToolChoice::None);
        assert!(request.tools.is_empty());
        if self.mode == 1 {
            events
                .send(StreamEvent::ThinkingDelta("forbidden".into()))
                .await
                .unwrap();
        }
        let text = if self.mode == 2 { " " } else { "summary" };
        let message = if self.mode == 3 {
            Message::Rich {
                rich_role: RichRole::Assistant,
                parts: vec![
                    HistoryPart::Text { text: text.into() },
                    HistoryPart::ProviderState {
                        state: ProviderState {
                            provider: "summary".into(),
                            model: Some("summarizer".into()),
                            format: "opaque".into(),
                            payload: json!({}),
                            required_for_replay: true,
                        },
                    },
                ],
            }
        } else {
            Message::Assistant {
                content: vec![ContentPart::Text { text: text.into() }],
                tool_calls: vec![],
                usage: None,
                stop: Some(StopReason::Stop),
            }
        };
        Ok(HistoryTurn {
            message,
            usage: Usage::default(),
            stop: StopReason::Stop,
            provider_metadata: json!({}),
        })
    }
}
fn cfg() -> SummaryConfig {
    SummaryConfig {
        context_budget_tokens: 4000,
        reserved_output_tokens: 100,
        retain_recent_exchanges: 1,
        summary_max_output_tokens: 100,
        summary_max_input_tokens: 12000,
        summary_max_bytes: 100,
    }
}
fn assistant(text: &str) -> Message {
    Message::Assistant {
        content: vec![ContentPart::Text { text: text.into() }],
        tool_calls: vec![],
        usage: None,
        stop: Some(StopReason::Stop),
    }
}
fn request(rich: bool) -> HistoryRequest {
    let old = if rich {
        Message::Rich {
            rich_role: RichRole::Assistant,
            parts: vec![HistoryPart::Text {
                text: "old answer".into(),
            }],
        }
    } else {
        assistant("old answer")
    };
    HistoryRequest {
        model: "model".into(),
        messages: vec![
            Message::user_text("old question ".repeat(400)),
            old,
            Message::user_text("recent question"),
            assistant("recent answer"),
            Message::user_text("current question"),
        ],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        temperature: None,
        max_tokens: None,
        metadata: json!({}),
    }
}
#[test]
fn configuration_rejects_invalid_budgets_and_model() {
    let mut bad = cfg();
    bad.summary_max_bytes = usize::MAX;
    assert!(ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), "s", bad).is_err());
    for field in 0..7 {
        let mut c = cfg();
        match field {
            0 => c.context_budget_tokens = 0,
            1 => c.reserved_output_tokens = 0,
            2 => c.reserved_output_tokens = c.context_budget_tokens,
            3 => c.retain_recent_exchanges = 0,
            4 => c.summary_max_output_tokens = 0,
            5 => c.summary_max_input_tokens = 0,
            _ => c.summary_max_bytes = 0,
        };
        assert!(ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), "s", c).is_err());
    }
    assert!(ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), " ", cfg()).is_err());
}
#[tokio::test]
async fn legacy_summary_replaces_only_completed_prefix() {
    let strategy =
        ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), "summarizer", cfg()).unwrap();
    assert!(strategy.drain_on_cancel());
    assert!(strategy.defer_rich_until_new_submission());
    let r = request(false);
    let output = strategy
        .compact(&r, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output[1..], &r.messages[2..]);
    assert!(Message::text_content(match &output[0] {
        Message::User { content } => content,
        _ => panic!(),
    })
    .contains("summary"));
}
#[tokio::test]
async fn native_summary_returns_validated_reset_proposal() {
    let strategy =
        ModelSummarizer::new(Arc::new(SummaryProvider { mode: 3 }), "summarizer", cfg()).unwrap();
    let proposal = strategy
        .propose(&request(true), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(proposal,CompactionProposal::NativeResetSummary{remove,retain_recent_exchanges:1,summary} if remove==(0..2)&&summary=="summary")
    );
}
#[tokio::test]
async fn small_request_is_unchanged_and_bad_summaries_fail() {
    let r = HistoryRequest {
        messages: vec![Message::user_text("short")],
        ..request(false)
    };
    let strategy =
        ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), "summarizer", cfg()).unwrap();
    assert!(strategy
        .compact(&r, CancellationToken::new())
        .await
        .unwrap()
        .is_none());
    for mode in [1, 2] {
        let strategy =
            ModelSummarizer::new(Arc::new(SummaryProvider { mode }), "summarizer", cfg()).unwrap();
        assert!(strategy
            .compact(&request(false), CancellationToken::new())
            .await
            .is_err());
    }
}
#[tokio::test]
async fn cancellation_and_insufficient_retained_history_fail() {
    let strategy =
        ModelSummarizer::new(Arc::new(SummaryProvider { mode: 0 }), "summarizer", cfg()).unwrap();
    let c = CancellationToken::new();
    c.cancel();
    assert!(strategy.compact(&request(false), c).await.is_err());
    let r = HistoryRequest {
        messages: vec![
            Message::user_text("x".repeat(5000)),
            assistant("answer"),
            Message::user_text("now"),
        ],
        ..request(false)
    };
    assert!(strategy
        .compact(&r, CancellationToken::new())
        .await
        .is_err());
}
#[test]
fn rendering_quotes_all_roles_and_omits_opaque_reasoning() {
    let mut out = vec![];
    let rich = [
        Message::Rich {
            rich_role: RichRole::User,
            parts: vec![
                HistoryPart::Text {
                    text: "quoted\nuser".into(),
                },
                HistoryPart::Image {
                    mime_type: "image/png".into(),
                    data_base64: "bytes".into(),
                },
            ],
        },
        Message::Rich {
            rich_role: RichRole::Assistant,
            parts: vec![
                HistoryPart::Text {
                    text: "answer".into(),
                },
                HistoryPart::ToolCall {
                    id: "id".into(),
                    name: "read".into(),
                    arguments_json: "{}".into(),
                },
                HistoryPart::Reasoning {
                    text: Some("private".into()),
                    state: None,
                },
            ],
        },
        Message::Rich {
            rich_role: RichRole::Tool,
            parts: vec![HistoryPart::ToolResult {
                call_id: "id".into(),
                name: "read".into(),
                content: vec![ContentPart::Text {
                    text: "result".into(),
                }],
                is_error: true,
                details: None,
            }],
        },
    ];
    for m in &rich {
        render_message(&mut out, m).unwrap();
    }
    let text = Message::text_content(&out);
    assert!(text.contains("call arguments JSON"));
    assert!(text.contains("result error"));
    assert!(!text.contains("private"));
    assert!(out.iter().any(|p| matches!(p, ContentPart::Image { .. })));
    let mut legacy = vec![];
    render_message(
        &mut legacy,
        &Message::ToolResult {
            tool_call_id: "id".into(),
            tool_name: "read".into(),
            content: vec![],
            is_error: false,
            details: None,
        },
    )
    .unwrap();
    assert!(render_message(
        &mut legacy,
        &Message::Rich {
            rich_role: RichRole::Tool,
            parts: vec![HistoryPart::Text { text: "bad".into() }]
        }
    )
    .is_ok());
    assert!(render_message(
        &mut legacy,
        &Message::Rich {
            rich_role: RichRole::Assistant,
            parts: vec![HistoryPart::Image {
                mime_type: "x".into(),
                data_base64: "x".into()
            }]
        }
    )
    .is_err());
}
#[test]
fn transcript_strips_sensitive_arguments_images_and_details_with_utf8_limits() {
    let messages = vec![
        Message::User {
            content: vec![ContentPart::Image {
                mime_type: "image/png".into(),
                data_base64: "secret-image".into(),
            }],
        },
        Message::Assistant {
            content: vec![ContentPart::Text {
                text: "猫".repeat(1000),
            }],
            tool_calls: vec![ToolCall {
                id: "id".into(),
                name: "read".into(),
                arguments: json!({"secret-arg":true}),
            }],
            usage: None,
            stop: None,
        },
        Message::ToolResult {
            tool_call_id: "id".into(),
            tool_name: "read".into(),
            content: vec![ContentPart::Text {
                text: "result".into(),
            }],
            is_error: true,
            details: Some(json!({"secret-detail":true})),
        },
    ];
    let range = 0..3;
    let text = transcript(&messages, std::slice::from_ref(&range), 600).unwrap();
    assert!(text.len() <= 600);
    assert!(text.contains("[truncated]"));
    assert!(!text.contains("secret"));
    assert!(transcript(&messages, std::slice::from_ref(&range), 1).is_err());
    let mut out = String::new();
    append_parts(
        &mut out,
        &[ContentPart::Text {
            text: "猫猫".into(),
        }],
        4,
    );
    assert_eq!(out, "猫 [truncated]");
}
#[test]
fn native_reset_renders_retained_exchange_and_checks_boundary() {
    let r = request(true);
    let result = native_reset_candidate(&r, 0..2, 1, "summary").unwrap();
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages.last(), r.messages.last());
    for range in [0..1, 1..2, 0..4] {
        assert!(native_reset_candidate(&r, range, 1, "summary").is_err());
    }
    assert!(native_reset_candidate(&r, 0..2, 0, "summary").is_err());
    assert!(native_reset_candidate(&r, 0..2, 1, " ").is_err());
}
