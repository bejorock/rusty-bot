use super::*;
use rusty_bot_core::{ToolCall, Usage};
use serde_json::json;

fn assistant(calls: Vec<ToolCall>) -> Message {
    Message::Assistant {
        content: vec![],
        tool_calls: calls,
        usage: Some(Usage::default()),
        stop: Some(StopReason::Stop),
    }
}
fn request(messages: Vec<Message>) -> HistoryRequest {
    HistoryRequest {
        model: "unit".into(),
        messages,
        tools: vec![],
        tool_choice: ToolChoice::None,
        temperature: None,
        max_tokens: None,
        metadata: json!({}),
    }
}
fn conversation() -> Vec<Message> {
    vec![
        Message::user_text("old"),
        assistant(vec![]),
        Message::user_text("recent"),
        assistant(vec![]),
        Message::user_text("current"),
    ]
}

#[test]
fn completed_exchange_ranges_exclude_current_unanswered_user() {
    assert_eq!(exchanges(&conversation()).unwrap(), vec![0..2, 2..4]);
}

#[test]
fn exchange_parser_refuses_mismatched_tool_result_identity() {
    let messages = vec![
        Message::user_text("read"),
        assistant(vec![ToolCall {
            id: "a".into(),
            name: "read".into(),
            arguments: json!({}),
        }]),
        Message::ToolResult {
            tool_call_id: "a".into(),
            tool_name: "wrong".into(),
            content: vec![],
            is_error: false,
            details: None,
        },
    ];
    assert!(exchanges(&messages).is_err());
}

#[test]
fn structured_summary_replaces_only_complete_prefix_and_preserves_request() {
    let original = request(conversation());
    let candidate = structured_candidate(&original, 0..2, "earlier work").unwrap();
    assert_eq!(&candidate.messages[1..], &original.messages[2..]);
    assert_eq!(
        candidate.messages[0],
        Message::user_text(format!("{SUMMARY_LABEL}earlier work"))
    );
    assert_eq!(candidate.metadata, original.metadata);
    assert_eq!(original.messages.len(), 5);
}

#[test]
fn structured_summary_refuses_split_exchanges_and_invalid_summary_sizes() {
    let original = request(conversation());
    assert!(structured_candidate(&original, 0..1, "summary").is_err());
    assert!(structured_candidate(&original, 2..4, "summary").is_err());
    assert!(structured_candidate(&original, 0..5, "summary").is_err());
    assert!(structured_candidate(&original, 0..2, "  ").is_err());
    assert!(structured_candidate(
        &original,
        0..2,
        &"x".repeat(MAX_STRUCTURED_SUMMARY_BYTES + 1)
    )
    .is_err());
}
