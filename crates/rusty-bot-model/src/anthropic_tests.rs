use super::*;

fn provider() -> AnthropicMessagesProvider {
    AnthropicMessagesProvider::with_static_credential(
        "unit",
        "https://example.invalid/v1/messages",
        ProviderCredential::new("fake"),
    )
    .unwrap()
}
fn request() -> HistoryRequest {
    HistoryRequest {
        model: "unit".into(),
        messages: vec![Message::user_text("hello")],
        tools: vec![],
        tool_choice: ToolChoice::None,
        temperature: None,
        max_tokens: Some(20),
        metadata: json!({}),
    }
}

#[test]
fn anthropic_body_encodes_tool_errors_as_user_tool_result_blocks() {
    let mut request = request();
    request.messages.push(Message::ToolResult {
        tool_call_id: "a".into(),
        tool_name: "read".into(),
        content: vec![ContentPart::Text {
            text: "missing".into(),
        }],
        is_error: true,
        details: None,
    });
    let body = provider().body(&request).unwrap();
    assert_eq!(body["max_tokens"], 20);
    assert_eq!(body["messages"][1]["role"], "user");
    assert_eq!(body["messages"][1]["content"][0]["type"], "tool_result");
    assert_eq!(body["messages"][1]["content"][0]["tool_use_id"], "a");
    assert_eq!(body["messages"][1]["content"][0]["is_error"], true);
}

#[test]
fn anthropic_body_rejects_invalid_options_before_transport() {
    let provider = provider();
    let mut request = request();
    request.temperature = Some(f32::NAN);
    assert_eq!(
        provider.body(&request).unwrap_err().kind,
        ModelErrorKind::InvalidRequest
    );
    request.temperature = None;
    request.metadata = json!({"model_options":{"thinking_level":"high"}});
    assert_eq!(
        provider.body(&request).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
}
