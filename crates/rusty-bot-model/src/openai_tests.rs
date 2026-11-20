use super::*;

fn provider() -> OpenAiCompatProvider {
    let mut config = OpenAiCompatConfig::new(
        "https://example.invalid/v1/chat/completions"
            .parse()
            .unwrap(),
        "default",
    );
    config.retry_jitter = false;
    OpenAiCompatProvider::new(config).unwrap()
}

#[test]
fn request_body_preserves_tools_and_rounds_temperature() {
    let request = ChatRequest {
        model: "selected".into(),
        messages: vec![ProviderMessage::User {
            content: "hello".into(),
        }],
        tools: vec![ToolSpec {
            name: "read".into(),
            description: "Read".into(),
            parameters: json!({"type":"object"}),
        }],
        tool_choice: ToolChoice::Named("read".into()),
        temperature: Some(0.2),
        max_tokens: Some(20),
        metadata: json!({}),
    };
    let body = provider().build_body(&request).unwrap();
    assert_eq!(body["model"], "selected");
    assert_eq!(body["temperature"], json!(0.2));
    assert_eq!(body["max_tokens"], 20);
    assert_eq!(body["tool_choice"]["function"]["name"], "read");
    assert_eq!(
        body["tools"][0]["function"]["parameters"],
        json!({"type":"object"})
    );
    assert_eq!(
        body["messages"][0],
        json!({"role":"user","content":"hello"})
    );
    assert_eq!(body["stream_options"]["include_usage"], true);
}

#[test]
fn http_errors_classify_retryable_and_permanent_failures() {
    let provider = provider();
    for (status, kind, retryable) in [
        (401, ModelErrorKind::Auth, false),
        (422, ModelErrorKind::InvalidRequest, false),
        (413, ModelErrorKind::ContextOverflow, false),
        (429, ModelErrorKind::RateLimited, true),
        (503, ModelErrorKind::Server, true),
    ] {
        let error = provider.status_error(reqwest::StatusCode::from_u16(status).unwrap());
        assert_eq!(error.kind, kind);
        assert_eq!(error.retryable, retryable);
        assert_eq!(error.status, Some(status));
    }
}

#[test]
fn retry_delay_caps_exponential_backoff_and_honors_retry_after() {
    let provider = provider();
    assert_eq!(provider.backoff(0, None), Duration::from_secs(2));
    assert_eq!(provider.backoff(2, None), Duration::from_secs(8));
    assert_eq!(provider.backoff(u32::MAX, None), Duration::from_secs(60));
    assert_eq!(
        provider.backoff(0, Some(Duration::from_secs(90))),
        Duration::from_secs(90)
    );
}

#[test]
fn text_only_provider_refuses_tool_images_before_serialization() {
    let messages = [ProviderMessage::Tool {
        tool_call_id: "a".into(),
        content: vec![ContentPart::Image {
            mime_type: "image/png".into(),
            data_base64: "invalid".into(),
        }],
    }];
    assert_eq!(
        provider().wire_messages(&messages).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
}
