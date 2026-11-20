use super::*;

#[test]
fn gemini_body_rejects_model_drift_from_endpoint() {
    let provider = GeminiGenerateContentProvider::with_static_credential(
        "unit",
        "https://example.invalid/v1/models/unit:streamGenerateContent",
        ProviderCredential::new("fake"),
    )
    .unwrap();
    let mut request = HistoryRequest {
        model: "unit".into(),
        messages: vec![Message::user_text("hello")],
        tools: vec![],
        tool_choice: ToolChoice::None,
        temperature: None,
        max_tokens: Some(25),
        metadata: json!({}),
    };
    assert!(provider.body(&request).is_ok());
    request.model = "different".into();
    assert_eq!(
        provider.body(&request).unwrap_err().kind,
        ModelErrorKind::InvalidRequest
    );
}
