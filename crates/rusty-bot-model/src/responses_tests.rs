use super::*;

#[test]
fn responses_body_disables_storage_and_maps_output_token_budget() {
    let provider = OpenAiResponsesProvider::with_static_credential(
        "unit",
        "https://example.invalid/v1/responses",
        ProviderCredential::new("fake"),
    )
    .unwrap();
    let request = HistoryRequest {
        model: "unit".into(),
        messages: vec![Message::user_text("hello")],
        tools: vec![],
        tool_choice: ToolChoice::Required,
        temperature: None,
        max_tokens: Some(25),
        metadata: json!({}),
    };
    let body = provider.body(&request).unwrap();
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
    assert_eq!(body["max_output_tokens"], 25);
    assert_eq!(body["tool_choice"], "required");
    assert_eq!(body["input"][0]["content"][0]["text"], "hello");
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
}
