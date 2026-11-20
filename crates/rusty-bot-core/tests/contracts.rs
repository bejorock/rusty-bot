use rusty_bot_core::{
    payload_validation::validate_provider_payload, truncate_text, ModelErrorKind,
};
use serde_json::json;

#[test]
fn truncation_preserves_content_at_exact_limits() {
    let text = "one\ntwo\n";
    let result = truncate_text(text, 2, text.len());
    assert_eq!(result.text, text);
    assert!(!result.truncated);
    assert_eq!((result.total_lines, result.total_bytes), (2, text.len()));
}

#[test]
fn truncation_preserves_head_tail_and_utf8_boundaries() {
    let text = "αβγδεζηθ";
    let result = truncate_text(text, 100, 8);
    assert!(result.truncated);
    assert!(result.text.starts_with("αβ"));
    assert!(result.text.ends_with("ηθ"));
    assert!(result.text.contains("truncated"));
    assert_eq!(result.total_bytes, text.len());
    let lines = truncate_text("first\nsecond\nthird\nlast", 2, 100);
    assert!(lines.text.starts_with("first\n"));
    assert!(lines.text.ends_with("\nlast"));
    assert!(lines.text.contains("2 lines"));
}

#[test]
fn payload_hook_can_only_change_allowed_generation_fields() {
    let original = json!({"model":"test", "messages":[{"role":"user","content":"hi"}]});
    let mut candidate = original.clone();
    candidate["temperature"] = json!(0.5);
    candidate["max_tokens"] = json!(100);
    validate_provider_payload("openai.chat_completions.v1", &original, &candidate).unwrap();
    candidate["messages"][0]["content"] = json!("changed");
    assert_eq!(
        validate_provider_payload("openai.chat_completions.v1", &original, &candidate)
            .unwrap_err()
            .kind,
        ModelErrorKind::InvalidRequest
    );
}

#[test]
fn payload_hook_rejects_invalid_generation_limits() {
    let original = json!({"model":"test"});
    for value in [json!(-1), json!(2.1), json!("0.5"), json!(null)] {
        let candidate = json!({"model":"test", "temperature":value});
        assert!(validate_provider_payload("openai.responses.v1", &original, &candidate).is_err());
    }
    for value in [json!(0), json!(1_000_001), json!(1.5)] {
        let candidate = json!({"model":"test", "max_output_tokens":value});
        assert!(validate_provider_payload("openai.responses.v1", &original, &candidate).is_err());
    }
    assert_eq!(
        validate_provider_payload("unknown", &original, &original)
            .unwrap_err()
            .kind,
        ModelErrorKind::Unsupported
    );
}

#[test]
fn anthropic_required_token_limit_cannot_be_removed() {
    let original = json!({"model":"test", "max_tokens":100});
    assert!(validate_provider_payload(
        "anthropic.messages.v1",
        &original,
        &json!({"model":"test"})
    )
    .is_err());
    assert!(validate_provider_payload(
        "anthropic.messages.v1",
        &original,
        &json!({"model":"test", "max_tokens":200, "temperature":1.1})
    )
    .is_err());
    validate_provider_payload(
        "anthropic.messages.v1",
        &original,
        &json!({"model":"test", "max_tokens":200, "temperature":1.0}),
    )
    .unwrap();
}

#[test]
fn gemini_hook_preserves_unrelated_nested_configuration() {
    let original = json!({"contents":[], "generationConfig":{"topP":0.8}});
    let valid = json!({"contents":[], "generationConfig":{"topP":0.8,"temperature":0.4,"maxOutputTokens":100}});
    validate_provider_payload("gemini.generate_content.v1", &original, &valid).unwrap();
    let mut invalid = valid;
    invalid["generationConfig"]["topP"] = json!(0.2);
    assert!(validate_provider_payload("gemini.generate_content.v1", &original, &invalid).is_err());
}
