use super::*;
use serde_json::json;

fn refused(api: &str, original: Value, candidate: Value) {
    let error = validate_provider_payload(api, &original, &candidate).unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    assert_eq!(error.message, "invalid provider payload mutation");
    assert!(!error.retryable);
}

#[test]
fn only_known_apis_and_object_payloads_are_accepted() {
    let error = validate_provider_payload("unknown", &json!({}), &json!({})).unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::Unsupported);
    refused("openai.responses.v1", json!(null), json!({}));
    refused("openai.responses.v1", json!({}), json!([]));
}

#[test]
fn openai_allows_only_temperature_and_token_changes_with_exact_boundaries() {
    for (api, tokens) in [
        ("openai.responses.v1", "max_output_tokens"),
        ("openai.chat_completions.v1", "max_tokens"),
    ] {
        let original = json!({"model":"fixed", "messages":[{"role":"user","content":"private"}]});
        for (temperature, budget) in [(0.0, 1), (2.0, 1_000_000)] {
            let mut candidate = original.clone();
            candidate["temperature"] = json!(temperature);
            candidate[tokens] = json!(budget);
            assert!(validate_provider_payload(api, &original, &candidate).is_ok());
            assert!(validate_provider_payload(api, &candidate, &original).is_ok());
        }
        let mut changed = original.clone();
        changed["model"] = json!("different");
        refused(api, original.clone(), changed);
        let mut changed = original.clone();
        changed["extra"] = json!(true);
        refused(api, original, changed);
    }
}

#[test]
fn token_and_temperature_type_range_errors_fail_closed() {
    for (field, value) in [
        ("temperature", json!(-0.1)),
        ("temperature", json!(2.1)),
        ("temperature", json!(null)),
        ("temperature", json!("1")),
        ("max_tokens", json!(0)),
        ("max_tokens", json!(1_000_001)),
        ("max_tokens", json!(-1)),
        ("max_tokens", json!(1.5)),
        ("max_tokens", json!(true)),
    ] {
        let mut candidate = json!({});
        candidate[field] = value;
        refused("openai.chat_completions.v1", json!({}), candidate);
    }
}

#[test]
fn anthropic_requires_integer_token_budget_and_temperature_at_most_one() {
    let original = json!({"model":"fixed", "max_tokens":10});
    assert!(validate_provider_payload(
        "anthropic.messages.v1",
        &original,
        &json!({"model":"fixed","max_tokens":1,"temperature":1})
    )
    .is_ok());
    refused(
        "anthropic.messages.v1",
        original.clone(),
        json!({"model":"fixed"}),
    );
    refused(
        "anthropic.messages.v1",
        original,
        json!({"model":"fixed","max_tokens":10,"temperature":1.01}),
    );
}

#[test]
fn gemini_can_add_only_nonempty_allowed_generation_configuration() {
    let original = json!({"contents":[]});
    assert!(validate_provider_payload("gemini.generate_content.v1", &original, &original).is_ok());
    assert!(validate_provider_payload(
        "gemini.generate_content.v1",
        &original,
        &json!({"contents":[],"generationConfig":{"temperature":2,"maxOutputTokens":1}})
    )
    .is_ok());
    for config in [
        json!({}),
        json!(null),
        json!({"topP":0.5}),
        json!({"maxOutputTokens":0}),
    ] {
        refused(
            "gemini.generate_content.v1",
            original.clone(),
            json!({"contents":[],"generationConfig":config}),
        );
    }
}

#[test]
fn gemini_preserves_nested_configuration_and_top_level_identity() {
    let original = json!({"contents":["private"],"generationConfig":{"topP":0.9,"thinkingConfig":{"budget":100},"temperature":1}});
    let mut candidate = original.clone();
    candidate["generationConfig"]
        .as_object_mut()
        .unwrap()
        .remove("temperature");
    candidate["generationConfig"]["maxOutputTokens"] = json!(2);
    assert!(validate_provider_payload("gemini.generate_content.v1", &original, &candidate).is_ok());
    candidate["generationConfig"]["thinkingConfig"]["budget"] = json!(101);
    refused("gemini.generate_content.v1", original.clone(), candidate);
    let mut candidate = original.clone();
    candidate
        .as_object_mut()
        .unwrap()
        .remove("generationConfig");
    refused("gemini.generate_content.v1", original.clone(), candidate);
    let mut candidate = original.clone();
    candidate["contents"] = json!([]);
    refused("gemini.generate_content.v1", original, candidate);
}
