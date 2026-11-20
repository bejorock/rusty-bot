use super::*;
use serde_json::json;

fn state() -> ProviderState {
    ProviderState {
        provider: "private-provider".into(),
        model: Some("private-model".into()),
        format: "private-format".into(),
        payload: json!({"secret":"private-token"}),
        required_for_replay: true,
    }
}

#[test]
fn provider_state_debug_redacts_every_field() {
    assert_eq!(format!("{:?}", state()), "ProviderState([REDACTED])");
}

#[test]
fn every_history_variant_debug_redacts_nested_sensitive_data() {
    let parts = [
        HistoryPart::Text {
            text: "secret-text".into(),
        },
        HistoryPart::Image {
            mime_type: "secret-mime".into(),
            data_base64: "secret-bytes".into(),
        },
        HistoryPart::ToolCall {
            id: "secret-id".into(),
            name: "secret-tool".into(),
            arguments_json: "secret-args".into(),
        },
        HistoryPart::ToolResult {
            call_id: "secret-id".into(),
            name: "secret-tool".into(),
            content: vec![ContentPart::Text {
                text: "secret-content".into(),
            }],
            is_error: true,
            details: Some(json!({"secret":"secret-detail"})),
        },
        HistoryPart::Reasoning {
            text: Some("secret-thought".into()),
            state: Some(state()),
        },
        HistoryPart::ProviderState { state: state() },
    ];
    for (part, kind) in parts.iter().zip([
        "Text",
        "Image",
        "ToolCall",
        "ToolResult",
        "Reasoning",
        "ProviderState",
    ]) {
        assert_eq!(format!("{part:?}"), format!("{kind}([REDACTED])"));
    }
    let nested = format!("{parts:?}");
    assert!(!nested.contains("secret"));
    assert!(!nested.contains("private"));
}

#[test]
fn provider_state_serialization_is_lossless_despite_debug_redaction() {
    let value = serde_json::to_value(state()).unwrap();
    assert_eq!(value["payload"]["secret"], "private-token");
    assert_eq!(
        serde_json::from_value::<ProviderState>(value).unwrap(),
        state()
    );
}

#[test]
fn provider_state_model_must_be_present_but_can_explicitly_be_null() {
    let mut value = serde_json::to_value(state()).unwrap();
    value["model"] = serde_json::Value::Null;
    assert_eq!(
        serde_json::from_value::<ProviderState>(value.clone())
            .unwrap()
            .model,
        None
    );
    value.as_object_mut().unwrap().remove("model");
    assert!(serde_json::from_value::<ProviderState>(value).is_err());
}

#[test]
fn provider_state_model_rejects_non_string_json_types() {
    for model in [json!(1), json!(false), json!([]), json!({})] {
        let mut value = serde_json::to_value(state()).unwrap();
        value["model"] = model;
        assert!(serde_json::from_value::<ProviderState>(value).is_err());
    }
}

#[test]
fn rich_assistant_keeps_tool_call_order_among_non_call_parts() {
    let message = Message::Rich {
        rich_role: RichRole::Assistant,
        parts: vec![
            HistoryPart::Reasoning {
                text: None,
                state: Some(state()),
            },
            HistoryPart::ToolCall {
                id: "two".into(),
                name: "read".into(),
                arguments_json: r#"{"path":"猫"}"#.into(),
            },
            HistoryPart::Text {
                text: "between".into(),
            },
            HistoryPart::ToolCall {
                id: "one".into(),
                name: "ls".into(),
                arguments_json: "{}".into(),
            },
        ],
    };
    let calls = canonical_tool_calls(&message).unwrap();
    assert_eq!(
        calls.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        ["two", "one"]
    );
    assert_eq!(calls[0].arguments, json!({"path":"猫"}));
}

#[test]
fn rich_nonassistant_roles_cannot_schedule_tool_calls() {
    for role in [RichRole::User, RichRole::Tool] {
        let message = Message::Rich {
            rich_role: role,
            parts: vec![HistoryPart::ToolCall {
                id: "id".into(),
                name: "read".into(),
                arguments_json: "{}".into(),
            }],
        };
        let error = canonical_tool_calls(&message).unwrap_err();
        assert_eq!(error.message, "expected assistant message");
    }
}

#[test]
fn credential_debug_redacts_secret_and_exposure_remains_explicit() {
    let credential = crate::ProviderCredential::new("private-token");
    assert_eq!(format!("{credential:?}"), "ProviderCredential([REDACTED])");
    assert_eq!(credential.expose_secret(), "private-token");
    assert_eq!(credential.clone(), credential);
}
