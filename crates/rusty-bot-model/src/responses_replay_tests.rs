use super::*;

fn call() -> Value {
    json!({"type":"function_call","id":"item","call_id":"c","name":"read","arguments":"{ \"path\": \"a\" }","status":"completed"})
}
fn message() -> Value {
    json!({"type":"message","id":"m","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer"}]})
}
fn reasoning() -> Value {
    json!({"type":"reasoning","id":"r","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"summary"}]})
}
fn refused(item: Value, kind: ModelErrorKind) {
    assert_eq!(parts_from_items(&[item]).unwrap_err().kind, kind);
}

#[test]
fn replay_preserves_order_and_exact_argument_text_without_exposing_reasoning() {
    let parts = parts_from_items(&[reasoning(), message(), call()]).unwrap();
    assert!(matches!(
        &parts[0],
        HistoryPart::Reasoning {
            text: None,
            state: None
        }
    ));
    assert!(matches!(&parts[1],HistoryPart::Text { text } if text=="answer"));
    assert!(
        matches!(&parts[2],HistoryPart::ToolCall { id,name,arguments_json } if id=="c" && name=="read" && arguments_json=="{ \"path\": \"a\" }")
    );
    assert!(parts_from_items(&[]).unwrap().is_empty());
}

#[test]
fn reasoning_requires_encrypted_state_and_valid_summary_shapes() {
    for field in ["id", "encrypted_content", "summary"] {
        let mut item = reasoning();
        item.as_object_mut().unwrap().remove(field);
        refused(item, ModelErrorKind::Protocol);
    }
    let mut item = reasoning();
    item["summary"] = json!([{"type":"other","text":"private"}]);
    refused(item, ModelErrorKind::Unsupported);
    let mut item = reasoning();
    item["summary"] = json!([{"type":"summary_text","text":3}]);
    refused(item, ModelErrorKind::Unsupported);
}

#[test]
fn messages_require_completed_assistant_identity_and_text_content() {
    for (field, value) in [
        ("role", json!("user")),
        ("status", json!("in_progress")),
        ("content", json!(null)),
    ] {
        let mut item = message();
        item[field] = value;
        refused(item, ModelErrorKind::Protocol);
    }
    let mut item = message();
    item["content"] = json!([{"type":"refusal","refusal":"private"}]);
    refused(item, ModelErrorKind::Unsupported);
    let mut item = message();
    item["content"][0]["text"] = json!(false);
    refused(item, ModelErrorKind::Protocol);
}

#[test]
fn tool_calls_refuse_indirect_namespaced_or_async_modes() {
    for (field, value) in [
        ("namespace", json!("external")),
        ("async", json!(true)),
        ("caller", json!({"type":"external"})),
    ] {
        let mut item = call();
        item[field] = value;
        refused(item, ModelErrorKind::Unsupported);
    }
    let mut item = call();
    item["namespace"] = Value::Null;
    item["async"] = json!(false);
    item["caller"] = json!({"type":"direct"});
    assert!(parts_from_items(&[item]).is_ok());
}

#[test]
fn tool_calls_require_unique_identity_completed_status_and_object_arguments() {
    assert_eq!(
        parts_from_items(&[call(), call()]).unwrap_err().kind,
        ModelErrorKind::Protocol
    );
    for (field, value) in [
        ("arguments", json!("[]")),
        ("arguments", json!("invalid")),
        ("arguments", Value::Null),
        ("status", json!("in_progress")),
        ("call_id", json!("")),
        ("name", json!("")),
    ] {
        let mut item = call();
        item[field] = value;
        refused(item, ModelErrorKind::Protocol);
    }
}

#[test]
fn unknown_items_and_missing_type_fail_closed_with_sanitized_errors() {
    let error = parts_from_items(&[json!({"type":"future","secret":"private"})]).unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::Unsupported);
    assert_eq!(error.message, "unsupported native output item");
    assert!(!error.retryable);
    refused(json!({"secret":"private"}), ModelErrorKind::Protocol);
}
