use super::*;

fn call() -> Value {
    json!({"functionCall":{"name":"read","args":{"path":"a"}},"thoughtSignature":"signed"})
}
fn refused(raw: &[Value], ids: &[Value], kind: ModelErrorKind) {
    assert_eq!(project(raw, ids, 4).unwrap_err().kind, kind);
}

#[test]
fn projection_preserves_text_reasoning_and_generated_tool_identity() {
    let parts = project(
        &[
            json!({"text":"answer"}),
            json!({"text":"reason","thought":true}),
            call(),
        ],
        &[Value::Null, Value::Null, json!("gemini_4_2")],
        4,
    )
    .unwrap();
    assert!(matches!(&parts[0],HistoryPart::Text { text } if text=="answer"));
    assert!(
        matches!(&parts[1],HistoryPart::Reasoning { text:Some(text),state:None } if text=="reason")
    );
    assert!(
        matches!(&parts[2],HistoryPart::ToolCall { id,name,arguments_json } if id=="gemini_4_2" && name=="read" && serde_json::from_str::<Value>(arguments_json).unwrap()==json!({"path":"a"}))
    );
}

#[test]
fn projection_requires_nonempty_aligned_part_and_identity_arrays() {
    refused(&[], &[], ModelErrorKind::Protocol);
    refused(&[json!({"text":"a"})], &[], ModelErrorKind::Protocol);
    refused(&[json!(null)], &[Value::Null], ModelErrorKind::Protocol);
    refused(
        &[json!({"text":"a"})],
        &[json!("unexpected")],
        ModelErrorKind::Protocol,
    );
}

#[test]
fn projection_rejects_unknown_fields_and_malformed_thought_metadata() {
    refused(
        &[json!({"text":"a","future":true})],
        &[Value::Null],
        ModelErrorKind::Unsupported,
    );
    for value in [
        json!({"text":"a","thought":"true"}),
        json!({"text":"a","thoughtSignature":""}),
        json!({"text":1}),
        json!({"text":"a","functionCall":{"name":"read"}}),
    ] {
        refused(&[value], &[Value::Null], ModelErrorKind::Protocol);
    }
    refused(
        &[json!({"partMetadata":{}})],
        &[Value::Null],
        ModelErrorKind::Unsupported,
    );
}

#[test]
fn explicit_call_ids_must_match_and_duplicates_are_refused() {
    let mut value = call();
    value["functionCall"]["id"] = json!("explicit");
    assert!(project(std::slice::from_ref(&value), &[json!("explicit")], 4).is_ok());
    refused(
        std::slice::from_ref(&value),
        &[json!("other")],
        ModelErrorKind::Protocol,
    );
    refused(
        &[value.clone(), value],
        &[json!("explicit"), json!("explicit")],
        ModelErrorKind::Protocol,
    );
    refused(
        &[call()],
        &[json!("wrong_generated_id")],
        ModelErrorKind::Protocol,
    );
}

#[test]
fn tool_calls_require_object_arguments_valid_name_and_no_reasoning_flag() {
    for (field, value) in [
        ("args", json!([])),
        ("name", json!("")),
        ("future", json!(true)),
    ] {
        let mut item = call();
        item["functionCall"][field] = value;
        refused(&[item], &[json!("gemini_4_0")], ModelErrorKind::Protocol);
    }
    let mut item = call();
    item["thought"] = json!(true);
    refused(&[item], &[json!("gemini_4_0")], ModelErrorKind::Protocol);
    let mut item = call();
    item["functionCall"].as_object_mut().unwrap().remove("args");
    let projected = project(&[item], &[json!("gemini_4_0")], 4).unwrap();
    assert!(
        matches!(&projected[0],HistoryPart::ToolCall { arguments_json,.. } if arguments_json=="{}")
    );
}

#[test]
fn first_tool_call_requires_a_nonempty_signature_but_text_only_does_not() {
    assert!(require_signature(&[json!({"text":"a"})]).is_ok());
    assert!(require_signature(&[call()]).is_ok());
    for signature in [Value::Null, json!(""), json!(3)] {
        let mut item = call();
        item["thoughtSignature"] = signature;
        assert_eq!(
            require_signature(&[item]).unwrap_err().kind,
            ModelErrorKind::Protocol
        );
    }
    let mut item = call();
    item.as_object_mut().unwrap().remove("thoughtSignature");
    assert_eq!(
        require_signature(&[item]).unwrap_err().kind,
        ModelErrorKind::Protocol
    );
}
