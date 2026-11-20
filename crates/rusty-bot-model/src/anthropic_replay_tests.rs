use super::*;

fn tool() -> Value {
    json!({"type":"tool_use","id":"c","name":"read","input":{"path":"a"}})
}
fn refused(blocks: &[Value], args: &[Value], kind: ModelErrorKind) {
    let error = parts_from_blocks(blocks, args).unwrap_err();
    assert_eq!(error.kind, kind);
    assert_eq!(error.retryable, kind == ModelErrorKind::Protocol);
}

#[test]
fn replay_preserves_order_text_reasoning_and_exact_tool_argument_string() {
    let raw = "{ \"path\" : \"a\" }";
    let parts = parts_from_blocks(
        &[
            json!({"type":"text","text":"hello"}),
            json!({"type":"thinking","thinking":"reason","signature":"signed"}),
            json!({"type":"redacted_thinking","data":"opaque"}),
            tool(),
        ],
        &[Value::Null, Value::Null, Value::Null, json!(raw)],
    )
    .unwrap();
    assert!(matches!(&parts[0],HistoryPart::Text { text } if text=="hello"));
    assert!(
        matches!(&parts[1],HistoryPart::Reasoning { text:Some(text), state:None } if text=="reason")
    );
    assert!(matches!(
        &parts[2],
        HistoryPart::Reasoning {
            text: None,
            state: None
        }
    ));
    assert!(
        matches!(&parts[3],HistoryPart::ToolCall { id,name,arguments_json } if id=="c" && name=="read" && arguments_json==raw)
    );
}

#[test]
fn replay_requires_one_argument_slot_per_block_and_null_for_non_tools() {
    let text = json!({"type":"text","text":"hello"});
    refused(std::slice::from_ref(&text), &[], ModelErrorKind::Protocol);
    refused(&[text], &[json!("{}")], ModelErrorKind::Protocol);
    assert!(parts_from_blocks(&[], &[]).unwrap().is_empty());
}

#[test]
fn reasoning_requires_nonempty_signature_or_redacted_data() {
    for block in [
        json!({"type":"thinking","thinking":"private"}),
        json!({"type":"thinking","thinking":"private","signature":""}),
        json!({"type":"redacted_thinking","data":""}),
    ] {
        refused(&[block], &[Value::Null], ModelErrorKind::Protocol);
    }
}

#[test]
fn tool_replay_refuses_missing_nonobject_invalid_or_mismatched_arguments() {
    for args in [
        Value::Null,
        json!("[]"),
        json!("broken"),
        json!("{\"path\":\"b\"}"),
    ] {
        refused(&[tool()], &[args], ModelErrorKind::Protocol);
    }
}

#[test]
fn duplicate_tool_identity_and_empty_name_are_refused() {
    refused(
        &[tool(), tool()],
        &[json!("{\"path\":\"a\"}"), json!("{\"path\":\"a\"}")],
        ModelErrorKind::Protocol,
    );
    let mut block = tool();
    block["name"] = json!("");
    refused(
        &[block],
        &[json!("{\"path\":\"a\"}")],
        ModelErrorKind::Protocol,
    );
}

#[test]
fn unsupported_blocks_and_malformed_text_are_distinguished() {
    refused(
        &[json!({"type":"future_block","data":"private"})],
        &[Value::Null],
        ModelErrorKind::Unsupported,
    );
    refused(
        &[json!({"type":"text","text":3})],
        &[Value::Null],
        ModelErrorKind::Protocol,
    );
    refused(
        &[json!({"type":""})],
        &[Value::Null],
        ModelErrorKind::Protocol,
    );
}
