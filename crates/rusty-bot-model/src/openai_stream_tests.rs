use super::*;
fn provider() -> OpenAiCompatProvider {
    OpenAiCompatProvider::new(OpenAiCompatConfig::new(
        url::Url::parse("https://example.invalid/chat").unwrap(),
        "unit",
    ))
    .unwrap()
}
fn request() -> ChatRequest {
    ChatRequest {
        model: "unit".into(),
        messages: vec![],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        temperature: None,
        max_tokens: None,
        metadata: Value::Null,
    }
}
async fn consume(
    chunks: Vec<Value>,
    done: bool,
    request: ChatRequest,
) -> (Result<AssistantTurn, ModelError>, Vec<StreamEvent>, bool) {
    let mut body = String::new();
    for c in chunks {
        body.push_str(&format!("data: {c}\n\n"));
    }
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    let response: reqwest::Response = http::Response::builder()
        .status(200)
        .body(body)
        .unwrap()
        .into();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let mut emitted = false;
    let result = provider()
        .consume(
            response,
            &request,
            &tx,
            &CancellationToken::new(),
            &mut emitted,
        )
        .await;
    drop(tx);
    let mut events = vec![];
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    (result, events, emitted)
}
#[tokio::test]
async fn complete_text_reasoning_and_usage_survive_in_memory_sse() {
    let(result,events,emitted)=consume(vec![json!({}),json!({"choices":[]}),json!({"choices":[{"delta":{"content":"hello","reasoning_content":"thought"}}]}),json!({"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":3}}})],true,request()).await;
    let result = result.unwrap();
    assert_eq!(result.stop, StopReason::Stop);
    assert_eq!(result.usage.input_tokens, 5);
    assert_eq!(result.usage.cache_read_tokens, Some(3));
    assert_eq!(result.provider_metadata["thinking"], "thought");
    assert_eq!(events.len(), 3);
    assert!(emitted);
}
#[tokio::test]
async fn tool_call_fragments_preserve_order_and_required_arguments() {
    let mut r = request();
    r.tools.push(ToolSpec {
        name: "read".into(),
        description: "read".into(),
        parameters: json!({"required":["path"]}),
    });
    let(result,events,_)=consume(vec![json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"id","function":{"name":"read","arguments":"{\"path\":"}}]}}]}),json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"猫\"}"}}]},"finish_reason":"tool_calls"}]})],true,r.clone()).await;
    let result = result.unwrap();
    assert_eq!(result.stop, StopReason::ToolUse);
    assert_eq!(result.tool_calls[0].arguments, json!({"path":"猫"}));
    assert_eq!(events.len(), 3);
    let(result,_,_)=consume(vec![json!({"choices":[{"delta":{"tool_calls":[{"id":"id","function":{"name":"read"}}]},"finish_reason":"tool_calls"}]})],true,r).await;
    assert!(result.unwrap_err().message.contains("requires parameters"));
}
#[tokio::test]
async fn empty_optional_arguments_become_object_and_stop_reasons_map() {
    let(result,_,_)=consume(vec![json!({"choices":[{"delta":{"tool_calls":[{"id":"id","function":{"name":"optional"}}]},"finish_reason":"function_call"}]})],true,request()).await;
    assert_eq!(result.unwrap().tool_calls[0].arguments, json!({}));
    for (reason, expected) in [
        ("length", StopReason::Length),
        ("content_filter", StopReason::Error),
    ] {
        let (result, _, _) = consume(
            vec![json!({"choices":[{"finish_reason":reason}]})],
            false,
            request(),
        )
        .await;
        assert_eq!(result.unwrap().stop, expected);
    }
    let (result, _, _) = consume(vec![], true, request()).await;
    assert_eq!(result.unwrap().stop, StopReason::Stop);
}
#[tokio::test]
async fn incomplete_unknown_and_malformed_call_streams_are_protocol_errors() {
    for chunk in [
        json!({"choices":[{"delta":{"content":"partial"}}]}),
        json!({"choices":[{"finish_reason":"unknown"}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"function":{"name":"x"}}]},"finish_reason":"tool_calls"}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"id":"id"}]},"finish_reason":"tool_calls"}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"id":"id","function":{"name":"x","arguments":"{"}}]},"finish_reason":"tool_calls"}]}),
    ] {
        let (result, _, _) = consume(vec![chunk], false, request()).await;
        assert_eq!(result.unwrap_err().kind, ModelErrorKind::Protocol);
    }
}
#[tokio::test]
async fn final_unterminated_event_is_processed_and_bad_json_is_sanitized() {
    for (body, valid) in [
        ("data: {\"choices\":[{\"finish_reason\":\"stop\"}]}", true),
        ("data: invalid\n\n", false),
        ("data: invalid", false),
    ] {
        let response: reqwest::Response = http::Response::builder()
            .body(body.to_owned())
            .unwrap()
            .into();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let result = provider()
            .consume(
                response,
                &request(),
                &tx,
                &CancellationToken::new(),
                &mut false,
            )
            .await;
        assert_eq!(result.is_ok(), valid);
    }
    let c = CancellationToken::new();
    c.cancel();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    tx.send(StreamEvent::TextDelta("fill".into()))
        .await
        .unwrap();
    assert!(send_event(&tx, StreamEvent::TextDelta("x".into()), &c)
        .await
        .is_err());
}
#[test]
fn wire_projection_handles_all_roles_and_tool_choices() {
    for choice in [
        ToolChoice::Auto,
        ToolChoice::None,
        ToolChoice::Required,
        ToolChoice::Named("read".into()),
    ] {
        assert!(!tool_choice_json(&choice).is_null());
    }
    let messages = vec![
        ProviderMessage::System {
            content: "system".into(),
        },
        ProviderMessage::User {
            content: "user".into(),
        },
        ProviderMessage::Assistant {
            content: Some("answer".into()),
            tool_calls: vec![rusty_bot_core::ProviderToolCall {
                id: "id".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
        },
        ProviderMessage::Tool {
            tool_call_id: "id".into(),
            content: vec![ContentPart::Text {
                text: "result".into(),
            }],
        },
    ];
    let wire = provider().wire_messages(&messages).unwrap();
    assert_eq!(
        wire.iter()
            .map(|m| m["role"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["system", "user", "assistant", "tool"]
    );
    assert_eq!(wire[2]["tool_calls"][0]["function"]["arguments"], "{}");
}
