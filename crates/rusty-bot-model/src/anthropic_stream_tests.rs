use super::*;
async fn feed(acc: &mut Accumulator, event: Value) -> Result<(), ModelError> {
    let response = crate::native_transport::test_response("unit-secret");
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    drop(rx);
    acc.event(
        &event.to_string(),
        &response,
        &tx,
        &CancellationToken::new(),
    )
    .await
}
// Keep the receiver alive while collecting each event so transport backpressure is exercised.
async fn step(acc: &mut Accumulator, event: Value) -> Result<Vec<StreamEvent>, ModelError> {
    let response = crate::native_transport::test_response("unit-secret");
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    acc.event(
        &event.to_string(),
        &response,
        &tx,
        &CancellationToken::new(),
    )
    .await?;
    drop(tx);
    let mut result = vec![];
    while let Some(e) = rx.recv().await {
        result.push(e)
    }
    Ok(result)
}
fn start() -> Value {
    json!({"type":"message_start","message":{"id":"m","type":"message","role":"assistant","content":[],"usage":{"input_tokens":3,"cache_read_input_tokens":1,"cache_creation_input_tokens":2}}})
}
#[tokio::test]
async fn text_blocks_usage_and_terminal_lifecycle() {
    let mut a = Accumulator::new();
    step(&mut a, start()).await.unwrap();
    step(&mut a, json!({"type":"ping"})).await.unwrap();
    step(
        &mut a,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    )
    .await
    .unwrap();
    let events=step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}})).await.unwrap();
    assert!(matches!(&events[0],StreamEvent::TextDelta(s) if s=="hello"));
    step(&mut a, json!({"type":"content_block_stop","index":0}))
        .await
        .unwrap();
    step(&mut a,json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}})).await.unwrap();
    step(&mut a, json!({"type":"message_stop"})).await.unwrap();
    assert!(a.stopped);
    assert_eq!(a.usage.input_tokens, 3);
    assert_eq!(a.usage.output_tokens, 5);
    assert_eq!(a.blocks[0]["text"], "hello");
    assert!(step(&mut a, json!({"type":"ping"})).await.is_err());
}
#[tokio::test]
async fn tool_arguments_accumulate_and_project_without_reformatting() {
    let mut a = Accumulator::new();
    step(&mut a, start()).await.unwrap();
    step(&mut a,json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"read","input":{}}})).await.unwrap();
    for piece in ["{\"path\":", "\"猫\"}"] {
        step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":piece}})).await.unwrap();
    }
    step(&mut a, json!({"type":"content_block_stop","index":0}))
        .await
        .unwrap();
    assert_eq!(a.blocks[0]["input"], json!({"path":"猫"}));
    assert!(
        matches!(&parts_from_blocks(&a.blocks,&a.args).unwrap()[0],HistoryPart::ToolCall{arguments_json,..} if arguments_json=="{\"path\":\"猫\"}")
    );
    step(&mut a,json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})).await.unwrap();
    assert_eq!(a.stop, Some(StopReason::ToolUse));
}
#[tokio::test]
async fn signed_thinking_and_redacted_blocks_remain_opaque() {
    let mut a = Accumulator::new();
    step(&mut a, start()).await.unwrap();
    step(&mut a,json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}})).await.unwrap();
    step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"thought"}})).await.unwrap();
    step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed"}})).await.unwrap();
    assert!(step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"late"}})).await.is_err());
    step(&mut a, json!({"type":"content_block_stop","index":0}))
        .await
        .unwrap();
    step(&mut a,json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"opaque"}})).await.unwrap();
    step(&mut a, json!({"type":"content_block_stop","index":1}))
        .await
        .unwrap();
    assert_eq!(parts_from_blocks(&a.blocks, &a.args).unwrap().len(), 2);
}
#[tokio::test]
async fn invalid_orders_stop_reasons_and_argument_shapes_fail_closed() {
    for event in [
        json!({"type":"message_stop"}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_delta","index":0,"delta":{}}),
        json!({"type":"message_delta","delta":{}}),
        json!({"type":"unknown"}),
        json!({"type":"error"}),
    ] {
        assert!(step(&mut Accumulator::new(), event).await.is_err());
    }
    let mut a = Accumulator::new();
    step(&mut a, start()).await.unwrap();
    assert!(step(&mut a, start()).await.is_err());
    assert!(step(
        &mut a,
        json!({"type":"message_delta","delta":{"stop_reason":"unknown"}})
    )
    .await
    .is_err());
    assert!(
        step(&mut a, json!({"type":"message_delta","delta":{"extra":1}}))
            .await
            .is_err()
    );
    step(&mut a,json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"id","name":"read","input":{}}})).await.unwrap();
    step(&mut a,json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"[]"}})).await.unwrap();
    assert!(step(&mut a, json!({"type":"content_block_stop","index":0}))
        .await
        .is_err());
}
#[tokio::test]
async fn credential_echo_is_refused_but_closed_observer_does_not_abort_generation() {
    let mut a = Accumulator::new();
    assert!(step(&mut a, json!({"type":"ping","secret":"unit-secret"}))
        .await
        .is_err());
    step(&mut a, start()).await.unwrap();
    step(
        &mut a,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    )
    .await
    .unwrap();
    assert!(feed(
        &mut a,
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}})
    )
    .await
    .is_ok());
}
