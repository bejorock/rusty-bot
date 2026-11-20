use super::*;
use rusty_bot_core::ToolSpec;
struct Harness {
    raw: Vec<Value>,
    ids: Vec<Value>,
    usage: Usage,
    stop: Option<StopReason>,
    suffix: String,
    events: Vec<StreamEvent>,
}
impl Harness {
    fn new() -> Self {
        Self {
            raw: vec![],
            ids: vec![],
            usage: Usage::default(),
            stop: None,
            suffix: String::new(),
            events: vec![],
        }
    }
    async fn feed(&mut self, v: Value) -> Result<(), ModelError> {
        let request = HistoryRequest {
            model: "m".into(),
            messages: vec![Message::user_text("hi")],
            tools: vec![ToolSpec {
                name: "read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }],
            tool_choice: ToolChoice::Auto,
            temperature: None,
            max_tokens: None,
            metadata: json!({}),
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let result = process_event(
            &v.to_string(),
            &crate::native_transport::test_response("unit-secret"),
            &request,
            &tx,
            &CancellationToken::new(),
            &mut self.raw,
            &mut self.ids,
            &mut self.usage,
            &mut self.stop,
            &mut self.suffix,
        )
        .await;
        drop(tx);
        while let Some(e) = rx.recv().await {
            self.events.push(e)
        }
        result
    }
}
#[tokio::test]
async fn text_thought_function_and_usage_are_projected_in_order() {
    let mut h = Harness::new();
    h.feed(json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hello"},{"text":"thinking","thought":true},{"functionCall":{"name":"read","args":{"path":"x"}},"thoughtSignature":"signed"}]}}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":5,"cachedContentTokenCount":1}})).await.unwrap();
    assert_eq!(h.raw.len(), 3);
    assert_eq!(h.ids[2], "gemini_1_2");
    assert_eq!(h.events.len(), 4);
    assert_eq!(h.usage.input_tokens, 3);
    assert_eq!(h.usage.output_tokens, 5);
    assert_eq!(h.usage.cache_read_tokens, Some(1));
    require_signature(&h.raw).unwrap();
    h.feed(json!({"candidates":[{"finishReason":"STOP"}]}))
        .await
        .unwrap();
    assert_eq!(h.stop, Some(StopReason::Stop));
    assert!(h
        .feed(json!({"candidates":[{"content":{"parts":[{"text":"late"}]}}]}))
        .await
        .is_err());
}
#[tokio::test]
async fn explicit_call_id_and_length_finish_are_preserved() {
    let mut h = Harness::new();
    h.feed(json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"explicit","name":"read","args":{}}}]},"finishReason":"MAX_TOKENS"}]})).await.unwrap();
    assert_eq!(h.ids[0], "explicit");
    assert_eq!(h.stop, Some(StopReason::Length));
    assert!(require_signature(&h.raw).is_err());
}
#[tokio::test]
async fn malformed_metadata_candidates_calls_and_stop_are_refused() {
    for value in [
        json!({"usageMetadata":[]}),
        json!({"usageMetadata":{"promptTokenCount":-1}}),
        json!({"candidates":[]}),
        json!({"candidates":[{},{}]}),
        json!({"candidates":[{"index":1}]}),
        json!({"candidates":[{"content":{"role":"user","parts":[]}}]}),
        json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"unknown","args":{}}}]}}]}),
        json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"","name":"read","args":{}}}]}}]}),
        json!({"candidates":[{"finishReason":"SAFETY"}]}),
        json!({"promptFeedback":{"blockReason":"SAFETY"}}),
        json!({"secret":"unit-secret"}),
    ] {
        assert!(
            Harness::new().feed(value.clone()).await.is_err(),
            "accepted {value}"
        );
    }
}
