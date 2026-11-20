use super::*;
struct Harness {
    state: StreamState,
    suffix: String,
    completed: Option<Value>,
    events: Vec<StreamEvent>,
}
impl Harness {
    fn new() -> Self {
        Self {
            state: StreamState::default(),
            suffix: String::new(),
            completed: None,
            events: vec![],
        }
    }
    async fn feed(&mut self, v: Value) -> Result<(), ModelError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let result = process_event(
            &v.to_string(),
            &crate::native_transport::test_response("unit-secret"),
            &tx,
            &CancellationToken::new(),
            &mut self.state,
            &mut self.suffix,
            &mut self.completed,
        )
        .await;
        drop(tx);
        while let Some(e) = rx.recv().await {
            self.events.push(e);
        }
        result
    }
}
#[tokio::test]
async fn text_lifecycle_reconciles_every_stream_fragment() {
    let mut h = Harness::new();
    for kind in ["response.created", "response.in_progress"] {
        h.feed(json!({"type":kind,"response":{"id":"r"}}))
            .await
            .unwrap();
    }
    let part = json!({"type":"output_text","text":"hello"});
    let item = json!({"id":"m","type":"message","role":"assistant","content":[part.clone()]});
    for v in [
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message","role":"assistant"}}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"m","part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m","delta":"hello"}),
        json!({"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"m","text":"hello"}),
        json!({"type":"response.content_part.done","output_index":0,"content_index":0,"item_id":"m","part":part}),
        json!({"type":"response.output_item.done","output_index":0,"item":item.clone()}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item.clone()]}}),
    ] {
        h.feed(v).await.unwrap();
    }
    h.state.reconcile(std::slice::from_ref(&item)).unwrap();
    let mut changed = item;
    changed["content"][0]["text"] = json!("changed");
    assert!(h.state.reconcile(&[changed]).is_err());
    assert!(h
        .feed(json!({"type":"response.created","response":{"id":"late"}}))
        .await
        .is_err());
    assert_eq!(h.events.len(), 1);
}
#[tokio::test]
async fn function_call_deltas_reconcile_exact_arguments() {
    let mut h = Harness::new();
    let item =
        json!({"id":"f","type":"function_call","call_id":"call","name":"read","arguments":"{}"});
    for v in [
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"f","type":"function_call","call_id":"call","name":"read"}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"f","delta":"{}"}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"f","arguments":"{}"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item.clone()}),
    ] {
        h.feed(v).await.unwrap();
    }
    h.state.reconcile(std::slice::from_ref(&item)).unwrap();
    assert_eq!(h.events.len(), 2);
    let mut changed = item;
    changed["arguments"] = json!("{ }");
    assert!(h.state.reconcile(&[changed]).is_err());
    assert!(h.feed(json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"f","delta":"late"})).await.is_err());
}
#[tokio::test]
async fn invalid_event_fields_and_lifecycle_are_rejected() {
    for v in [
        json!({"type":"error"}),
        json!({"type":"response.failed"}),
        json!({"type":"response.incomplete"}),
        json!({"type":"unknown"}),
        json!({"type":"response.output_text.delta","output_index":-1}),
        json!({"type":"response.completed","response":{"status":"incomplete"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"a","type":"unknown"}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"x","delta":"{}"}),
    ] {
        assert!(Harness::new().feed(v).await.is_err());
    }
    let mut h = Harness::new();
    h.feed(json!({"type":"response.created","response":{"id":"r"}}))
        .await
        .unwrap();
    assert!(h
        .feed(json!({"type":"response.created","response":{"id":"r"}}))
        .await
        .is_err());
    assert!(h
        .feed(json!({"type":"response.in_progress","response":{"id":"other"}}))
        .await
        .is_err());
}
#[test]
fn reconciliation_rejects_unknown_items_identity_changes_and_duplicate_ids() {
    let mut s = StreamState::default();
    s.item(0, "a").unwrap();
    assert!(s.item(1, "a").is_err());
    assert!(s.item(0, "b").is_err());
    assert!(s.reconcile(&[]).is_err());
    assert!(s.reconcile(&[json!({"id":"b"})]).is_err());
    s.items.get_mut(&0).unwrap().added = Some(json!({"type":"message"}));
    assert!(s
        .reconcile(&[json!({"id":"a","type":"function_call"})])
        .is_err());
    assert!(event_index(&json!({"i":4294967296u64}), "i").is_err());
}
