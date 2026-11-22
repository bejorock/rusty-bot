use super::*;
use crate::{CredentialRequest, ProviderCredential};
use rusty_bot_messages::{
    ContentBlock as B, ConversationMessage as M, FinishReason, MediaSource, MessageAccumulator,
    ReplayState, ToolInvocation,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct Credentials(pub(crate) AtomicUsize);
#[async_trait]
impl CredentialResolver for Credentials {
    async fn resolve(
        &self,
        _: &CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ProviderCredential::new("offline-secret")))
    }
}
pub(crate) fn config() -> ProviderConfig {
    ProviderConfig::new("http://127.0.0.1:1/unused", "requested-model")
}
pub(crate) fn request(messages: Vec<M>) -> GenerationRequest {
    GenerationRequest {
        message_id: "answer".into(),
        history: ChatHistory {
            schema_version: 1,
            entries: messages
                .into_iter()
                .enumerate()
                .map(|(i, m)| HistoryEntry::completed(format!("h{i}"), m))
                .collect(),
        },
        tools: vec![],
    }
}
fn build(api: Api, req: &GenerationRequest) -> Value {
    request::build(api, &config(), req).unwrap()
}
fn decode(api: Api, frames: Vec<Value>, done: bool) -> (HistoryEntry, Vec<MessageStreamEvent>) {
    let mut decoder = wire::Decoder::new(api, "requested-model", "answer");
    let mut events = vec![MessageStreamEvent {
        message_id: "answer".into(),
        sequence: 1,
        event: MessageStreamPayload::Start,
    }];
    for frame in frames {
        for event in decoder.push(&frame.to_string()).unwrap() {
            events.push(MessageStreamEvent {
                message_id: "answer".into(),
                sequence: events.len() as u64 + 1,
                event,
            });
        }
    }
    if done {
        for event in decoder.push("[DONE]").unwrap() {
            events.push(MessageStreamEvent {
                message_id: "answer".into(),
                sequence: events.len() as u64 + 1,
                event,
            });
        }
    }
    let entry = decoder.finish().unwrap();
    events.push(MessageStreamEvent {
        message_id: "answer".into(),
        sequence: events.len() as u64 + 1,
        event: MessageStreamPayload::Completed {
            response: Box::new(entry.clone()),
        },
    });
    let mut accumulator = MessageAccumulator::default();
    for event in &events {
        accumulator.apply(event.clone()).unwrap();
    }
    assert_eq!(accumulator.response(), Some(&entry));
    (entry, events)
}
pub(crate) fn chat(delta: Value, finish: Value) -> Value {
    json!({"id":"wire-id", "model":"actual-model", "choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
}
fn call() -> ToolInvocation {
    ToolInvocation {
        id: "call1".into(),
        name: "lookup".into(),
        arguments: json!({"x":1}),
    }
}

#[test]
fn responses_default_and_explicit_chat() {
    let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
    let req = request(vec![
        M::system("rules"),
        M::Developer {
            content: vec![B::text("dev")],
        },
        M::user("hello"),
    ]);
    let p = OpenAIProvider::new(config(), credentials.clone()).unwrap();
    let b = p.build_request(&req).unwrap();
    assert_eq!(b["store"], false);
    assert_eq!(b["include"][0], "reasoning.encrypted_content");
    assert_eq!(b["input"][1]["role"], "developer");
    let p = OpenAIProvider::with_protocol(
        config(),
        OpenAIProtocol::ChatCompletions,
        credentials.clone(),
    )
    .unwrap();
    assert!(p.build_request(&req).unwrap().get("messages").is_some());
    assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
    assert!(!format!("{p:?}").contains("127.0.0.1"));
}
#[test]
fn declarations_and_tool_results() {
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let mut req = request(vec![
            M::user("go"),
            M::Assistant {
                content: vec![B::ToolCall { call: call() }],
            },
            M::Tool {
                call_id: "call1".into(),
                name: "lookup".into(),
                content: vec![B::text("result")],
                is_error: false,
            },
        ]);
        req.tools.push(ToolSpec {
            name: "lookup".into(),
            description: "lookup value".into(),
            parameters: json!({"type":"object"}),
        });
        let body = build(api, &req);
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        let text = body.to_string();
        assert!(text.contains("call1"));
        assert!(text.contains("result"));
        if api == Api::Deepseek {
            assert!(
                body.get("thinking").is_none(),
                "default must not override provider thinking"
            );
        }
    }
}
#[test]
fn refuses_roles_blocks_media_and_mismatched_replay() {
    for api in [Api::Anthropic, Api::Deepseek] {
        assert!(request::build(
            api,
            &config(),
            &request(vec![M::Developer {
                content: vec![B::text("dev")]
            }])
        )
        .is_err());
    }
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        for block in [
            B::Json { value: json!({}) },
            B::Reasoning {
                text: "unbacked".into(),
            },
            B::Extension {
                namespace: "unknown".into(),
                kind: "x".into(),
                value: json!(0),
            },
            B::Annotated {
                id: Some("id".into()),
                block: Box::new(B::text("x")),
                annotations: vec![],
                replay: vec![],
            },
            B::Replay {
                state: ReplayState {
                    provider: "other".into(),
                    model: None,
                    format: "x".into(),
                    version: 1,
                    payload: json!([]),
                    required_for_replay: false,
                },
            },
        ] {
            assert!(request::build(
                api,
                &config(),
                &request(vec![M::Assistant {
                    content: vec![block]
                }])
            )
            .is_err());
        }
        for source in [
            MediaSource::Url {
                mime_type: "image/png".into(),
                url: "http://invalid.test/a".into(),
            },
            MediaSource::Reference {
                mime_type: "image/png".into(),
                key: "key".into(),
            },
            MediaSource::Inline {
                mime_type: "image/png".into(),
                base64: "garbage".into(),
            },
        ] {
            assert!(request::build(
                api,
                &config(),
                &request(vec![M::User {
                    content: vec![B::Image { source }]
                }])
            )
            .is_err());
        }
    }
}
#[test]
fn valid_static_inline_image() {
    use base64::Engine;
    let image = image::DynamicImage::new_rgb8(1, 1);
    let mut bytes = std::io::Cursor::new(Vec::new());
    image.write_to(&mut bytes, image::ImageFormat::Png).unwrap();
    let req = request(vec![M::User {
        content: vec![
            B::text("describe"),
            B::Image {
                source: MediaSource::Inline {
                    mime_type: "image/png".into(),
                    base64: base64::engine::general_purpose::STANDARD.encode(bytes.into_inner()),
                },
            },
        ],
    }]);
    // All three providers now expose vision-capable models. Model capability
    // remains a caller choice; do not reject DeepSeek at the protocol boundary.
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let body = request::build(api, &config(), &req).unwrap();
        if api == Api::Deepseek {
            assert!(body["messages"][0]["content"][1]["image_url"]["url"]
                .as_str()
                .unwrap()
                .starts_with("data:image/png;base64,"));
        }
    }
}
#[test]
fn deepseek_reasoning_survives_tool_turn_and_exact_replay() {
    let (entry, _) = decode(
        Api::Deepseek,
        vec![
            chat(
                json!({"role":"assistant","reasoning_content":"think"}),
                Value::Null,
            ),
            chat(json!({"content":"checking"}), Value::Null),
            chat(
                json!({"tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"lookup","arguments":"{\"x\":"}}]}),
                Value::Null,
            ),
            chat(
                json!({"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}),
                json!("tool_calls"),
            ),
            json!({"id":"wire-id","model":"actual-model","choices":[],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12,"completion_tokens_details":{"reasoning_tokens":2},"prompt_cache_hit_tokens":3}}),
        ],
        true,
    );
    let blocks = entry.message.content();
    assert!(matches!(blocks[0], B::Reasoning { .. }));
    assert!(matches!(blocks[1], B::Text { .. }));
    assert!(matches!(blocks[2], B::ToolCall { .. }));
    let meta = entry.generation.as_ref().unwrap();
    assert_eq!(meta.model.as_deref(), Some("actual-model"));
    assert_eq!(meta.finish_reason, Some(FinishReason::ToolCalls));
    assert_eq!(
        meta.usage.as_ref().unwrap().output_details["reasoning_tokens"],
        2
    );
    let mut req = request(vec![
        M::user("go"),
        entry.message.clone(),
        M::Tool {
            call_id: "call1".into(),
            name: "lookup".into(),
            content: vec![B::text("done")],
            is_error: false,
        },
    ]);
    assert_eq!(
        build(Api::Deepseek, &req)["messages"][1]["reasoning_content"],
        "think"
    );
    let mut thinking = config();
    thinking.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: None,
    };
    assert_eq!(
        request::build(Api::Deepseek, &thinking, &req).unwrap()["messages"][1]["reasoning_content"],
        "think"
    );
    // Incompatible native state now permits a lossy visible-content continuation.
    let portable = request::build(Api::Chat, &config(), &req).unwrap();
    assert_eq!(portable["messages"][1]["content"], "checking");
    assert_eq!(portable["messages"][1]["tool_calls"][0]["id"], "call1");
    assert!(portable["messages"][1].get("reasoning_content").is_none());
    let mut cfg = config();
    cfg.model = "another".into();
    assert!(request::build(Api::Deepseek, &cfg, &req).is_err());
    cfg.reasoning = Reasoning::Disabled;
    let portable = request::build(Api::Deepseek, &cfg, &req).unwrap();
    assert!(portable["messages"][1].get("reasoning_content").is_none());
    if let M::Assistant { content } = &mut req.history.entries[1].message {
        content[0] = B::Reasoning {
            text: "edited".into(),
        };
    }
    assert!(request::build(Api::Deepseek, &config(), &req).is_err());
}
pub(crate) fn anthropic_frames() -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"a1","model":"actual-model","role":"assistant","content":[],"usage":{"input_tokens":9,"output_tokens":0,"cache_read_input_tokens":2}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"plan"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque-signature"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"opaque-data"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"answer"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}),
        json!({"type":"message_stop"}),
    ]
}
#[test]
fn anthropic_opaque_thinking_redacted_and_usage() {
    let (entry, _) = decode(Api::Anthropic, anthropic_frames(), false);
    assert!(
        matches!(&entry.message.content()[1],B::Extension {kind,..} if kind=="redacted_thinking")
    );
    let body = build(Api::Anthropic, &request(vec![entry.message]));
    assert_eq!(
        body["messages"][0]["content"][0]["signature"],
        "opaque-signature"
    );
    assert_eq!(body["messages"][0]["content"][1]["data"], "opaque-data");
    let u = entry.generation.unwrap().usage.unwrap();
    // Missing cache-write count means total input is unknown, not just cold input.
    assert_eq!(u.input_tokens, None);
    assert_eq!(u.input_details.get("cold_input_tokens"), Some(&9));
    assert_eq!(u.output_tokens, Some(5));
    assert_eq!(u.input_details["cache_read_input_tokens"], 2);
}
pub(crate) fn responses_frames() -> Vec<Value> {
    let items = vec![
        json!({"id":"r1","type":"reasoning","summary":[{"type":"summary_text","text":"plan"}],"encrypted_content":"encrypted-opaque"}),
        json!({"id":"m1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer","annotations":[]}]}),
        json!({"id":"f1","type":"function_call","status":"completed","call_id":"call1","name":"lookup","arguments":"{\"x\":1}"}),
    ];
    let mut frames =
        vec![json!({"type":"response.created","response":{"id":"resp1","model":"actual-model"}})];
    for (i, item) in items.iter().enumerate() {
        frames.push(json!({"type":"response.output_item.added","output_index":i,"item":{"id":item["id"],"type":item["type"]}}));
        if i < 2 {
            frames.push(json!({"type":if i==0 {"response.reasoning_summary_text.delta"} else {"response.output_text.delta"},"output_index":i,"item_id":item["id"],"delta":if i==0 {"plan"} else {"answer"}}));
        }
        frames.push(json!({"type":"response.output_item.done","output_index":i,"item":item}));
    }
    frames.push(json!({"type":"response.completed","response":{"id":"resp1","model":"actual-model","status":"completed","output":items,"usage":{"input_tokens":7,"output_tokens":8,"total_tokens":15,"output_tokens_details":{"reasoning_tokens":3}}}}));
    frames
}
#[test]
fn responses_authoritative_order_and_encrypted_replay() {
    let (entry, _) = decode(Api::Responses, responses_frames(), false);
    assert!(matches!(entry.message.content()[0], B::Reasoning { .. }));
    assert!(matches!(entry.message.content()[1], B::Text { .. }));
    assert!(matches!(entry.message.content()[2], B::ToolCall { .. }));
    let req = request(vec![
        entry.message,
        M::Tool {
            call_id: "call1".into(),
            name: "lookup".into(),
            content: vec![B::text("done")],
            is_error: false,
        },
    ]);
    let body = build(Api::Responses, &req);
    assert_eq!(body["input"][0]["encrypted_content"], "encrypted-opaque");
    assert_eq!(body["store"], false);
}
#[test]
fn malformed_calls_are_nonexecutable_and_not_replayed() {
    let (entry, _) = decode(
        Api::Chat,
        vec![chat(
            json!({"tool_calls":[{"index":0,"id":"bad","type":"function","function":{"name":"lookup","arguments":"{"}}]}),
            json!("tool_calls"),
        )],
        true,
    );
    assert!(matches!(
        entry.message.content()[0],
        B::InvalidToolCall { .. }
    ));
    assert!(request::build(Api::Chat, &config(), &request(vec![entry.message])).is_err());
}
#[test]
fn terminal_and_order_validation() {
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    d.push(&chat(json!({"content":"partial"}), Value::Null).to_string())
        .unwrap();
    assert!(d.finish().is_err());
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    assert!(d.push("[DONE]").is_err());
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    d.push(&chat(json!({}), json!("stop")).to_string()).unwrap();
    d.push("[DONE]").unwrap();
    assert!(d.push("[DONE]").is_err());
    let mut frames = responses_frames();
    frames.remove(2);
    let mut d = wire::Decoder::new(Api::Responses, "requested-model", "answer"); // omit a delta: final snapshot still authoritative
    for frame in frames {
        d.push(&frame.to_string()).unwrap();
    }
    assert!(d.finish().is_ok());
    let mut frames = responses_frames();
    frames.retain(|v| v["type"] != "response.output_item.done");
    let mut d = wire::Decoder::new(Api::Responses, "requested-model", "answer");
    assert!(frames.into_iter().any(|v| d.push(&v.to_string()).is_err()));
    let mut frames = anthropic_frames();
    frames[3]["delta"]["type"] = json!("thinking_delta");
    frames[3]["delta"]["thinking"] = json!("more");
    frames.remove(3); // now thinking lacks signature
    let mut d = wire::Decoder::new(Api::Anthropic, "requested-model", "answer");
    assert!(frames.into_iter().any(|v| d.push(&v.to_string()).is_err()));
}
#[test]
fn sse_arbitrary_boundaries_utf8_crlf_and_limits() {
    let input = "event: message\r\ndata: {\"text\":\"héllo\"}\r\n\r\ndata: [DONE]".as_bytes();
    for split in 0..=input.len() {
        let mut s = sse::Sse::default();
        let mut events = s.push(&input[..split]).unwrap();
        events.extend(s.push(&input[split..]).unwrap());
        events.extend(s.finish().unwrap());
        assert_eq!(events, vec!["{\"text\":\"héllo\"}", "[DONE]"]);
    }
    let mut s = sse::Sse::default();
    assert!(s.push(b"data: \xff\n").is_err());
    let mut s = sse::Sse::default();
    assert!(s.push(&vec![b'x'; 4 * 1024 * 1024 + 1]).is_err());
}
#[tokio::test]
async fn refusal_before_credentials_and_precancel() {
    let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
    let p = DeepseekProvider::new(config(), credentials.clone()).unwrap();
    let (tx, _rx) = mpsc::channel(8);
    assert!(p
        .stream(
            &request(vec![M::Developer {
                content: vec![B::text("dev")]
            }]),
            tx,
            CancellationToken::new()
        )
        .await
        .is_err());
    assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (tx, _rx) = mpsc::channel(8);
    let error = p
        .stream(&request(vec![M::user("hello")]), tx, cancel)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::Cancelled);
    assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
}
struct Hanging;
#[async_trait]
impl CredentialResolver for Hanging {
    async fn resolve(
        &self,
        _: &CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn timeout_and_cancel_during_resolution() {
    let mut cfg = config();
    cfg.timeout = Duration::from_millis(10);
    let p = OpenAIProvider::new(cfg, Arc::new(Hanging)).unwrap();
    let (tx, _rx) = mpsc::channel(8);
    let e = p
        .stream(&request(vec![M::user("hi")]), tx, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.message, "provider operation timed out");
    let p = OpenAIProvider::new(config(), Arc::new(Hanging)).unwrap();
    let (tx, _rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let child = cancel.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        child.cancel();
    });
    assert_eq!(
        p.stream(&request(vec![M::user("hi")]), tx, cancel)
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Cancelled
    );
    task.await.unwrap();
}

#[test]
fn deadline_and_cancellation_cover_queued_validation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
        let mut cfg = config();
        cfg.timeout = Duration::from_millis(20);
        let provider = DeepseekProvider::new(cfg, credentials.clone()).unwrap();
        let req = request(vec![M::user("hello")]);
        let (tx, _rx) = mpsc::channel(8);
        let error = provider
            .stream(&req, tx, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Network);
        assert_eq!(error.message, "provider operation timed out");
        let cancel = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(8);
        let stream = provider.stream(&req, tx, cancel.clone());
        tokio::pin!(stream);
        tokio::select! {
            result = &mut stream => panic!("stream finished before cancellation: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
        cancel.cancel();
        assert_eq!(stream.await.unwrap_err().kind, ModelErrorKind::Cancelled);
        assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
    });
}
