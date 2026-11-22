//! Deterministic loopback HTTP fixtures. Never contact an external service.
use super::*;
use crate::tests::{chat, config, request, Credentials};
use rusty_bot_messages::{ConversationMessage, MessageAccumulator};
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn fixture(
    status: &str,
    body: String,
    delay_tail: bool,
) -> (String, tokio::task::JoinHandle<Value>) {
    fixture_with_extra_length(status, body, delay_tail, 0).await
}

async fn fixture_with_extra_length(
    status: &str,
    body: String,
    delay_tail: bool,
    extra_length: usize,
) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat", listener.local_addr().unwrap());
    let status = status.to_owned();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (header_end, length) = loop {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
            if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..i]);
                let len = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (i + 4, len);
            }
        };
        while bytes.len() < header_end + length {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        let request = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
        let header = format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nx-request-id: req_fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len() + extra_length
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        if delay_tail {
            let n = body.len().min(220);
            let _ = socket.write_all(&body.as_bytes()[..n]).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            let _ = socket.write_all(&body.as_bytes()[n..]).await;
        } else {
            // Deliberately split field prefixes, JSON and multibyte UTF-8.
            for chunk in body.as_bytes().chunks(7) {
                if socket.write_all(chunk).await.is_err() {
                    break;
                }
            }
        }
        request
    });
    (endpoint, task)
}
fn frames(frames: &[Value], done: bool) -> String {
    let mut body = frames
        .iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect::<String>();
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body
}
#[tokio::test]
async fn http_responses_and_anthropic_authoritative_streams() {
    for api in [Api::Responses, Api::Anthropic] {
        let values = if api == Api::Responses {
            crate::tests::responses_frames()
        } else {
            crate::tests::anthropic_frames()
        };
        let (endpoint, server) = fixture("200 OK", frames(&values, false), false).await;
        let mut cfg = config();
        cfg.endpoint = endpoint;
        let client = Client::new(cfg, api, resolver()).unwrap();
        let (tx, mut rx) = mpsc::channel(64);
        let entry = client
            .stream(
                &request(vec![ConversationMessage::user("hi")]),
                tx,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut accumulator = MessageAccumulator::default();
        while let Some(event) = rx.recv().await {
            accumulator.apply(event).unwrap();
        }
        assert_eq!(accumulator.response(), Some(&entry));
        let body = server.await.unwrap();
        if api == Api::Responses {
            assert_eq!(body["store"], false);
        } else {
            assert_eq!(body["messages"][0]["role"], "user");
        }
    }
}
fn resolver() -> Arc<Credentials> {
    Arc::new(Credentials(AtomicUsize::new(0)))
}

#[tokio::test]
async fn http_sse_stream_final_metadata_and_request() {
    let body = frames(
        &[
            chat(json!({"role":"assistant","content":"héllo"}), Value::Null),
            chat(json!({}), json!("stop")),
            json!({"id":"wire-id","model":"actual-model","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}),
        ],
        true,
    );
    let (endpoint, server) = fixture("200 OK", body, false).await;
    let mut cfg = config();
    cfg.endpoint = endpoint;
    let p =
        OpenAIProvider::with_protocol(cfg, OpenAIProtocol::ChatCompletions, resolver()).unwrap();
    let (tx, mut rx) = mpsc::channel(32);
    let entry = p
        .stream(
            &request(vec![ConversationMessage::user("hi")]),
            tx,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut accumulator = MessageAccumulator::default();
    while let Some(event) = rx.recv().await {
        accumulator.apply(event).unwrap();
    }
    assert_eq!(accumulator.response(), Some(&entry));
    assert_eq!(
        entry.generation.unwrap().model.as_deref(),
        Some("actual-model")
    );
    assert_eq!(
        server.await.unwrap()["messages"][0]["content"][0]["text"],
        "hi"
    );
}
#[tokio::test]
async fn http_error_is_sanitized_and_truncation_has_no_completed() {
    for (status, body) in [
        ("400 Bad Request", "RAW-WIRE-PRIVATE".into()),
        (
            "200 OK",
            frames(&[chat(json!({"content":"partial"}), Value::Null)], false),
        ),
    ] {
        let (endpoint, server) = fixture(status, body, false).await;
        let mut cfg = config();
        cfg.endpoint = endpoint;
        let p = DeepseekProvider::new(cfg, resolver()).unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        let error = p
            .stream(
                &request(vec![ConversationMessage::user("hi")]),
                tx,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("RAW-WIRE-PRIVATE"));
        while let Some(event) = rx.recv().await {
            assert!(!matches!(
                event.event,
                MessageStreamPayload::Completed { .. }
            ));
        }
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_while_reading_http_and_sink_timeout() {
    let body = frames(
        &[
            chat(json!({"content":"partial"}), Value::Null),
            chat(json!({}), json!("stop")),
        ],
        true,
    );
    let (endpoint, server) = fixture("200 OK", body.clone(), true).await;
    let mut cfg = config();
    cfg.endpoint = endpoint;
    let p = DeepseekProvider::new(cfg, resolver()).unwrap();
    let (tx, mut rx) = mpsc::channel(32);
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        trigger.cancel();
    });
    let error = p
        .stream(&request(vec![ConversationMessage::user("hi")]), tx, cancel)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::Cancelled);
    while let Some(event) = rx.recv().await {
        assert!(!matches!(
            event.event,
            MessageStreamPayload::Completed { .. }
        ));
    }
    cancel_task.await.unwrap();
    server.abort();
    let (endpoint, server) = fixture("200 OK", body, false).await;
    let mut cfg = config();
    cfg.endpoint = endpoint;
    cfg.timeout = Duration::from_millis(30);
    let p = DeepseekProvider::new(cfg, resolver()).unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let error = p
        .stream(
            &request(vec![ConversationMessage::user("hi")]),
            tx,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.message, "provider operation timed out");
    server.await.unwrap();
}

// Sanitized router capture: synthetic identity, model, timestamp, and token counts.
const ROUTER_CAPTURE: &str = concat!(
    "data: {\"id\":\"r\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"42\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"r\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4,\"prompt_tokens_details\":{\"cached_tokens\":2}}}\n\n"
);

#[tokio::test]
async fn router_clean_eof_requires_explicit_chat_opt_in() {
    for (policy, body, extra_length, succeeds) in [
        (ChatStreamTermination::RequireDone, ROUTER_CAPTURE, 0, false),
        (
            ChatStreamTermination::FinishReasonAndEof,
            ROUTER_CAPTURE,
            0,
            true,
        ),
        // A broken HTTP body must fail even after a finish reason was received.
        (
            ChatStreamTermination::FinishReasonAndEof,
            ROUTER_CAPTURE,
            10,
            false,
        ),
        (
            ChatStreamTermination::FinishReasonAndEof,
            ROUTER_CAPTURE.split("\n\n").next().unwrap(),
            0,
            false,
        ),
    ] {
        let (endpoint, server) =
            fixture_with_extra_length("200 OK", body.into(), false, extra_length).await;
        let mut cfg = config();
        cfg.endpoint = endpoint;
        cfg.chat_stream_termination = policy;
        let p =
            OpenAIProvider::with_protocol(cfg.clone(), OpenAIProtocol::ChatCompletions, resolver())
                .unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        let result = p
            .stream(
                &request(vec![ConversationMessage::user("hi")]),
                tx,
                CancellationToken::new(),
            )
            .await;
        let mut acc = MessageAccumulator::default();
        let mut completed = false;
        while let Some(event) = rx.recv().await {
            completed |= matches!(event.event, MessageStreamPayload::Completed { .. });
            acc.apply(event).unwrap();
        }
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(completed, succeeds);
        if let Ok(entry) = result {
            use rusty_bot_messages::{ContentBlock, FinishReason};
            assert_eq!(acc.response(), Some(&entry));
            assert_eq!(entry.message.content()[0], ContentBlock::text("42"));
            let generation = entry.generation.as_ref().unwrap();
            assert_eq!(generation.response_id.as_deref(), Some("r"));
            assert_eq!(generation.model.as_deref(), Some("m"));
            assert_eq!(generation.finish_reason, Some(FinishReason::Stop));
            assert_eq!(generation.raw_finish_reason.as_deref(), Some("stop"));
            assert_eq!(generation.usage.as_ref().unwrap().total_tokens, Some(4));
            assert_eq!(
                generation.extras["native_usage"]["prompt_tokens_details"]["cached_tokens"],
                2
            );
            let ContentBlock::Replay { state } = entry.message.content().last().unwrap() else {
                panic!("missing replay")
            };
            let replay = state.payload.clone();
            let body = request::build(Api::Chat, &cfg, &request(vec![entry.message])).unwrap();
            assert_eq!(body["messages"][0], replay);
        }
        server.await.unwrap();
    }
}

#[test]
fn eof_policy_is_chat_only() {
    let mut cfg = config();
    assert_eq!(
        cfg.chat_stream_termination,
        ChatStreamTermination::RequireDone
    );
    cfg.chat_stream_termination = ChatStreamTermination::FinishReasonAndEof;
    for api in [Api::Responses, Api::Anthropic, Api::Deepseek] {
        assert!(Client::new(cfg.clone(), api, resolver()).is_err());
    }
    assert!(Client::new(cfg, Api::Chat, resolver()).is_ok());
}

#[tokio::test]
async fn eof_opt_in_still_validates_finish_and_order() {
    let content = chat(json!({"content":"42"}), Value::Null);
    for (body, succeeds) in [
        (
            frames(&[content.clone(), chat(json!({}), json!("stop"))], true),
            true,
        ),
        (
            frames(&[content.clone(), chat(json!({}), json!("unknown"))], false),
            false,
        ),
        (
            frames(
                &[
                    content.clone(),
                    chat(json!({}), json!("stop")),
                    content.clone(),
                ],
                false,
            ),
            false,
        ),
        (
            frames(&[content, chat(json!({}), json!("tool_calls"))], false),
            false,
        ),
    ] {
        let (endpoint, server) = fixture("200 OK", body, false).await;
        let mut cfg = config();
        cfg.endpoint = endpoint;
        cfg.chat_stream_termination = ChatStreamTermination::FinishReasonAndEof;
        let p = OpenAIProvider::with_protocol(cfg, OpenAIProtocol::ChatCompletions, resolver())
            .unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        let result = p
            .stream(
                &request(vec![ConversationMessage::user("hi")]),
                tx,
                CancellationToken::new(),
            )
            .await;
        let mut completed = false;
        while let Some(event) = rx.recv().await {
            completed |= matches!(event.event, MessageStreamPayload::Completed { .. });
        }
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(completed, succeeds);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn decoded_credential_echo_is_never_completed_or_replayed() {
    // JSON escapes defeat raw-wire scanning, including across multiple SSE events.
    for body in [
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            chat(json!({"content":"PLACEHOLDER"}), Value::Null)
                .to_string()
                .replace("PLACEHOLDER", "\\u006ffline-secret")
        ),
        format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            chat(json!({"content":"PLACEHOLDER"}), Value::Null)
                .to_string()
                .replace("PLACEHOLDER", "\\u006ffline-"),
            chat(json!({"content":"PLACEHOLDER"}), Value::Null)
                .to_string()
                .replace("PLACEHOLDER", "\\u0073ecret")
        ),
        format!(
            "data: {}\n\n",
            chat(
                json!({"content":"safe", "offline-secret": "x"}),
                Value::Null
            )
            .to_string()
            .replace("offline-secret", "\\u006ffline-secret")
        ),
    ] {
        let (endpoint, server) = fixture("200 OK", body, false).await;
        let mut cfg = config();
        cfg.endpoint = endpoint;
        let client = Client::new(cfg, Api::Chat, resolver()).unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        let error = client
            .stream(
                &request(vec![ConversationMessage::user("hi")]),
                tx,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Protocol);
        assert!(!format!("{error:?}").contains("offline-secret"));
        let mut accumulator = MessageAccumulator::default();
        while let Some(event) = rx.recv().await {
            assert!(!format!("{event:?}").contains("offline-secret"));
            assert!(!matches!(
                event.event,
                MessageStreamPayload::Completed { .. }
            ));
            accumulator.apply(event).unwrap();
        }
        assert!(accumulator.response().is_none());
        assert!(!format!("{accumulator:?}").contains("offline-secret"));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn truncated_stream_preserves_validated_request_id() {
    let (endpoint, server) = fixture(
        "200 OK",
        frames(
            &[chat(
                json!({"role":"assistant","content":"partial"}),
                Value::Null,
            )],
            false,
        ),
        false,
    )
    .await;
    let mut cfg = config();
    cfg.endpoint = endpoint;
    cfg.retry.retries = 0;
    let client = Client::new(cfg, Api::Chat, resolver()).unwrap();
    let (tx, _rx) = mpsc::channel(64);
    let err = client
        .stream(
            &request(vec![ConversationMessage::user("hi")]),
            tx,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.provider_request_id.as_deref(), Some("req_fixture"));
    assert_eq!(err.status, Some(200));
    assert_eq!(err.kind, ModelErrorKind::Protocol);
    server.await.unwrap();
}
