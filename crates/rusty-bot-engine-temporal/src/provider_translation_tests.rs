//! Opt-in Temporal integration with actual providers and offline loopback SSE fixtures.
use super::*;
use rusty_bot_core::ProviderCredential;
use rusty_bot_providers::{AnthropicProvider, OpenAIProtocol, OpenAIProvider, ProviderConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn sse_server(body: String) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/fixture", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before HTTP headers");
            bytes.extend_from_slice(&buf[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse::<usize>().unwrap())
                    })
                    .expect("Content-Length");
                break (end + 4, length);
            }
        };
        while bytes.len() < start + length {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before HTTP body");
            bytes.extend_from_slice(&buf[..n]);
        }
        let request = serde_json::from_slice(&bytes[start..start + length]).unwrap();
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        request
    });
    (endpoint, task)
}

fn frames(values: &[Value], done: bool) -> String {
    let mut body = values
        .iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect::<String>();
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body
}

fn options(task_queue: String) -> EngineOptions {
    EngineOptions {
        address: std::env::var("TEMPORAL_ADDRESS").unwrap(),
        namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into()),
        task_queue,
        idle_timeout: Duration::from_secs(30),
    }
}

#[tokio::test]
#[ignore = "requires existing TEMPORAL_ADDRESS; real providers use loopback fixtures only"]
async fn actual_provider_translation_preserves_history_across_temporal_sessions() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let source_server = test_support::TestServer::connect("translation-source").await;
        let target_server = test_support::TestServer::connect("translation-target").await;
        assert_ne!(source_server.task_queue, target_server.task_queue);
        let source_frames = vec![
            json!({"type":"message_start","message":{"id":"source-response","model":"source-model","role":"assistant","content":[],"usage":{"input_tokens":9,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private-plan"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque-signature"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"cedar"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}),
            json!({"type":"message_stop"}),
        ];
        let (source_endpoint, source_http) = sse_server(frames(&source_frames, false)).await;
        let source_provider = Arc::new(AnthropicProvider::with_api_key(
            ProviderConfig::new(source_endpoint, "source-model"), ProviderCredential::new("fake-source-key"),
        ).unwrap());
        let source_storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let source_engine = Engine::connect(options(source_server.task_queue), source_provider, source_storage.clone()).await.unwrap();
        let source = source_engine.start_session(source_server.workflow_id, "source-model".into()).await.unwrap();
        source_engine.run_until(async {
            assert_eq!(source.send_message("Remember the tree".into()).await.unwrap(), TurnResult::Completed("cedar".into()));
            source.close().await.unwrap();
        }).await.unwrap();
        let source_request = source_http.await.unwrap();
        assert_eq!(source_request["max_tokens"], 4096);
        let history = source.history().await.unwrap();
        assert_eq!(history.entries.len(), 2);
        assert_eq!(history.entries[0].message, ConversationMessage::user("Remember the tree"));
        let source_response = &history.entries[1];
        assert!(source_response.message.content().iter().any(|block| matches!(block, ContentBlock::Reasoning { text } if text == "private-plan")));
        assert!(source_response.message.content().iter().any(|block| matches!(block, ContentBlock::Replay { state } if state.provider == "anthropic" && state.format == "messages.content" && state.required_for_replay && state.payload.to_string().contains("opaque-signature"))));
        assert_eq!(source_response.generation.as_ref().unwrap().finish_reason, Some(FinishReason::Stop));
        assert!(source_response.generation.as_ref().unwrap().usage.is_some());
        assert_eq!(source_storage.read(source.id()).await.unwrap().turns[0].response.as_ref(), Some(source_response));

        let target_frames = vec![
            json!({"id":"target-response","model":"target-model","choices":[{"index":0,"delta":{"role":"assistant","content":"cedar"},"finish_reason":null}]}),
            json!({"id":"target-response","model":"target-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
        ];
        let (target_endpoint, target_http) = sse_server(frames(&target_frames, true)).await;
        let target_provider = Arc::new(OpenAIProvider::with_protocol_and_api_key(
            ProviderConfig::new(target_endpoint, "target-model"), OpenAIProtocol::ChatCompletions, ProviderCredential::new("fake-target-key"),
        ).unwrap());
        let target_storage = Arc::new(rusty_bot_session::MemorySessionStorage::default());
        let target_engine = Engine::connect(options(target_server.task_queue), target_provider, target_storage.clone()).await.unwrap();
        let imported = history.clone().for_session_import();
        for (index, (original, entry)) in history.entries.iter().zip(&imported.entries).enumerate() {
            assert_eq!(entry.id, format!("import:{index}"));
            let mut expected = original.clone();
            expected.id = format!("import:{index}");
            assert_eq!(entry, &expected);
        }
        let target = target_engine.start_session_with_history(target_server.workflow_id, "target-model".into(), imported.clone()).await.unwrap();
        target_engine.run_until(async {
            assert_eq!(target.send_message("remember?".into()).await.unwrap(), TurnResult::Completed("cedar".into()));
            target.close().await.unwrap();
        }).await.unwrap();
        let request = target_http.await.unwrap();
        assert_eq!(request["messages"], json!([
            {"role":"user","content":[{"type":"text","text":"Remember the tree"}]},
            {"role":"assistant","content":"cedar"},
            {"role":"user","content":[{"type":"text","text":"remember?"}]}
        ]));
        let wire = request.to_string();
        for forbidden in ["opaque-signature", "private-plan", "signature", "reasoning", "Replay", "thinking"] {
            assert!(!wire.contains(forbidden), "unexpected native source content: {forbidden}");
        }
        let target_history = target.history().await.unwrap();
        assert_eq!(target_history.entries.len(), 4);
        assert_eq!(&target_history.entries[..2], &imported.entries[..]);
        assert_eq!(target_history.entries[2].message, ConversationMessage::user("remember?"));
        let response = &target_history.entries[3];
        assert_eq!(rusty_bot_session::visible_text(&response.message), "cedar");
        assert_eq!(response.generation.as_ref().unwrap().finish_reason, Some(FinishReason::Stop));
        assert!(response.message.content().iter().any(|block| matches!(block, ContentBlock::Replay { state } if state.provider == "openai" && state.format == "chat.message" && state.required_for_replay)));
        let stored = target_storage.read(target.id()).await.unwrap();
        assert_eq!(stored.config.initial_history, imported);
        assert_eq!(stored.turns.len(), 1);
        assert_eq!(stored.turns[0].response.as_ref(), Some(response));
    }).await.expect("integration test timed out");
}
