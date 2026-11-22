use super::*;
use crate::tests::{request, Credentials};
use rusty_bot_messages::ConversationMessage;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn resolver() -> Arc<dyn CredentialResolver> {
    Arc::new(Credentials(AtomicUsize::new(0)))
}
fn items() -> Vec<BatchRequest> {
    vec![BatchRequest {
        custom_id: "one".into(),
        request: request(vec![ConversationMessage::user("hello")]),
    }]
}
fn response() -> Value {
    json!({"id":"r1","model":"actual","status":"completed","output":[{"id":"m1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}],"usage":{"input_tokens":3,"output_tokens":1}})
}
fn anthropic_response() -> Value {
    json!({"id":"r1","type":"message","model":"actual","role":"assistant","content":[{"type":"text","text":"hello","citations":null}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}})
}
fn openai_job() -> Value {
    json!({"id":"job_1","status":"completed","request_counts":{"total":1,"completed":1,"failed":0},"output_file_id":"file_out","error_file_id":null,"results_url":"https://evil.invalid/steal"})
}
fn anth_job() -> Value {
    json!({"id":"job_1","processing_status":"ended","request_counts":{"processing":0,"succeeded":1,"errored":0,"canceled":0,"expired":0},"results_url":"https://evil.invalid/steal"})
}

async fn server(
    replies: Vec<(String, String)>,
) -> (String, tokio::task::JoinHandle<Vec<(String, Vec<u8>)>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/proxy/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for (expected, reply) in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let (end, len) = loop {
                let mut b = [0; 4096];
                let n = socket.read(&mut b).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&b[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..i]);
                    let len = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (i + 4, len);
                }
            };
            while bytes.len() < end + len {
                let mut b = [0; 4096];
                let n = socket.read(&mut b).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&b[..n]);
            }
            let head = String::from_utf8_lossy(&bytes[..end]).to_string();
            assert_eq!(
                head.lines().next().unwrap().split(" HTTP/").next().unwrap(),
                expected
            );
            requests.push((head, bytes[end..end + len].to_vec()));
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                reply.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
        requests
    });
    (base, task)
}
#[test]
fn offline_translation_and_replay_validation() {
    let o = OpenAIProvider::new(
        ProviderConfig::new("http://127.0.0.1:1/proxy/v1/responses", "requested-model"),
        resolver(),
    )
    .unwrap();
    let body = o.build_submission(&items()).unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["url"], "/v1/responses");
    assert!(body["body"].get("stream").is_none());
    let entry = o.decode_response("answer", &response()).unwrap();
    let history = ChatHistory {
        schema_version: 1,
        entries: vec![entry],
    };
    let next = GenerationRequest {
        message_id: "next".into(),
        history,
        tools: vec![],
    };
    assert!(o.build_request(&next).is_ok());
    let mut rich = response();
    rich["output"][0]["content"][0]["annotations"] = json!([{"type":"citation"}]);
    assert!(o.decode_response("answer", &rich).is_err());
    let mut duplicate = items();
    duplicate.extend(items());
    assert!(o.build_submission(&duplicate).is_err());
    duplicate[1].custom_id = "../escape".into();
    assert!(o.build_submission(&duplicate).is_err());
    let d = DeepseekProvider::new(
        ProviderConfig::new("http://127.0.0.1:1/v1/chat/completions", "m"),
        resolver(),
    )
    .unwrap();
    assert_eq!(
        d.build_submission(&items()).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
}
#[tokio::test]
async fn openai_native_lifecycle() {
    let result =
        json!({"custom_id":"one","response":{"status_code":200,"body":response()},"error":null})
            .to_string()
            + "\n";
    let (base, server) = server(vec![
        (
            "POST /proxy/v1/files".into(),
            json!({"id":"file_in"}).to_string(),
        ),
        ("POST /proxy/v1/batches".into(), openai_job().to_string()),
        (
            "GET /proxy/v1/batches/job_1".into(),
            openai_job().to_string(),
        ),
        (
            "GET /proxy/v1/batches/job_1".into(),
            openai_job().to_string(),
        ),
        ("GET /proxy/v1/files/file_out/content".into(), result),
        (
            "POST /proxy/v1/batches/job_1/cancel".into(),
            openai_job().to_string(),
        ),
    ])
    .await;
    let provider = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let p: &dyn BatchProvider = &provider;
    let t: &dyn BatchTranslator = &provider;
    assert!(!t.build_submission(&items()).unwrap().is_empty());
    let h = p.submit(&items(), CancellationToken::new()).await.unwrap();
    let h: BatchHandle = serde_json::from_str(&serde_json::to_string(&h).unwrap()).unwrap();
    assert_eq!(
        p.status(&h, CancellationToken::new()).await.unwrap().status,
        BatchStatus::Completed
    );
    let out = p.results(&h, CancellationToken::new()).await.unwrap();
    match &out.items[0].outcome {
        BatchOutcome::Success(entry) => {
            assert_eq!(**entry, t.decode_response("answer", &response()).unwrap());
        }
        _ => panic!("expected successful batch result"),
    }
    p.cancel(&h, CancellationToken::new()).await.unwrap();
    let reqs = server.await.unwrap();
    let multipart = String::from_utf8_lossy(&reqs[0].1);
    assert!(multipart.contains("name=\"purpose\"\r\n\r\nbatch"));
    assert!(multipart.contains("name=\"file\""));
    let create: Value = serde_json::from_slice(&reqs[1].1).unwrap();
    assert_eq!(
        create,
        json!({"input_file_id":"file_in","endpoint":"/v1/responses","completion_window":"24h"})
    );
}
#[tokio::test]
async fn anthropic_native_lifecycle() {
    let result =
        json!({"custom_id":"one","result":{"type":"succeeded","message":anthropic_response()}})
            .to_string()
            + "\n";
    let (base, server) = server(vec![
        (
            "POST /proxy/v1/messages/batches".into(),
            anth_job().to_string(),
        ),
        (
            "GET /proxy/v1/messages/batches/job_1".into(),
            anth_job().to_string(),
        ),
        (
            "GET /proxy/v1/messages/batches/job_1".into(),
            anth_job().to_string(),
        ),
        (
            "GET /proxy/v1/messages/batches/job_1/results".into(),
            result,
        ),
        (
            "POST /proxy/v1/messages/batches/job_1/cancel".into(),
            anth_job().to_string(),
        ),
    ])
    .await;
    let p = AnthropicProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let h = p.submit(&items(), CancellationToken::new()).await.unwrap();
    assert_eq!(
        p.status(&h, CancellationToken::new()).await.unwrap().status,
        BatchStatus::Completed
    );
    let results = p.results(&h, CancellationToken::new()).await.unwrap();
    match &results.items[0].outcome {
        BatchOutcome::Success(entry) => {
            assert_eq!(
                **entry,
                p.decode_response("answer", &anthropic_response()).unwrap()
            );
        }
        _ => panic!("expected successful batch result"),
    }
    p.cancel(&h, CancellationToken::new()).await.unwrap();
    let requests = server.await.unwrap();
    let body: Value = serde_json::from_slice(&requests[0].1).unwrap();
    assert_eq!(body["requests"][0]["custom_id"], "one");
    assert!(body["requests"][0]["params"].get("stream").is_none());
}
#[tokio::test]
async fn rejects_bad_ids_duplicates_missing_and_credential_echo() {
    for lines in [
        json!({"custom_id":"unknown","result":{"type":"canceled"}}).to_string(),
        format!("{}\n{}",json!({"custom_id":"one","result":{"type":"canceled"}}),json!({"custom_id":"one","result":{"type":"canceled"}})),
        String::new(),
        json!({"custom_id":"one","result":{"type":"succeeded","message":anthropic_response().to_string().replace("hello","offline-secret")}}).to_string(),
    ] {
        let (base,task)=server(vec![("GET /proxy/v1/messages/batches/job_1".into(),anth_job().to_string()),("GET /proxy/v1/messages/batches/job_1/results".into(),lines)]).await;
        let p=AnthropicProvider::from_base_url(&base,"requested-model",resolver()).unwrap();
        let h=BatchHandle { provider:"anthropic".into(),protocol:"messages".into(),model:"requested-model".into(),endpoint:format!("{base}/messages"),job_id:"job_1".into(),message_ids:[("one".into(),"answer".into())].into_iter().collect() };
        assert!(p.results(&h,CancellationToken::new()).await.is_err()); task.await.unwrap();
        let mut h=h; h.job_id="../steal".into(); assert!(p.status(&h,CancellationToken::new()).await.is_err());
    }
}

#[tokio::test]
async fn pending_cancelled_and_timeout_are_enforced() {
    let mut pending = anth_job();
    pending["processing_status"] = json!("in_progress");
    pending["request_counts"]["processing"] = json!(1);
    pending["request_counts"]["succeeded"] = json!(0);
    let (base, task) = server(vec![(
        "GET /proxy/v1/messages/batches/job_1".into(),
        pending.to_string(),
    )])
    .await;
    let p = AnthropicProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let h = BatchHandle {
        provider: "anthropic".into(),
        protocol: "messages".into(),
        model: "requested-model".into(),
        endpoint: format!("{base}/messages"),
        job_id: "job_1".into(),
        message_ids: [("one".into(), "answer".into())].into_iter().collect(),
    };
    assert!(p.results(&h, CancellationToken::new()).await.is_err());
    task.await.unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        p.status(&h, cancelled).await.unwrap_err().kind,
        ModelErrorKind::Cancelled
    );
    struct Slow;
    #[async_trait]
    impl CredentialResolver for Slow {
        async fn resolve(
            &self,
            _: &crate::CredentialRequest,
            _: CancellationToken,
        ) -> Result<Option<crate::ProviderCredential>, ModelError> {
            std::future::pending().await
        }
    }
    let mut cfg = ProviderConfig::new("http://127.0.0.1:1/v1/messages", "requested-model");
    cfg.timeout = Duration::from_millis(10);
    let p = AnthropicProvider::new(cfg, Arc::new(Slow)).unwrap();
    let mut h = h;
    h.endpoint = "http://127.0.0.1:1/v1/messages".into();
    assert_eq!(
        p.status(&h, CancellationToken::new())
            .await
            .unwrap_err()
            .message,
        "provider operation timed out"
    );
}

#[tokio::test]
async fn openai_partial_terminal_jobs_and_strict_complete_coverage() {
    for status in ["failed", "cancelled", "expired", "completed"] {
        let mut job = openai_job();
        job["status"] = json!(status);
        job["request_counts"] = json!({"total":2,"completed":1,"failed":0});
        let line = json!({"custom_id":"one","response":{"status_code":200,"body":response()},"error":null}).to_string();
        let (base, task) = server(vec![
            ("GET /proxy/v1/batches/job_1".into(), job.to_string()),
            ("GET /proxy/v1/files/file_out/content".into(), line),
        ])
        .await;
        let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
        let h = BatchHandle {
            provider: "openai".into(),
            protocol: "responses".into(),
            model: "requested-model".into(),
            endpoint: format!("{base}/responses"),
            job_id: "job_1".into(),
            message_ids: [
                ("one".into(), "answer".into()),
                ("two".into(), "answer2".into()),
            ]
            .into_iter()
            .collect(),
        };
        let result = p.results(&h, CancellationToken::new()).await;
        if status == "completed" {
            assert!(result.is_err());
        } else {
            let result = result.unwrap();
            assert_eq!(result.items.len(), 1);
            assert_eq!(result.unreported_ids, vec!["two"]);
            assert_eq!(
                result.job.status,
                match status {
                    "failed" => BatchStatus::Failed,
                    "cancelled" => BatchStatus::Cancelled,
                    _ => BatchStatus::Expired,
                }
            );
        }
        task.await.unwrap();
    }
    let (base, task) = server(vec![("GET /proxy/v1/batches/job_1".into(),json!({"id":"job_1","status":"failed","request_counts":{"total":0,"completed":0,"failed":0},"output_file_id":null,"error_file_id":null}).to_string())]).await;
    let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let h = BatchHandle {
        provider: "openai".into(),
        protocol: "responses".into(),
        model: "requested-model".into(),
        endpoint: format!("{base}/responses"),
        job_id: "job_1".into(),
        message_ids: [("one".into(), "answer".into())].into_iter().collect(),
    };
    let r = p.results(&h, CancellationToken::new()).await.unwrap();
    assert!(r.items.is_empty());
    assert_eq!(r.unreported_ids, vec!["one"]);
    task.await.unwrap();
}

#[tokio::test]
async fn foreign_endpoint_and_huge_timeout_are_rejected_before_http() {
    let p = OpenAIProvider::from_base_url("http://127.0.0.1:1/v1", "requested-model", resolver())
        .unwrap();
    let h = BatchHandle {
        provider: "openai".into(),
        protocol: "responses".into(),
        model: "requested-model".into(),
        endpoint: "http://127.0.0.1:2/v1/responses".into(),
        job_id: "job_1".into(),
        message_ids: [("one".into(), "answer".into())].into_iter().collect(),
    };
    assert_eq!(
        p.status(&h, CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::InvalidRequest
    );
    let mut cfg = ProviderConfig::new("http://127.0.0.1:1/v1/responses", "requested-model");
    cfg.timeout = Duration::MAX;
    let p = OpenAIProvider::new(cfg, resolver()).unwrap();
    assert_eq!(
        p.status(&h, CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::InvalidRequest
    );
}

#[test]
fn batch_wire_options_ids_and_rich_replay() {
    let a = AnthropicProvider::new(
        ProviderConfig::new("http://127.0.0.1:1/v1/messages", "requested-model"),
        resolver(),
    )
    .unwrap();
    let mut long = items();
    long[0].custom_id = "a".repeat(64);
    assert!(a.build_submission(&long).is_ok());
    long[0].custom_id.push('a');
    assert!(a.build_submission(&long).is_err());
    let mut ar = anthropic_response();
    ar["stop_reason"] = json!("stop_sequence");
    ar["stop_sequence"] = json!("END");
    ar["content"] = json!([{"type":"thinking","thinking":"plan","signature":"sig"},{"type":"text","text":"answer"}]);
    let ae = a.decode_response("answer", &ar).unwrap();
    assert_eq!(
        ae.generation.as_ref().unwrap().extras["stop_sequence"],
        "END"
    );
    let req = request(vec![ae.message]);
    let replay = a.build_request(&req).unwrap();
    assert_eq!(replay["messages"][0]["content"][0]["signature"], "sig");
    assert!(a.decode_response(" ", &ar).is_err());
    let o = OpenAIProvider::new(
        ProviderConfig::new("http://127.0.0.1:1/v1/responses", "requested-model"),
        resolver(),
    )
    .unwrap();
    let mut r = response();
    r["output"] = json!([{"id":"reason","type":"reasoning","summary":[{"type":"summary_text","text":"plan"}],"encrypted_content":"opaque"},{"id":"call","type":"function_call","call_id":"c1","name":"lookup","arguments":"{}"}]);
    let e = o.decode_response("answer", &r).unwrap();
    let replay = o
        .build_request(&request(vec![
            e.message,
            ConversationMessage::Tool {
                call_id: "c1".into(),
                name: "lookup".into(),
                content: vec![rusty_bot_messages::ContentBlock::text("done")],
                is_error: false,
            },
        ]))
        .unwrap();
    assert_eq!(replay["input"][0]["encrypted_content"], "opaque");
    assert_eq!(replay["input"][1]["call_id"], "c1");
    let cfg = ProviderConfig::new("http://127.0.0.1:1/v1/chat/completions", "requested-model");
    let c =
        OpenAIProvider::with_protocol(cfg, OpenAIProtocol::ChatCompletions, resolver()).unwrap();
    let mut chat = json!({"id":"r1","model":"actual","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"annotations":[],"audio":null,"function_call":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}});
    let e = c.decode_response("answer", &chat).unwrap();
    let replay = c
        .build_request(&request(vec![
            e.message,
            ConversationMessage::Tool {
                call_id: "c1".into(),
                name: "lookup".into(),
                content: vec![rusty_bot_messages::ContentBlock::text("done")],
                is_error: false,
            },
        ]))
        .unwrap();
    assert_eq!(replay["messages"][0]["tool_calls"][0]["id"], "c1");
    assert!(replay["messages"][0].get("annotations").is_none());
    for (key, value) in [
        ("annotations", json!([{"type":"url_citation"}])),
        ("audio", json!({"id":"a"})),
        ("function_call", json!({"name":"f"})),
    ] {
        chat["choices"][0]["message"][key] = value;
        assert!(c.decode_response("answer", &chat).is_err());
        chat["choices"][0]["message"][key] = Value::Null;
    }
}

#[tokio::test]
async fn openai_duplicate_unknown_and_decoded_credential_echo_rejected() {
    for lines in [
        json!({"custom_id":"other","error":{"code":"batch_expired"}}).to_string(),
        format!(
            "{}\n{}",
            json!({"custom_id":"one","error":{"code":"batch_expired"}}),
            json!({"custom_id":"one","error":{"code":"batch_expired"}})
        ),
        json!({"custom_id":"one","response":{"status_code":200,"body":response()},"error":null})
            .to_string()
            .replace("hello", "\\u006fffline-secret"),
    ] {
        let (base, task) = server(vec![
            (
                "GET /proxy/v1/batches/job_1".into(),
                openai_job().to_string(),
            ),
            ("GET /proxy/v1/files/file_out/content".into(), lines),
        ])
        .await;
        let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
        let h = BatchHandle {
            provider: "openai".into(),
            protocol: "responses".into(),
            model: "requested-model".into(),
            endpoint: format!("{base}/responses"),
            job_id: "job_1".into(),
            message_ids: [("one".into(), "answer".into())].into_iter().collect(),
        };
        assert!(p.results(&h, CancellationToken::new()).await.is_err());
        task.await.unwrap();
    }
}

#[tokio::test]
async fn aggregate_download_budget_is_shared_between_files() {
    let mut j = openai_job();
    j["error_file_id"] = json!("file_err");
    let first = " ".repeat(9 * 1024 * 1024);
    let second = " ".repeat(8 * 1024 * 1024);
    let (base, task) = server(vec![
        ("GET /proxy/v1/batches/job_1".into(), j.to_string()),
        ("GET /proxy/v1/files/file_out/content".into(), first),
        ("GET /proxy/v1/files/file_err/content".into(), second),
    ])
    .await;
    let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let h = BatchHandle {
        provider: "openai".into(),
        protocol: "responses".into(),
        model: "requested-model".into(),
        endpoint: format!("{base}/responses"),
        job_id: "job_1".into(),
        message_ids: [("one".into(), "answer".into())].into_iter().collect(),
    };
    assert!(p.results(&h, CancellationToken::new()).await.is_err());
    // The client may close the second download as soon as the budget is exceeded.
    let _ = task.await;
}

#[test]
fn nonstream_and_streaming_replay_match() {
    fn stream(api: Api, frames: Vec<Value>, done: bool) -> HistoryEntry {
        let mut d = wire::Decoder::new(api, "requested-model", "answer");
        for frame in frames {
            d.push(&frame.to_string()).unwrap();
        }
        if done {
            d.push("[DONE]").unwrap();
        }
        d.finish().unwrap()
    }
    let frames = crate::tests::responses_frames();
    let final_response = frames.last().unwrap()["response"].clone();
    let nonstream =
        wire::Decoder::from_nonstream(Api::Responses, "requested-model", "answer", &final_response)
            .unwrap();
    assert_eq!(
        nonstream.message,
        stream(Api::Responses, frames, false).message
    );
    let mut frames = crate::tests::anthropic_frames();
    let delta = frames
        .iter_mut()
        .find(|v| v["type"] == "message_delta")
        .unwrap();
    delta["delta"]["stop_reason"] = json!("stop_sequence");
    delta["delta"]["stop_sequence"] = json!("END");
    let nonstream = json!({"id":"a1","type":"message","model":"actual-model","role":"assistant","content":[{"type":"thinking","thinking":"plan","signature":"opaque-signature"},{"type":"redacted_thinking","data":"opaque-data"},{"type":"text","text":"answer"}],"stop_reason":"stop_sequence","stop_sequence":"END","usage":{"input_tokens":9,"output_tokens":5,"cache_read_input_tokens":2}});
    let a = wire::Decoder::from_nonstream(Api::Anthropic, "requested-model", "answer", &nonstream)
        .unwrap();
    let b = stream(Api::Anthropic, frames, false);
    assert_eq!(a.message, b.message);
    assert_eq!(
        a.generation.unwrap().extras["stop_sequence"],
        b.generation.unwrap().extras["stop_sequence"]
    );
    let frame = |delta: Value, finish: Value| json!({"id":"r1","model":"actual","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    let frames = vec![
        frame(json!({"role":"assistant","content":null}), Value::Null),
        frame(
            json!({"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}),
            json!("tool_calls"),
        ),
    ];
    let chat = json!({"id":"r1","model":"actual","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]});
    let a = wire::Decoder::from_nonstream(Api::Chat, "requested-model", "answer", &chat).unwrap();
    assert_eq!(a.message, stream(Api::Chat, frames, true).message);
}

#[tokio::test]
async fn openai_unknown_counts_preserve_status_and_unreported_ids() {
    for status in [
        "failed",
        "validating",
        "in_progress",
        "cancelled",
        "expired",
        "completed",
    ] {
        for absent in [false, true] {
            let mut j = json!({"id":"job_1","status":status,"request_counts":null});
            if absent {
                j.as_object_mut().unwrap().remove("request_counts");
            }
            let mut replies = vec![("GET /proxy/v1/batches/job_1".into(), j.to_string())];
            if status == "failed" {
                replies.push(("GET /proxy/v1/batches/job_1".into(), j.to_string()));
            }
            let (base, task) = server(replies).await;
            let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
            let h = BatchHandle {
                provider: "openai".into(),
                protocol: "responses".into(),
                model: "requested-model".into(),
                endpoint: format!("{base}/responses"),
                job_id: "job_1".into(),
                message_ids: [("one".into(), "answer".into())].into_iter().collect(),
            };
            let result = p.status(&h, CancellationToken::new()).await;
            if status == "completed" {
                assert_eq!(result.unwrap_err().kind, ModelErrorKind::Protocol);
            } else {
                let job = result.unwrap();
                assert_eq!(job.counts, None);
                assert_eq!(
                    job.status,
                    match status {
                        "failed" => BatchStatus::Failed,
                        "validating" | "in_progress" => BatchStatus::Running,
                        "cancelled" => BatchStatus::Cancelled,
                        "expired" => BatchStatus::Expired,
                        _ => unreachable!(),
                    }
                );
                if status == "failed" {
                    let results = p.results(&h, CancellationToken::new()).await.unwrap();
                    assert_eq!(results.job, job);
                    assert!(results.items.is_empty());
                    assert_eq!(results.unreported_ids, vec!["one"]);
                }
            }
            task.await.unwrap();
        }
    }
}

#[tokio::test]
async fn missing_or_malformed_required_batch_counts_are_rejected() {
    for counts in [json!({"completed":0,"failed":0}), json!("invalid")] {
        let j = json!({"id":"job_1","status":"failed","request_counts":counts});
        let (base, task) =
            server(vec![("GET /proxy/v1/batches/job_1".into(), j.to_string())]).await;
        let p = OpenAIProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
        let h = BatchHandle {
            provider: "openai".into(),
            protocol: "responses".into(),
            model: "requested-model".into(),
            endpoint: format!("{base}/responses"),
            job_id: "job_1".into(),
            message_ids: [("one".into(), "answer".into())].into_iter().collect(),
        };
        assert_eq!(
            p.status(&h, CancellationToken::new())
                .await
                .unwrap_err()
                .kind,
            ModelErrorKind::Protocol
        );
        task.await.unwrap();
    }
    let (base, task) = server(vec![(
        "GET /proxy/v1/messages/batches/job_1".into(),
        json!({"id":"job_1","processing_status":"ended","request_counts":null}).to_string(),
    )])
    .await;
    let p = AnthropicProvider::from_base_url(&base, "requested-model", resolver()).unwrap();
    let h = BatchHandle {
        provider: "anthropic".into(),
        protocol: "messages".into(),
        model: "requested-model".into(),
        endpoint: format!("{base}/messages"),
        job_id: "job_1".into(),
        message_ids: [("one".into(), "answer".into())].into_iter().collect(),
    };
    assert_eq!(
        p.status(&h, CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Protocol
    );
    task.await.unwrap();
}

#[test]
fn chat_null_tool_calls_are_optional() {
    let chat = json!({"id":"r1","model":"actual","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hello","tool_calls":null}}]});
    let entry =
        wire::Decoder::from_nonstream(Api::Chat, "requested-model", "answer", &chat).unwrap();
    assert!(matches!(
        &entry.message.content()[0],
        rusty_bot_messages::ContentBlock::Text { .. }
    ));
    let mut malformed = chat;
    malformed["choices"][0]["message"]["tool_calls"] = json!({});
    assert_eq!(
        wire::Decoder::from_nonstream(Api::Chat, "requested-model", "answer", &malformed)
            .unwrap_err()
            .kind,
        ModelErrorKind::Protocol
    );
}

#[test]
fn responses_incomplete_function_calls_never_become_executable() {
    for status in ["incomplete", "in_progress", "unknown"] {
        let mut frames = crate::tests::responses_frames();
        let done = frames
            .iter_mut()
            .find(|f| {
                f["type"] == "response.output_item.done" && f["item"]["type"] == "function_call"
            })
            .unwrap();
        done["item"]["status"] = json!(status);
        let last = frames.last_mut().unwrap();
        last["type"] = json!("response.incomplete");
        last["response"]["status"] = json!("incomplete");
        last["response"]["incomplete_details"] = json!({"reason":"max_output_tokens"});
        last["response"]["output"][2]["status"] = json!(status);
        assert_eq!(
            wire::Decoder::from_nonstream(
                Api::Responses,
                "requested-model",
                "answer",
                &last["response"]
            )
            .unwrap_err()
            .kind,
            ModelErrorKind::Protocol
        );
        let mut d = wire::Decoder::new(Api::Responses, "requested-model", "answer");
        let mut failure = None;
        for frame in &frames {
            if let Err(e) = d.push(&frame.to_string()) {
                failure = Some(e);
                break;
            }
        }
        assert_eq!(failure.unwrap().kind, ModelErrorKind::Protocol);
        assert_eq!(d.finish().unwrap_err().kind, ModelErrorKind::Protocol);
    }
    // A completed call remains valid even if the overall response hit the token limit.
    let mut frames = crate::tests::responses_frames();
    let last = frames.last_mut().unwrap();
    last["type"] = json!("response.incomplete");
    last["response"]["status"] = json!("incomplete");
    last["response"]["incomplete_details"] = json!({"reason":"max_output_tokens"});
    let nonstream = wire::Decoder::from_nonstream(
        Api::Responses,
        "requested-model",
        "answer",
        &last["response"],
    )
    .unwrap();
    assert!(nonstream
        .message
        .content()
        .iter()
        .any(|b| matches!(b, rusty_bot_messages::ContentBlock::ToolCall { .. })));
    let mut d = wire::Decoder::new(Api::Responses, "requested-model", "answer");
    for frame in frames {
        d.push(&frame.to_string()).unwrap();
    }
    assert_eq!(d.finish().unwrap().message, nonstream.message);
}
