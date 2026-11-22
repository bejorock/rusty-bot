use super::*;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Resolver(AtomicUsize);
#[async_trait::async_trait]
impl CredentialResolver for Resolver {
    async fn resolve(
        &self,
        _: &CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ProviderCredential::new("secret-fixture-value")))
    }
}
fn request() -> EmbeddingRequest {
    EmbeddingRequest {
        input: vec!["first".into(), "second".into()],
        dimensions: Some(2),
        user: Some("user".into()),
    }
}
fn response() -> Value {
    json!({"object":"list", "model":"embed-model", "data":[{"object":"embedding","index":1,"embedding":[3.0,4.0]},{"object":"embedding","index":0,"embedding":[1.0,2.0]}], "usage":{"prompt_tokens":7,"total_tokens":7}})
}
fn provider(url: String, resolver: Arc<Resolver>) -> OpenAIEmbeddingProvider {
    let mut config = EmbeddingConfig::new(url, "embed-model");
    config.retry.initial_backoff = Duration::from_millis(1);
    config.retry.max_backoff = Duration::from_millis(2);
    OpenAIEmbeddingProvider::new(config, resolver).unwrap()
}
enum Reply {
    Json(Value),
    Raw(String),
    Status(&'static str, &'static str),
    Stall,
}
async fn server(
    replies: Vec<Reply>,
) -> (
    String,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<Vec<String>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            seen.fetch_add(1, Ordering::SeqCst);
            let mut data = Vec::new();
            let header_end = loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break 0;
                }
                data.extend_from_slice(&buf[..n]);
                if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
            let len: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|n| n.trim().parse().ok())
                })
                .unwrap_or(0);
            while data.len() < header_end + len {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&buf[..n]);
            }
            requests.push(String::from_utf8_lossy(&data).to_string());
            let (status, extra, body) = match reply {
                Reply::Json(v) => ("200 OK", "", v.to_string()),
                Reply::Raw(v) => ("200 OK", "", v),
                Reply::Status(s, e) => (s, e, String::new()),
                Reply::Stall => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };
            let msg = format!("HTTP/1.1 {status}\r\nx-request-id: fixture-id\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = socket.write_all(msg.as_bytes()).await;
        }
        requests
    });
    (url, count, task)
}
#[tokio::test]
async fn success_request_response_and_redaction() {
    let (url, _, task) = server(vec![Reply::Json(response())]).await;
    let p = provider(url, Arc::new(Resolver(AtomicUsize::new(0))));
    let req = request();
    let body = p.build_embedding_request(&req).unwrap();
    assert_eq!(
        body,
        json!({"model":"embed-model","input":["first","second"],"encoding_format":"float","dimensions":2,"user":"user"})
    );
    assert!(!format!("{req:?}").contains("first"));
    assert!(!format!("{:?}", EmbeddingConfig::new("sensitive", "model")).contains("sensitive"));
    let result = p.embed(&req, CancellationToken::new()).await.unwrap();
    assert_eq!(result.embeddings[0].vector, vec![1., 2.]);
    assert_eq!(result.embeddings[1].index, 1);
    assert_eq!(result.usage.input_tokens, 7);
    assert_eq!(result.provider_request_id.as_deref(), Some("fixture-id"));
    assert!(!format!("{:?}", result.embeddings[0]).contains("1.0"));
    let captured = task.await.unwrap();
    assert!(captured[0]
        .to_ascii_lowercase()
        .contains("authorization: bearer secret-fixture-value"));
    assert_eq!(
        serde_json::from_str::<Value>(captured[0].split_once("\r\n\r\n").unwrap().1).unwrap(),
        body
    );
}
#[tokio::test]
async fn rejects_bad_protocol_without_retry() {
    let mut cases = vec![];
    let mut v = response();
    v["data"][0]["index"] = json!(0);
    cases.push(v);
    let mut v = response();
    v["data"].as_array_mut().unwrap().pop();
    cases.push(v);
    let mut v = response();
    v["data"][0]["index"] = json!(2);
    cases.push(v);
    let mut v = response();
    v["data"][0]["embedding"] = json!([1]);
    cases.push(v);
    let mut v = response();
    v["data"][0]["embedding"] = json!([1e100, 1]);
    cases.push(v);
    let mut v = response();
    v["usage"].as_object_mut().unwrap().remove("prompt_tokens");
    cases.push(v);
    for v in cases {
        let (url, count, task) = server(vec![Reply::Json(v)]).await;
        let p = provider(url, Arc::new(Resolver(AtomicUsize::new(0))));
        let err = p
            .embed(&request(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.kind, ModelErrorKind::Protocol);
        assert!(!err.retryable);
        assert_eq!(err.provider_request_id.as_deref(), Some("fixture-id"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.await.unwrap();
    }
    let (url, _, task) = server(vec![Reply::Raw("{\"data\":[NaN]}".into())]).await;
    assert_eq!(
        provider(url, Arc::new(Resolver(AtomicUsize::new(0))))
            .embed(&request(), CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Protocol
    );
    task.await.unwrap();
}
#[tokio::test]
async fn invalid_and_unsupported_never_resolve() {
    let resolver = Arc::new(Resolver(AtomicUsize::new(0)));
    let p = provider("http://127.0.0.1:1/embeddings".into(), resolver.clone());
    for req in [
        EmbeddingRequest::default(),
        EmbeddingRequest {
            input: vec!["".into()],
            ..request()
        },
        EmbeddingRequest {
            dimensions: Some(0),
            ..request()
        },
        EmbeddingRequest {
            input: vec!["a".into(); 2049],
            ..request()
        },
        EmbeddingRequest {
            input: vec!["x".repeat(16 * 1024 * 1024)],
            ..request()
        },
    ] {
        assert_eq!(
            p.embed(&req, CancellationToken::new())
                .await
                .unwrap_err()
                .kind,
            ModelErrorKind::InvalidRequest
        );
    }
    let cfg = || ProviderConfig::new("http://127.0.0.1:1/chat", "chat");
    let providers: Vec<Box<dyn EmbeddingProvider>> = vec![
        Box::new(OpenAIProvider::new(cfg(), resolver.clone()).unwrap()),
        Box::new(AnthropicProvider::new(cfg(), resolver.clone()).unwrap()),
        Box::new(DeepseekProvider::new(cfg(), resolver.clone()).unwrap()),
    ];
    for p in providers {
        assert_eq!(
            p.embed(&EmbeddingRequest::default(), CancellationToken::new())
                .await
                .unwrap_err()
                .kind,
            ModelErrorKind::Unsupported
        );
    }
    assert_eq!(resolver.0.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn retries_auth_timeout_cancel_and_secret_echo() {
    let (url, count, task) = server(vec![
        Reply::Status("503 Service Unavailable", "Retry-After: 0\r\n"),
        Reply::Json(response()),
    ])
    .await;
    let p = provider(url, Arc::new(Resolver(AtomicUsize::new(0))));
    assert!(p.embed(&request(), CancellationToken::new()).await.is_ok());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    task.await.unwrap();
    let (url, count, task) = server(vec![Reply::Status("401 Unauthorized", "")]).await;
    let p = provider(url, Arc::new(Resolver(AtomicUsize::new(0))));
    assert_eq!(
        p.embed(&request(), CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Auth
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.await.unwrap();
    let (url, _, task) = server(vec![Reply::Stall]).await;
    let mut cfg = EmbeddingConfig::new(url, "embed-model");
    cfg.timeout = Duration::from_millis(30);
    let p = OpenAIEmbeddingProvider::new(cfg, Arc::new(Resolver(AtomicUsize::new(0)))).unwrap();
    assert_eq!(
        p.embed(&request(), CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Network
    );
    task.abort();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        p.embed(&request(), cancel).await.unwrap_err().kind,
        ModelErrorKind::Cancelled
    );
    let mut v = response();
    v["extra"] = json!("secret-fixture-value");
    let (url, _, task) = server(vec![Reply::Json(v)]).await;
    let err = provider(url, Arc::new(Resolver(AtomicUsize::new(0))))
        .embed(&request(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ModelErrorKind::Protocol);
    assert!(!format!("{err:?}").contains("secret-fixture-value"));
    task.await.unwrap();
}

// Abort even on an assertion or an outer timeout, so an unconsumed reply cannot
// leave a listener or a stalled socket behind.
struct BoundedServer(tokio::task::JoinHandle<Vec<String>>);
impl Drop for BoundedServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl BoundedServer {
    async fn finish(mut self) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(2), &mut self.0)
            .await
            .expect("fixture server stalled")
            .expect("fixture server failed")
    }
}
async fn bounded_server(replies: Vec<Reply>) -> (String, Arc<AtomicUsize>, BoundedServer) {
    let (url, count, task) = server(replies).await;
    (url, count, BoundedServer(task))
}

#[tokio::test]
async fn retry_exhaustion_default_and_disabled() {
    tokio::time::timeout(Duration::from_secs(6), async {
        for (retries, expected) in [(None, 4), (Some(0), 1)] {
            let replies = (0..expected)
                .map(|_| Reply::Status("503 Service Unavailable", ""))
                .collect();
            let (url, count, task) = bounded_server(replies).await;
            let mut config = EmbeddingConfig::new(url, "embed-model");
            if let Some(retries) = retries {
                config.retry.retries = retries;
            }
            let p = OpenAIEmbeddingProvider::new(config, Arc::new(Resolver(AtomicUsize::new(0))))
                .unwrap();
            let error = p
                .embed(&request(), CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.kind, ModelErrorKind::Server);
            assert_eq!(count.load(Ordering::SeqCst), expected);
            assert_eq!(task.finish().await.len(), expected);
        }
    })
    .await
    .expect("retry test timed out");
}

#[tokio::test]
async fn retry_after_is_a_minimum_and_obeys_operation_deadline() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (url, count, _task) = bounded_server(vec![
            Reply::Status("503 Service Unavailable", "Retry-After: 1\r\n"),
            Reply::Json(response()),
        ])
        .await;
        let mut config = EmbeddingConfig::new(url, "embed-model");
        config.timeout = Duration::from_millis(250);
        config.retry.initial_backoff = Duration::from_millis(1);
        config.retry.max_backoff = Duration::from_millis(2);
        let p =
            OpenAIEmbeddingProvider::new(config, Arc::new(Resolver(AtomicUsize::new(0)))).unwrap();
        let error = p
            .embed(&request(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Network);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("retry-after test timed out");
}

struct PendingResolver {
    started: tokio::sync::Notify,
    dropped: AtomicUsize,
}
struct ResolveGuard<'a>(&'a AtomicUsize);
impl Drop for ResolveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl CredentialResolver for PendingResolver {
    async fn resolve(
        &self,
        _: &CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        let _guard = ResolveGuard(&self.dropped);
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn deadline_and_cancellation_interrupt_pending_credential_resolution() {
    tokio::time::timeout(Duration::from_secs(4), async {
        for cancel_during_resolution in [false, true] {
            let (url, count, _task) = bounded_server(vec![Reply::Stall]).await;
            let resolver = Arc::new(PendingResolver {
                started: tokio::sync::Notify::new(),
                dropped: AtomicUsize::new(0),
            });
            let mut config = EmbeddingConfig::new(url, "embed-model");
            config.timeout = if cancel_during_resolution {
                Duration::from_secs(2)
            } else {
                Duration::from_millis(150)
            };
            let p = OpenAIEmbeddingProvider::new(config, resolver.clone()).unwrap();
            let cancel = CancellationToken::new();
            let req = request();
            let operation = p.embed(&req, cancel.clone());
            tokio::pin!(operation);
            tokio::select! {
                _ = resolver.started.notified() => {},
                result = &mut operation => panic!("operation completed before resolver started: {result:?}"),
            }
            if cancel_during_resolution {
                cancel.cancel();
            }
            let error = operation.await.unwrap_err();
            assert_eq!(error.kind, if cancel_during_resolution { ModelErrorKind::Cancelled } else { ModelErrorKind::Network });
            assert_eq!(resolver.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
    }).await.expect("resolver interruption test timed out");
}

#[tokio::test]
async fn base_url_preserves_proxy_path_and_rejects_unsafe_urls() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (url, _, task) = bounded_server(vec![Reply::Json(response())]).await;
        let base = url.strip_suffix("/v1/embeddings").unwrap();
        let resolver = Arc::new(Resolver(AtomicUsize::new(0)));
        let p = OpenAIEmbeddingProvider::from_base_url(
            &format!("{base}/proxy/v1/"),
            "embed-model",
            resolver.clone(),
        )
        .unwrap();
        p.embed(&request(), CancellationToken::new()).await.unwrap();
        let captured = task.finish().await;
        assert!(captured[0].starts_with("POST /proxy/v1/embeddings HTTP/1.1\r\n"));
        for url in [
            "http://example.com/v1",
            "ftp://127.0.0.1/v1",
            "http://user:pass@127.0.0.1/v1",
            "http://@127.0.0.1/v1",
            "http://127.0.0.1/v1?key=value",
            "http://127.0.0.1/v1#fragment",
            "not a url",
        ] {
            let err = OpenAIEmbeddingProvider::from_base_url(url, "embed-model", resolver.clone())
                .unwrap_err();
            assert_eq!(err.kind, ModelErrorKind::InvalidRequest, "{url}");
        }
        assert_eq!(resolver.0.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("base URL test timed out");
}

#[tokio::test]
async fn escaped_credential_in_json_is_detected_after_decoding() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut v = response();
        v["extra"] = json!("placeholder");
        let raw = v
            .to_string()
            .replace("placeholder", r"s\u0065cret-fixture-value");
        assert!(!raw.contains("secret-fixture-value"));
        let (url, count, task) = bounded_server(vec![Reply::Raw(raw)]).await;
        let err = provider(url, Arc::new(Resolver(AtomicUsize::new(0))))
            .embed(&request(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.kind, ModelErrorKind::Protocol);
        assert_eq!(err.message, "native response contained credential");
        assert!(!format!("{err:?}").contains("secret-fixture-value"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.finish().await;
    })
    .await
    .expect("escaped credential test timed out");
}

#[tokio::test]
async fn oversized_response_fails_boundedly() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
        let task = BoundedServer(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // The request is small; the server need only receive it before replying.
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            let size = 16 * 1024 * 1024 + 1;
            let header =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n");
            socket.write_all(header.as_bytes()).await.unwrap();
            let chunk = [b' '; 8192];
            for _ in 0..size / chunk.len() {
                if socket.write_all(&chunk).await.is_err() {
                    return vec![];
                }
            }
            let _ = socket.write_all(&chunk[..size % chunk.len()]).await;
            vec![]
        }));
        let p = provider(url, Arc::new(Resolver(AtomicUsize::new(0))));
        let err = p
            .embed(&request(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.kind, ModelErrorKind::Protocol);
        assert_eq!(err.message, "native response too large");
        task.finish().await;
    })
    .await
    .expect("oversized response test timed out");
}

#[tokio::test]
async fn malformed_response_metadata_and_unrequested_dimensions_are_rejected() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut cases = Vec::new();
        for (key, value) in [
            ("object", json!("embedding")),
            ("model", json!("")),
            ("model", Value::Null),
            ("usage", Value::Null),
        ] {
            let mut v = response();
            v[key] = value;
            cases.push(v);
        }
        for (key, value) in [
            ("prompt_tokens", json!(-1)),
            ("prompt_tokens", json!(1.5)),
            ("total_tokens", json!("7")),
            ("total_tokens", Value::Null),
        ] {
            let mut v = response();
            v["usage"][key] = value;
            cases.push(v);
        }
        let mut v = response();
        v["data"][0]["object"] = json!("list");
        cases.push(v);
        let mut v = response();
        v["data"][0]["embedding"] = json!([]);
        cases.push(v);
        let mut v = response();
        v["data"][0]["embedding"] = json!([1.0]);
        cases.push(v);
        let mut req = request();
        req.dimensions = None;
        for v in cases {
            let (url, count, task) = bounded_server(vec![Reply::Json(v)]).await;
            let err = provider(url, Arc::new(Resolver(AtomicUsize::new(0))))
                .embed(&req, CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(err.kind, ModelErrorKind::Protocol);
            assert!(!err.retryable);
            assert_eq!(count.load(Ordering::SeqCst), 1);
            task.finish().await;
        }
        // No dimensions requested is valid when the returned vectors agree.
        let (url, _, task) = bounded_server(vec![Reply::Json(response())]).await;
        assert!(provider(url, Arc::new(Resolver(AtomicUsize::new(0))))
            .embed(&req, CancellationToken::new())
            .await
            .is_ok());
        task.finish().await;
    })
    .await
    .expect("malformed response test timed out");
}
