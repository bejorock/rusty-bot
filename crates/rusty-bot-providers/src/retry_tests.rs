//! Offline loopback retry and deadline fixtures.

#[test]
fn discovery_schema_is_shared_and_serializable() {
    let page = super::ModelPage {
        models: vec![super::DiscoveredModel {
            id: "example-model".into(),
            display_name: None,
            max_input_tokens: Some(100_000),
            max_output_tokens: None,
            raw: serde_json::json!({"provider_extra": true}),
        }],
        next_cursor: Some("next".into()),
    };
    let json = serde_json::to_value(&page).unwrap();
    assert_eq!(json["models"][0]["id"], "example-model");
    assert_eq!(
        serde_json::from_value::<super::ModelPage>(json).unwrap(),
        page
    );
}
use super::*;
use crate::tests::{chat, config, request, Credentials};
use rusty_bot_messages::ConversationMessage;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

enum Reply {
    Status(&'static str, &'static str),
    Body(String),
    Truncated(String),
    Stall,
}
async fn server(replies: Vec<Reply>) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/chat", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            seen.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..n]);
                if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let (status, extra, body, length) = match reply {
                Reply::Status(status, extra) => (status, extra, String::new(), 0),
                Reply::Body(body) => {
                    let n = body.len();
                    ("200 OK", "", body, n)
                }
                Reply::Truncated(body) => {
                    let n = body.len() + 100;
                    ("200 OK", "", body, n)
                }
                Reply::Stall => {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };
            let header = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Length: {length}\r\nConnection: close\r\n\r\n"
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(body.as_bytes()).await;
        }
    });
    (url, count, task)
}
fn success() -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        chat(json!({"role":"assistant","content":"hello"}), Value::Null),
        chat(json!({}), json!("stop"))
    )
}
fn client(endpoint: String, retry: RetryPolicy, timeout: Duration) -> Client {
    let mut cfg = config();
    cfg.endpoint = endpoint;
    cfg.retry = retry;
    cfg.timeout = timeout;
    Client::new(cfg, Api::Chat, Arc::new(Credentials(AtomicUsize::new(0)))).unwrap()
}
fn fast() -> RetryPolicy {
    RetryPolicy {
        retries: 3,
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(10),
    }
}
async fn generate(
    client: &Client,
    cancel: CancellationToken,
) -> (Result<HistoryEntry, ModelError>, Vec<MessageStreamEvent>) {
    let (tx, mut rx) = mpsc::channel(32);
    let result = client
        .stream(&request(vec![ConversationMessage::user("hi")]), tx, cancel)
        .await;
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    (result, events)
}
#[tokio::test]
async fn stream_retry_success_exhaustion_zero_and_auth() {
    for (replies, policy, expected_count, success_expected, kind) in [
        (
            vec![Reply::Status("503 Unavailable", ""), Reply::Body(success())],
            fast(),
            2,
            true,
            ModelErrorKind::Server,
        ),
        (
            vec![
                Reply::Status("503 Unavailable", ""),
                Reply::Status("503 Unavailable", ""),
                Reply::Status("503 Unavailable", ""),
                Reply::Status("503 Unavailable", ""),
            ],
            RetryPolicy::default(),
            4,
            false,
            ModelErrorKind::Server,
        ),
        (
            vec![Reply::Status("503 Unavailable", ""), Reply::Body(success())],
            RetryPolicy {
                retries: 0,
                ..fast()
            },
            1,
            false,
            ModelErrorKind::Server,
        ),
        (
            vec![
                Reply::Status("401 Unauthorized", ""),
                Reply::Body(success()),
            ],
            fast(),
            1,
            false,
            ModelErrorKind::Auth,
        ),
    ] {
        let (url, count, server) = server(replies).await;
        let (result, events) = generate(
            &client(url, policy, Duration::from_secs(4)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(count.load(Ordering::SeqCst), expected_count);
        if success_expected {
            assert!(result.is_ok());
            assert!(matches!(events[0].event, MessageStreamPayload::Start));
        } else {
            assert_eq!(result.unwrap_err().kind, kind);
            assert!(events.is_empty());
        }
        server.abort();
    }
}
#[tokio::test]
async fn stream_network_failure_after_partial_is_not_retried() {
    let partial = format!(
        "data: {}\n\n:{}\n\n",
        chat(
            json!({"role":"assistant","content":"partial output"}),
            Value::Null
        ),
        "x".repeat(128)
    );
    let (url, count, server) =
        server(vec![Reply::Truncated(partial), Reply::Body(success())]).await;
    let (result, events) = generate(
        &client(url, fast(), Duration::from_secs(2)),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(result.unwrap_err().kind, ModelErrorKind::Network);
    assert!(!events.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}
#[tokio::test]
async fn retry_after_cancel_and_deadline_cover_backoff() {
    for cancel_it in [false, true] {
        let (url, count, server) = server(vec![
            Reply::Status("429 Too Many Requests", "Retry-After: 2\r\n"),
            Reply::Body(success()),
        ])
        .await;
        let token = CancellationToken::new();
        if cancel_it {
            let t = token.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                t.cancel();
            });
        }
        let now = tokio::time::Instant::now();
        let (result, events) =
            generate(&client(url, fast(), Duration::from_millis(150)), token).await;
        assert_eq!(
            result.unwrap_err().kind,
            if cancel_it {
                ModelErrorKind::Cancelled
            } else {
                ModelErrorKind::Network
            }
        );
        assert!(events.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(now.elapsed() < Duration::from_secs(1));
        server.abort();
    }
}
#[tokio::test]
async fn discovery_retries_and_respects_retry_after_and_deadline() {
    for delayed in [false, true] {
        let (url, count, server) = server(vec![
            Reply::Status(
                "503 Unavailable",
                if delayed { "Retry-After: 2\r\n" } else { "" },
            ),
            Reply::Body("{\"data\":[{\"id\":\"model\"}]}".into()),
        ])
        .await;
        let mut cfg = config();
        cfg.endpoint = url.clone();
        cfg.list_models_endpoint = Some(url);
        cfg.retry = fast();
        cfg.timeout = Duration::from_millis(120);
        let p = OpenAIProvider::new(cfg, Arc::new(Credentials(AtomicUsize::new(0)))).unwrap();
        let result = p
            .list_models(DiscoveryRequest::default(), CancellationToken::new())
            .await;
        if delayed {
            assert_eq!(result.unwrap_err().kind, ModelErrorKind::Network);
            assert_eq!(count.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(result.unwrap().models[0].id, "model");
            assert_eq!(count.load(Ordering::SeqCst), 2);
        }
        server.abort();
    }
}
#[tokio::test]
async fn deadline_includes_discovery_http() {
    let (url, count, server) = server(vec![Reply::Stall]).await;
    let mut cfg = config();
    cfg.endpoint = url.clone();
    cfg.list_models_endpoint = Some(url);
    cfg.timeout = Duration::from_millis(50);
    let p = OpenAIProvider::new(cfg, Arc::new(Credentials(AtomicUsize::new(0)))).unwrap();
    assert_eq!(
        p.list_models(DiscoveryRequest::default(), CancellationToken::new())
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Network
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn stream_network_before_event_retries_and_resolves_again() {
    let (url, count, server) = server(vec![
        Reply::Truncated(String::new()),
        Reply::Body(success()),
    ])
    .await;
    let resolver = Arc::new(Credentials(AtomicUsize::new(0)));
    let mut cfg = config();
    cfg.endpoint = url;
    cfg.retry = fast();
    let client = Client::new(cfg, Api::Chat, resolver.clone()).unwrap();
    let (result, events) = generate(&client, CancellationToken::new()).await;
    assert!(result.is_ok());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(resolver.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, MessageStreamPayload::Start))
            .count(),
        1
    );
    server.abort();
}

#[tokio::test]
async fn discovery_cancellation_interrupts_backoff() {
    let (url, count, server) = server(vec![
        Reply::Status("429 Too Many Requests", "Retry-After: 2\r\n"),
        Reply::Body("{\"data\":[]}".into()),
    ])
    .await;
    let mut cfg = config();
    cfg.endpoint = url.clone();
    cfg.list_models_endpoint = Some(url);
    cfg.retry = fast();
    let p = OpenAIProvider::new(cfg, Arc::new(Credentials(AtomicUsize::new(0)))).unwrap();
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });
    assert_eq!(
        p.list_models(DiscoveryRequest::default(), token)
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::Cancelled
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}

#[test]
fn retry_after_and_exponential_policy() {
    let p = fast();
    assert_eq!(p.delay(0, None), Duration::from_millis(5));
    assert_eq!(p.delay(1, None), Duration::from_millis(10));
    assert_eq!(p.delay(20, None), Duration::from_millis(10));
    assert_eq!(
        p.delay(0, Some(Duration::from_secs(1))),
        Duration::from_secs(1)
    );
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
    assert_eq!(retry::retry_after(&headers), Some(Duration::from_secs(2)));
    headers.insert(
        reqwest::header::RETRY_AFTER,
        "Sun, 06 Nov 1994 08:49:37 GMT".parse().unwrap(),
    );
    assert_eq!(retry::retry_after(&headers), Some(Duration::ZERO));
    headers.insert(reqwest::header::RETRY_AFTER, "invalid".parse().unwrap());
    assert_eq!(retry::retry_after(&headers), None);
}
