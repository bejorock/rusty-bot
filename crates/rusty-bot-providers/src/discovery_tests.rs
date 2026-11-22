use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server(
    status: &'static str,
    body: String,
    delay: bool,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/proxy/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&buf[..n]);
            if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        if delay {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let header = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = socket.write_all(header.as_bytes()).await;
        let _ = socket.write_all(body.as_bytes()).await;
        String::from_utf8(bytes).unwrap()
    });
    (base, task)
}
fn key() -> ProviderCredential {
    ProviderCredential::new("private-token")
}
#[tokio::test]
async fn openai_get_and_anthropic_paging() {
    let (base, task) = server("200 OK", r#"{"data":[{"id":"one"}]}"#.into(), false).await;
    let mut c = base_config(&base, "m", "responses").unwrap();
    let p = OpenAIProvider::with_api_key(c.clone(), key()).unwrap();
    let page = p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(page.models[0].id, "one");
    assert!(page.models[0].max_input_tokens.is_none());
    assert!(page.next_cursor.is_none());
    let got = task.await.unwrap().to_lowercase();
    assert!(got.starts_with("get /proxy/v1/models http/1.1"));
    assert!(got.contains("authorization: bearer private-token"));
    assert!(p
        .list_models(
            DiscoveryRequest {
                after: None,
                limit: Some(1)
            },
            CancellationToken::new()
        )
        .await
        .is_err());
    let (base, task) = server("200 OK", r#"{"data":[{"id":"claude","display_name":"Claude","max_input_tokens":100,"max_tokens":10,"capabilities":{"thinking":true}}],"has_more":true,"last_id":"claude"}"#.into(), false).await;
    c.endpoint = format!("{base}/messages");
    c.list_models_endpoint = None;
    let p = AnthropicProvider::with_api_key(c, key()).unwrap();
    let page = p
        .list_models(
            DiscoveryRequest {
                after: Some("prev".into()),
                limit: Some(5),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(page.next_cursor.as_deref(), Some("claude"));
    assert_eq!(page.models[0].max_input_tokens, Some(100));
    let got = task.await.unwrap().to_lowercase();
    assert!(got.starts_with("get /proxy/v1/models?after_id=prev&limit=5 "));
    assert!(got.contains("x-api-key: private-token"));
    assert!(got.contains("anthropic-version: 2023-06-01"));
}
#[tokio::test]
async fn discovery_errors_cancellation_and_echo() {
    for (status, body) in [
        ("200 OK", "not json"),
        ("401 Unauthorized", "private-token"),
        ("200 OK", r#"{"data":[{"id":"private-token"}]}"#),
        ("200 OK", r#"{"data":[{"id":"private\u002dtoken"}]}"#),
    ] {
        let (base, task) = server(status, body.into(), false).await;
        let p = OpenAIProvider::with_api_key(base_config(&base, "m", "responses").unwrap(), key())
            .unwrap();
        let err = p
            .list_models(DiscoveryRequest::default(), CancellationToken::new())
            .await
            .unwrap_err();
        assert!(!format!("{err:?}").contains("private-token"));
        task.await.unwrap();
    }
    let (base, task) = server("200 OK", r#"{"data":[]}"#.into(), true).await;
    let mut c = base_config(&base, "m", "responses").unwrap();
    c.timeout = Duration::from_millis(20);
    let p = OpenAIProvider::with_api_key(c, key()).unwrap();
    assert!(p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .is_err());
    task.abort();
    let token = CancellationToken::new();
    token.cancel();
    assert!(p
        .list_models(DiscoveryRequest::default(), token)
        .await
        .is_err());
    let p =
        OpenAIProvider::with_api_key(ProviderConfig::new("http://127.0.0.1:1/custom", "m"), key())
            .unwrap();
    assert!(p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .is_err());
}

#[tokio::test]
async fn override_redirect_limits_and_query_secret_guard() {
    let (base, task) = server(
        "200 OK",
        r#"{"data":[],"has_more":false,"last_id":null}"#.into(),
        false,
    )
    .await;
    let mut c = ProviderConfig::new("http://127.0.0.1:1/custom", "m");
    c.list_models_endpoint = Some(format!("{base}/custom-models"));
    let p = AnthropicProvider::with_api_key(c, key()).unwrap();
    let page = p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .unwrap();
    assert!(page.models.is_empty());
    assert!(task
        .await
        .unwrap()
        .starts_with("GET /proxy/v1/custom-models "));
    let (base, task) = server("302 Found", "private-token".into(), false).await;
    let p =
        DeepseekProvider::with_api_key(base_config(&base, "m", "chat/completions").unwrap(), key())
            .unwrap();
    assert!(p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .is_err());
    task.await.unwrap();
    let (base, task) = server(
        "200 OK",
        format!(
            "{{\"data\":[{{\"id\":\"{}\"}}]}}",
            "x".repeat(4 * 1024 * 1024)
        ),
        false,
    )
    .await;
    let p =
        OpenAIProvider::with_api_key(base_config(&base, "m", "responses").unwrap(), key()).unwrap();
    assert!(p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .is_err());
    task.await.unwrap();
    let p = AnthropicProvider::with_api_key(
        ProviderConfig::new("http://127.0.0.1:1/messages", "m"),
        key(),
    )
    .unwrap();
    assert!(p
        .list_models(
            DiscoveryRequest {
                after: Some("private-token".into()),
                limit: None
            },
            CancellationToken::new()
        )
        .await
        .is_err());
}

#[tokio::test]
async fn deepseek_metadata_and_encoded_proxy_prefix() {
    let (base, task) = server(
        "200 OK",
        r#"{"data":[{"id":"deepseek-reasoner","owned_by":"deepseek","extra":{"value":42}}]}"#
            .into(),
        false,
    )
    .await;
    let endpoint = format!("{base}/tenant%2Fone/chat/completions");
    let p = DeepseekProvider::with_api_key(ProviderConfig::new(endpoint, "caller-model"), key())
        .unwrap();
    let page = p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(page.models.len(), 1);
    assert_eq!(page.models[0].id, "deepseek-reasoner");
    assert_eq!(
        page.models[0].raw,
        serde_json::json!({"id":"deepseek-reasoner","owned_by":"deepseek","extra":{"value":42}})
    );
    assert_eq!(page.models[0].max_input_tokens, None);
    assert_eq!(page.models[0].max_output_tokens, None);
    assert!(task
        .await
        .unwrap()
        .starts_with("GET /proxy/v1/tenant%2Fone/models "));
}

#[tokio::test]
async fn anthropic_malformed_pagination_and_duplicate_ids() {
    for (response, after) in [
        (r#"{"data":[],"has_more":"true","last_id":"x"}"#, None),
        (
            r#"{"data":[{"id":"x"}],"has_more":true,"last_id":null}"#,
            None,
        ),
        (r#"{"data":[],"has_more":true,"last_id":"x"}"#, None),
        (
            r#"{"data":[{"id":"x"}],"has_more":true,"last_id":"y"}"#,
            None,
        ),
        (
            r#"{"data":[{"id":"x"}],"has_more":true,"last_id":"x"}"#,
            Some("x"),
        ),
        (
            r#"{"data":[{"id":"x"}],"has_more":false,"last_id":42}"#,
            None,
        ),
        (
            r#"{"data":[{"id":"x"},{"id":"x"}],"has_more":false,"last_id":"x"}"#,
            None,
        ),
    ] {
        let (base, task) = server("200 OK", response.into(), false).await;
        let p =
            AnthropicProvider::with_api_key(base_config(&base, "m", "messages").unwrap(), key())
                .unwrap();
        let err = p
            .list_models(
                DiscoveryRequest {
                    after: after.map(str::to_owned),
                    limit: None,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, ModelErrorKind::Protocol, "{response}");
        task.await.unwrap();
    }
}

struct HangingResolver(tokio::sync::Notify);
#[async_trait]
impl CredentialResolver for HangingResolver {
    async fn resolve(
        &self,
        _: &crate::CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        self.0.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn discovery_timeout_and_cancel_during_resolution() {
    let resolver = Arc::new(HangingResolver(tokio::sync::Notify::new()));
    let mut c = ProviderConfig::new("http://127.0.0.1:1/responses", "private-model");
    c.timeout = Duration::from_millis(20);
    let p = OpenAIProvider::new(c, resolver.clone()).unwrap();
    let e = p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.kind, ModelErrorKind::Network);
    assert_eq!(e.message, "provider operation timed out");
    assert!(!format!("{e:?}").contains("private-model"));
    // Consume the notification from the timed-out invocation before checking cancellation.
    resolver.0.notified().await;
    let cancel = CancellationToken::new();
    let child = cancel.clone();
    let r = resolver.clone();
    let task = tokio::spawn(async move {
        r.0.notified().await;
        child.cancel();
    });
    let e = p
        .list_models(DiscoveryRequest::default(), cancel)
        .await
        .unwrap_err();
    assert_eq!(e.kind, ModelErrorKind::Cancelled);
    assert!(!format!("{e:?}").contains("private-model"));
    task.await.unwrap();
}

struct FailingResolver;
#[async_trait]
impl CredentialResolver for FailingResolver {
    async fn resolve(
        &self,
        _: &crate::CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        Err(ModelError::new(
            ModelErrorKind::Auth,
            "private-token resolver detail",
            false,
        ))
    }
}

#[tokio::test]
async fn discovery_resolver_errors_are_sanitized() {
    let p = OpenAIProvider::new(
        ProviderConfig::new("http://127.0.0.1:1/responses", "m"),
        Arc::new(FailingResolver),
    )
    .unwrap();
    let err = p
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ModelErrorKind::Auth);
    assert_eq!(err.message, "credential resolution failed");
    assert!(!format!("{err:?}").contains("private-token"));
}
