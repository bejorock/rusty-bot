//! Classification of provider failures without exposing provider-supplied messages.
use crate::{ModelError, ModelErrorKind};
use serde_json::Value;

const MAX_ERROR_BYTES: usize = 64 * 1024;

/// Read at most a bounded response body. Oversized, malformed and interrupted
/// bodies are deliberately treated as unknown rather than echoed.
pub(crate) async fn body(response: &mut reqwest::Response) -> Option<Value> {
    let mut data = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if data.len().checked_add(chunk.len())? > MAX_ERROR_BYTES {
                    return None;
                }
                data.extend_from_slice(&chunk);
            }
            Ok(None) => return serde_json::from_slice(&data).ok(),
            Err(_) => return None,
        }
    }
}

pub(crate) fn request_id(
    headers: &reqwest::header::HeaderMap,
    patterns: &[Vec<u8>],
) -> Option<String> {
    headers
        .get("x-request-id")
        .or_else(|| headers.get("request-id"))?
        .to_str()
        .ok()
        .filter(|v| {
            !v.is_empty()
                && v.len() <= 128
                && v.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
        .filter(|v| {
            !patterns
                .iter()
                .any(|p| !p.is_empty() && v.as_bytes().windows(p.len()).any(|w| w == p))
        })
        .map(str::to_owned)
}

// Only explicitly recognized identifiers are retained: arbitrary provider data
// may contain a credential even if it looks syntactically like an error code.
fn known(code: &str) -> Option<(ModelErrorKind, bool)> {
    use ModelErrorKind::*;
    Some(match code {
        "insufficient_quota" | "quota_exceeded" | "resource_exhausted" => (QuotaExceeded, false),
        "billing_error" | "billing_not_active" | "insufficient_balance" | "payment_required" => {
            (Billing, false)
        }
        "model_not_found" => (ModelNotFound, false),
        "context_length_exceeded" | "context_window_exceeded" | "prompt_too_long" => {
            (ContextOverflow, false)
        }
        "permission_error" | "permission_denied" | "forbidden" => (Permission, false),
        "authentication_error" | "invalid_api_key" => (Auth, false),
        "rate_limit_error" | "rate_limit_exceeded" | "rate_limit_reached" => (RateLimited, true),
        "overloaded_error" | "server_error" | "api_error" | "internal_error" => (Server, true),
        "invalid_request_error" => (InvalidRequest, false),
        _ => return None,
    })
}

pub(crate) fn classify(
    status: Option<u16>,
    value: Option<&Value>,
    patterns: &[Vec<u8>],
) -> ModelError {
    use ModelErrorKind::*;
    let nested = value
        .and_then(|v| v.get("error"))
        .filter(|v| v.is_object())
        .or(value);
    // Prefer a specific code over a broad provider type. If the code is unknown,
    // a recognized type still supplies a useful category.
    let recognized = nested.and_then(|v| {
        ["code", "type"]
            .into_iter()
            .filter_map(|k| v.get(k).and_then(Value::as_str))
            .find_map(|s| known(s).map(|k| (s, k)))
    });
    let (kind, retryable) = recognized.map(|(_, k)| k).unwrap_or_else(|| match status {
        Some(401) => (Auth, false),
        Some(402) => (Billing, false),
        Some(403) => (Permission, false),
        Some(429) => (RateLimited, true),
        Some(413) => (InvalidRequest, false),
        Some(400..=499) => (InvalidRequest, false),
        Some(500..=599) => (Server, true),
        _ => (Protocol, false),
    });
    let mut error = ModelError::new(kind, "native provider request failed", retryable);
    error.status = status;
    error.provider_code = recognized
        .map(|(s, _)| s)
        .filter(|s| {
            !patterns
                .iter()
                .any(|p| !p.is_empty() && s.as_bytes().windows(p.len()).any(|w| w == p))
        })
        .map(str::to_owned);
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    async fn server(body: String, request_id: &str) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let request_id = request_id.to_owned();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let _ = socket.read(&mut buf).await.unwrap();
            let header = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nx-request-id: {}\r\nConnection: close\r\n\r\n",
                body.len(),
                request_id
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            let _ = socket.write_all(body.as_bytes()).await;
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn transport_and_discovery_use_bounded_structured_errors() {
        use crate::ProviderCredential;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;
        let resolver = Arc::new(crate::StaticCredentialResolver::new(
            ProviderCredential::new("offline-secret"),
        ));
        let (base, task) = server(
            json!({"error":{"code":"insufficient_quota","message":"offline-secret"}}).to_string(),
            "req_123",
        )
        .await;
        let transport = crate::transport::NativeTransport::with_auth(
            &base,
            resolver.clone(),
            crate::transport::NativeAuth::Bearer,
        )
        .unwrap();
        let error = match transport
            .post("openai", "m", &json!({}), CancellationToken::new())
            .await
        {
            Err(e) => e.error,
            Ok(_) => panic!("expected error"),
        };
        task.await.unwrap();
        assert_eq!(error.kind, ModelErrorKind::QuotaExceeded);
        assert!(!error.retryable);
        assert_eq!(error.status, Some(429));
        assert_eq!(error.provider_request_id.as_deref(), Some("req_123"));
        assert!(!format!("{error:?}").contains("offline-secret"));
        let (base, task) = server(
            json!({"error":{"code":"model_not_found"}}).to_string(),
            "offline-secret",
        )
        .await;
        let config = crate::ProviderConfig::new(format!("{base}/responses"), "m");
        let discovery =
            crate::discovery::Discovery::new(&config, crate::Api::Responses, resolver).unwrap();
        let error = discovery
            .list(crate::DiscoveryRequest::default(), CancellationToken::new())
            .await
            .unwrap_err();
        task.await.unwrap();
        assert_eq!(error.kind, ModelErrorKind::ModelNotFound);
        assert_eq!(error.provider_request_id, None);
    }

    #[tokio::test]
    async fn oversized_and_malformed_error_bodies_fall_back() {
        for body in ["x".repeat(MAX_ERROR_BYTES + 1), "{not json".to_owned()] {
            let (endpoint, task) = server(body, "req_safe").await;
            let mut response = reqwest::get(endpoint).await.unwrap();
            let parsed = super::body(&mut response).await;
            assert!(parsed.is_none());
            let error = classify(Some(429), parsed.as_ref(), &[]);
            assert_eq!(error.kind, ModelErrorKind::RateLimited);
            assert!(error.retryable);
            assert!(error.provider_code.is_none());
            task.await.unwrap();
        }
    }

    #[test]
    fn stream_errors_are_classified_for_all_protocols() {
        for (api, event) in [
            (
                crate::Api::Chat,
                json!({"error":{"code":"insufficient_quota","message":"sensitive"}}),
            ),
            (
                crate::Api::Deepseek,
                json!({"error":{"code":"insufficient_quota"}}),
            ),
            (
                crate::Api::Anthropic,
                json!({"type":"error","error":{"type":"permission_error"}}),
            ),
            (
                crate::Api::Responses,
                json!({"type":"response.failed","response":{"error":{"code":"model_not_found"}}}),
            ),
            (
                crate::Api::Responses,
                json!({"type":"error","code":"context_length_exceeded"}),
            ),
        ] {
            let mut decoder = crate::wire::Decoder::new(api, "model", "id");
            let error = decoder.push(&event.to_string()).unwrap_err();
            assert!(matches!(
                error.kind,
                ModelErrorKind::QuotaExceeded
                    | ModelErrorKind::Permission
                    | ModelErrorKind::ModelNotFound
                    | ModelErrorKind::ContextOverflow
            ));
            assert!(!error.retryable);
            assert!(!format!("{error:?}").contains("sensitive"));
        }
    }

    #[test]
    fn classifies_without_echoing_untrusted_fields() {
        for (code, kind, retry) in [
            ("insufficient_quota", ModelErrorKind::QuotaExceeded, false),
            ("billing_error", ModelErrorKind::Billing, false),
            ("model_not_found", ModelErrorKind::ModelNotFound, false),
            (
                "context_length_exceeded",
                ModelErrorKind::ContextOverflow,
                false,
            ),
            ("permission_error", ModelErrorKind::Permission, false),
            ("rate_limit_exceeded", ModelErrorKind::RateLimited, true),
        ] {
            let error = classify(
                Some(429),
                Some(&json!({"error":{"code":code,"message":"secret"}})),
                &[],
            );
            assert_eq!(
                (error.kind, error.retryable, error.status),
                (kind, retry, Some(429))
            );
            assert_eq!(error.provider_code.as_deref(), Some(code));
            assert!(!error.message.contains("secret"));
        }
        let error = classify(
            Some(400),
            Some(&json!({"error":{"code":"secret","message":"secret"}})),
            &[],
        );
        assert_eq!(error.provider_code, None);
        assert!(!format!("{error:?}").contains("secret"));
        let error = classify(
            Some(429),
            Some(&json!({"error":{"code":"insufficient_quota"}})),
            &[b"quota".to_vec()],
        );
        assert_eq!(error.provider_code, None);
    }
}
