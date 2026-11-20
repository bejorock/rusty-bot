use super::*;
use rusty_bot_core::ProviderPayloadHook;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

fn resolver() -> Arc<dyn CredentialResolver> {
    Arc::new(StaticCredentialResolver::new(ProviderCredential::new(
        "fake-secret",
    )))
}

#[test]
fn endpoints_refuse_remote_plaintext_credentials_query_and_fragments() {
    for endpoint in [
        "not a URL",
        "ftp://example.invalid/api",
        "http://example.invalid/api",
        "https://user:password@example.invalid/api",
        "https://@example.invalid/api",
        "https://example.invalid/api?key=private",
        "https://example.invalid/api#private",
    ] {
        let error = NativeTransport::new(endpoint, resolver()).unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
        assert_eq!(error.message, "invalid native endpoint");
        assert!(!error.retryable);
    }
}

#[test]
fn endpoints_accept_https_and_loopback_and_only_transport_adds_gemini_query() {
    for endpoint in [
        "https://example.invalid/api",
        "http://localhost/api",
        "http://127.0.0.1:1234/api",
        "http://[::1]/api",
    ] {
        let transport = NativeTransport::new(endpoint, resolver()).unwrap();
        assert!(transport.endpoint.query().is_none());
        assert_eq!(format!("{transport:?}"), "NativeTransport([REDACTED])");
    }
    let transport = NativeTransport::with_auth(
        "https://example.invalid/api",
        resolver(),
        NativeAuth::Gemini,
    )
    .unwrap();
    assert_eq!(transport.endpoint.query(), Some("alt=sse"));
    assert_eq!(
        format!(
            "{:?}",
            StaticCredentialResolver::new(ProviderCredential::new("private"))
        ),
        "StaticCredentialResolver([REDACTED])"
    );
}

#[test]
fn credential_echo_patterns_detect_wire_encodings_but_not_unrelated_text() {
    let secret = "key+\"/猫?";
    let patterns = credential_patterns(secret);
    assert_eq!(patterns.len(), 7);
    let json = serde_json::to_string(secret).unwrap();
    for wire in [
        secret.to_owned(),
        json[1..json.len() - 1].to_owned(),
        url::form_urlencoded::byte_serialize(secret.as_bytes()).collect(),
        general_purpose::STANDARD.encode(secret),
        general_purpose::URL_SAFE_NO_PAD.encode(secret),
    ] {
        assert!(contains_credential(
            format!("prefix {wire} suffix").as_bytes(),
            &patterns
        ));
    }
    assert!(!contains_credential(b"unrelated", &patterns));
    assert!(!contains_credential(b"", &patterns));
    assert!(credential_patterns("").is_empty());
}

struct Hook {
    calls: Arc<AtomicUsize>,
    candidate: Value,
    fail: bool,
}
#[async_trait::async_trait]
impl ProviderPayloadHook for Hook {
    async fn apply(
        &self,
        api: &str,
        model: &str,
        payload: Value,
        _: CancellationToken,
    ) -> Result<Value, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(api, "openai.responses.v1");
        assert_eq!(model, "fixed");
        assert_eq!(payload, json!({"model":"fixed"}));
        if self.fail {
            Err(ModelError::protocol("hook refused"))
        } else {
            Ok(self.candidate.clone())
        }
    }
}
fn hook(candidate: Value, fail: bool) -> (Arc<dyn ProviderPayloadHook>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Hook {
            calls: calls.clone(),
            candidate,
            fail,
        }),
        calls,
    )
}

#[tokio::test]
async fn payload_hook_validates_candidate_against_unchanged_original() {
    let original = json!({"model":"fixed"});
    let (valid, calls) = hook(json!({"model":"fixed","temperature":0.5}), false);
    let candidate = apply_payload_hook(
        "openai.responses.v1",
        "fixed",
        &original,
        Some(&valid),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(candidate["temperature"], 0.5);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(original, json!({"model":"fixed"}));
    let (invalid, _) = hook(json!({"model":"changed"}), false);
    assert_eq!(
        apply_payload_hook(
            "openai.responses.v1",
            "fixed",
            &original,
            Some(&invalid),
            &CancellationToken::new()
        )
        .await
        .unwrap_err()
        .kind,
        ModelErrorKind::InvalidRequest
    );
}

struct PendingHook(tokio::sync::oneshot::Sender<()>);
// A pending hook uses a Mutex only to hand off a deterministic start signal.
struct Pending(std::sync::Mutex<Option<PendingHook>>);
#[async_trait::async_trait]
impl ProviderPayloadHook for Pending {
    async fn apply(
        &self,
        _: &str,
        _: &str,
        _: Value,
        _: CancellationToken,
    ) -> Result<Value, ModelError> {
        self.0.lock().unwrap().take().unwrap().0.send(()).unwrap();
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancellation_refuses_hook_admission_and_stops_an_inflight_hook() {
    let token = CancellationToken::new();
    token.cancel();
    let (hook, calls) = hook(json!({}), false);
    assert_eq!(
        apply_payload_hook(
            "openai.responses.v1",
            "fixed",
            &json!({"model":"fixed"}),
            Some(&hook),
            &token
        )
        .await
        .unwrap_err()
        .kind,
        ModelErrorKind::Cancelled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let token = CancellationToken::new();
    let (started, ready) = tokio::sync::oneshot::channel();
    let pending: Arc<dyn ProviderPayloadHook> =
        Arc::new(Pending(std::sync::Mutex::new(Some(PendingHook(started)))));
    let original = json!({});
    let run = apply_payload_hook(
        "openai.responses.v1",
        "fixed",
        &original,
        Some(&pending),
        &token,
    );
    let cancel = async {
        ready.await.unwrap();
        token.cancel();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(run, cancel)
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind, ModelErrorKind::Cancelled);
}

#[tokio::test]
async fn absent_hook_returns_original_and_hook_errors_propagate() {
    let original = json!({"model":"fixed"});
    assert_eq!(
        apply_payload_hook(
            "openai.responses.v1",
            "fixed",
            &original,
            None,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        original
    );
    let (hook, calls) = hook(json!({}), true);
    let error = apply_payload_hook(
        "openai.responses.v1",
        "fixed",
        &original,
        Some(&hook),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, ModelErrorKind::Protocol);
    assert_eq!(error.message, "hook refused");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
