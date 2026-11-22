use crate::retry::{retry_after, AttemptError};
use std::sync::Arc;

use crate::{CredentialRequest, CredentialResolver, ModelError, ModelErrorKind};
use base64::{engine::general_purpose, Engine as _};
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

/// Transport for native bearer-authenticated APIs. No custom headers or
/// redirects are permitted. An HTTP endpoint is accepted only on loopback.
pub struct NativeTransport {
    endpoint: Url,
    client: reqwest::Client,
    resolver: Arc<dyn CredentialResolver>,
    auth: NativeAuth,
}

#[derive(Clone, Copy)]
pub enum NativeAuth {
    Bearer,
    Anthropic,
}

impl std::fmt::Debug for NativeTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeTransport([REDACTED])")
    }
}

fn invalid_endpoint() -> ModelError {
    ModelError::new(
        ModelErrorKind::InvalidRequest,
        "invalid native endpoint",
        false,
    )
}

impl NativeTransport {
    pub fn with_auth(
        endpoint: &str,
        resolver: Arc<dyn CredentialResolver>,
        auth: NativeAuth,
    ) -> Result<Self, ModelError> {
        let url = Url::parse(endpoint).map_err(|_| invalid_endpoint())?;
        let loopback = match url.host() {
            Some(Host::Ipv4(addr)) => addr.is_loopback(),
            Some(Host::Ipv6(addr)) => addr.is_loopback(),
            Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            None => false,
        };
        if !matches!(url.scheme(), "http" | "https")
            || (url.scheme() == "http" && !loopback)
            || !url.username().is_empty() || url.password().is_some()
            || url.query().is_some() || url.fragment().is_some()
            // The URL parser normalizes an empty userinfo away.
            || endpoint.split("://").nth(1).and_then(|s| s.split(['/', '?', '#']).next()).is_some_and(|authority| authority.contains('@'))
        {
            return Err(invalid_endpoint());
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                ModelError::new(
                    ModelErrorKind::Network,
                    "native transport unavailable",
                    true,
                )
            })?;
        Ok(Self {
            endpoint: url,
            client,
            resolver,
            auth,
        })
    }

    /// Resolve a fresh credential and perform one POST. Resolver/transport error
    /// details and response bodies are deliberately never copied into errors.
    pub async fn post(
        &self,
        provider: &str,
        model: &str,
        body: &serde_json::Value,
        cancel: CancellationToken,
    ) -> Result<NativeResponse, AttemptError> {
        self.request(
            provider,
            model,
            reqwest::Method::POST,
            self.endpoint.clone(),
            Some((
                "application/json",
                serde_json::to_vec(body).map_err(|_| ModelError::protocol("invalid request"))?,
            )),
            cancel,
        )
        .await
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub(crate) async fn request(
        &self,
        provider: &str,
        model: &str,
        method: reqwest::Method,
        url: Url,
        body: Option<(&str, Vec<u8>)>,
        cancel: CancellationToken,
    ) -> Result<NativeResponse, AttemptError> {
        if url.origin() != self.endpoint.origin()
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid_endpoint().into());
        }
        let request = CredentialRequest {
            provider: provider.to_owned(),
            model: model.to_owned(),
        };
        let credential = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ModelError::cancelled().into()),
            result = self.resolver.resolve(&request, cancel.child_token()) => result.map_err(|_| ModelError::new(ModelErrorKind::Auth, "credential resolution failed", false))?,
        }.filter(|credential| !credential.expose_secret().is_empty())
            .ok_or_else(|| ModelError::new(ModelErrorKind::Auth, "credential unavailable", false))?;
        let patterns = credential_patterns(credential.expose_secret());
        let builder = self.client.request(method, url);
        let builder = match self.auth {
            NativeAuth::Bearer => builder.bearer_auth(credential.expose_secret()),
            NativeAuth::Anthropic => builder
                .header("x-api-key", credential.expose_secret())
                .header("anthropic-version", "2023-06-01"),
        };
        let builder = if let Some((content_type, body)) = body {
            builder.header("content-type", content_type).body(body)
        } else {
            builder
        };
        let mut response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ModelError::cancelled().into()),
            result = builder.send() => result.map_err(|_| ModelError::new(ModelErrorKind::Network, "native transport request failed", true))?,
        };
        let status = response.status().as_u16();
        let request_id = crate::error::request_id(response.headers(), &patterns);
        if !response.status().is_success() {
            let retry_after = retry_after(response.headers());
            let body = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ModelError::cancelled().into()),
                body = crate::error::body(&mut response) => body,
            };
            let mut error = crate::error::classify(Some(status), body.as_ref(), &patterns);
            error.provider_request_id = request_id;
            return Err(AttemptError { error, retry_after });
        }
        Ok(NativeResponse {
            response,
            request_id,
            patterns,
            pending: Vec::new(),
            done: false,
            cancel,
            bytes_read: 0,
        })
    }
}

fn credential_patterns(secret: &str) -> Vec<Vec<u8>> {
    let json = serde_json::to_string(secret).expect("string serialization cannot fail");
    let encoded = url::form_urlencoded::byte_serialize(secret.as_bytes()).collect::<String>();
    let patterns = [
        secret.to_owned(),
        json[1..json.len() - 1].to_owned(),
        encoded,
        general_purpose::STANDARD.encode(secret),
        general_purpose::URL_SAFE.encode(secret),
        general_purpose::STANDARD_NO_PAD.encode(secret),
        general_purpose::URL_SAFE_NO_PAD.encode(secret),
    ];
    patterns
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(String::into_bytes)
        .collect()
}

fn contains_credential(data: &[u8], patterns: &[Vec<u8>]) -> bool {
    patterns
        .iter()
        .any(|pattern| data.windows(pattern.len()).any(|window| window == pattern))
}

/// A response with bounded credential-echo detection across arbitrary chunks.
/// It intentionally does not expose headers, URL, or raw reqwest errors.
pub struct NativeResponse {
    response: reqwest::Response,
    request_id: Option<String>,
    patterns: Vec<Vec<u8>>,
    pending: Vec<u8>,
    done: bool,
    cancel: CancellationToken,
    bytes_read: usize,
}

impl std::fmt::Debug for NativeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeResponse([REDACTED])")
    }
}

impl NativeResponse {
    pub(crate) fn check_value(&self, value: &serde_json::Value) -> Result<(), ModelError> {
        fn visit(value: &serde_json::Value, patterns: &[Vec<u8>]) -> bool {
            match value {
                serde_json::Value::String(s) => contains_credential(s.as_bytes(), patterns),
                serde_json::Value::Array(a) => a.iter().any(|v| visit(v, patterns)),
                serde_json::Value::Object(o) => o.iter().any(|(k, v)| {
                    contains_credential(k.as_bytes(), patterns) || visit(v, patterns)
                }),
                _ => false,
            }
        }
        if visit(value, &self.patterns) {
            Err(ModelError::protocol("native response contained credential"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_delta(&self, suffix: &mut String, delta: &str) -> Result<(), ModelError> {
        suffix.push_str(delta);
        if contains_credential(suffix.as_bytes(), &self.patterns) {
            return Err(ModelError::protocol("native response contained credential"));
        }
        let hold = self.patterns.iter().map(Vec::len).max().unwrap_or(1) - 1;
        let mut start = suffix.len().saturating_sub(hold);
        while !suffix.is_char_boundary(start) {
            start -= 1;
        }
        suffix.drain(..start);
        Ok(())
    }

    pub fn provider_request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns bytes only after checking for complete credential encodings,
    /// retaining a suffix so an encoding cannot straddle released chunks.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, ModelError> {
        if self.done {
            return Ok(None);
        }
        loop {
            let chunk = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(ModelError::cancelled()),
                chunk = self.response.chunk() => chunk.map_err(|_| ModelError::new(ModelErrorKind::Network, "native response read failed", true))?,
            };
            match chunk {
                Some(bytes) => {
                    self.bytes_read = self.bytes_read.saturating_add(bytes.len());
                    if self.bytes_read > 16 * 1024 * 1024 {
                        self.done = true;
                        return Err(ModelError::protocol("native response too large"));
                    }
                    self.pending.extend_from_slice(&bytes);
                    if contains_credential(&self.pending, &self.patterns) {
                        self.done = true;
                        self.pending.clear();
                        return Err(ModelError::protocol("native response contained credential"));
                    }
                    let hold = self.patterns.iter().map(Vec::len).max().unwrap_or(1) - 1;
                    if self.pending.len() > hold {
                        let rest = self.pending.split_off(self.pending.len() - hold);
                        return Ok(Some(std::mem::replace(&mut self.pending, rest)));
                    }
                }
                None => {
                    self.done = true;
                    if self.pending.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(std::mem::take(&mut self.pending)));
                }
            }
        }
    }
}
