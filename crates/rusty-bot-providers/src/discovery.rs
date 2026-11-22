use crate::{invalid, protocol, Api, ProviderConfig};
use crate::{CredentialRequest, CredentialResolver, ModelError, ModelErrorKind};
use base64::{engine::general_purpose, Engine};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

/// One page only. OpenAI and DeepSeek do not support these pagination options.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiscoveryRequest {
    pub after: Option<String>,
    pub limit: Option<u32>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DiscoveredModel {
    pub id: String,
    pub display_name: Option<String>,
    /// The server-reported maximum input tokens, not the context window.
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    /// Provider metadata, including capabilities, as returned by the service.
    pub raw: Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelPage {
    pub models: Vec<DiscoveredModel>,
    pub next_cursor: Option<String>,
}

pub(crate) fn validate_url(s: &str) -> Result<Url, ModelError> {
    let url = Url::parse(s).map_err(|_| invalid("invalid native endpoint"))?;
    let loopback = match url.host() {
        Some(Host::Ipv4(a)) => a.is_loopback(),
        Some(Host::Ipv6(a)) => a.is_loopback(),
        Some(Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        None => false,
    };
    if !matches!(url.scheme(), "http" | "https")
        || (url.scheme() == "http" && !loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || s.split("://")
            .nth(1)
            .and_then(|s| s.split(['/', '?', '#']).next())
            .is_some_and(|a| a.contains('@'))
    {
        return Err(invalid("invalid native endpoint"));
    }
    Ok(url)
}

pub(crate) struct Discovery {
    url: Option<Url>,
    api: Api,
    model: String,
    timeout: Duration,
    retry: crate::RetryPolicy,
    resolver: Arc<dyn CredentialResolver>,
    client: reqwest::Client,
}
impl Discovery {
    pub fn new(
        config: &ProviderConfig,
        api: Api,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        let endpoint = validate_url(&config.endpoint)?;
        let suffix = match api {
            Api::Responses => "/responses",
            Api::Chat | Api::Deepseek => "/chat/completions",
            Api::Anthropic => "/messages",
        };
        let url = if let Some(override_url) = &config.list_models_endpoint {
            Some(validate_url(override_url)?)
        } else if let Some(prefix) = endpoint.path().strip_suffix(suffix) {
            let mut url = endpoint.clone();
            url.set_path(&format!("{prefix}/models"));
            Some(url)
        } else {
            None
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                ModelError::new(
                    ModelErrorKind::Network,
                    "discovery transport unavailable",
                    true,
                )
            })?;
        Ok(Self {
            url,
            api,
            model: config.model.clone(),
            timeout: config.timeout,
            retry: config.retry.clone(),
            resolver,
            client,
        })
    }
    pub async fn list(
        &self,
        request: DiscoveryRequest,
        cancel: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| invalid("provider timeout is too large"))?;
        let Some(mut url) = self.url.clone() else {
            return Err(invalid("model discovery endpoint required"));
        };
        if self.api != Api::Anthropic && (request.after.is_some() || request.limit.is_some()) {
            return Err(invalid("pagination unsupported for provider"));
        }
        if request
            .after
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 1024)
            || request.limit.is_some_and(|n| !(1..=1000).contains(&n))
        {
            return Err(invalid("invalid discovery pagination"));
        }
        if request.after.is_some() || request.limit.is_some() {
            let mut query = url.query_pairs_mut();
            if let Some(after) = &request.after {
                query.append_pair("after_id", after);
            }
            if let Some(limit) = request.limit {
                query.append_pair("limit", &limit.to_string());
            }
        }
        let child = cancel.child_token();
        let result = tokio::select! { biased; _ = cancel.cancelled() => Err(ModelError::cancelled()), r = tokio::time::timeout_at(deadline, self.retry_fetch(url, child.clone(), request.after.as_deref(), deadline)) => r.unwrap_or_else(|_| Err(ModelError::new(ModelErrorKind::Network, "provider operation timed out", false))) };
        child.cancel();
        result
    }
    async fn retry_fetch(
        &self,
        url: Url,
        cancel: CancellationToken,
        after: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<ModelPage, ModelError> {
        for retry in 0..=self.retry.retries {
            if cancel.is_cancelled() {
                return Err(ModelError::cancelled());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ModelError::new(
                    ModelErrorKind::Network,
                    "provider operation timed out",
                    false,
                ));
            }
            match self.fetch(url.clone(), cancel.clone(), after).await {
                Ok(page) => return Ok(page),
                Err(error) if retry < self.retry.retries && error.retryable() => {
                    crate::retry::sleep(self.retry.delay(retry, error.retry_after)).await;
                }
                Err(error) => return Err(error.error),
            }
        }
        unreachable!()
    }
    async fn fetch(
        &self,
        url: Url,
        cancel: CancellationToken,
        after: Option<&str>,
    ) -> Result<ModelPage, crate::retry::AttemptError> {
        let request = CredentialRequest {
            provider: self.api.provider().into(),
            model: self.model.clone(),
        };
        let credential = self
            .resolver
            .resolve(&request, cancel.clone())
            .await
            .map_err(|_| {
                ModelError::new(ModelErrorKind::Auth, "credential resolution failed", false)
            })?
            .filter(|c| !c.expose_secret().is_empty())
            .ok_or_else(|| {
                ModelError::new(ModelErrorKind::Auth, "credential unavailable", false)
            })?;
        let secret = credential.expose_secret();
        let mut patterns = vec![
            secret.to_owned(),
            serde_json::to_string(secret)
                .map_err(|_| protocol())?
                .trim_matches('"')
                .to_owned(),
            url::form_urlencoded::byte_serialize(secret.as_bytes()).collect(),
            general_purpose::STANDARD.encode(secret),
            general_purpose::URL_SAFE.encode(secret),
            general_purpose::STANDARD_NO_PAD.encode(secret),
            general_purpose::URL_SAFE_NO_PAD.encode(secret),
        ];
        patterns.retain(|s| !s.is_empty());
        if url
            .query()
            .is_some_and(|query| patterns.iter().any(|p| query.contains(p)))
        {
            return Err(invalid("credential in discovery query").into());
        }
        let builder = self.client.get(url);
        let builder = if self.api == Api::Anthropic {
            builder
                .header("x-api-key", secret)
                .header("anthropic-version", "2023-06-01")
        } else {
            builder.bearer_auth(secret)
        };
        let mut response = builder.send().await.map_err(|_| {
            ModelError::new(ModelErrorKind::Network, "discovery request failed", true)
        })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retry_after = crate::retry::retry_after(response.headers());
            let byte_patterns: Vec<Vec<u8>> =
                patterns.iter().map(|p| p.as_bytes().to_vec()).collect();
            let request_id = crate::error::request_id(response.headers(), &byte_patterns);
            let body = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ModelError::cancelled().into()),
                body = crate::error::body(&mut response) => body,
            };
            let mut error = crate::error::classify(Some(status), body.as_ref(), &byte_patterns);
            error.provider_request_id = request_id;
            return Err(crate::retry::AttemptError { error, retry_after });
        }
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            ModelError::new(ModelErrorKind::Network, "discovery response failed", true)
        })? {
            if data.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
                return Err(protocol().into());
            }
            data.extend_from_slice(&chunk);
        }
        if patterns
            .iter()
            .any(|p| data.windows(p.len()).any(|w| w == p.as_bytes()))
        {
            return Err(ModelError::protocol("discovery response contained credential").into());
        }
        let value: Value = serde_json::from_slice(&data).map_err(|_| protocol())?;
        fn check(v: &Value, patterns: &[String], depth: usize) -> bool {
            if depth > 32 {
                return false;
            }
            match v {
                Value::String(s) => s.len() <= 16384 && !patterns.iter().any(|p| s.contains(p)),
                Value::Array(a) => {
                    a.len() <= 10000 && a.iter().all(|v| check(v, patterns, depth + 1))
                }
                Value::Object(o) => {
                    o.len() <= 10000
                        && o.iter().all(|(k, v)| {
                            k.len() <= 16384
                                && !patterns.iter().any(|p| k.contains(p))
                                && check(v, patterns, depth + 1)
                        })
                }
                _ => true,
            }
        }
        if !check(&value, &patterns, 0) {
            return Err(protocol().into());
        }
        let entries = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(protocol)?;
        if entries.len() > 10000 {
            return Err(protocol().into());
        }
        let mut models = Vec::with_capacity(entries.len());
        let mut ids = std::collections::HashSet::new();
        for item in entries {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && s.len() <= 1024)
                .ok_or_else(protocol)?;
            if !ids.insert(id) {
                return Err(protocol().into());
            }
            let display_name = optional_string(item, "display_name")?;
            let max_input_tokens = optional_number(item, "max_input_tokens")?;
            let max_output_tokens = optional_number(item, "max_tokens")?;
            models.push(DiscoveredModel {
                id: id.to_owned(),
                display_name,
                max_input_tokens,
                max_output_tokens,
                raw: item.clone(),
            });
        }
        let next_cursor = if self.api == Api::Anthropic {
            let last_id = optional_string(&value, "last_id")?;
            match value.get("has_more").and_then(Value::as_bool) {
                Some(true) => {
                    let last_id = last_id.filter(|s| !s.is_empty()).ok_or_else(protocol)?;
                    if models.last().map(|model| model.id.as_str()) != Some(last_id.as_str())
                        || after == Some(last_id.as_str())
                    {
                        return Err(protocol().into());
                    }
                    Some(last_id)
                }
                Some(false) => None,
                None => return Err(protocol().into()),
            }
        } else {
            None
        };
        Ok(ModelPage {
            models,
            next_cursor,
        })
    }
}
fn optional_string(value: &Value, key: &str) -> Result<Option<String>, ModelError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.len() <= 1024 => Ok(Some(s.clone())),
        _ => Err(protocol()),
    }
}
fn optional_number(value: &Value, key: &str) -> Result<Option<u64>, ModelError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(Some).ok_or_else(protocol),
    }
}
