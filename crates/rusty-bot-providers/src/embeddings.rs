//! Text-only embeddings, independent of generation model configuration.
use crate::transport::{NativeAuth, NativeTransport};
use crate::{
    retry, AnthropicProvider, CredentialResolver, DeepseekProvider, ModelError, ModelErrorKind,
    OpenAIProvider, ProviderCredential, RetryPolicy,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const MAX_BYTES: usize = 16 * 1024 * 1024;

/// Inputs are sent verbatim; token and model-specific limits are enforced by the server.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub input: Vec<String>,
    pub dimensions: Option<u32>,
    pub user: Option<String>,
}
impl fmt::Debug for EmbeddingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EmbeddingRequest([REDACTED])")
    }
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Embedding {
    pub index: u32,
    pub vector: Vec<f32>,
}
impl fmt::Debug for Embedding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Embedding")
            .field("index", &self.index)
            .field("vector", &"[REDACTED]")
            .finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingUsage {
    pub input_tokens: u64,
    pub total_tokens: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingResponse {
    pub model: String,
    pub embeddings: Vec<Embedding>,
    pub usage: EmbeddingUsage,
    pub provider_request_id: Option<String>,
}

/// Complete embedding POST endpoint and embedding model, separate from chat settings.
#[derive(Clone)]
pub struct EmbeddingConfig {
    pub endpoint: String,
    pub model: String,
    pub timeout: Duration,
    pub retry: RetryPolicy,
}
impl fmt::Debug for EmbeddingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EmbeddingConfig([REDACTED])")
    }
}
impl EmbeddingConfig {
    pub fn new(endpoint: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            model: model.into(),
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
        }
    }
}

/// Offline request translation and asynchronous embedding, without generation or persistence.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    fn build_embedding_request(&self, request: &EmbeddingRequest) -> Result<Value, ModelError>;
    async fn embed(
        &self,
        request: &EmbeddingRequest,
        cancel: CancellationToken,
    ) -> Result<EmbeddingResponse, ModelError>;
}

pub struct OpenAIEmbeddingProvider {
    config: EmbeddingConfig,
    transport: NativeTransport,
}
impl fmt::Debug for OpenAIEmbeddingProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenAIEmbeddingProvider([REDACTED])")
    }
}
impl OpenAIEmbeddingProvider {
    pub fn new(
        config: EmbeddingConfig,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        if config.model.trim().is_empty() || config.timeout.is_zero() {
            return Err(crate::invalid("invalid embedding configuration"));
        }
        let transport = NativeTransport::with_auth(&config.endpoint, resolver, NativeAuth::Bearer)?;
        Ok(Self { config, transport })
    }
    pub fn with_api_key(
        config: EmbeddingConfig,
        key: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::new(config, crate::static_resolver(key))
    }
    pub fn from_base_url(
        base: &str,
        model: impl Into<String>,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        let url = crate::discovery::validate_url(base)?;
        let prefix = url.as_str().trim_end_matches('/');
        Self::new(
            EmbeddingConfig::new(format!("{prefix}/embeddings"), model),
            resolver,
        )
    }
    async fn attempt(
        &self,
        body: &[u8],
        request: &EmbeddingRequest,
        cancel: CancellationToken,
        id: &Mutex<Option<String>>,
    ) -> Result<EmbeddingResponse, retry::AttemptError> {
        *id.lock().unwrap_or_else(|e| e.into_inner()) = None;
        let mut response = match self
            .transport
            .request(
                "openai",
                &self.config.model,
                reqwest::Method::POST,
                self.transport.endpoint().clone(),
                Some(("application/json", body.to_vec())),
                cancel,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                *id.lock().unwrap_or_else(|e| e.into_inner()) =
                    error.error.provider_request_id.clone();
                return Err(error);
            }
        };
        let request_id = response.provider_request_id().map(str::to_owned);
        *id.lock().unwrap_or_else(|e| e.into_inner()) = request_id.clone();
        let attach = |mut e: ModelError| {
            e.status = Some(200);
            e.provider_request_id = request_id.clone();
            e
        };
        response
            .check_value(&json!({"model": self.config.model}))
            .map_err(&attach)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next_chunk().await.map_err(&attach)? {
            bytes.extend_from_slice(&chunk);
        }
        let count = request.input.len();
        let dimensions = request.dimensions;
        // Parsing a bounded but large JSON vector must not block the async
        // executor: the caller's deadline/cancellation can drop this await.
        // Like request validation, an already running bounded job may finish
        // after cancellation; it never makes HTTP requests or publishes output.
        let mut result = tokio::task::spawn_blocking(move || {
            let value: Value = serde_json::from_slice(&bytes).map_err(|_| bad_response())?;
            response.check_value(&value)?;
            parse_response(&value, count, dimensions)
        })
        .await
        .map_err(|_| attach(bad_response()))?
        .map_err(&attach)?;
        result.provider_request_id = request_id;
        Ok(result)
    }
}
fn bad_response() -> ModelError {
    ModelError::new(
        ModelErrorKind::Protocol,
        "invalid embedding response",
        false,
    )
}
fn timeout() -> ModelError {
    ModelError::new(
        ModelErrorKind::Network,
        "embedding operation timed out",
        false,
    )
}
// This bounded, allocation-free preflight must also run before cloning a borrowed
// request onto the blocking pool. There are at most 2048 string lengths to inspect.
fn preflight(model: &str, request: &EmbeddingRequest) -> Result<(), ModelError> {
    if request.input.is_empty()
        || request.input.len() > 2048
        || request.input.iter().any(|s| s.is_empty())
        || request.dimensions == Some(0)
    {
        return Err(crate::invalid("invalid embedding request"));
    }
    // Bound unescaped input before cloning it into JSON; escaped JSON is checked below.
    let size = request
        .input
        .iter()
        .try_fold(model.len(), |n, s| n.checked_add(s.len()))
        .and_then(|n| n.checked_add(request.user.as_ref().map_or(0, String::len)));
    if size.is_none_or(|n| n > MAX_BYTES) {
        return Err(crate::invalid("embedding request too large"));
    }
    Ok(())
}
fn build(model: &str, request: &EmbeddingRequest) -> Result<Value, ModelError> {
    preflight(model, request)?;
    let mut value = json!({"model": model, "input": request.input, "encoding_format": "float"});
    if let Some(d) = request.dimensions {
        value["dimensions"] = json!(d);
    }
    if let Some(user) = &request.user {
        value["user"] = json!(user);
    }
    if serde_json::to_vec(&value)
        .map_err(|_| crate::invalid("invalid embedding request"))?
        .len()
        > MAX_BYTES
    {
        return Err(crate::invalid("embedding request too large"));
    }
    Ok(value)
}
fn parse_response(
    v: &Value,
    input_count: usize,
    requested_dimensions: Option<u32>,
) -> Result<EmbeddingResponse, ModelError> {
    if v.get("object").and_then(Value::as_str) != Some("list") {
        return Err(bad_response());
    }
    let model = v
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(bad_response)?;
    let usage = v
        .get("usage")
        .and_then(Value::as_object)
        .ok_or_else(bad_response)?;
    let input_tokens = usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(bad_response)?;
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(bad_response)?;
    let data = v
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(bad_response)?;
    if data.len() != input_count {
        return Err(bad_response());
    }
    let mut output: Vec<Option<Embedding>> = vec![None; data.len()];
    let mut dimensions = None;
    for item in data {
        if item.get("object").and_then(Value::as_str) != Some("embedding") {
            return Err(bad_response());
        }
        let index = item
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(bad_response)?;
        let vector = item
            .get("embedding")
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty())
            .ok_or_else(bad_response)?;
        if dimensions.is_some_and(|d| d != vector.len())
            || requested_dimensions.is_some_and(|d| d as usize != vector.len())
        {
            return Err(bad_response());
        }
        dimensions = Some(vector.len());
        let vector = vector
            .iter()
            .map(|n| {
                let x = n.as_f64().ok_or_else(bad_response)? as f32;
                if x.is_finite() {
                    Ok(x)
                } else {
                    Err(bad_response())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let slot = output.get_mut(index as usize).ok_or_else(bad_response)?;
        if slot.is_some() {
            return Err(bad_response());
        }
        *slot = Some(Embedding { index, vector });
    }
    Ok(EmbeddingResponse {
        model: model.to_owned(),
        embeddings: output
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(bad_response)?,
        usage: EmbeddingUsage {
            input_tokens,
            total_tokens,
        },
        provider_request_id: None,
    })
}
#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddingProvider {
    fn build_embedding_request(&self, request: &EmbeddingRequest) -> Result<Value, ModelError> {
        build(&self.config.model, request)
    }
    async fn embed(
        &self,
        request: &EmbeddingRequest,
        cancel: CancellationToken,
    ) -> Result<EmbeddingResponse, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.config.timeout)
            .ok_or_else(|| crate::invalid("embedding timeout is too large"))?;
        preflight(&self.config.model, request)?;
        let id = Mutex::new(None);
        let child = cancel.child_token();
        let result = tokio::select! { biased;
            _ = cancel.cancelled() => Err(ModelError::cancelled()),
            result = tokio::time::timeout_at(deadline, async {
                let owned = request.clone();
                let model = self.config.model.clone();
                let body = tokio::task::spawn_blocking(move || {
                    let value = build(&model, &owned)?;
                    serde_json::to_vec(&value).map_err(|_| crate::invalid("invalid embedding request"))
                }).await.map_err(|_| crate::invalid("embedding validation failed"))??;
                if tokio::time::Instant::now() >= deadline { return Err(timeout()); }
                for attempt in 0..=self.config.retry.retries {
                    if tokio::time::Instant::now() >= deadline { return Err(timeout()); }
                    if child.is_cancelled() { return Err(ModelError::cancelled()); }
                    match self.attempt(&body, request, child.clone(), &id).await {
                        Ok(result) => return Ok(result),
                        Err(error) if attempt < self.config.retry.retries && error.retryable() => retry::sleep(self.config.retry.delay(attempt, error.retry_after)).await,
                        Err(error) => return Err(error.error),
                    }
                }
                unreachable!()
            }) => result.unwrap_or_else(|_| Err(timeout())),
        };
        child.cancel();
        let result = if cancel.is_cancelled() {
            Err(ModelError::cancelled())
        } else if tokio::time::Instant::now() >= deadline {
            Err(timeout())
        } else {
            result
        };
        result.map_err(|mut error| {
            if error.provider_request_id.is_none() {
                error.provider_request_id = id.lock().unwrap_or_else(|e| e.into_inner()).clone();
            }
            error
        })
    }
}
macro_rules! unsupported_embedding {
    ($provider:ty, $message:expr) => {
        #[async_trait]
        impl EmbeddingProvider for $provider {
            fn build_embedding_request(&self, _: &EmbeddingRequest) -> Result<Value, ModelError> {
                Err(ModelError::unsupported($message))
            }
            async fn embed(
                &self,
                _: &EmbeddingRequest,
                _: CancellationToken,
            ) -> Result<EmbeddingResponse, ModelError> {
                Err(ModelError::unsupported($message))
            }
        }
    };
}
unsupported_embedding!(
    OpenAIProvider,
    "configure a separate OpenAIEmbeddingProvider for embeddings"
);
unsupported_embedding!(
    AnthropicProvider,
    "Anthropic has no embedding model; Voyage is a separate provider"
);
unsupported_embedding!(
    DeepseekProvider,
    "DeepSeek has no documented embedding endpoint"
);
