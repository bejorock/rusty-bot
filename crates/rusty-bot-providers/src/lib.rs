#![doc = include_str!("../README.md")]
mod credentials;
mod embeddings;
pub use embeddings::{
    Embedding, EmbeddingConfig, EmbeddingProvider, EmbeddingRequest, EmbeddingResponse,
    EmbeddingUsage, OpenAIEmbeddingProvider,
};
#[cfg(test)]
mod embedding_tests;
pub mod image_validation;
mod model_types;
pub use credentials::{
    CredentialRequest, CredentialResolver, ProviderCredential, StaticCredentialResolver,
};
pub use model_types::{ImageLimits, ModelError, ModelErrorKind, ToolSpec, MAX_TOOL_IMAGE_BYTES};
pub use rusty_bot_messages as messages;
pub use rusty_bot_messages as rusty_bot_session;
mod batch;
pub use batch::{
    BatchCounts, BatchHandle, BatchItemResult, BatchJob, BatchOutcome, BatchProvider, BatchRequest,
    BatchResults, BatchStatus, BatchTranslator,
};
mod capabilities;
mod estimation;
pub use estimation::{estimate_input_tokens, EstimationEncoding, TokenEstimate};
mod discovery;
#[cfg(test)]
mod estimation_tests;
pub use capabilities::ModelCapabilities;
mod convenience;
pub use convenience::{
    generate, generate_structured, Observation, ObservedOperation, ObservedProvider, Observer,
    StructuredResponse,
};
mod cache;
#[cfg(test)]
mod convenience_tests;
mod error;
pub mod usage;
pub use cache::{
    AnthropicCacheTtl, CacheMarker, OpenAiCacheRetention, OpenAiResponsesCache,
    OpenAiResponsesCacheMode, OpenAiResponsesCacheTtl, PromptCache,
};
#[cfg(test)]
mod cache_tests;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod extra_tests;
#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod image_url_tests;
#[cfg(test)]
mod live_fixture_tests;
#[cfg(test)]
mod portable_tests;
mod request;
mod retry;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod settings_tests;
mod sse;
#[cfg(test)]
mod tests;
mod transport;
mod wire;

use async_trait::async_trait;
pub use discovery::{DiscoveredModel, DiscoveryRequest, ModelPage};
pub use retry::RetryPolicy;
use rusty_bot_messages::{ChatHistory, HistoryEntry, MessageStreamEvent, MessageStreamPayload};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transport::{NativeAuth, NativeTransport};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OpenAIProtocol {
    #[default]
    Responses,
    ChatCompletions,
}
/// Completion policy for OpenAI Chat Completions streams.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChatStreamTermination {
    /// Require the standard `[DONE]` marker.
    #[default]
    RequireDone,
    /// Accept a clean HTTP EOF after a validated finish-reason chunk, even without `[DONE]`.
    FinishReasonAndEof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Api {
    Responses,
    Chat,
    Anthropic,
    Deepseek,
}
impl Api {
    fn provider(self) -> &'static str {
        match self {
            Self::Responses | Self::Chat => "openai",
            Self::Anthropic => "anthropic",
            Self::Deepseek => "deepseek",
        }
    }
    fn format(self) -> &'static str {
        match self {
            Self::Responses => "responses.output",
            Self::Chat => "chat.message",
            Self::Anthropic => "messages.content",
            Self::Deepseek => "chat.reasoning_message",
        }
    }
}

/// Reasoning controls are model/API dependent and may be rejected remotely.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Reasoning {
    #[default]
    Default,
    Disabled,
    Enabled {
        effort: Option<ReasoningEffort>,
        budget_tokens: Option<u32>,
    },
    Adaptive {
        effort: Option<ReasoningEffort>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReasoningEffort {
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}
impl ReasoningEffort {
    fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    Named {
        name: String,
    },
}

/// Per-tool provider request controls. These do not execute tools or validate arguments locally.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolOptions {
    /// Request the provider's strict schema mode; `None` leaves its default unchanged.
    pub strict: Option<bool>,
}

/// Provider-native output constraint. Use `generate_structured` for checked typed output.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum OutputFormat {
    #[default]
    Text,
    JsonObject,
    JsonSchema {
        name: String,
        schema: Value,
        strict: bool,
    },
}

/// Endpoint is the complete POST URL, not a base URL. Never inferred from credentials.
#[derive(Clone)]
pub struct ProviderConfig {
    pub endpoint: String,
    pub model: String,
    /// Bounds the whole operation, including validation, resolution and downstream backpressure.
    /// A bounded, noninterruptible CPU validation job may finish after timeout or cancellation.
    pub timeout: Duration,
    /// Retry settings for transient errors before any event is emitted.
    pub retry: RetryPolicy,
    pub max_output_tokens: u32,
    /// Legacy Anthropic thinking budget; mutually exclusive with non-default `reasoning`.
    pub thinking_budget: Option<u32>,
    pub reasoning: Reasoning,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub tool_choice: ToolChoice,
    /// Provider request options keyed by declared tool name; unknown names are rejected.
    pub tool_options: BTreeMap<String, ToolOptions>,
    /// Provider request control, not a guarantee about local agent execution.
    pub parallel_tool_calls: Option<bool>,
    /// Native provider output constraint, defaulting to unconstrained text.
    pub output_format: OutputFormat,
    pub prompt_cache: Option<PromptCache>,
    /// Non-default termination is supported only for OpenAI Chat Completions.
    pub chat_stream_termination: ChatStreamTermination,
    /// Explicit GET URL for model discovery; never derived from server responses.
    pub list_models_endpoint: Option<String>,
}
impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderConfig([REDACTED])")
    }
}
impl ProviderConfig {
    pub fn new(endpoint: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            model: model.into(),
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
            max_output_tokens: 4096,
            thinking_budget: None,
            reasoning: Reasoning::Default,
            temperature: None,
            top_p: None,
            tool_choice: ToolChoice::Auto,
            tool_options: BTreeMap::new(),
            parallel_tool_calls: None,
            output_format: OutputFormat::Text,
            prompt_cache: None,
            chat_stream_termination: ChatStreamTermination::RequireDone,
            list_models_endpoint: None,
        }
    }
}

/// Completed, tool-resolved rich context. IDs and timestamps remain caller-owned.
#[derive(Clone, Debug)]
pub struct GenerationRequest {
    pub message_id: String,
    pub history: ChatHistory,
    pub tools: Vec<ToolSpec>,
}

/// No tool execution, persistence, or protocol fallback. Returned entries are authoritative.
/// A failed stream never emits Completed. The caller must discard provisional output on error.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Configured model identity, if known to the provider.
    fn model_id(&self) -> Option<&str> {
        None
    }
    /// Provider API identity, if known; does not identify endpoint or account.
    fn provider_id(&self) -> Option<&str> {
        None
    }
    fn output_format(&self) -> &OutputFormat {
        &OutputFormat::Text
    }
    /// Optional whole-operation budget, also honored by observation wrappers.
    fn operation_timeout(&self) -> Option<Duration> {
        None
    }
    /// Strict offline translation, performed before credential resolution or HTTP.
    fn build_request(&self, request: &GenerationRequest) -> Result<Value, ModelError>;
    async fn list_models(
        &self,
        request: DiscoveryRequest,
        cancel: CancellationToken,
    ) -> Result<ModelPage, ModelError>;
    async fn stream(
        &self,
        request: &GenerationRequest,
        events: mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError>;
}

struct Client {
    config: ProviderConfig,
    api: Api,
    transport: NativeTransport,
    discovery: discovery::Discovery,
}
impl Client {
    fn new(
        config: ProviderConfig,
        api: Api,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        if config.model.trim().is_empty()
            || config.timeout.is_zero()
            || config.max_output_tokens == 0
            || (api != Api::Chat
                && config.chat_stream_termination != ChatStreamTermination::RequireDone)
            || config
                .thinking_budget
                .is_some_and(|b| api != Api::Anthropic || b < 1024 || b >= config.max_output_tokens)
        {
            return Err(invalid("invalid provider configuration"));
        }
        let auth = if api == Api::Anthropic {
            NativeAuth::Anthropic
        } else {
            NativeAuth::Bearer
        };
        request::validate_config(api, &config)?;
        let discovery = discovery::Discovery::new(&config, api, resolver.clone())?;
        let transport = NativeTransport::with_auth(&config.endpoint, resolver, auth)?;
        Ok(Self {
            config,
            api,
            transport,
            discovery,
        })
    }
    async fn stream(
        &self,
        request: &GenerationRequest,
        events: mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.config.timeout)
            .ok_or_else(|| invalid("provider timeout is too large"))?;
        let child = cancel.child_token();
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ModelError::cancelled()),
            result = tokio::time::timeout_at(deadline, async {
                // The borrowed request must be owned before moving it to a 'static worker.
                let owned = request.clone();
                // Cloning borrowed input cannot be preempted, but it must not
                // buy a new validation or HTTP budget after the deadline.
                if tokio::time::Instant::now() >= deadline {
                    return Err(ModelError::new(ModelErrorKind::Network, "provider operation timed out", false));
                }
                if child.is_cancelled() {
                    return Err(ModelError::cancelled());
                }
                let config = self.config.clone();
                let api = self.api;
                let body = tokio::task::spawn_blocking(move || request::build(api, &config, &owned))
                    .await
                    .map_err(|_| invalid("request validation failed"))??;
                if tokio::time::Instant::now() >= deadline {
                    return Err(ModelError::new(ModelErrorKind::Network, "provider operation timed out", false));
                }
                if child.is_cancelled() {
                    return Err(ModelError::cancelled());
                }
                self.retry_stream(request, &body, &events, child.clone(), deadline).await
            }) =>
                result.unwrap_or_else(|_| Err(ModelError::new(ModelErrorKind::Network, "provider operation timed out", false))),
        };
        child.cancel();
        // Errors are returned, not delivered through a potentially blocked event sink.
        result
    }
    async fn retry_stream(
        &self,
        request: &GenerationRequest,
        body: &Value,
        events: &mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<HistoryEntry, ModelError> {
        for retry in 0..=self.config.retry.retries {
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
            let mut emitted = false;
            match self
                .run(request, body, events, cancel.clone(), &mut emitted)
                .await
            {
                Ok(entry) => return Ok(entry),
                Err(error)
                    if !emitted && retry < self.config.retry.retries && error.retryable() =>
                {
                    retry::sleep(self.config.retry.delay(retry, error.retry_after)).await;
                }
                Err(error) => return Err(error.error),
            }
        }
        unreachable!()
    }
    async fn run(
        &self,
        request: &GenerationRequest,
        body: &Value,
        events: &mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
        emitted: &mut bool,
    ) -> Result<HistoryEntry, retry::AttemptError> {
        let mut response = self
            .transport
            .post(self.api.provider(), &self.config.model, body, cancel)
            .await?;
        let mut sse = sse::Sse::default();
        let mut decoder = wire::Decoder::new(self.api, &self.config.model, &request.message_id);
        response.check_value(
            &serde_json::json!({"model": self.config.model, "id": request.message_id}),
        )?;
        let mut sequence = 0;
        let mut suffix = String::new();
        while let Some(chunk) = response
            .next_chunk()
            .await
            .map_err(|e| stream_error(e, &response))?
        {
            for data in sse.push(&chunk).map_err(|e| stream_error(e, &response))? {
                for event in decoder
                    .push_checked(&data, |value| response.check_value(value))
                    .map_err(|error| stream_error(error, &response))?
                {
                    check_event(&response, &mut suffix, &event)?;
                    start(events, &request.message_id, &mut sequence, emitted).await?;
                    send(events, &request.message_id, &mut sequence, event).await?;
                }
            }
        }
        if let Some(data) = sse.finish().map_err(|e| stream_error(e, &response))? {
            for event in decoder
                .push_checked(&data, |value| response.check_value(value))
                .map_err(|error| stream_error(error, &response))?
            {
                check_event(&response, &mut suffix, &event)?;
                start(events, &request.message_id, &mut sequence, emitted).await?;
                send(events, &request.message_id, &mut sequence, event).await?;
            }
        }
        for event in decoder.eof(self.config.chat_stream_termination) {
            check_event(&response, &mut suffix, &event)?;
            start(events, &request.message_id, &mut sequence, emitted).await?;
            send(events, &request.message_id, &mut sequence, event).await?;
        }
        let mut entry = decoder.finish().map_err(|e| stream_error(e, &response))?;
        if let Some(id) = response.provider_request_id() {
            entry
                .metadata
                .insert("request_id".into(), Value::String(id.into()));
        }
        response.check_value(&serde_json::to_value(&entry).map_err(|_| protocol())?)?;
        start(events, &request.message_id, &mut sequence, emitted).await?;
        send(
            events,
            &request.message_id,
            &mut sequence,
            MessageStreamPayload::Completed {
                response: Box::new(entry.clone()),
            },
        )
        .await?;
        Ok(entry)
    }
}
fn stream_error(mut error: ModelError, response: &transport::NativeResponse) -> ModelError {
    error.status = Some(200);
    error.provider_request_id = response.provider_request_id().map(str::to_owned);
    error
}
fn check_event(
    response: &transport::NativeResponse,
    suffix: &mut String,
    event: &MessageStreamPayload,
) -> Result<(), ModelError> {
    match event {
        MessageStreamPayload::BlockDelta { delta, .. } => response.check_delta(suffix, delta),
        MessageStreamPayload::BlockStart { block, .. } => {
            response.check_value(&serde_json::to_value(block).map_err(|_| protocol())?)
        }
        _ => Ok(()),
    }
}
async fn start(
    events: &mpsc::Sender<MessageStreamEvent>,
    id: &str,
    sequence: &mut u64,
    emitted: &mut bool,
) -> Result<(), ModelError> {
    if !*emitted {
        send(events, id, sequence, MessageStreamPayload::Start).await?;
        *emitted = true;
    }
    Ok(())
}
async fn send(
    events: &mpsc::Sender<MessageStreamEvent>,
    id: &str,
    sequence: &mut u64,
    event: MessageStreamPayload,
) -> Result<(), ModelError> {
    *sequence += 1;
    events
        .send(MessageStreamEvent {
            message_id: id.into(),
            sequence: *sequence,
            event,
        })
        .await
        .map_err(|_| ModelError::cancelled())
}
fn invalid(message: &'static str) -> ModelError {
    ModelError::new(ModelErrorKind::InvalidRequest, message, false)
}
fn protocol() -> ModelError {
    ModelError::new(
        ModelErrorKind::Protocol,
        "invalid or unsupported provider stream",
        false,
    )
}
fn unsupported() -> ModelError {
    ModelError::unsupported("content is outside this provider's supported subset")
}

pub struct OpenAIProvider(Client);
pub struct AnthropicProvider(Client);
pub struct DeepseekProvider(Client);
fn base_config(
    base: &str,
    model: impl Into<String>,
    suffix: &str,
) -> Result<ProviderConfig, ModelError> {
    let url = discovery::validate_url(base)?;
    let mut prefix = url.to_string();
    while prefix.ends_with('/') {
        prefix.pop();
    }
    let mut config = ProviderConfig::new(format!("{prefix}/{suffix}"), model);
    config.list_models_endpoint = Some(format!("{prefix}/models"));
    Ok(config)
}
fn static_resolver(key: ProviderCredential) -> Arc<dyn CredentialResolver> {
    Arc::new(StaticCredentialResolver::new(key))
}
impl OpenAIProvider {
    pub fn from_base_url(
        base: &str,
        model: impl Into<String>,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Self::from_base_url_with_protocol(base, model, OpenAIProtocol::Responses, resolver)
    }
    pub fn from_base_url_with_protocol(
        base: &str,
        model: impl Into<String>,
        protocol: OpenAIProtocol,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        let suffix = match protocol {
            OpenAIProtocol::Responses => "responses",
            OpenAIProtocol::ChatCompletions => "chat/completions",
        };
        Self::with_protocol(base_config(base, model, suffix)?, protocol, resolver)
    }
    pub fn with_api_key(
        config: ProviderConfig,
        key: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::new(config, static_resolver(key))
    }
    pub fn with_protocol_and_api_key(
        config: ProviderConfig,
        protocol: OpenAIProtocol,
        key: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::with_protocol(config, protocol, static_resolver(key))
    }

    /// Responses is the default. Chat Completions must be selected explicitly.
    pub fn new(
        config: ProviderConfig,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Self::with_protocol(config, OpenAIProtocol::default(), resolver)
    }
    pub fn with_protocol(
        config: ProviderConfig,
        protocol: OpenAIProtocol,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Ok(Self(Client::new(
            config,
            match protocol {
                OpenAIProtocol::Responses => Api::Responses,
                OpenAIProtocol::ChatCompletions => Api::Chat,
            },
            resolver,
        )?))
    }
}
impl AnthropicProvider {
    pub fn from_base_url(
        base: &str,
        model: impl Into<String>,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Self::new(base_config(base, model, "messages")?, resolver)
    }
    pub fn with_api_key(
        config: ProviderConfig,
        key: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::new(config, static_resolver(key))
    }

    pub fn new(
        config: ProviderConfig,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Ok(Self(Client::new(config, Api::Anthropic, resolver)?))
    }
}
impl DeepseekProvider {
    pub fn from_base_url(
        base: &str,
        model: impl Into<String>,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Self::new(base_config(base, model, "chat/completions")?, resolver)
    }
    pub fn with_api_key(
        config: ProviderConfig,
        key: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::new(config, static_resolver(key))
    }

    pub fn new(
        config: ProviderConfig,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        Ok(Self(Client::new(config, Api::Deepseek, resolver)?))
    }
}
macro_rules! provider {
    ($ty:ty) => {
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($ty), "([REDACTED])"))
            }
        }
        #[async_trait]
        impl Provider for $ty {
            fn model_id(&self) -> Option<&str> {
                Some(&self.0.config.model)
            }
            fn provider_id(&self) -> Option<&str> {
                Some(self.0.api.provider())
            }
            fn operation_timeout(&self) -> Option<Duration> {
                Some(self.0.config.timeout)
            }
            fn output_format(&self) -> &OutputFormat {
                &self.0.config.output_format
            }
            fn build_request(&self, request: &GenerationRequest) -> Result<Value, ModelError> {
                request::build(self.0.api, &self.0.config, request)
            }
            async fn list_models(
                &self,
                request: DiscoveryRequest,
                cancel: CancellationToken,
            ) -> Result<ModelPage, ModelError> {
                self.0.discovery.list(request, cancel).await
            }
            async fn stream(
                &self,
                request: &GenerationRequest,
                events: mpsc::Sender<MessageStreamEvent>,
                cancel: CancellationToken,
            ) -> Result<HistoryEntry, ModelError> {
                self.0.stream(request, events, cancel).await
            }
        }
    };
}
provider!(OpenAIProvider);
provider!(AnthropicProvider);
provider!(DeepseekProvider);

#[cfg(test)]
mod tool_options_tests;

#[cfg(test)]
mod batch_tests;

#[cfg(test)]
mod identity_tests {
    use super::*;

    struct NoopObserver;
    impl Observer for NoopObserver {
        fn observe(&self, _: Observation) {}
    }

    #[test]
    fn concrete_and_observed_provider_identity() {
        let config = || ProviderConfig::new("https://example.invalid/v1/messages", "model-α");
        let key = || ProviderCredential::new("offline-fixture".to_string());
        let providers: Vec<(Arc<dyn Provider>, &str)> = vec![
            (
                Arc::new(OpenAIProvider::with_api_key(config(), key()).unwrap()),
                "openai",
            ),
            (
                Arc::new(
                    OpenAIProvider::with_protocol_and_api_key(
                        config(),
                        OpenAIProtocol::ChatCompletions,
                        key(),
                    )
                    .unwrap(),
                ),
                "openai",
            ),
            (
                Arc::new(AnthropicProvider::with_api_key(config(), key()).unwrap()),
                "anthropic",
            ),
            (
                Arc::new(DeepseekProvider::with_api_key(config(), key()).unwrap()),
                "deepseek",
            ),
        ];
        for (provider, expected) in providers {
            assert_eq!(provider.model_id(), Some("model-α"));
            assert_eq!(provider.provider_id(), Some(expected));
            let observed = ObservedProvider::new(provider, Arc::new(NoopObserver));
            assert_eq!(observed.model_id(), Some("model-α"));
            assert_eq!(observed.provider_id(), Some(expected));
        }
    }
}
