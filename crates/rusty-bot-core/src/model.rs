use serde::{Deserialize, Serialize};

use crate::message::{ContentPart, Message, ToolCall};
pub use crate::message::{StopReason, Usage};

/// JSON Schema for a tool, produced once and reused for provider requests and
/// local argument validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ProviderMessage>,
    pub tools: Vec<ToolSpec>,
    pub tool_choice: ToolChoice,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub metadata: serde_json::Value,
}

/// Ordered stored history, including provider replay state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub tool_choice: ToolChoice,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryTurn {
    pub message: Message,
    pub usage: Usage,
    pub stop: StopReason,
    pub provider_metadata: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Named(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ProviderMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        content: Option<String>,
        tool_calls: Vec<ProviderToolCall>,
    },
    Tool {
        tool_call_id: String,
        content: Vec<ContentPart>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderToolCall {
    pub id: String,
    pub name: String,
    /// JSON-encoded arguments string, as sent on the wire.
    pub arguments: String,
}

/// A complete, internally consistent assistant response. Streamed deltas are
/// provisional; only this value may be committed by the engine.
#[derive(Clone, Debug, PartialEq)]
pub struct AssistantTurn {
    pub message: Message,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    pub stop: StopReason,
    pub provider_metadata: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    Start {
        provider_request_id: Option<String>,
    },
    TextDelta(String),
    ThinkingDelta(String),
    ToolCallStart {
        index: u32,
        id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        index: u32,
        delta: String,
    },
    Usage(Usage),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelErrorKind {
    Auth,
    RateLimited,
    InvalidRequest,
    ContextOverflow,
    Server,
    Network,
    Cancelled,
    Protocol,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelError {
    pub kind: ModelErrorKind,
    pub message: String,
    pub retryable: bool,
    pub status: Option<u16>,
    pub provider_request_id: Option<String>,
}

impl ModelError {
    pub fn new(kind: ModelErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable,
            status: None,
            provider_request_id: None,
        }
    }

    pub fn cancelled() -> Self {
        Self::new(ModelErrorKind::Cancelled, "model request cancelled", false)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ModelErrorKind::Protocol, message, true)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ModelErrorKind::Unsupported, message, false)
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }
}

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(status) => write!(
                f,
                "model {:?} (HTTP {}): {}",
                self.kind, status, self.message
            ),
            None => write!(f, "model {:?}: {}", self.kind, self.message),
        }
    }
}

impl std::error::Error for ModelError {}

/// Maximum bytes persisted for a single image regardless of caller limits.
pub const MAX_TOOL_IMAGE_BYTES: usize = 5 * 1024 * 1024;

/// Explicit image input capability supplied by the caller, never inferred from a model name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageLimits {
    pub max_bytes: usize,
    pub max_width: u32,
    pub max_height: u32,
}

#[async_trait::async_trait]
pub trait ProviderPayloadHook: Send + Sync {
    async fn apply(
        &self,
        api: &str,
        model: &str,
        payload: serde_json::Value,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<serde_json::Value, ModelError>;
}

/// The replaceable model boundary. The provider streams provisional events and
/// returns exactly one terminal assistant turn.
#[async_trait::async_trait]
pub trait ModelProvider: Send + Sync {
    fn id(&self) -> &str;
    fn image_limits(&self) -> Option<ImageLimits> {
        None
    }
    /// Locally validate a reconstructed native history before dispatch.
    /// Providers must explicitly opt in only when their protocol safely supports
    /// the altered sequence. This must be local and side-effect-free; the
    /// default fails closed and performs no dispatch.
    fn validate_compacted_history(&self, _request: &HistoryRequest) -> Result<(), ModelError> {
        Err(ModelError::unsupported(
            "compacted native replay unsupported",
        ))
    }
    /// Validate both the original and independently projected reset locally.
    fn validate_native_context_reset(
        &self,
        _original: &HistoryRequest,
        _candidate: &HistoryRequest,
    ) -> Result<(), ModelError> {
        Err(ModelError::unsupported("native context reset unsupported"))
    }
    async fn stream(
        &self,
        request: ChatRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<AssistantTurn, ModelError>;

    async fn stream_history_with_payload_hook(
        &self,
        request: HistoryRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        cancel: tokio_util::sync::CancellationToken,
        payload_hook: Option<std::sync::Arc<dyn ProviderPayloadHook>>,
    ) -> Result<HistoryTurn, ModelError> {
        if payload_hook.is_some() {
            return Err(ModelError::new(
                ModelErrorKind::Unsupported,
                "provider payload hooks unsupported",
                false,
            ));
        }
        self.stream_history(request, events, cancel).await
    }

    /// Legacy providers inherit the historical projection; native providers
    /// override this to preserve full ordered history and replay state.
    async fn stream_history(
        &self,
        request: HistoryRequest,
        events: tokio::sync::mpsc::Sender<StreamEvent>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<HistoryTurn, ModelError> {
        let request = crate::history_bridge::project_history(request)?;
        let turn = self.stream(request, events, cancel).await?;
        crate::history_bridge::history_turn_from_legacy(turn)
    }
}
