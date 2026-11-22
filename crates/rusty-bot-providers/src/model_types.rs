use serde::{Deserialize, Serialize};

/// JSON Schema for a tool, produced once and reused for provider requests and
/// local argument validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelErrorKind {
    Auth,
    RateLimited,
    Billing,
    QuotaExceeded,
    ModelNotFound,
    Permission,
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
    /// Recognized, sanitized provider error code or type, when available.
    #[serde(default)]
    pub provider_code: Option<String>,
}

impl ModelError {
    pub fn new(kind: ModelErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable,
            status: None,
            provider_request_id: None,
            provider_code: None,
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
