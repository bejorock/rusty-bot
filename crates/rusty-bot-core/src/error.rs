use crate::extension::RegistryError;
use crate::fs::FileError;
use crate::model::ModelError;
use crate::shell::ExecutionError;
use crate::storage::StorageError;
use crate::tool::ToolError;
use crate::workspace::WorkspaceError;

/// Aggregates every failure the engine can surface to a host.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("model: {0}")]
    Model(#[from] ModelError),
    #[error("tool: {0}")]
    Tool(#[from] ToolError),
    #[error("execution: {0}")]
    Execution(#[from] ExecutionError),
    #[error("workspace: {0}")]
    Workspace(#[from] WorkspaceError),
    #[error("file: {0}")]
    File(#[from] FileError),
    #[error("registry: {0}")]
    Registry(#[from] RegistryError),
    #[error("contract violation: {0}")]
    ContractViolation(String),
    #[error("cancelled")]
    Cancelled,
    #[error("unsupported config version: {0}")]
    UnsupportedConfigVersion(u32),
    #[error("registry drift: expected {expected}, found {actual}")]
    RegistryDrift { expected: String, actual: String },
    #[error("workspace mismatch: expected {expected}, found {actual}")]
    WorkspaceMismatch { expected: String, actual: String },
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type SessionResult<T> = Result<T, SessionError>;

/// Best-effort redaction for error messages that may have crossed a secret
/// boundary. The model adapter redacts before constructing errors; this is a
/// second line of defence for tool/exec messages.
pub fn redact_secrets(message: &str, secrets: &[String]) -> String {
    let mut redacted = message.to_string();
    for secret in secrets {
        if !secret.is_empty() {
            redacted = redacted.replace(secret.as_str(), "[REDACTED]");
        }
    }
    redacted
}
