use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::fs::FileSystem;
use crate::shell::Shell;

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceKey(pub String);

impl WorkspaceKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for WorkspaceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Caller-selected workspace and optional execution environment.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceSpec {
    pub key: WorkspaceKey,
    pub root: Option<PathBuf>,
    pub execution: ExecutionSelection,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionSelection {
    /// Files only; shell-requiring tools report `shell_unavailable`.
    None,
    /// Create or join a local execution environment.
    Local {
        env_id: Option<String>,
        cwd: Option<PathBuf>,
    },
    /// Attach to an environment that must already exist in this process.
    Existing { env_id: String },
}

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("unknown execution env: {0}")]
    UnknownEnv(String),
    #[error("invalid workspace: {0}")]
    Invalid(String),
    #[error("workspace io error: {0}")]
    Io(String),
}

impl WorkspaceError {
    pub fn io(message: impl Into<String>) -> Self {
        Self::Io(message.into())
    }
}

/// A resolved workspace. `files()` is always available; execution is optional.
pub trait Workspace: Send + Sync {
    fn key(&self) -> &WorkspaceKey;
    fn root(&self) -> &Path;
    fn files(&self) -> Arc<dyn FileSystem>;
    fn execution(&self) -> Option<Arc<dyn Shell>>;
    fn execution_env_id(&self) -> Option<&str>;
    fn host_id(&self) -> &str;
    /// The validated, reopenable binding, when the workspace adapter supports it.
    fn workspace_spec(&self) -> Option<WorkspaceSpec> {
        None
    }
}

/// Opens/releases workspaces. Sessions share a workspace; they never own an
/// execution environment exclusively.
#[async_trait::async_trait]
pub trait WorkspaceProvider: Send + Sync {
    async fn open(&self, spec: WorkspaceSpec) -> Result<Arc<dyn Workspace>, WorkspaceError>;
    async fn release(&self, key: &WorkspaceKey) -> Result<(), WorkspaceError>;
}
