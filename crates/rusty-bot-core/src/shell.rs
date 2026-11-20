use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Execution error codes reuse Pi's public vocabulary deliberately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionErrorCode {
    Aborted,
    Timeout,
    ShellUnavailable,
    SpawnError,
    CallbackError,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionError {
    pub code: ExecutionErrorCode,
    pub message: String,
    pub spill_path: Option<PathBuf>,
    #[serde(default)]
    pub partial_output: Option<String>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub truncation: Option<serde_json::Value>,
    #[serde(default)]
    pub last_line_bytes: usize,
}

impl ExecutionError {
    pub fn new(code: ExecutionErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            spill_path: None,
            partial_output: None,
            truncated: false,
            truncation: None,
            last_line_bytes: 0,
        }
    }
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "execution {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandSpec {
    Shell { command: String },
    Argv { program: String, args: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecOptions {
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub timeout: Option<Duration>,
    pub output: OutputMode,
    pub spill: SpillPolicy,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            cwd: None,
            env: BTreeMap::new(),
            inherit_env: true,
            timeout: None,
            output: OutputMode::Buffer,
            spill: SpillPolicy::default(),
        }
    }
}

#[derive(Clone)]
pub enum OutputMode {
    Buffer,
    Stream(OutputCallback),
}

impl std::fmt::Debug for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputMode::Buffer => f.write_str("Buffer"),
            OutputMode::Stream(_) => f.write_str("Stream(..)"),
        }
    }
}

impl PartialEq for OutputMode {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (OutputMode::Buffer, OutputMode::Buffer))
    }
}

impl Eq for OutputMode {}

pub type OutputCallback = Arc<dyn Fn(OutputChunk) + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputChunk {
    pub stream: OutputStream,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillPolicy {
    pub max_bytes: usize,
    pub max_lines: usize,
    pub dir: PathBuf,
}

impl Default for SpillPolicy {
    fn default() -> Self {
        Self {
            max_bytes: crate::truncate::DEFAULT_MAX_BYTES,
            max_lines: crate::truncate::DEFAULT_MAX_LINES,
            dir: std::env::temp_dir(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellExecResult {
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub signal: Option<i32>,
    #[serde(default)]
    pub total_bytes: usize,
    #[serde(default)]
    pub full_output: Option<String>,
    /// Combined stdout/stderr in adapter arrival order.
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
    pub spill_path: Option<PathBuf>,
    pub duration_ms: u64,
    /// Coding-agent-compatible bounded tail statistics, when captured by the adapter.
    #[serde(default)]
    pub truncation: Option<serde_json::Value>,
    #[serde(default)]
    pub last_line_bytes: usize,
}

/// Cheap cloneable environment identity. Multiple sessions may share one.
#[async_trait::async_trait]
pub trait Shell: Send + Sync {
    fn id(&self) -> &str;
    async fn exec(
        &self,
        spec: CommandSpec,
        opts: ExecOptions,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<ShellExecResult, ExecutionError>;
    async fn cleanup(&self) -> Result<(), ExecutionError>;
}
