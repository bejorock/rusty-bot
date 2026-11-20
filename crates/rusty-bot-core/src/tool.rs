use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::fs::FileSystem;
use crate::ids::{ConversationId, SessionId, TaskId};
use crate::live::LiveSink;
use crate::message::{ContentPart, Message};
use crate::model::ToolSpec;
use crate::shell::Shell;
use crate::task::ReplayPolicy;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Tools may be executed concurrently in a future milestone. M1 runs one
    /// call at a time even though this is the default policy.
    #[default]
    Parallel,
    Sequential,
}

pub struct ToolContext {
    pub session_id: SessionId,
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
    pub files: Arc<dyn FileSystem>,
    pub shell: Option<Arc<dyn Shell>>,
    pub live: Arc<dyn LiveSink>,
    pub cancel: tokio_util::sync::CancellationToken,
    pub now_ms: i64,
    pub image_limits: Option<crate::model::ImageLimits>,
    /// Engine-owned invocation binding. Standalone contexts must set this to `None`.
    #[doc(hidden)]
    pub interrupt_port: Option<Arc<dyn ToolInterruptPort>>,
}

/// Internal engine side channel; callers should use [`ToolContext::interrupt`].
#[doc(hidden)]
pub trait ToolInterruptPort: Send + Sync {
    fn interrupt(&self, payload: serde_json::Value) -> Result<serde_json::Value, ToolError>;
}

impl ToolContext {
    /// Return a recorded positional answer or request an interrupt of this tool.
    ///
    /// Propagate errors with `?`. A resumed tool restarts from its beginning;
    /// effects before this call may happen again. Calls must be sequenced for
    /// deterministic positional replay. This does not suspend a live future.
    pub async fn interrupt(
        &self,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        match &self.interrupt_port {
            Some(port) => port.interrupt(payload),
            None => Err(ToolError::new(
                "interrupt_unavailable",
                "no active interrupt-capable invocation",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolOutput {
    #[serde(deserialize_with = "deserialize_content")]
    pub content: Vec<ContentPart>,
    pub details: Option<serde_json::Value>,
    /// Tool-level failure (e.g. file not found), not a Rust failure. The model
    /// sees this as an error tool result and may react.
    pub is_error: bool,
}

fn deserialize_content<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ContentPart>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CompatibleContent {
        Legacy(String),
        Parts(Vec<ContentPart>),
    }
    Ok(match CompatibleContent::deserialize(deserializer)? {
        CompatibleContent::Legacy(text) => vec![ContentPart::Text { text }],
        CompatibleContent::Parts(parts) => parts,
    })
}

impl ToolOutput {
    /// Display-only text extraction; do not use for model propagation.
    pub fn text_content(&self) -> String {
        Message::text_content(&self.content)
    }

    pub fn success(content: impl Into<String>) -> Self {
        Self {
            content: vec![ContentPart::Text {
                text: content.into(),
            }],
            details: None,
            is_error: false,
        }
    }

    pub fn error(content: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            content: vec![ContentPart::Text {
                text: content.into(),
            }],
            details: Some(serde_json::json!({ "code": code.into() })),
            is_error: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ToolError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
}

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tool {}: {}", self.code, self.message)
    }
}

impl std::error::Error for ToolError {}

#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Unsafe
    }
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::Parallel
    }
    async fn execute(
        &self,
        arguments: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<ToolOutput, ToolError>;
}
