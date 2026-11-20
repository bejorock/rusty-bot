use serde::{Deserialize, Serialize};

use crate::ids::{ConversationId, EntryId, OwnerRef, ParentRef, SubmissionId, TaskId, TimestampMs};
use crate::message::{ContextEdit, Message};

/// Product entry kinds. Mapping to Pi's `pi.*` kinds is documented in
/// `docs/parity.md`.
pub mod entry_kind {
    pub const USER: &str = "rusty-bot.user";
    pub const ASSISTANT: &str = "rusty-bot.assistant";
    pub const TOOL_RESULT: &str = "rusty-bot.tool-result";
    pub const SYSTEM: &str = "rusty-bot.system";
    pub const ERROR: &str = "rusty-bot.error";
    pub const LIVE: &str = "rusty-bot.live";
    pub const INBOX: &str = "rusty-bot.inbox";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConversationRecord {
    pub id: ConversationId,
    pub parent: Option<ParentRef>,
    pub owner: Option<OwnerRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryRecord {
    pub id: EntryId,
    pub conversation_id: ConversationId,
    pub kind: String,
    pub model: Vec<Message>,
    pub data: serde_json::Value,
    pub head: Option<EntryId>,
    pub edits: Vec<ContextEdit>,
    pub by_task_id: Option<TaskId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubmissionRecord {
    pub id: SubmissionId,
    pub conversation_id: ConversationId,
    pub request_id: Option<String>,
    pub kind: SubmissionKind,
    pub status: SubmissionStatus,
    pub payload: serde_json::Value,
    pub entry_id: Option<EntryId>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionKind {
    Input,
    Write,
    Continue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionStatus {
    Queued,
    Placed,
    Done,
    Unanswered,
}

/// Journal row for the effect sandwich. Written with `Intent` before any
/// external effect; the idempotency key is deterministic per tool call.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolOperationRecord {
    pub task_id: TaskId,
    pub idempotency_key: String,
    pub tool_name: String,
    pub status: ToolOperationStatus,
    pub request: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub updated_at: TimestampMs,
}

impl std::fmt::Debug for ToolOperationRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolOperationRecord")
            .field("task_id", &self.task_id)
            .field("idempotency_key", &self.idempotency_key)
            .field("tool_name", &self.tool_name)
            .field("status", &self.status)
            .field("request", &"[REDACTED]")
            .field("result", &self.result.as_ref().map(|_| "[REDACTED]"))
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOperationStatus {
    Intent,
    Running,
    Succeeded,
    Failed,
    /// Terminal cancellation/crash-recovery record, not a resumable tool pause.
    Interrupted,
    Uncertain,
}

impl ToolOperationStatus {
    /// Statuses for which an effect may have occurred but no outcome is known.
    pub fn is_uncertain(&self) -> bool {
        matches!(
            self,
            ToolOperationStatus::Intent
                | ToolOperationStatus::Running
                | ToolOperationStatus::Interrupted
                | ToolOperationStatus::Uncertain
        )
    }
}
