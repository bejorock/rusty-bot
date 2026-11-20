use serde::{Deserialize, Serialize};

use crate::document::DocumentWrite;
use crate::ids::{ConversationId, Seq, SessionId, TaskId, TimestampMs};
use crate::records::{ConversationRecord, EntryRecord, SubmissionRecord, ToolOperationRecord};
use crate::task::TaskRecord;

/// Commit scope for one batch. Events carry the session/conversation identity.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitContext {
    pub session_id: SessionId,
    pub conversation_id: Option<ConversationId>,
    pub task_id: Option<TaskId>,
}

impl CommitContext {
    pub fn session(session_id: SessionId) -> Self {
        Self {
            session_id,
            conversation_id: None,
            task_id: None,
        }
    }

    pub fn conversation(session_id: SessionId, conversation_id: ConversationId) -> Self {
        Self {
            session_id,
            conversation_id: Some(conversation_id),
            task_id: None,
        }
    }

    pub fn task(session_id: SessionId, conversation_id: ConversationId, task_id: TaskId) -> Self {
        Self {
            session_id,
            conversation_id: Some(conversation_id),
            task_id: Some(task_id),
        }
    }
}

/// A commit batch is stored in one transaction and receives one `seq`.
/// Batches must contain at least one write.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitBatch {
    pub writes: Vec<Write>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// Adjacent tagging avoids colliding with DocumentWrite's own `type` tag.
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Write {
    /// Insert a new session atomically with its root conversation.
    Session(crate::storage::SessionRecord),
    Conversation(ConversationRecord),
    /// Insert-only. Entries are immutable.
    Entry(EntryRecord),
    /// Optimistic version check.
    Task(TaskRecord),
    Submission(SubmissionRecord),
    Document(DocumentWrite),
    ToolOperation(ToolOperationRecord),
    /// Stored in the same transaction as the batch; ordinal assigned in write
    /// order by storage.
    Event(SessionEventDraft),
}

impl Write {
    pub fn is_table_write(&self) -> bool {
        !matches!(self, Write::Document(_) | Write::Event(_))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEventDraft {
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub seq: Seq,
    pub ordinal: u32,
    pub session_id: SessionId,
    pub conversation_id: Option<ConversationId>,
    pub kind: String,
    pub payload: serde_json::Value,
    pub created_at_ms: TimestampMs,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CommitReceipt {
    /// Durable prefix, in batch order.
    pub batches: Vec<StoredBatch>,
    pub refused: Option<BatchRefusal>,
}

impl CommitReceipt {
    pub fn is_fully_committed(&self) -> bool {
        self.refused.is_none()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredBatch {
    pub index: usize,
    pub seq: Seq,
    pub event_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchRefusal {
    pub batch_index: usize,
    pub reason: RefusalReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefusalReason {
    VersionConflict { expected: u32, actual: u32 },
    DuplicateRequestId { request_id: String },
    InvalidTransition { from: String, to: String },
    PreconditionFailed(String),
}

impl std::fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefusalReason::VersionConflict { expected, actual } => {
                write!(f, "version conflict: expected {expected}, actual {actual}")
            }
            RefusalReason::DuplicateRequestId { request_id } => {
                write!(f, "duplicate request_id: {request_id}")
            }
            RefusalReason::InvalidTransition { from, to } => {
                write!(f, "invalid transition: {from} -> {to}")
            }
            RefusalReason::PreconditionFailed(message) => {
                write!(f, "precondition failed: {message}")
            }
        }
    }
}

/// Durable event kinds emitted by the engine.
pub mod event_kind {
    pub const SUBMISSION_QUEUED: &str = "submission.queued";
    pub const SUBMISSION_PLACED: &str = "submission.placed";
    pub const SUBMISSION_DONE: &str = "submission.done";
    pub const ENTRY_COMMITTED: &str = "entry.committed";
    pub const TASK_CHANGED: &str = "task.changed";
    pub const TASK_ABORT_REQUESTED: &str = "task.abort-requested";
    pub const TURN_FINISHED: &str = "turn.finished";
    pub const TOOL_STARTED: &str = "tool.started";
    pub const TOOL_FINISHED: &str = "tool.finished";
    pub const SESSION_OPENED: &str = "session.opened";
}
