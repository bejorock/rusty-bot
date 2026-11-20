use serde::{Deserialize, Serialize};

use crate::commit::{CommitBatch, CommitContext, CommitReceipt, StoredEvent};
use crate::document::{
    DocumentAddress, DocumentPoint, DocumentQuery, DocumentValue, DocumentWrite,
};
use crate::ids::{
    ConversationId, EntryId, Id, OwnerRef, ParentRef, Seq, SessionId, SubmissionId, TaskId,
    TimestampMs,
};
use crate::records::{
    ConversationRecord, EntryRecord, SubmissionRecord, SubmissionStatus, ToolOperationRecord,
    ToolOperationStatus,
};
use crate::task::TaskRecord;

#[derive(Clone)]
pub struct StoredEntry {
    pub record: EntryRecord,
    pub commit_seq: Seq,
}

#[derive(Clone)]
pub struct StoredTask {
    pub record: TaskRecord,
    pub commit_seq: Seq,
}

#[derive(Clone)]
pub struct StoredSubmission {
    pub record: SubmissionRecord,
    pub commit_seq: Seq,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: SessionId,
    /// Root conversation for this session, persisted independently of events.
    #[serde(default)]
    pub conversation_id: Option<ConversationId>,
    pub workspace_key: String,
    /// Serialized `WorkspaceSpec`.
    pub workspace: serde_json::Value,
    /// Serialized `SessionConfig`, including `config_version`.
    pub config: serde_json::Value,
    pub registry_version: String,
    pub extension_versions: serde_json::Value,
    pub host_id: String,
    /// `active | failed | archived`
    pub status: String,
    pub created_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntryQuery {
    pub conversation_id: ConversationId,
    pub kind: Option<String>,
    pub by_task_id: Option<TaskId>,
    pub ascending: bool,
}

pub type EntryCursor = EntryId;

#[derive(Clone, Debug, PartialEq)]
pub struct TaskQuery {
    pub conversation_id: Option<ConversationId>,
    pub status: Option<String>,
    pub kind: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventCursor {
    pub seq: Seq,
    pub ordinal: u32,
}

impl EventCursor {
    pub fn start() -> Self {
        Self { seq: 0, ordinal: 0 }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventPage {
    pub events: Vec<StoredEvent>,
    pub next: Option<EventCursor>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Page<T, C> {
    pub items: Vec<T>,
    pub next: Option<C>,
}

impl<T, C> Page<T, C> {
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageErrorKind {
    Unavailable,
    Timeout,
    Corrupt,
    Constraint,
    Unsupported,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageError {
    pub kind: StorageErrorKind,
    pub message: String,
    pub retryable: bool,
}

impl StorageError {
    pub fn new(kind: StorageErrorKind, message: impl Into<String>) -> Self {
        let retryable = matches!(
            kind,
            StorageErrorKind::Unavailable | StorageErrorKind::Timeout
        );
        Self {
            kind,
            message: message.into(),
            retryable,
        }
    }

    pub fn constraint(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Constraint, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Internal, message)
    }

    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Corrupt, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unsupported, message)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage {:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StorageError {}

/// A persisted progress frame and its adapter-local, monotonically increasing cursor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionFrame {
    pub cursor: u64,
    pub session_id: SessionId,
    pub frame: crate::LiveFrame,
}

/// Abstract session adapter. Backends provide transactional state and an
/// append-only progress log; readers stream by following that log, not a channel.
/// Each batch is atomic and returns a durable prefix plus optional refusal.
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Append before publishing progress. Synchronous so subprocess callbacks can
    /// persist without spawning unordered tasks. Never silently discard frames.
    fn append_frame(
        &self,
        _session: SessionId,
        _frame: crate::LiveFrame,
    ) -> Result<(), StorageError> {
        Err(StorageError::unsupported("adapter has no progress log"))
    }

    async fn read_frames(
        &self,
        _session: SessionId,
        _after: u64,
        _limit: usize,
    ) -> Result<Vec<SessionFrame>, StorageError> {
        Err(StorageError::unsupported("adapter has no progress log"))
    }

    /// Latest progress cursor for this session, or zero for an empty log.
    async fn last_frame_cursor(&self, session: SessionId) -> Result<u64, StorageError> {
        let mut cursor = 0;
        loop {
            let frames = self.read_frames(session, cursor, 256).await?;
            match frames.last() {
                Some(frame) => cursor = frame.cursor,
                None => return Ok(cursor),
            }
        }
    }

    async fn mint_id(&self) -> Result<Id, StorageError>;
    async fn commit(
        &self,
        batches: Vec<CommitBatch>,
        ctx: &CommitContext,
    ) -> Result<CommitReceipt, StorageError>;

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, StorageError>;
    async fn get_entry(
        &self,
        conv: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>, StorageError>;
    async fn scan_entries(
        &self,
        query: EntryQuery,
        cursor: Option<EntryCursor>,
        limit: usize,
    ) -> Result<Page<StoredEntry, EntryCursor>, StorageError>;
    async fn latest_head_marker(
        &self,
        conv: ConversationId,
    ) -> Result<Option<StoredEntry>, StorageError>;
    async fn latest_entry_id(&self, conv: ConversationId) -> Result<Option<EntryId>, StorageError>;
    async fn find_entry_by_task(
        &self,
        conv: ConversationId,
        task: TaskId,
        kind: &str,
    ) -> Result<Option<StoredEntry>, StorageError>;

    async fn get_task(&self, id: TaskId) -> Result<Option<StoredTask>, StorageError>;
    async fn scan_tasks(&self, query: TaskQuery) -> Result<Vec<StoredTask>, StorageError>;

    async fn get_submission(
        &self,
        id: SubmissionId,
    ) -> Result<Option<StoredSubmission>, StorageError>;
    async fn find_submission_by_request(
        &self,
        conv: ConversationId,
        request_id: &str,
    ) -> Result<Option<StoredSubmission>, StorageError>;

    async fn get_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentValue>, StorageError>;
    async fn scan_documents(
        &self,
        query: DocumentQuery,
    ) -> Result<Vec<DocumentValue>, StorageError>;

    async fn get_operation(
        &self,
        task: TaskId,
        key: &str,
    ) -> Result<Option<ToolOperationRecord>, StorageError>;

    async fn scan_events(
        &self,
        session: SessionId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StorageError>;
    async fn last_event_cursor(
        &self,
        session: SessionId,
    ) -> Result<Option<EventCursor>, StorageError>;

    async fn get_session(&self, id: SessionId) -> Result<Option<SessionRecord>, StorageError>;
    async fn put_session(&self, record: SessionRecord) -> Result<(), StorageError>;
    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>, StorageError>;
}

#[derive(Clone)]
pub struct StoredConversation {
    pub record: ConversationRecord,
    pub commit_seq: Seq,
}

// Re-export shapes used by in-memory and PG implementations so both start from
// the same types when validating documents and operations.
pub fn document_write_address(write: &DocumentWrite) -> &DocumentAddress {
    write.address()
}

pub fn operation_can_advance(from: ToolOperationStatus, to: ToolOperationStatus) -> bool {
    use ToolOperationStatus::*;
    match (from, to) {
        (a, b) if a == b => true,
        (Intent, Running | Succeeded | Failed | Interrupted | Uncertain) => true,
        (Running, Succeeded | Failed | Interrupted | Uncertain) => true,
        (Interrupted, Succeeded | Failed | Uncertain) => true,
        (Uncertain, Succeeded | Failed | Interrupted) => true,
        (Succeeded | Failed, _) => false,
        (_, Intent) => false,
        _ => false,
    }
}

/// Helper to build the standard "pending submission" event payload.
pub fn submission_event_payload(record: &SubmissionRecord) -> serde_json::Value {
    serde_json::json!({
        "submission_id": record.id.0,
        "kind": record.kind,
        "status": record.status,
        "request_id": record.request_id,
    })
}

/// Convenience predicate used by the engine to find placed submissions.
pub fn submission_is_open(status: SubmissionStatus) -> bool {
    matches!(status, SubmissionStatus::Queued | SubmissionStatus::Placed)
}

/// Small helper for adapters that need to reconstruct an owner ref.
pub fn owner_ref(conversation_id: ConversationId, task_id: TaskId) -> OwnerRef {
    OwnerRef {
        conversation_id,
        task_id,
    }
}

/// Parent ref helper for forks (deferred feature, stored shape only).
pub fn parent_ref(conversation_id: ConversationId, at: EntryId) -> ParentRef {
    ParentRef {
        conversation_id,
        at,
    }
}
