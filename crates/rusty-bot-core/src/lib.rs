//! Core domain types, records, and replaceable provider interfaces for Rusty Bot.
//!
//! This crate deliberately has no dependency on a concrete storage, HTTP, model,
//! filesystem, or hosting implementation. Every backend boundary is a trait;
//! the engine composes them.

pub mod clock;
pub mod commit;
pub mod compaction;
pub mod credentials;
pub mod discovery;
pub mod document;
pub mod error;
pub mod extension;
pub mod fs;
pub mod history;
pub mod history_bridge;
pub mod hook;
pub mod hook_job;
pub mod ids;
pub mod image_validation;
pub mod live;
pub mod message;
pub mod model;
pub mod payload_validation;
pub mod records;
pub mod shell;
pub mod storage;
pub mod task;
pub mod tool;
pub mod truncate;
pub mod workspace;

pub use clock::{Clock, SystemClock};
pub use commit::{
    event_kind, BatchRefusal, CommitBatch, CommitContext, CommitReceipt, RefusalReason,
    SessionEventDraft, StoredBatch, StoredEvent, Write,
};
pub use compaction::{CompactionError, CompactionProposal, RequestCompactor};
pub use credentials::{CredentialRequest, CredentialResolver, ProviderCredential};
pub use discovery::{
    ModelCapabilities, ModelDiscovery, ModelInfo, ModelPage, ModelPageRequest, Support,
};
pub use document::{
    DocumentAddress, DocumentFork, DocumentPoint, DocumentQuery, DocumentScope, DocumentValue,
    DocumentWrite,
};
pub use error::SessionError;
pub use extension::RegistryError;
pub use extension::{Extension, PromptSection};
pub use fs::{
    DirEntry, FileByteStream, FileError, FileErrorCode, FileKind, FileSystem, FindQuery, GrepQuery,
    ListQuery, SearchPage, SearchRequest,
};
pub use history::{canonical_tool_calls, HistoryPart, ProviderState, RichRole};
pub use history_bridge::{history_turn_from_legacy, project_history};
pub use hook::{
    sanitize_hook_code, AfterTools, AfterToolsJobs, BeforeTool, Hook, HookContext, HookDescriptor,
    HookError, HookKey, HookPoint, HookPoints, HookReport, HookReporter, RunReason, RunSummary,
    YieldDecision, YieldJobs,
};
pub use hook_job::{
    HookInvocationGuard, HookJob, HookJobError, HookJobPolicy, HookJobSpawner, HookJobStatus,
    HookJobSupervisor,
};
pub use ids::{
    ConversationId, DocumentId, EntryId, Id, OwnerRef, ParentRef, Seq, SessionId, SubmissionId,
    TaskId, TimestampMs,
};
pub use live::{LiveFrame, LiveKind, LiveSink, NullLiveSink};
pub use message::{ContentPart, ContextEdit, EditKind, Message, StopReason, ToolCall, Usage};
pub use model::{
    AssistantTurn, ChatRequest, HistoryRequest, HistoryTurn, ImageLimits, ModelError,
    ModelErrorKind, ModelProvider, ProviderMessage, ProviderPayloadHook, ProviderToolCall,
    StreamEvent, ToolChoice, ToolSpec,
};
pub use payload_validation::validate_provider_payload;
pub use records::{
    entry_kind, ConversationRecord, EntryRecord, SubmissionKind, SubmissionRecord,
    SubmissionStatus, ToolOperationRecord, ToolOperationStatus,
};
pub use shell::{
    CommandSpec, ExecOptions, ExecutionError, ExecutionErrorCode, OutputCallback, OutputChunk,
    OutputMode, OutputStream, Shell, ShellExecResult, SpillPolicy,
};
pub use storage::{
    operation_can_advance, EntryCursor, EntryQuery, EventCursor, EventPage, Page, SessionRecord,
    Storage, StorageError, StorageErrorKind, StoredEntry, StoredSubmission, StoredTask, TaskQuery,
};
pub use task::{
    ReplayPolicy, TaskOutcome, TaskRecord, TaskState, ToolCheckpoint, WaitPolicy, WakeSpec,
};
pub use tool::{Tool, ToolContext, ToolError, ToolOutput};
pub use truncate::{truncate_text, TruncatedText, DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES};
pub use workspace::{
    ExecutionSelection, Workspace, WorkspaceError, WorkspaceKey, WorkspaceProvider, WorkspaceSpec,
};

/// Crate version string, useful for diagnostics.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use storage::SessionFrame;
/// Replaceable session persistence and progress-log interface.
pub use storage::Storage as SessionAdapter;

#[cfg(test)]
mod unit_tests;
