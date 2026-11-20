//! Rusty Bot session engine.
//!
//! The engine owns all session writes: submissions, turn tasks, tool-operation
//! journals, transcript entries, documents, and durable events. Model and tool
//! effects always happen outside storage transactions, sandwiched by committed
//! intent and outcome records.

pub mod builtin;
pub mod compaction;
pub mod deferred;
mod hooks;
// The persistence primitive is deliberately dormant until dispatch integration.
#[allow(dead_code)]
pub(crate) mod native_context;
pub mod prompt;
pub mod registry;
pub mod scheduler;
pub mod session;
pub mod submission;
pub mod tool_task;
pub mod tools;
pub mod transaction;

pub use builtin::{builtin_extension, builtin_registry};
pub use compaction::{ModelSummarizer, SummaryConfig, SummaryConfigError};
pub use deferred::DeferredBuffer;
pub use prompt::{build_chat_request, build_history_request, load_entries, map_message};
pub use registry::{Registry, RegistryBuilder, ResolvedExtensions, ResolvedToolSet};
pub use scheduler::TURN_TASK_KIND;
pub use session::{
    CompactionConfig, ExtensionSelection, ModelSelection, ProgressConfig, RejectReason, RunLimits,
    RunOutcome, SessionConfig, SessionDeps, SessionEngine, SessionSpec, SubmitOutcome,
    SubmitRequest, ThinkingLevel, ToolFilter, ToolRunOutcome, WorkItem,
};
pub use submission::recover_queued;
pub use tool_task::{idempotency_key, recovery_decision, RecoveryDecision};
pub use transaction::{Transaction, TxError};
