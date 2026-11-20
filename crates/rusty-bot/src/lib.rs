//! Embedded, process-local Rusty Bot SDK.
//!
//! Local execution is trusted-host execution, not a sandbox. Sessions use a
//! durable JSONL adapter by default. Inject MemoryStorage for ephemeral state.
mod runtime;

pub use runtime::{Bot, BotBuilder, RunHandle, RunResult, RunStatus, SdkError, Session};
pub use rusty_bot_core::*;
pub use rusty_bot_engine::{
    CompactionConfig, ExtensionSelection, ModelSelection, ModelSummarizer, ProgressConfig,
    RejectReason, RunLimits, RunOutcome, SessionConfig, SubmitOutcome, SummaryConfig,
    SummaryConfigError, ThinkingLevel, ToolFilter,
};
pub use rusty_bot_local::LocalWorkspaceProvider;
pub use rusty_bot_memory::MemoryStorage;

pub use rusty_bot_memory::FileSessionAdapter;
