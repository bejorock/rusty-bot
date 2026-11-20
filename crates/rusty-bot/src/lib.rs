//! Embedded, process-local Rusty Bot SDK.
//!
//! Local execution is trusted-host execution, not a sandbox. Memory storage
//! does not survive process exit. See the SDK guide for lifecycle limitations.
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
