//! Effect-sandwich decisions for tool execution.
//!
//! The intent journal row is written before the external effect and the
//! outcome afterwards. Recovery must never repeat an effect that may already
//! have happened unless both the tool and the recorded checkpoint are safe.

use rusty_bot_core::{ReplayPolicy, ToolOperationRecord, ToolOperationStatus};
use serde_json::{json, Value};

#[cfg(test)]
#[path = "tool_task_tests.rs"]
mod tests;

const PARALLEL_METADATA: &str = "rusty_bot_parallel_intent";

/// Versioned metadata is separate from legacy result payloads.
pub(crate) fn parallel_intent(replay: ReplayPolicy, replayed: bool) -> Value {
    json!({PARALLEL_METADATA: {"version": 1, "replay": replay, "replayed": replayed}, "replayed": replayed})
}

pub(crate) fn recorded_replay(operation: &ToolOperationRecord) -> Result<Option<ReplayPolicy>, ()> {
    let Some(value) = operation
        .result
        .as_ref()
        .and_then(|v| v.get(PARALLEL_METADATA))
    else {
        return Ok(None);
    };
    if value.get("version").and_then(Value::as_u64) != Some(1)
        || value.get("replayed").and_then(Value::as_bool).is_none()
    {
        return Err(());
    }
    serde_json::from_value(value.get("replay").cloned().ok_or(())?)
        .map(Some)
        .map_err(|_| ())
}

/// Deterministic idempotency key for a tool call. Retries reuse the key.
pub fn idempotency_key(tool_name: &str, tool_call_id: &str) -> String {
    format!("{tool_name}:{tool_call_id}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryDecision {
    /// No journal row exists; execute normally.
    Execute,
    /// A recorded outcome exists; reuse it without executing.
    Reuse,
    /// The effect may have happened but both policies are safe; execute once.
    Reexecute,
    /// The effect may have happened and must not be repeated. Settle the
    /// operation terminally; no host answer or resumable pause is involved.
    Stop,
}

/// Replay decision for an interrupted tool operation.
pub fn recovery_decision(
    stored: Option<&ToolOperationRecord>,
    checkpoint_replay: ReplayPolicy,
    tool_replay: ReplayPolicy,
) -> RecoveryDecision {
    let Some(stored) = stored else {
        return RecoveryDecision::Execute;
    };
    match stored.status {
        ToolOperationStatus::Succeeded | ToolOperationStatus::Failed => RecoveryDecision::Reuse,
        ToolOperationStatus::Intent
        | ToolOperationStatus::Running
        | ToolOperationStatus::Interrupted
        | ToolOperationStatus::Uncertain => {
            let already_replayed = stored
                .result
                .as_ref()
                .and_then(|result| {
                    if let Some(metadata) = result.get(PARALLEL_METADATA) {
                        metadata
                            .get("replayed")
                            .and_then(Value::as_bool)
                            .or(Some(true))
                    } else {
                        result.get("replayed").and_then(Value::as_bool)
                    }
                })
                .unwrap_or(false);
            if checkpoint_replay == ReplayPolicy::Safe
                && tool_replay == ReplayPolicy::Safe
                && !already_replayed
            {
                RecoveryDecision::Reexecute
            } else {
                RecoveryDecision::Stop
            }
        }
    }
}

/// Status recorded for a completed tool execution.
pub fn completion_status(output: &rusty_bot_core::ToolOutput) -> ToolOperationStatus {
    if output.is_error {
        ToolOperationStatus::Failed
    } else {
        ToolOperationStatus::Succeeded
    }
}
