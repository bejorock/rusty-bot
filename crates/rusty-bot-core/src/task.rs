use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{ConversationId, OwnerRef, TaskId, TimestampMs};

/// Durable task record. Every write to an existing record must increment
/// `version`; storage enforces optimistic concurrency.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: TaskId,
    pub conversation_id: ConversationId,
    pub kind: String,
    pub version: u32,
    pub input: serde_json::Value,
    pub owner: Option<OwnerRef>,
    pub background: bool,
    pub abort_requested: bool,
    pub abort_reason: Option<String>,
    pub abandon_on_restart: bool,
    pub started_at: Option<TimestampMs>,
    pub ended_at: Option<TimestampMs>,
    pub restarts: u32,
    pub state: TaskState,
    pub memos: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum TaskState {
    Pending {
        checkpoint: Option<ToolCheckpoint>,
        wake: Option<WakeSpec>,
    },
    Running {
        checkpoint: Option<ToolCheckpoint>,
        wake: Option<WakeSpec>,
    },
    Waiting {
        checkpoint: Option<ToolCheckpoint>,
        on: Vec<String>,
        policy: WaitPolicy,
    },
    Sleeping {
        checkpoint: Option<ToolCheckpoint>,
        until: Option<TimestampMs>,
        signals: Vec<String>,
    },
    Completing {
        outcome: TaskOutcome,
    },
    Terminal {
        outcome: TaskOutcome,
    },
}

impl TaskState {
    pub fn phase(&self) -> &'static str {
        match self {
            TaskState::Pending { .. } => "pending",
            TaskState::Running { .. } => "running",
            TaskState::Waiting { .. } => "waiting",
            TaskState::Sleeping { .. } => "sleeping",
            TaskState::Completing { .. } => "completing",
            TaskState::Terminal { .. } => "terminal",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Terminal { .. })
    }

    pub fn checkpoint(&self) -> Option<&ToolCheckpoint> {
        match self {
            TaskState::Pending { checkpoint, .. }
            | TaskState::Running { checkpoint, .. }
            | TaskState::Waiting { checkpoint, .. }
            | TaskState::Sleeping { checkpoint, .. } => checkpoint.as_ref(),
            TaskState::Completing { .. } | TaskState::Terminal { .. } => None,
        }
    }

    /// The legal state machine:
    /// pending -> running
    /// running -> waiting | sleeping | completing | terminal
    /// waiting -> running | terminal
    /// sleeping -> running | terminal
    /// completing -> terminal
    /// terminal -> (none)
    pub fn can_transition_to(&self, next: &TaskState) -> bool {
        use TaskState::*;
        match (self, next) {
            (Pending { .. }, Running { .. }) => true,
            (
                Running { .. },
                Waiting { .. } | Sleeping { .. } | Completing { .. } | Terminal { .. },
            ) => true,
            (Waiting { .. }, Running { .. } | Terminal { .. }) => true,
            (Sleeping { .. }, Running { .. } | Terminal { .. }) => true,
            (Completing { .. }, Terminal { .. }) => true,
            // Same-phase updates (checkpoint/memo changes) do not transition.
            (a, b) if a.phase() == b.phase() => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TaskOutcome {
    Completed {
        result: serde_json::Value,
    },
    Failed {
        error: String,
        result: Option<serde_json::Value>,
    },
    Aborted {
        reason: Option<String>,
        result: Option<serde_json::Value>,
    },
    Orphaned {
        reason: String,
    },
    Faulted {
        error: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ToolCheckpoint {
    Call,
    Execute {
        arguments: serde_json::Value,
        replay: ReplayPolicy,
        wake: Option<WakeSpec>,
        checkpoint: Option<Box<ToolCheckpoint>>,
    },
    Parked {
        arguments: serde_json::Value,
        replay: ReplayPolicy,
        checkpoint: Option<Box<ToolCheckpoint>>,
    },
}

impl ToolCheckpoint {
    pub fn replay(&self) -> Option<ReplayPolicy> {
        match self {
            ToolCheckpoint::Call => None,
            ToolCheckpoint::Execute { replay, .. } | ToolCheckpoint::Parked { replay, .. } => {
                Some(*replay)
            }
        }
    }
}

/// Whether an interrupted tool effect may be repeated.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPolicy {
    Safe,
    /// The default: an interrupted effect may have occurred and must not be
    /// repeated automatically.
    #[default]
    Unsafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitPolicy {
    All,
    Any,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeSpec {
    pub on: Vec<String>,
    pub policy: WaitPolicy,
}
