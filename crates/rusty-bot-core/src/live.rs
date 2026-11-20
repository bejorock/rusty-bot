use serde::{Deserialize, Serialize};

use crate::ids::{SessionId, TaskId};

/// Transient progress frames for push transports. Never durable state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveFrame {
    pub task_id: Option<TaskId>,
    pub kind: LiveKind,
    pub data: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveKind {
    TextDelta,
    ThinkingDelta,
    ToolDelta,
    Progress,
}

/// Synchronous, non-blocking, lossy sink. Implementations must never panic and
/// must never block the caller on a database round trip.
pub trait LiveSink: Send + Sync {
    fn publish(&self, session_id: SessionId, frame: LiveFrame);
}

#[derive(Debug, Default)]
pub struct NullLiveSink;

impl LiveSink for NullLiveSink {
    fn publish(&self, _session_id: SessionId, _frame: LiveFrame) {}
}
