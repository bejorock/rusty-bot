use serde::{Deserialize, Serialize};

use crate::ids::{SessionId, TaskId};

/// Progress data appended to the session adapter before readers observe it.
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

/// Synchronous publication hook. SDK sinks persist through the session adapter;
/// persistence failures cancel the run and are returned by RunHandle::wait.
pub trait LiveSink: Send + Sync {
    fn publish(&self, session_id: SessionId, frame: LiveFrame);
}

#[derive(Debug, Default)]
pub struct NullLiveSink;

impl LiveSink for NullLiveSink {
    fn publish(&self, _session_id: SessionId, _frame: LiveFrame) {}
}
