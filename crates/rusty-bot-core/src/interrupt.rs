//! Developer-called interrupt identities and payloads. These values may be sensitive.
use serde::{Deserialize, Serialize};

use crate::{ConversationId, SessionId, TaskId};

/// Opaque interrupt identity. Engines generate a fresh ID for every encounter.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InterruptId(pub String);

/// A committed, externally answerable tool interrupt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingInterrupt {
    pub id: InterruptId,
    pub session_id: SessionId,
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
    pub round: u64,
    pub ordinal: usize,
    pub index: u64,
    pub payload: serde_json::Value,
}
