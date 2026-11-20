//! Explicit review boundary types. Registration alone does not enable review.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ConversationId, HookContext, HookError, SessionId, TaskId, ToolCall};

/// Opaque identifier generated and persisted by the review driver.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReviewId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DecisionKind {
    Approve,
    Edit,
    Reject,
    Respond,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReviewDecision {
    Approve,
    Edit { arguments: Value },
    Reject { reason: String },
    Respond { text: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewAnswer {
    pub id: ReviewId,
    pub decision: ReviewDecision,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReviewAction {
    Tool { pub_call: ToolCall },
    Question { question: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingReview {
    pub id: ReviewId,
    pub session_id: SessionId,
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
    pub round: u64,
    pub ordinal: usize,
    pub action: ReviewAction,
    pub allowed_decisions: BTreeSet<DecisionKind>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewRequest {
    pub allowed_decisions: BTreeSet<DecisionKind>,
}

/// Trusted in-process review policy, distinct from fail-open ordinary hooks.
#[async_trait::async_trait]
pub trait ReviewGate: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> &str;
    async fn review(
        &self,
        call: &ToolCall,
        ctx: &HookContext,
    ) -> Result<Option<ReviewRequest>, HookError>;
}
