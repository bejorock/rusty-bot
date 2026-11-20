use std::ops::Range;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{HistoryRequest, Message};

/// Application-provided, request-local history transformation. Called before
/// request hooks on each engine-driven generation. `None` leaves history intact.
/// It must preserve opaque rich replay messages and coherent tool exchanges;
/// provider-aware replay rewriting requires a separate validated path.
#[async_trait]
pub trait RequestCompactor: Send + Sync {
    /// Whether Rich compaction should be deferred until a new external submission.
    fn defer_rich_until_new_submission(&self) -> bool {
        false
    }

    /// Whether cancellation must be observed and drained by the strategy itself.
    /// The default preserves the historical immediate-cancellation behavior.
    fn drain_on_cancel(&self) -> bool {
        false
    }

    async fn compact(
        &self,
        request: &HistoryRequest,
        cancel: CancellationToken,
    ) -> Result<Option<Vec<Message>>, CompactionError>;

    /// Propose a transformation. The engine must independently validate every
    /// proposal; this default preserves existing compactor implementations.
    async fn propose(
        &self,
        request: &HistoryRequest,
        cancel: CancellationToken,
    ) -> Result<CompactionProposal, CompactionError> {
        Ok(match self.compact(request, cancel).await? {
            Some(messages) => CompactionProposal::Replacement(messages),
            None => CompactionProposal::Unchanged,
        })
    }
}

/// An untrusted request-local compaction proposal.
#[derive(Clone, Debug, PartialEq)]
pub enum CompactionProposal {
    Unchanged,
    Replacement(Vec<Message>),
    CompletedPrefixSummary {
        remove: Range<usize>,
        summary: String,
    },
    /// A new semantic context, not a replayable native prefix. Exhaustive
    /// matches on this enum must handle this additive variant.
    NativeResetSummary {
        remove: Range<usize>,
        retain_recent_exchanges: usize,
        summary: String,
    },
}

/// Strategy failure. Details are deliberately not propagated into session history.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("request compaction failed")]
pub struct CompactionError;
