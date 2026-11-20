//! Private persistence primitive for native reset replay.
//!
//! The caller must serialize all operations for this conversation under its existing
//! single-owner gate; task versions are NOT a conversation-wide CAS. Before dispatch,
//! it must enforce provider validation, hook/manual restrictions, and, on ordinary
//! continuation, that the previously sent prefix is unchanged. Only an independently
//! validated new reset may replace that prefix. Save the exact post-hook request and
//! the inclusive last source-entry frontier; use the returned task record thereafter.
//! Load on every reopen/continuation, even when the compactor is no longer installed.
//! No method logs checkpoint contents. The storage backend determines durability.

use std::io::{self, Write as IoWrite};

use rusty_bot_core::{
    CommitContext, ConversationId, DocumentAddress, DocumentPoint, DocumentScope, DocumentWrite,
    EntryId, EntryQuery, EntryRecord, HistoryRequest, Message, SessionError, SessionId, Storage,
    StoredEntry, TaskId, TaskRecord, Write,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{prompt, transaction::Transaction};

pub(crate) const NATIVE_CONTEXT_KIND: &str = "rusty-bot.internal.native-reset-context.v1";
const MARKER_KIND: &str = "rusty-bot.internal.native-reset-marker.v1";
const VERSION: u32 = 1;
const MAX_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// Sensitive, versioned exact post-hook request. Do not log or expose this record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeContextCheckpoint {
    pub(crate) version: u32,
    pub(crate) provider_id: String,
    pub(crate) request: HistoryRequest,
    pub(crate) task_id: TaskId,
    pub(crate) frontier: EntryId,
    pub(crate) epoch: Uuid,
    pub(crate) marker_id: EntryId,
}

fn invalid() -> SessionError {
    SessionError::ContractViolation("invalid native context checkpoint".into())
}

fn address(conversation: ConversationId) -> DocumentAddress {
    DocumentAddress::new(
        NATIVE_CONTEXT_KIND,
        DocumentScope::Conversation(conversation),
        "",
    )
}

// Bound serialized bytes before constructing a JSON value, including escaping and
// base64 expansion. This also bounds input validation on already stored values.
struct Bounded(Vec<u8>);
impl IoWrite for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_CHECKPOINT_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("checkpoint too large"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bounded_json<T: Serialize>(value: &T) -> Result<serde_json::Value, SessionError> {
    let mut out = Bounded(Vec::new());
    serde_json::to_writer(&mut out, value).map_err(|_| invalid())?;
    serde_json::from_slice(&out.0).map_err(|_| invalid())
}

fn identity_matches(a: &HistoryRequest, b: &HistoryRequest) -> bool {
    a.model == b.model
        && a.tools == b.tools
        && a.tool_choice == b.tool_choice
        && a.temperature == b.temperature
        && a.max_tokens == b.max_tokens
        && a.metadata == b.metadata
        && matches!((a.messages.first(), b.messages.first()),
            (Some(x @ Message::System { .. }), Some(y @ Message::System { .. })) if x == y)
}

async fn markers(
    storage: &dyn Storage,
    conversation: ConversationId,
) -> Result<Vec<StoredEntry>, SessionError> {
    let mut found = Vec::new();
    let mut cursor = None;
    loop {
        let page = storage
            .scan_entries(
                EntryQuery {
                    conversation_id: conversation,
                    kind: Some(MARKER_KIND.into()),
                    by_task_id: None,
                    ascending: true,
                },
                cursor,
                500,
            )
            .await?;
        let count = page.items.len();
        found.extend(page.items);
        match page.next {
            Some(next) if count > 0 => cursor = Some(next),
            _ => break,
        }
    }
    Ok(found)
}

/// Return the stored checkpoint and reconstructed effective request, or None
/// only when neither document nor discovery marker exists. `composed` supplies
/// the current options and composed system, not a comparable raw transcript.
pub(crate) async fn load(
    storage: &dyn Storage,
    conversation: ConversationId,
    provider_id: &str,
    composed: &HistoryRequest,
) -> Result<Option<(NativeContextCheckpoint, HistoryRequest)>, SessionError> {
    let markers = markers(storage, conversation).await?;
    let document = storage
        .get_document(&address(conversation), DocumentPoint::Current)
        .await?;
    if markers.is_empty() && document.is_none() {
        return Ok(None);
    }
    if markers.len() != 1 {
        return Err(invalid());
    }
    let marker = &markers[0].record;
    let document = document.ok_or_else(invalid)?;
    let value = bounded_json(&document.value)?;
    let checkpoint: NativeContextCheckpoint =
        serde_json::from_value(value).map_err(|_| invalid())?;
    if checkpoint.version != VERSION
        || checkpoint.provider_id.is_empty()
        || checkpoint.provider_id != provider_id
        || !identity_matches(&checkpoint.request, composed)
        || marker.id != checkpoint.marker_id
        || !marker.model.is_empty()
        || marker.head.is_some()
        || !marker.edits.is_empty()
        || marker.data != serde_json::json!({"epoch": checkpoint.epoch})
        || checkpoint.epoch.is_nil()
        || checkpoint.frontier == marker.id
    {
        return Err(invalid());
    }
    let entries = prompt::load_entries(storage, conversation).await?;
    let frontier = entries
        .iter()
        .position(|e| e.record.id == checkpoint.frontier)
        .ok_or_else(invalid)?;
    let mut request = checkpoint.request.clone();
    request.messages.extend(
        entries[frontier + 1..]
            .iter()
            .flat_map(|entry| entry.record.model.iter())
            .filter(|message| !matches!(message, Message::System { .. }))
            .cloned(),
    );
    Ok(Some((checkpoint, request)))
}

/// Persist a new effective baseline and advance task version in one batch.
/// Caller must hold the single-owner gate across load, validation, save, and
/// dispatch; it must enforce prefix preservation except on a validated reset.
/// `frontier` must already exist in the currently visible committed entries.
/// `task` is the current committed task; only the returned version is usable on
/// success. A refusal or error never permits dispatch.
pub(crate) async fn save(
    storage: &dyn Storage,
    session: SessionId,
    conversation: ConversationId,
    task: &TaskRecord,
    provider_id: &str,
    request: &HistoryRequest,
    frontier: EntryId,
) -> Result<TaskRecord, SessionError> {
    if task.conversation_id != conversation
        || provider_id.is_empty()
        || !matches!(request.messages.first(), Some(Message::System { .. }))
    {
        return Err(invalid());
    }
    let entries = prompt::load_entries(storage, conversation).await?;
    let position = entries
        .iter()
        .position(|e| e.record.id == frontier && e.record.kind != MARKER_KIND)
        .ok_or_else(invalid)?;
    let previous = load(storage, conversation, provider_id, request).await?;
    let (epoch, marker_id, marker) = if let Some((previous, _)) = previous {
        let old_position = entries
            .iter()
            .position(|e| e.record.id == previous.frontier)
            .ok_or_else(invalid)?;
        if position < old_position {
            return Err(invalid());
        }
        (previous.epoch, previous.marker_id, None)
    } else {
        let epoch = Uuid::new_v4();
        let id = EntryId(storage.mint_id().await?);
        let marker = EntryRecord {
            id,
            conversation_id: conversation,
            kind: MARKER_KIND.into(),
            model: Vec::new(),
            data: serde_json::json!({"epoch": epoch}),
            head: None,
            edits: Vec::new(),
            by_task_id: Some(task.id),
        };
        (epoch, id, Some(marker))
    };
    // Check the borrowed request before cloning potentially large image data.
    bounded_json(request)?;
    let checkpoint = NativeContextCheckpoint {
        version: VERSION,
        provider_id: provider_id.into(),
        request: request.clone(),
        task_id: task.id,
        frontier,
        epoch,
        marker_id,
    };
    let value = bounded_json(&checkpoint)?;
    let mut updated = task.clone();
    updated.version = task.version.checked_add(1).ok_or_else(invalid)?;
    let mut tx = Transaction::new(storage, CommitContext::task(session, conversation, task.id));
    if let Some(marker) = marker {
        tx.write(Write::Entry(marker))?;
    }
    tx.write(Write::Document(DocumentWrite::PutLatest {
        address: address(conversation),
        value,
    }))?;
    tx.write(Write::Task(updated.clone()))?;
    let receipt = tx.commit().await?;
    if receipt.refused.is_some() || receipt.batches.len() != 1 {
        return Err(invalid());
    }
    Ok(updated)
}
