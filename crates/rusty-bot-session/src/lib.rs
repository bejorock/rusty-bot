//! Shared session persistence contract. No Temporal, provider or database dependency.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
mod history;
pub use history::*;
mod generation;
pub use generation::*;
mod message_stream;
pub use message_stream::*;
mod prompt;
pub use prompt::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", content = "text", rename_all = "snake_case")]
pub enum ChatMessage {
    User(String),
    Assistant(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageStatus {
    Generating,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    pub turn_id: u64,
    pub message: ChatMessage,
    pub status: MessageStatus,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub messages: Vec<SessionMessage>,
    pub generating: bool,
    /// The most recent execution ended, not a permanent prohibition on resuming.
    pub closing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionConfig {
    pub id: String,
    pub model: String,
    pub namespace: String,
    pub task_queue: String,
    pub idle_timeout_ms: u64,
    /// Immutable rendered context. Templates are resolved by the application before storage.
    #[serde(default)]
    pub initial_history: ChatHistory,
}

impl SessionConfig {
    pub fn validate(&self) -> StorageResult<()> {
        if self.id.trim().is_empty()
            || self.model.trim().is_empty()
            || self.namespace.trim().is_empty()
            || self.task_queue.trim().is_empty()
            || self.idle_timeout_ms == 0
        {
            return Err(StorageError::Conflict(
                "invalid session configuration".into(),
            ));
        }
        self.initial_history
            .validate_complete()
            .map_err(|e| StorageError::Conflict(e.to_string()))?;
        if self
            .initial_history
            .entries
            .iter()
            .any(|entry| entry.id.starts_with("turn:"))
        {
            return Err(StorageError::Conflict(
                "initial history IDs must not use reserved turn: prefix".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTurn {
    pub id: u64,
    pub request_id: String,
    pub text: String,
    pub draft: String,
    pub sequence: u64,
    pub result: Option<Result<String, String>>,
    /// Canonical assistant response. Legacy turns have no rich response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<HistoryEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSession {
    pub config: SessionConfig,
    pub workflow_id: String,
    pub owner_run: Option<String>,
    pub retired_runs: Vec<String>,
    pub closed: bool,
    pub turns: Vec<StoredTurn>,
}
impl StoredSession {
    pub fn new(config: SessionConfig, workflow_id: String) -> Self {
        Self {
            config,
            workflow_id,
            owner_run: None,
            retired_runs: vec![],
            closed: false,
            turns: vec![],
        }
    }
    /// Full provider-neutral transcript. Legacy snapshot remains a text-turn view.
    pub fn history(&self) -> ChatHistory {
        let mut history = self.config.initial_history.clone();
        for turn in &self.turns {
            let mut user = HistoryEntry::completed(
                format!("turn:{}:user", turn.id),
                ConversationMessage::user(turn.text.clone()),
            );
            user.turn_id = Some(turn.id);
            history.entries.push(user);
            if let Some(response) = &turn.response {
                history.entries.push(response.clone());
                continue;
            }
            let (text, status, failure) = match &turn.result {
                Some(Ok(text)) => (text.clone(), MessageStatus::Completed, None),
                Some(Err(error)) => (
                    turn.draft.clone(),
                    MessageStatus::Failed,
                    Some(error.clone()),
                ),
                None => (turn.draft.clone(), MessageStatus::Generating, None),
            };
            history.entries.push(HistoryEntry {
                id: format!("turn:{}:assistant", turn.id),
                turn_id: Some(turn.id),
                message: ConversationMessage::assistant(text),
                status,
                failure,
                metadata: BTreeMap::new(),
                created_at_ms: None,
                generation: None,
                artifact: None,
            });
        }
        history
    }
    pub fn snapshot(&self) -> SessionSnapshot {
        let mut messages = vec![];
        for turn in &self.turns {
            messages.push(SessionMessage {
                turn_id: turn.id,
                message: ChatMessage::User(turn.text.clone()),
                status: MessageStatus::Completed,
            });
            let (text, status) = match &turn.result {
                Some(Ok(text)) => (text.clone(), MessageStatus::Completed),
                Some(Err(_)) => (turn.draft.clone(), MessageStatus::Failed),
                None => (turn.draft.clone(), MessageStatus::Generating),
            };
            messages.push(SessionMessage {
                turn_id: turn.id,
                message: ChatMessage::Assistant(text),
                status,
            });
        }
        SessionSnapshot {
            messages,
            generating: self.turns.iter().any(|t| t.result.is_none()),
            closing: self.closed,
        }
    }
    /// Legacy text turns only. Use history() for the full canonical transcript.
    pub fn completed_context(&self) -> Vec<ChatMessage> {
        self.turns
            .iter()
            .filter(|t| t.response.is_none())
            .filter_map(|t| match &t.result {
                Some(Ok(reply)) => Some([
                    ChatMessage::User(t.text.clone()),
                    ChatMessage::Assistant(reply.clone()),
                ]),
                _ => None,
            })
            .flatten()
            .collect()
    }
    /// Legacy model-context projection refuses any rich initial/response history.
    pub fn try_completed_context(&self) -> Result<Vec<ChatMessage>, HistoryError> {
        if self.requires_history_translation() {
            return Err(HistoryError(
                "provider-neutral history requires explicit provider translation".into(),
            ));
        }
        Ok(self.completed_context())
    }
    /// Safe admission guard for the legacy text engine, before beginning a new turn.
    pub fn requires_history_translation(&self) -> bool {
        !self.config.initial_history.entries.is_empty()
            || self.turns.iter().any(|t| t.response.is_some())
    }
    /// Pure transition shared by all adapters. Apply atomically under the session lock.
    /// Errors never mutate state. A caller must persist the entire result or nothing.
    pub fn apply(&mut self, change: &SessionChange) -> StorageResult<()> {
        match change {
            SessionChange::Claim { run_id } => {
                if run_id.trim().is_empty() {
                    return Err(StorageError::Conflict("blank run ID".into()));
                }
                if self.owner_run.as_ref() == Some(run_id) {
                    return Ok(());
                }
                if self.retired_runs.contains(run_id) {
                    return Err(StorageError::Conflict(
                        "retired run cannot reclaim session".into(),
                    ));
                }
                if let Some(old) = self.owner_run.replace(run_id.clone()) {
                    self.retired_runs.push(old);
                }
                for turn in &mut self.turns {
                    if turn.result.is_none() {
                        turn.result =
                            Some(Err("previous execution ended before turn committed".into()));
                    }
                }
                self.closed = false;
                return Ok(());
            }
            _ if self.owner_run.as_deref() != Some(change.run_id()) => {
                return Err(StorageError::Conflict("stale session owner".into()))
            }
            _ => {}
        }
        match change {
            SessionChange::Begin {
                request_id, text, ..
            } => {
                if request_id.trim().is_empty() || text.trim().is_empty() {
                    return Err(StorageError::Conflict("blank request ID or message".into()));
                }
                if let Some(old) = self.turns.iter().find(|t| t.request_id == *request_id) {
                    return if old.text == *text {
                        Ok(())
                    } else {
                        Err(StorageError::Conflict(
                            "request ID reused with different text".into(),
                        ))
                    };
                }
                if self.closed || self.turns.iter().any(|t| t.result.is_none()) {
                    return Err(StorageError::Conflict(
                        "session closed or turn active".into(),
                    ));
                }
                let id = self.turns.last().map_or(1, |t| t.id + 1);
                self.turns.push(StoredTurn {
                    id,
                    request_id: request_id.clone(),
                    text: text.clone(),
                    draft: String::new(),
                    sequence: 0,
                    result: None,
                    response: None,
                });
            }
            SessionChange::Draft {
                turn_id,
                sequence,
                text,
                ..
            } => {
                let turn = self.turn_mut(*turn_id)?;
                if turn.result.is_none() && *sequence > turn.sequence {
                    turn.draft = text.clone();
                    turn.sequence = *sequence;
                }
            }
            SessionChange::Finish {
                turn_id, result, ..
            } => {
                let turn = self.turn_mut(*turn_id)?;
                if turn.response.is_some() {
                    return Err(StorageError::Conflict(
                        "canonical response already committed".into(),
                    ));
                }
                if let Some(old) = &turn.result {
                    if old != result {
                        return Err(StorageError::Conflict(
                            "turn already committed with a different result".into(),
                        ));
                    }
                } else {
                    turn.result = Some(result.clone());
                }
            }
            SessionChange::FinishResponse {
                turn_id, response, ..
            } => {
                if response.id != format!("turn:{turn_id}:assistant")
                    || response.turn_id != Some(*turn_id)
                    || response.status != MessageStatus::Completed
                    || response.failure.is_some()
                    || !matches!(response.message, ConversationMessage::Assistant { .. })
                {
                    return Err(StorageError::Conflict(
                        "invalid canonical assistant response identity/status".into(),
                    ));
                }
                response
                    .validate()
                    .map_err(|e| StorageError::Conflict(e.to_string()))?;
                let turn = self.turn_mut(*turn_id)?;
                if let Some(old) = &turn.response {
                    if old != response.as_ref() {
                        return Err(StorageError::Conflict(
                            "different canonical response already committed".into(),
                        ));
                    }
                } else {
                    if turn.result.is_some() {
                        return Err(StorageError::Conflict(
                            "legacy result already committed".into(),
                        ));
                    }
                    // Snapshot is a legacy display view, never authoritative model context.
                    turn.result = Some(Ok(response.message.text_only().unwrap_or_default()));
                    turn.response = Some(*response.clone());
                }
            }
            SessionChange::Close { .. } => {
                if self.turns.iter().any(|t| t.result.is_none()) {
                    return Err(StorageError::Conflict("cannot close during a turn".into()));
                }
                self.closed = true;
            }
            SessionChange::Claim { .. } => unreachable!(),
        }
        Ok(())
    }
    fn turn_mut(&mut self, id: u64) -> StorageResult<&mut StoredTurn> {
        self.turns
            .iter_mut()
            .find(|t| t.id == id)
            .ok_or(StorageError::NotFound)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SessionChange {
    Claim {
        run_id: String,
    },
    Begin {
        run_id: String,
        request_id: String,
        text: String,
    },
    Draft {
        run_id: String,
        turn_id: u64,
        sequence: u64,
        text: String,
    },
    Finish {
        run_id: String,
        turn_id: u64,
        result: Result<String, String>,
    },
    FinishResponse {
        run_id: String,
        turn_id: u64,
        response: Box<HistoryEntry>,
    },
    Close {
        run_id: String,
    },
}
impl SessionChange {
    pub fn run_id(&self) -> &str {
        match self {
            Self::Claim { run_id }
            | Self::Begin { run_id, .. }
            | Self::Draft { run_id, .. }
            | Self::Finish { run_id, .. }
            | Self::FinishResponse { run_id, .. }
            | Self::Close { run_id } => run_id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("session or turn not found")]
    NotFound,
    #[error("session conflict: {0}")]
    Conflict(String),
    #[error("session backend: {0}")]
    Backend(String),
}
pub type StorageResult<T> = Result<T, StorageError>;

/// Shared, transactional session adapter injected by the host application.
/// `create` atomically selects one record per session ID, including across independent
/// adapter instances sharing the same backend/keyspace. It is idempotent for identical
/// config: every racing caller receives the existing Workflow ID, never its losing
/// candidate. Incompatible config must fail without replacing the record or identity.
/// This immutable mapping plus Temporal's namespace-level admission provides one active
/// Workflow run per session; adapters must not delete/reassign active session records.
/// `change` atomically validates and applies `StoredSession::apply`; no lost updates.
/// No adapter may acknowledge a write before it is durable to its advertised scope.
#[async_trait]
pub trait SessionStorage: Send + Sync {
    async fn create(&self, session: StoredSession) -> StorageResult<StoredSession>;
    async fn read(&self, id: &str) -> StorageResult<StoredSession>;
    async fn change(&self, id: &str, change: SessionChange) -> StorageResult<StoredSession>;
}

/// Process-local test adapter. Not suitable for a distributed worker deployment.
#[derive(Clone, Default)]
pub struct MemorySessionStorage(Arc<Mutex<BTreeMap<String, StoredSession>>>);
#[async_trait]
impl SessionStorage for MemorySessionStorage {
    async fn create(&self, session: StoredSession) -> StorageResult<StoredSession> {
        session.config.validate()?;
        let mut state = self.0.lock().await;
        if let Some(old) = state.get(&session.config.id) {
            if old.config != session.config {
                return Err(StorageError::Conflict(
                    "session configuration mismatch".into(),
                ));
            }
            return Ok(old.clone());
        }
        state.insert(session.config.id.clone(), session.clone());
        Ok(session)
    }
    async fn read(&self, id: &str) -> StorageResult<StoredSession> {
        self.0
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound)
    }
    async fn change(&self, id: &str, change: SessionChange) -> StorageResult<StoredSession> {
        let mut state = self.0.lock().await;
        let session = state.get_mut(id).ok_or(StorageError::NotFound)?;
        session.apply(&change)?;
        Ok(session.clone())
    }
}

#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod message_contract_tests;
#[cfg(test)]
mod prompt_tests;
#[cfg(test)]
mod tests;
