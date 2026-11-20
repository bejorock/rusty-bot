//! Session adapters with an in-memory index and optional durable JSONL journal.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusty_bot_core::{
    operation_can_advance, BatchRefusal, CommitBatch, CommitContext, CommitReceipt, ConversationId,
    DocumentAddress, DocumentId, DocumentPoint, DocumentQuery, DocumentValue, DocumentWrite,
    EntryCursor, EntryId, EntryQuery, EventCursor, EventPage, Id, Page, RefusalReason, Seq,
    SessionId, SessionRecord, Storage, StorageError, StoredBatch, StoredEntry, StoredEvent,
    StoredSubmission, StoredTask, SubmissionId, TaskId, TaskQuery, TaskRecord, ToolOperationRecord,
    ToolOperationStatus, Write,
};
use std::sync::Mutex;

mod jsonl;
pub use jsonl::FileSessionAdapter;
use jsonl::{Journal, LogRecord};

use rusty_bot_core::storage::{document_write_address, StoredConversation};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
struct Revision {
    seq: Seq,
    version: u32,
    value: serde_json::Value,
}

#[derive(Clone, Debug)]
struct DocumentRow {
    id: DocumentId,
    address: DocumentAddress,
    created_seq: Seq,
    created_at: i64,
    retired_seq: Option<Seq>,
    retired_at: Option<i64>,
    version: u32,
    revisions: Vec<Revision>,
}

impl DocumentRow {
    fn visible_at(&self, point: DocumentPoint) -> bool {
        match point {
            DocumentPoint::Current => self.retired_at.is_none(),
            DocumentPoint::Seq(seq) => {
                self.created_seq <= seq && self.retired_seq.map(|r| r > seq).unwrap_or(true)
            }
        }
    }

    fn revision_at(&self, point: DocumentPoint) -> Option<&Revision> {
        match point {
            DocumentPoint::Current => self.revisions.last(),
            DocumentPoint::Seq(seq) => self.revisions.iter().rev().find(|r| r.seq <= seq),
        }
    }

    fn value_at(&self, point: DocumentPoint) -> Option<DocumentValue> {
        if !self.visible_at(point) {
            return None;
        }
        let revision = self.revision_at(point)?;
        Some(DocumentValue {
            id: self.id,
            address: self.address.clone(),
            version: revision.version,
            value: revision.value.clone(),
            created_at: self.created_at,
            retired_at: self.retired_at,
        })
    }
}

#[derive(Clone, Default)]
struct Inner {
    next_id: Id,
    next_seq: Seq,
    commits: Vec<(Seq, SessionId)>,
    conversations: HashMap<ConversationId, StoredConversation>,
    entries: HashMap<ConversationId, BTreeMap<EntryId, StoredEntry>>,
    tasks: HashMap<TaskId, StoredTask>,
    submissions: HashMap<SubmissionId, StoredSubmission>,
    request_index: HashMap<(ConversationId, String), SubmissionId>,
    documents: Vec<DocumentRow>,
    operations: HashMap<(TaskId, String), ToolOperationRecord>,
    events: Vec<StoredEvent>,
    sessions: HashMap<SessionId, SessionRecord>,
    frames: Vec<rusty_bot_core::storage::SessionFrame>,
}

impl Inner {
    fn new() -> Self {
        Self {
            next_id: 1,
            next_seq: 1,
            ..Default::default()
        }
    }

    fn mint_id(&mut self) -> Id {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

enum ApplyError {
    Refusal(RefusalReason),
    Fatal(StorageError),
}

impl From<RefusalReason> for ApplyError {
    fn from(value: RefusalReason) -> Self {
        ApplyError::Refusal(value)
    }
}

impl From<StorageError> for ApplyError {
    fn from(value: StorageError) -> Self {
        ApplyError::Fatal(value)
    }
}

/// Process-local storage. It applies one atomic transaction per batch
/// and implements the same refusal semantics expected from PostgreSQL.
pub struct InMemoryStorage {
    inner: Mutex<Inner>,
    journal: Option<Mutex<Journal>>,
}

/// Supported production name for the process-local store.
pub type MemoryStorage = InMemoryStorage;

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            journal: None,
        }
    }

    fn persist(&self, record: &LogRecord) -> Result<(), StorageError> {
        if let Some(journal) = &self.journal {
            journal
                .lock()
                .map_err(|_| StorageError::internal("journal lock poisoned"))?
                .append(record)?;
        }
        Ok(())
    }

    pub fn shared() -> Arc<dyn Storage> {
        Arc::new(Self::new())
    }
}

#[async_trait::async_trait]
impl Storage for InMemoryStorage {
    fn append_frame(
        &self,
        session: SessionId,
        frame: rusty_bot_core::LiveFrame,
    ) -> Result<(), StorageError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let record = rusty_bot_core::storage::SessionFrame {
            cursor: inner.frames.last().map_or(1, |f| f.cursor + 1),
            session_id: session,
            frame,
        };
        self.persist(&LogRecord::Frame {
            record: record.clone(),
        })?;
        inner.frames.push(record);
        Ok(())
    }

    async fn read_frames(
        &self,
        session: SessionId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<rusty_bot_core::storage::SessionFrame>, StorageError> {
        if limit == 0 {
            return Err(StorageError::constraint("frame limit must be nonzero"));
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .frames
            .iter()
            .skip(inner.frames.partition_point(|f| f.cursor <= after))
            .filter(|f| f.session_id == session)
            .take(limit)
            .cloned()
            .collect())
    }
    async fn last_frame_cursor(&self, session: SessionId) -> Result<u64, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .frames
            .iter()
            .rev()
            .find(|f| f.session_id == session)
            .map_or(0, |f| f.cursor))
    }

    async fn mint_id(&self) -> Result<Id, StorageError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let id = inner.next_id;
        self.persist(&LogRecord::Mint { id })?;
        inner.mint_id();
        Ok(id)
    }

    async fn commit(
        &self,
        batches: Vec<CommitBatch>,
        ctx: &CommitContext,
    ) -> Result<CommitReceipt, StorageError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut receipt = CommitReceipt::default();
        for (index, batch) in batches.into_iter().enumerate() {
            let now = now_ms();
            match inner.validate_commit(&batch) {
                Ok(()) => {
                    self.persist(&LogRecord::Batch {
                        writes: batch.writes.clone(),
                        session_id: ctx.session_id,
                        conversation_id: ctx.conversation_id,
                        task_id: ctx.task_id,
                        now,
                    })?;
                    let (seq, event_count) = inner.apply_validated(&batch, ctx, now);
                    receipt.batches.push(StoredBatch {
                        index,
                        seq,
                        event_count,
                    });
                }
                Err(ApplyError::Refusal(reason)) => {
                    receipt.refused = Some(BatchRefusal {
                        batch_index: index,
                        reason,
                    });
                    return Ok(receipt);
                }
                Err(ApplyError::Fatal(error)) => return Err(error),
            }
        }
        Ok(receipt)
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .conversations
            .get(&id)
            .map(|stored| StoredConversation {
                record: stored.record.clone(),
                commit_seq: stored.commit_seq,
            }))
    }

    async fn get_entry(
        &self,
        conv: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .entries
            .get(&conv)
            .and_then(|entries| entries.get(&id))
            .map(|stored| StoredEntry {
                record: stored.record.clone(),
                commit_seq: stored.commit_seq,
            }))
    }

    async fn scan_entries(
        &self,
        query: EntryQuery,
        cursor: Option<EntryCursor>,
        limit: usize,
    ) -> Result<Page<StoredEntry, EntryCursor>, StorageError> {
        let limit = limit.clamp(1, 1000);
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut entries: Vec<&StoredEntry> = inner
            .entries
            .get(&query.conversation_id)
            .map(|entries| entries.values().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .filter(|entry| {
                query
                    .kind
                    .as_ref()
                    .map(|kind| &entry.record.kind == kind)
                    .unwrap_or(true)
            })
            .filter(|entry| {
                query
                    .by_task_id
                    .map(|task| entry.record.by_task_id == Some(task))
                    .unwrap_or(true)
            })
            .filter(|entry| {
                cursor
                    .map(|cursor| {
                        if query.ascending {
                            entry.record.id > cursor
                        } else {
                            entry.record.id < cursor
                        }
                    })
                    .unwrap_or(true)
            })
            .collect();
        entries.sort_by_key(|entry| entry.record.id);
        if !query.ascending {
            entries.reverse();
        }
        let next = if entries.len() > limit {
            Some(entries[limit - 1].record.id)
        } else {
            None
        };
        let items = entries
            .into_iter()
            .take(limit)
            .map(|entry| StoredEntry {
                record: entry.record.clone(),
                commit_seq: entry.commit_seq,
            })
            .collect();
        Ok(Page { items, next })
    }

    async fn latest_head_marker(
        &self,
        conv: ConversationId,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .entries
            .get(&conv)
            .and_then(|entries| {
                entries
                    .values()
                    .rev()
                    .find(|entry| entry.record.head.is_some())
            })
            .map(|entry| StoredEntry {
                record: entry.record.clone(),
                commit_seq: entry.commit_seq,
            }))
    }

    async fn latest_entry_id(&self, conv: ConversationId) -> Result<Option<EntryId>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .entries
            .get(&conv)
            .and_then(|entries| entries.keys().next_back().copied()))
    }

    async fn find_entry_by_task(
        &self,
        conv: ConversationId,
        task: TaskId,
        kind: &str,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .entries
            .get(&conv)
            .and_then(|entries| {
                entries.values().rev().find(|entry| {
                    entry.record.by_task_id == Some(task) && entry.record.kind == kind
                })
            })
            .map(|entry| StoredEntry {
                record: entry.record.clone(),
                commit_seq: entry.commit_seq,
            }))
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<StoredTask>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner.tasks.get(&id).map(|stored| StoredTask {
            record: stored.record.clone(),
            commit_seq: stored.commit_seq,
        }))
    }

    async fn scan_tasks(&self, query: TaskQuery) -> Result<Vec<StoredTask>, StorageError> {
        let limit = query.limit.clamp(1, 1000);
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut tasks: Vec<&StoredTask> = inner
            .tasks
            .values()
            .filter(|task| {
                query
                    .conversation_id
                    .map(|conv| task.record.conversation_id == conv)
                    .unwrap_or(true)
            })
            .filter(|task| {
                query
                    .status
                    .as_ref()
                    .map(|status| task.record.state.phase() == status)
                    .unwrap_or(true)
            })
            .filter(|task| {
                query
                    .kind
                    .as_ref()
                    .map(|kind| &task.record.kind == kind)
                    .unwrap_or(true)
            })
            .collect();
        tasks.sort_by_key(|task| task.record.id);
        Ok(tasks
            .into_iter()
            .take(limit)
            .map(|task| StoredTask {
                record: task.record.clone(),
                commit_seq: task.commit_seq,
            })
            .collect())
    }

    async fn get_submission(
        &self,
        id: SubmissionId,
    ) -> Result<Option<StoredSubmission>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner.submissions.get(&id).map(|stored| StoredSubmission {
            record: stored.record.clone(),
            commit_seq: stored.commit_seq,
        }))
    }

    async fn find_submission_by_request(
        &self,
        conv: ConversationId,
        request_id: &str,
    ) -> Result<Option<StoredSubmission>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let id = inner.request_index.get(&(conv, request_id.to_string()));
        Ok(id
            .and_then(|id| inner.submissions.get(id))
            .map(|stored| StoredSubmission {
                record: stored.record.clone(),
                commit_seq: stored.commit_seq,
            }))
    }

    async fn get_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentValue>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let row = inner
            .documents
            .iter()
            .filter(|row| row.visible_at(at))
            .filter(|row| row.address.kind == address.kind)
            .filter(|row| row.address.scope == address.scope)
            .filter(|row| row.address.key == address.key)
            .max_by_key(|row| (row.created_seq, row.id.0));
        Ok(row.and_then(|row| row.value_at(at)))
    }

    async fn scan_documents(
        &self,
        query: DocumentQuery,
    ) -> Result<Vec<DocumentValue>, StorageError> {
        let limit = query.limit.clamp(1, 1000);
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut values: Vec<&DocumentRow> = inner
            .documents
            .iter()
            .filter(|row| row.visible_at(query.at))
            .filter(|row| {
                query
                    .kind
                    .as_ref()
                    .map(|kind| &row.address.kind == kind)
                    .unwrap_or(true)
            })
            .filter(|row| {
                query
                    .scope
                    .as_ref()
                    .map(|scope| &row.address.scope == scope)
                    .unwrap_or(true)
            })
            .filter(|row| {
                query
                    .key
                    .as_ref()
                    .map(|key| &row.address.key == key)
                    .unwrap_or(true)
            })
            .collect();
        values.sort_by_key(|row| (row.created_seq, row.id.0));
        Ok(values
            .into_iter()
            .filter_map(|row| row.value_at(query.at))
            .take(limit)
            .collect())
    }

    async fn get_operation(
        &self,
        task: TaskId,
        key: &str,
    ) -> Result<Option<ToolOperationRecord>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner.operations.get(&(task, key.to_string())).cloned())
    }

    async fn scan_events(
        &self,
        session: SessionId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StorageError> {
        let limit = limit.clamp(1, 1000);
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut events: Vec<&StoredEvent> = inner
            .events
            .iter()
            .filter(|event| event.session_id == session)
            .filter(|event| (event.seq, event.ordinal) > (after.seq, after.ordinal))
            .collect();
        events.sort_by_key(|event| (event.seq, event.ordinal));
        let next = if events.len() > limit {
            let last = events[limit - 1];
            Some(EventCursor {
                seq: last.seq,
                ordinal: last.ordinal,
            })
        } else {
            None
        };
        Ok(EventPage {
            events: events.into_iter().take(limit).cloned().collect(),
            next,
        })
    }

    async fn last_event_cursor(
        &self,
        session: SessionId,
    ) -> Result<Option<EventCursor>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner
            .events
            .iter()
            .filter(|event| event.session_id == session)
            .max_by_key(|event| (event.seq, event.ordinal))
            .map(|event| EventCursor {
                seq: event.seq,
                ordinal: event.ordinal,
            }))
    }

    async fn get_session(&self, id: SessionId) -> Result<Option<SessionRecord>, StorageError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        Ok(inner.sessions.get(&id).cloned())
    }

    async fn put_session(&self, record: SessionRecord) -> Result<(), StorageError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let created_at_ms = inner
            .sessions
            .get(&record.session_id)
            .map(|existing| existing.created_at_ms)
            .unwrap_or(record.created_at_ms);
        self.persist(&LogRecord::Session {
            record: record.clone(),
        })?;
        inner.sessions.insert(
            record.session_id,
            SessionRecord {
                created_at_ms,
                ..record
            },
        );
        Ok(())
    }

    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>, StorageError> {
        let limit = limit.clamp(1, 1000);
        let inner = self
            .inner
            .lock()
            .map_err(|_| StorageError::internal("session lock poisoned"))?;
        let mut sessions: Vec<&SessionRecord> = inner.sessions.values().collect();
        sessions.sort_by_key(|session| (session.created_at_ms, session.session_id.0));
        Ok(sessions.into_iter().take(limit).cloned().collect())
    }
}

impl Inner {
    fn apply_batch_at(
        &mut self,
        batch: &CommitBatch,
        ctx: &CommitContext,
        now: i64,
    ) -> Result<(Seq, u32), ApplyError> {
        self.validate_commit(batch)?;
        Ok(self.apply_validated(batch, ctx, now))
    }

    fn validate_commit(&self, batch: &CommitBatch) -> Result<(), ApplyError> {
        if batch.writes.is_empty() {
            return Err(RefusalReason::PreconditionFailed(
                "commit batch contains no writes".into(),
            )
            .into());
        }
        self.validate_batch(batch)
    }

    // Validation must happen under the same lock before persistence. Application
    // cannot fail, so successful journal writes and the visible index agree.
    fn apply_validated(
        &mut self,
        batch: &CommitBatch,
        ctx: &CommitContext,
        now: i64,
    ) -> (Seq, u32) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.commits.push((seq, ctx.session_id));
        let mut ordinal = 0u32;
        let mut event_count = 0u32;

        for write in &batch.writes {
            match write {
                Write::Session(record) => {
                    self.sessions.insert(record.session_id, record.clone());
                }
                Write::Conversation(record) => {
                    self.conversations.insert(
                        record.id,
                        StoredConversation {
                            record: record.clone(),
                            commit_seq: seq,
                        },
                    );
                }
                Write::Entry(record) => {
                    self.entries
                        .entry(record.conversation_id)
                        .or_default()
                        .insert(
                            record.id,
                            StoredEntry {
                                record: record.clone(),
                                commit_seq: seq,
                            },
                        );
                }
                Write::Task(record) => {
                    if record.version == 0 {
                        self.tasks.insert(
                            record.id,
                            StoredTask {
                                record: record.clone(),
                                commit_seq: seq,
                            },
                        );
                    } else if let Some(stored) = self.tasks.get_mut(&record.id) {
                        stored.record = record.clone();
                        stored.commit_seq = seq;
                    }
                }
                Write::Submission(record) => {
                    if let Some(request_id) = &record.request_id {
                        self.request_index
                            .insert((record.conversation_id, request_id.clone()), record.id);
                    }
                    self.submissions.insert(
                        record.id,
                        StoredSubmission {
                            record: record.clone(),
                            commit_seq: seq,
                        },
                    );
                }
                Write::Document(write) => {
                    self.apply_document(write, seq, now);
                }
                Write::ToolOperation(record) => {
                    self.operations.insert(
                        (record.task_id, record.idempotency_key.clone()),
                        record.clone(),
                    );
                }
                Write::Event(draft) => {
                    self.events.push(StoredEvent {
                        seq,
                        ordinal,
                        session_id: ctx.session_id,
                        conversation_id: ctx.conversation_id,
                        kind: draft.kind.clone(),
                        payload: draft.payload.clone(),
                        created_at_ms: now,
                    });
                    ordinal += 1;
                    event_count += 1;
                }
            }
        }
        (seq, event_count)
    }

    fn validate_batch(&self, batch: &CommitBatch) -> Result<(), ApplyError> {
        let mut staged_tasks: HashMap<TaskId, &TaskRecord> = HashMap::new();
        let mut staged_requests: HashMap<(ConversationId, String), SubmissionId> = HashMap::new();
        let mut staged_operations: HashMap<(TaskId, String), ToolOperationStatus> = HashMap::new();

        for write in &batch.writes {
            match write {
                Write::Session(record) => {
                    if self.sessions.contains_key(&record.session_id) {
                        return Err(RefusalReason::PreconditionFailed(
                            "session already exists".into(),
                        )
                        .into());
                    }
                }
                Write::Conversation(record) => {
                    if self.conversations.contains_key(&record.id) {
                        return Err(StorageError::constraint(format!(
                            "conversation {} already exists",
                            record.id
                        ))
                        .into());
                    }
                }
                Write::Entry(record) => {
                    if self
                        .entries
                        .get(&record.conversation_id)
                        .map(|entries| entries.contains_key(&record.id))
                        .unwrap_or(false)
                    {
                        return Err(StorageError::constraint(format!(
                            "entry {} already exists and is immutable",
                            record.id
                        ))
                        .into());
                    }
                }
                Write::Task(record) => {
                    if staged_tasks.insert(record.id, record).is_some() {
                        return Err(RefusalReason::PreconditionFailed(format!(
                            "task {} written twice in one batch",
                            record.id
                        ))
                        .into());
                    }
                    if let Some(existing) = self.tasks.get(&record.id) {
                        if existing.record.state.is_terminal() {
                            return Err(RefusalReason::InvalidTransition {
                                from: "terminal".into(),
                                to: record.state.phase().into(),
                            }
                            .into());
                        }
                        if record.version != existing.record.version + 1 {
                            return Err(RefusalReason::VersionConflict {
                                expected: record.version,
                                actual: existing.record.version,
                            }
                            .into());
                        }
                        if !existing.record.state.can_transition_to(&record.state) {
                            return Err(RefusalReason::InvalidTransition {
                                from: existing.record.state.phase().into(),
                                to: record.state.phase().into(),
                            }
                            .into());
                        }
                    } else if record.version != 0 {
                        return Err(RefusalReason::PreconditionFailed(format!(
                            "task {} does not exist",
                            record.id
                        ))
                        .into());
                    }
                }
                Write::Submission(record) => {
                    if let Some(request_id) = &record.request_id {
                        let key = (record.conversation_id, request_id.clone());
                        // A unique request_id identifies one submission row.
                        // Status updates of that same row are not duplicates;
                        // a different submission reusing the id is.
                        let duplicate = self
                            .request_index
                            .get(&key)
                            .or_else(|| staged_requests.get(&key))
                            .map(|existing| *existing != record.id)
                            .unwrap_or(false);
                        if duplicate {
                            return Err(RefusalReason::DuplicateRequestId {
                                request_id: request_id.clone(),
                            }
                            .into());
                        }
                        staged_requests.insert(key, record.id);
                    }
                }
                Write::Document(_) => {}
                Write::ToolOperation(record) => {
                    let key = (record.task_id, record.idempotency_key.clone());
                    let previous = staged_operations
                        .get(&key)
                        .copied()
                        .or_else(|| self.operations.get(&key).map(|row| row.status));
                    if let Some(previous) = previous {
                        if !operation_can_advance(previous, record.status) {
                            return Err(RefusalReason::PreconditionFailed(format!(
                                "tool operation {}:{} cannot move {:?} -> {:?}",
                                record.task_id, record.idempotency_key, previous, record.status
                            ))
                            .into());
                        }
                    }
                    staged_operations.insert(key, record.status);
                }
                Write::Event(_) => {}
            }
        }
        Ok(())
    }

    fn apply_document(&mut self, write: &DocumentWrite, seq: Seq, now: i64) {
        let address = document_write_address(write).clone();
        match write {
            DocumentWrite::PutLatest { value, .. } => {
                if address.family {
                    for row in &mut self.documents {
                        if row.retired_at.is_none()
                            && row.address.kind == address.kind
                            && row.address.scope == address.scope
                            && row.address.key != address.key
                        {
                            row.retired_at = Some(now);
                            row.retired_seq = Some(seq);
                        }
                    }
                }
                if let Some(index) = self.documents.iter().position(|row| {
                    row.retired_at.is_none()
                        && row.address.kind == address.kind
                        && row.address.scope == address.scope
                        && row.address.key == address.key
                }) {
                    let row = &mut self.documents[index];
                    row.version += 1;
                    let version = row.version;
                    row.revisions.push(Revision {
                        seq,
                        version,
                        value: value.clone(),
                    });
                } else {
                    let id = DocumentId(self.mint_id());
                    self.documents.push(DocumentRow {
                        id,
                        address,
                        created_seq: seq,
                        created_at: now,
                        retired_seq: None,
                        retired_at: None,
                        version: 1,
                        revisions: vec![Revision {
                            seq,
                            version: 1,
                            value: value.clone(),
                        }],
                    });
                }
            }
            DocumentWrite::PutRewindable { value, .. } => {
                let id = DocumentId(self.mint_id());
                self.documents.push(DocumentRow {
                    id,
                    address,
                    created_seq: seq,
                    created_at: now,
                    retired_seq: None,
                    retired_at: None,
                    version: 1,
                    revisions: vec![Revision {
                        seq,
                        version: 1,
                        value: value.clone(),
                    }],
                });
            }
            DocumentWrite::Delta { ops, .. } => {
                if let Some(index) = self.documents.iter().position(|row| {
                    row.retired_at.is_none()
                        && row.address.kind == address.kind
                        && row.address.scope == address.scope
                        && row.address.key == address.key
                }) {
                    let row = &mut self.documents[index];
                    let current = row
                        .revisions
                        .last()
                        .map(|revision| revision.value.clone())
                        .unwrap_or(serde_json::Value::Null);
                    let value = apply_delta(&current, ops);
                    row.version += 1;
                    let version = row.version;
                    row.revisions.push(Revision {
                        seq,
                        version,
                        value,
                    });
                } else {
                    let id = DocumentId(self.mint_id());
                    let value = apply_delta(&serde_json::Value::Null, ops);
                    self.documents.push(DocumentRow {
                        id,
                        address,
                        created_seq: seq,
                        created_at: now,
                        retired_seq: None,
                        retired_at: None,
                        version: 1,
                        revisions: vec![Revision {
                            seq,
                            version: 1,
                            value,
                        }],
                    });
                }
            }
            DocumentWrite::Retire { .. } => {
                for row in &mut self.documents {
                    if row.retired_at.is_none()
                        && row.address.kind == address.kind
                        && row.address.scope == address.scope
                        && row.address.key == address.key
                    {
                        row.retired_at = Some(now);
                        row.retired_seq = Some(seq);
                    }
                }
            }
        }
    }
}

/// v1 document deltas are applied in application code. `{"set": v}` replaces,
/// `{"merge": {...}}` shallow-merges an object, anything else replaces.
#[cfg(test)]
#[path = "memory_unit_tests.rs"]
mod unit_tests;

pub fn apply_delta(current: &serde_json::Value, ops: &serde_json::Value) -> serde_json::Value {
    if let Some(value) = ops.get("set") {
        return value.clone();
    }
    if let Some(merge) = ops.get("merge") {
        if let (Some(base), Some(extra)) = (current.as_object(), merge.as_object()) {
            let mut merged = base.clone();
            for (key, value) in extra {
                merged.insert(key.clone(), value.clone());
            }
            return serde_json::Value::Object(merged);
        }
        return merge.clone();
    }
    ops.clone()
}
