//! Real PostgreSQL implementation of [`rusty_bot_core::Storage`].
//!
//! Every commit batch is one transaction with its own `seq`. Refusals roll the
//! batch back but leave earlier batches durable. Durable events are rows in the
//! same transaction, written with per-batch ordinals; `pg_notify` is only a
//! latency hint issued after commit.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rusty_bot_core::{
    operation_can_advance, BatchRefusal, CommitBatch, CommitContext, CommitReceipt, ConversationId,
    DocumentAddress, DocumentPoint, DocumentQuery, DocumentScope, DocumentValue, DocumentWrite,
    EntryCursor, EntryId, EntryQuery, EventCursor, EventPage, Id, Page, RefusalReason, Seq,
    SessionId, SessionRecord, Storage, StorageError, StorageErrorKind, StoredBatch, StoredEntry,
    StoredEvent, StoredSubmission, StoredTask, SubmissionId, SubmissionRecord, TaskId, TaskQuery,
    TaskRecord, ToolOperationRecord, Write,
};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnection, PgPool};
use sqlx::PgPool as _PgPoolAlias;

use rusty_bot_core::storage::StoredConversation;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn cap(limit: usize) -> i64 {
    // Contract: 0 clamps to 1; never return an unbounded page.
    limit.clamp(1, 1000) as i64
}

#[derive(Debug)]
enum DbFailure {
    Retry,
    Fatal(StorageError),
}

impl From<sqlx::Error> for DbFailure {
    fn from(error: sqlx::Error) -> Self {
        if is_retryable_sqlstate(&error) {
            Self::Retry
        } else {
            Self::Fatal(map_error(error))
        }
    }
}

fn map_error(error: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(database) = &error {
        match database.code().as_deref() {
            Some("40001") | Some("40P01") => {
                return StorageError::new(StorageErrorKind::Unavailable, database.message())
            }
            Some("23505") => {
                return StorageError::constraint(database.message());
            }
            Some("57014") => {
                return StorageError::new(StorageErrorKind::Timeout, database.message());
            }
            Some(code) => {
                return StorageError::new(
                    StorageErrorKind::Internal,
                    format!("sqlstate {code}: {}", database.message()),
                );
            }
            None => {}
        }
    }
    match &error {
        sqlx::Error::PoolTimedOut => {
            StorageError::new(StorageErrorKind::Unavailable, "postgres pool timed out")
        }
        sqlx::Error::Io(_) => StorageError::new(StorageErrorKind::Unavailable, "postgres io error"),
        sqlx::Error::Tls(_) => {
            StorageError::new(StorageErrorKind::Unavailable, "postgres tls error")
        }
        _ => StorageError::internal(error.to_string()),
    }
}

fn is_retryable_sqlstate(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database) => {
            matches!(database.code().as_deref(), Some("40001") | Some("40P01"))
        }
        _ => false,
    }
}

fn corrupt(context: &str, error: impl std::fmt::Display) -> StorageError {
    StorageError::corrupt(format!("{context}: {error}"))
}

#[derive(Clone)]
pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn with_read_retry<T, F, Fut>(&self, operation: F) -> Result<T, StorageError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        // Reads are safe to retry. Commits are never retried blindly by the
        // engine; serialization failures are retried inside `commit_one` only.
        let mut attempt = 0u32;
        let mut operation = operation;
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if is_retryable_sqlstate(&error) && attempt < 2 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt))).await;
                }
                Err(error) => return Err(map_error(error)),
            }
        }
    }

    async fn commit_one(
        &self,
        batch: &CommitBatch,
        ctx: &CommitContext,
        index: usize,
        batch_count: usize,
    ) -> Result<BatchOutcome, StorageError> {
        if batch.writes.is_empty() {
            return Ok(BatchOutcome::Refused(RefusalReason::PreconditionFailed(
                "commit batch contains no writes".into(),
            )));
        }
        let mut attempt = 0u32;
        loop {
            match self.try_commit_batch(batch, ctx, index, batch_count).await {
                Ok(outcome) => return Ok(outcome),
                Err(ApplyError::Refusal(reason)) => return Ok(BatchOutcome::Refused(reason)),
                Err(ApplyError::Failure(DbFailure::Retry)) if attempt < 2 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt))).await;
                }
                Err(ApplyError::Failure(DbFailure::Retry)) => {
                    return Err(StorageError::new(
                        StorageErrorKind::Unavailable,
                        "serialization failure after retries",
                    ))
                }
                Err(ApplyError::Failure(DbFailure::Fatal(error))) => return Err(error),
            }
        }
    }

    async fn try_commit_batch(
        &self,
        batch: &CommitBatch,
        ctx: &CommitContext,
        _index: usize,
        batch_count: usize,
    ) -> Result<BatchOutcome, ApplyError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(ctx.session_id.to_string())
            .execute(&mut *tx)
            .await?;
        let seq: Seq = sqlx::query_scalar(
            "UPDATE rusty_bot_meta SET next_seq = next_seq + 1 WHERE singleton RETURNING next_seq - 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO commits (seq, session_id, batch_count) VALUES ($1, $2, $3)")
            .bind(seq)
            .bind(ctx.session_id.0)
            .bind(batch_count as i32)
            .execute(&mut *tx)
            .await?;

        let mut events = Vec::new();
        for write in &batch.writes {
            match write {
                Write::Event(draft) => events.push(draft.clone()),
                other => apply_write(&mut tx, other, ctx, seq).await?,
            }
        }
        let mut event_count = 0u32;
        for (ordinal, draft) in events.iter().enumerate() {
            sqlx::query(
                "INSERT INTO session_events (seq, ordinal, session_id, conversation_id, kind, payload)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(seq)
            .bind(ordinal as i32)
            .bind(ctx.session_id.0)
            .bind(ctx.conversation_id.map(|id| id.0))
            .bind(&draft.kind)
            .bind(&draft.payload)
            .execute(&mut *tx)
            .await?;
            event_count += 1;
        }

        tx.commit().await?;

        let payload = json!({ "seq": seq, "session_id": ctx.session_id.0 }).to_string();
        // Notifications are latency only. A failed NOTIFY must not turn an
        // already-durable commit into an error; readers still catch up by rows.
        if let Err(error) = sqlx::query("SELECT pg_notify('rusty_bot_events', $1)")
            .bind(payload)
            .execute(&self.pool)
            .await
        {
            tracing::warn!(%error, seq, "pg_notify(rusty_bot_events) failed after commit");
        }

        Ok(BatchOutcome::Committed { seq, event_count })
    }

    async fn read_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentValue>, StorageError> {
        let columns = AddressColumns::from(address);
        let row = match at {
            DocumentPoint::Current => {
                sqlx::query_as::<_, (i64, Value, i64, Option<i64>)>(
                    "SELECT id, record, created_at, retired_at FROM documents
                     WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND key_value = $4
                       AND retired_at IS NULL
                     ORDER BY (record->>'created_seq')::bigint DESC, id DESC LIMIT 1",
                )
                .bind(&columns.kind)
                .bind(columns.scope_kind)
                .bind(columns.owner_id)
                .bind(&columns.key)
                .fetch_optional(&self.pool)
                .await
            }
            DocumentPoint::Seq(seq) => {
                sqlx::query_as::<_, (i64, Value, i64, Option<i64>)>(
                    "SELECT id, record, created_at, retired_at FROM documents
                     WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND key_value = $4
                       AND (record->>'created_seq')::bigint <= $5
                       AND (record->>'retired_seq' IS NULL OR (record->>'retired_seq')::bigint > $5)
                     ORDER BY (record->>'created_seq')::bigint DESC, id DESC LIMIT 1",
                )
                .bind(&columns.kind)
                .bind(columns.scope_kind)
                .bind(columns.owner_id)
                .bind(&columns.key)
                .bind(seq)
                .fetch_optional(&self.pool)
                .await
            }
        }
        .map_err(map_error)?;

        let Some((id, record, created_at, retired_at)) = row else {
            return Ok(None);
        };
        let revision = self.revision_value(id, at).await?;
        let Some((version, value)) = revision else {
            return Ok(None);
        };
        let stored_address: DocumentAddress = serde_json::from_value(
            record
                .get("address")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(|error| corrupt("document address", error))?;
        Ok(Some(DocumentValue {
            id: rusty_bot_core::DocumentId(id),
            address: stored_address,
            version,
            value,
            created_at,
            retired_at,
        }))
    }

    async fn revision_value(
        &self,
        document_id: i64,
        at: DocumentPoint,
    ) -> Result<Option<(u32, Value)>, StorageError> {
        let mut connection =
            self.pool.acquire().await.map_err(|error| {
                StorageError::new(StorageErrorKind::Unavailable, error.to_string())
            })?;
        revision_value_connection(&mut connection, document_id, at).await
    }
}

enum BatchOutcome {
    Committed { seq: Seq, event_count: u32 },
    Refused(RefusalReason),
}

enum ApplyError {
    Refusal(RefusalReason),
    Failure(DbFailure),
}

impl From<sqlx::Error> for ApplyError {
    fn from(error: sqlx::Error) -> Self {
        Self::Failure(DbFailure::from(error))
    }
}

impl From<DbFailure> for ApplyError {
    fn from(value: DbFailure) -> Self {
        Self::Failure(value)
    }
}

fn refusal(reason: RefusalReason) -> ApplyError {
    ApplyError::Refusal(reason)
}

#[derive(Clone, Debug)]
struct AddressColumns {
    kind: String,
    family: bool,
    key: String,
    scope_kind: &'static str,
    owner_id: i64,
}

impl From<&DocumentAddress> for AddressColumns {
    fn from(address: &DocumentAddress) -> Self {
        let (scope_kind, owner_id) = match &address.scope {
            DocumentScope::Session(id) => ("session", id.0.as_u128() as i64),
            DocumentScope::Conversation(id) => ("conversation", id.0),
            DocumentScope::Task(id) => ("task", id.0),
        };
        Self {
            kind: address.kind.clone(),
            family: address.family,
            key: address.key.clone(),
            scope_kind,
            owner_id,
        }
    }
}

fn document_record(
    address: &DocumentAddress,
    history: &str,
    version: u32,
    created_seq: Seq,
    retired_seq: Option<Seq>,
) -> Value {
    json!({
        "history": history,
        "address": address,
        "version": version,
        "created_seq": created_seq,
        "retired_seq": retired_seq,
    })
}

fn value_revision(value: &Value) -> Value {
    json!({ "kind": "value", "value": value })
}

fn ops_revision(ops: &Value, value: &Value) -> Value {
    json!({ "kind": "ops", "ops": ops, "value": value })
}

/// v1 document deltas: `{"set": v}` replaces, `{"merge": {...}}` shallow-merges
/// an object, any other value replaces. Ops are stored as JSON and evaluated in
/// application code, never translated to SQL.
fn apply_delta(current: &Value, ops: &Value) -> Value {
    if let Some(value) = ops.get("set") {
        return value.clone();
    }
    if let Some(merge) = ops.get("merge") {
        if let (Some(base), Some(extra)) = (current.as_object(), merge.as_object()) {
            let mut merged = base.clone();
            for (key, value) in extra {
                merged.insert(key.clone(), value.clone());
            }
            return Value::Object(merged);
        }
        return merge.clone();
    }
    ops.clone()
}

async fn alloc_id(connection: &mut PgConnection) -> Result<Id, ApplyError> {
    Ok(sqlx::query_scalar(
        "UPDATE rusty_bot_meta SET next_id = next_id + 1 WHERE singleton RETURNING next_id - 1",
    )
    .fetch_one(connection)
    .await?)
}

async fn apply_write(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    write: &Write,
    ctx: &CommitContext,
    seq: Seq,
) -> Result<(), ApplyError> {
    match write {
        Write::Session(record) => {
            if record.session_id != ctx.session_id || record.conversation_id != ctx.conversation_id
            {
                return Err(ApplyError::Refusal(RefusalReason::PreconditionFailed(
                    "session binding does not match commit context".into(),
                )));
            }
            let result = sqlx::query(
                "INSERT INTO sessions (session_id, conversation_id, workspace_key, workspace, config, registry_version, extension_versions, host_id, status, created_at, updated_at)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10::double precision / 1000.0),to_timestamp($11::double precision / 1000.0)) ON CONFLICT (session_id) DO NOTHING",
            )
            .bind(record.session_id.0).bind(record.conversation_id.map(|id| id.0))
            .bind(&record.workspace_key).bind(&record.workspace).bind(&record.config)
            .bind(&record.registry_version).bind(&record.extension_versions).bind(&record.host_id)
            .bind(&record.status)
            .bind(record.created_at_ms).bind(record.updated_at_ms)
            .execute(&mut **tx).await?;
            if result.rows_affected() != 1 {
                return Err(ApplyError::Refusal(RefusalReason::PreconditionFailed(
                    "session already exists".into(),
                )));
            }
        }
        Write::Conversation(record) => {
            let (parent_conversation_id, parent_at) = record
                .parent
                .as_ref()
                .map(|parent| (Some(parent.conversation_id.0), Some(parent.at.0)))
                .unwrap_or((None, None));
            let (owner_conversation_id, owner_task_id) = record
                .owner
                .as_ref()
                .map(|owner| (Some(owner.conversation_id.0), Some(owner.task_id.0)))
                .unwrap_or((None, None));
            let result = sqlx::query(
                "INSERT INTO conversations
                   (id, parent_conversation_id, parent_at, owner_conversation_id, owner_task_id, commit_seq, record)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(record.id.0)
            .bind(parent_conversation_id)
            .bind(parent_at)
            .bind(owner_conversation_id)
            .bind(owner_task_id)
            .bind(seq)
            .bind(serde_json::to_value(record).map_err(|error| {
                ApplyError::Failure(DbFailure::Fatal(corrupt("conversation record", error)))
            })?)
            .execute(&mut **tx)
            .await;
            map_constraint(result, "conversation already exists")?;
        }
        Write::Entry(record) => {
            let result = sqlx::query(
                "INSERT INTO entries (id, conversation_id, head, kind, commit_seq, record)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(record.id.0)
            .bind(record.conversation_id.0)
            .bind(record.head.map(|head| head.0))
            .bind(&record.kind)
            .bind(seq)
            .bind(serde_json::to_value(record).map_err(|error| {
                ApplyError::Failure(DbFailure::Fatal(corrupt("entry record", error)))
            })?)
            .execute(&mut **tx)
            .await;
            map_constraint(result, "entry already exists and is immutable")?;
        }
        Write::Task(record) => {
            apply_task(tx, record, seq).await?;
        }
        Write::Submission(record) => {
            apply_submission(tx, record, seq).await?;
        }
        Write::Document(write) => {
            apply_document(&mut *tx, write, seq).await?;
        }
        Write::ToolOperation(record) => {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT status FROM tool_operations
                 WHERE task_id = $1 AND idempotency_key = $2 FOR UPDATE",
            )
            .bind(record.task_id.0)
            .bind(&record.idempotency_key)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some(existing) = &existing {
                let from = parse_operation_status(existing)?;
                if !operation_can_advance(from, record.status) {
                    return Err(refusal(RefusalReason::PreconditionFailed(format!(
                        "tool operation {}:{} cannot move {existing} -> {:?}",
                        record.task_id, record.idempotency_key, record.status
                    ))));
                }
            }
            sqlx::query(
                "INSERT INTO tool_operations
                   (task_id, idempotency_key, tool_name, status, request, result, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (task_id, idempotency_key) DO UPDATE SET
                   tool_name = EXCLUDED.tool_name,
                   status = EXCLUDED.status,
                   request = EXCLUDED.request,
                   result = EXCLUDED.result,
                   updated_at = EXCLUDED.updated_at",
            )
            .bind(record.task_id.0)
            .bind(&record.idempotency_key)
            .bind(&record.tool_name)
            .bind(operation_status_str(record.status))
            .bind(&record.request)
            .bind(&record.result)
            .bind(record.updated_at)
            .execute(&mut **tx)
            .await?;
        }
        Write::Event(_) => unreachable!("events are buffered by the caller"),
    }
    let _ = ctx;
    Ok(())
}

fn map_constraint(
    result: Result<sqlx::postgres::PgQueryResult, sqlx::Error>,
    message: &str,
) -> Result<(), ApplyError> {
    match result {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(database)) if database.code().as_deref() == Some("23505") => Err(
            ApplyError::Failure(DbFailure::Fatal(StorageError::constraint(message))),
        ),
        Err(error) => Err(error.into()),
    }
}

async fn apply_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record: &TaskRecord,
    seq: Seq,
) -> Result<(), ApplyError> {
    let status = record.state.phase();
    if record.version == 0 {
        let result = sqlx::query(
            "INSERT INTO tasks
               (id, conversation_id, kind, version, status, abort_requested, background,
                started_at, ended_at, commit_seq, record)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(record.id.0)
        .bind(record.conversation_id.0)
        .bind(&record.kind)
        .bind(record.version as i32)
        .bind(status)
        .bind(record.abort_requested)
        .bind(record.background)
        .bind(record.started_at)
        .bind(record.ended_at)
        .bind(seq)
        .bind(serde_json::to_value(record).map_err(|error| {
            ApplyError::Failure(DbFailure::Fatal(corrupt("task record", error)))
        })?)
        .execute(&mut **tx)
        .await;
        return map_constraint(result, "task already exists");
    }

    let existing: Option<(i32, String, Value)> =
        sqlx::query_as("SELECT version, status, record FROM tasks WHERE id = $1 FOR UPDATE")
            .bind(record.id.0)
            .fetch_optional(&mut **tx)
            .await?;
    let Some((actual_version, status, existing_record)) = existing else {
        return Err(refusal(RefusalReason::PreconditionFailed(format!(
            "task {} does not exist",
            record.id
        ))));
    };
    if status == "terminal" {
        return Err(refusal(RefusalReason::InvalidTransition {
            from: "terminal".into(),
            to: record.state.phase().into(),
        }));
    }
    if record.version != (actual_version as u32) + 1 {
        return Err(refusal(RefusalReason::VersionConflict {
            expected: record.version,
            actual: actual_version as u32,
        }));
    }
    let existing_task: TaskRecord = serde_json::from_value(existing_record)
        .map_err(|error| ApplyError::Failure(DbFailure::Fatal(corrupt("task record", error))))?;
    if !existing_task.state.can_transition_to(&record.state) {
        return Err(refusal(RefusalReason::InvalidTransition {
            from: existing_task.state.phase().into(),
            to: record.state.phase().into(),
        }));
    }
    sqlx::query(
        "UPDATE tasks SET conversation_id = $2, kind = $3, version = $4, status = $5,
           abort_requested = $6, background = $7, started_at = $8, ended_at = $9,
           commit_seq = $10, record = $11
         WHERE id = $1",
    )
    .bind(record.id.0)
    .bind(record.conversation_id.0)
    .bind(&record.kind)
    .bind(record.version as i32)
    .bind(record.state.phase())
    .bind(record.abort_requested)
    .bind(record.background)
    .bind(record.started_at)
    .bind(record.ended_at)
    .bind(seq)
    .bind(
        serde_json::to_value(record).map_err(|error| {
            ApplyError::Failure(DbFailure::Fatal(corrupt("task record", error)))
        })?,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn apply_submission(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record: &SubmissionRecord,
    seq: Seq,
) -> Result<(), ApplyError> {
    if let Some(request_id) = &record.request_id {
        let existing: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM submissions
             WHERE conversation_id = $1 AND request_id = $2 FOR UPDATE",
        )
        .bind(record.conversation_id.0)
        .bind(request_id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(existing_id) = existing {
            if existing_id != record.id.0 {
                return Err(refusal(RefusalReason::DuplicateRequestId {
                    request_id: request_id.clone(),
                }));
            }
        }
    }

    let kind = submission_kind_str(record.kind);
    let status = submission_status_str(record.status);
    let result = sqlx::query(
        "INSERT INTO submissions (id, conversation_id, request_id, kind, status, commit_seq, record)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (id) DO UPDATE SET
           conversation_id = EXCLUDED.conversation_id,
           request_id = EXCLUDED.request_id,
           kind = EXCLUDED.kind,
           status = EXCLUDED.status,
           commit_seq = EXCLUDED.commit_seq,
           record = EXCLUDED.record",
    )
    .bind(record.id.0)
    .bind(record.conversation_id.0)
    .bind(&record.request_id)
    .bind(kind)
    .bind(status)
    .bind(seq)
    .bind(serde_json::to_value(record).map_err(|error| {
        ApplyError::Failure(DbFailure::Fatal(corrupt("submission record", error)))
    })?)
    .execute(&mut **tx)
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(database))
            if database.code().as_deref() == Some("23505")
                && database
                    .constraint()
                    .map(|name| name.contains("submissions_request_unique"))
                    .unwrap_or(false) =>
        {
            // Concurrent insert of the same (conversation_id, request_id).
            Err(refusal(RefusalReason::DuplicateRequestId {
                request_id: record.request_id.clone().unwrap_or_default(),
            }))
        }
        Err(sqlx::Error::Database(database)) if database.code().as_deref() == Some("23505") => {
            Err(ApplyError::Failure(DbFailure::Fatal(
                StorageError::constraint(database.message()),
            )))
        }
        Err(error) => Err(error.into()),
    }
}

async fn apply_document(
    connection: &mut PgConnection,
    write: &DocumentWrite,
    seq: Seq,
) -> Result<(), ApplyError> {
    let address = write.address();
    let columns = AddressColumns::from(address);
    let now = now_ms();
    match write {
        DocumentWrite::PutLatest { value, .. } => {
            if address.family {
                sqlx::query(
                    "UPDATE documents
                     SET retired_at = $1,
                         record = jsonb_set(record, '{retired_seq}', to_jsonb($2::bigint))
                     WHERE kind = $3 AND scope_kind = $4 AND owner_id = $5
                       AND key_value <> $6 AND retired_at IS NULL",
                )
                .bind(now)
                .bind(seq)
                .bind(&columns.kind)
                .bind(columns.scope_kind)
                .bind(columns.owner_id)
                .bind(&columns.key)
                .execute(&mut *connection)
                .await?;
            }
            let existing: Option<(i64, Value)> = sqlx::query_as(
                "SELECT id, record FROM documents
                 WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND key_value = $4
                   AND retired_at IS NULL AND record->>'history' = 'latest'
                 FOR UPDATE",
            )
            .bind(&columns.kind)
            .bind(columns.scope_kind)
            .bind(columns.owner_id)
            .bind(&columns.key)
            .fetch_optional(&mut *connection)
            .await?;
            if let Some((document_id, record)) = existing {
                let version = record.get("version").and_then(Value::as_u64).unwrap_or(1) as u32 + 1;
                let created_seq = record
                    .get("created_seq")
                    .and_then(Value::as_i64)
                    .unwrap_or(seq);
                let updated = document_record(address, "latest", version, created_seq, None);
                sqlx::query("UPDATE documents SET record = $2 WHERE id = $1")
                    .bind(document_id)
                    .bind(&updated)
                    .execute(&mut *connection)
                    .await?;
                insert_revision(
                    connection,
                    document_id,
                    seq,
                    "delta",
                    version,
                    &value_revision(value),
                )
                .await?;
            } else {
                let document_id = alloc_id(connection).await?;
                sqlx::query(
                    "INSERT INTO documents
                       (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8)",
                )
                .bind(document_id)
                .bind(&columns.kind)
                .bind(columns.family)
                .bind(&columns.key)
                .bind(columns.scope_kind)
                .bind(columns.owner_id)
                .bind(now)
                .bind(document_record(address, "latest", 1, seq, None))
                .execute(&mut *connection)
                .await?;
                insert_revision(
                    connection,
                    document_id,
                    seq,
                    "base",
                    1,
                    &value_revision(value),
                )
                .await?;
            }
        }
        DocumentWrite::PutRewindable { value, .. } => {
            let document_id = alloc_id(connection).await?;
            sqlx::query(
                "INSERT INTO documents
                   (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8)",
            )
            .bind(document_id)
            .bind(&columns.kind)
            .bind(columns.family)
            .bind(&columns.key)
            .bind(columns.scope_kind)
            .bind(columns.owner_id)
            .bind(now)
            .bind(document_record(address, "rewindable", 1, seq, None))
            .execute(&mut *connection)
            .await?;
            insert_revision(
                connection,
                document_id,
                seq,
                "base",
                1,
                &value_revision(value),
            )
            .await?;
        }
        DocumentWrite::Delta { ops, .. } => {
            let existing: Option<(i64, Value)> = sqlx::query_as(
                "SELECT id, record FROM documents
                 WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND key_value = $4
                   AND retired_at IS NULL
                 ORDER BY (record->>'created_seq')::bigint DESC, id DESC LIMIT 1
                 FOR UPDATE",
            )
            .bind(&columns.kind)
            .bind(columns.scope_kind)
            .bind(columns.owner_id)
            .bind(&columns.key)
            .fetch_optional(&mut *connection)
            .await?;
            if let Some((document_id, record)) = existing {
                let current =
                    revision_value_connection(connection, document_id, DocumentPoint::Current)
                        .await
                        .map_err(|error| ApplyError::Failure(DbFailure::Fatal(error)))?
                        .map(|(_, value)| value)
                        .unwrap_or(Value::Null);
                let value = apply_delta(&current, ops);
                let version = record.get("version").and_then(Value::as_u64).unwrap_or(1) as u32 + 1;
                let created_seq = record
                    .get("created_seq")
                    .and_then(Value::as_i64)
                    .unwrap_or(seq);
                let history = record
                    .get("history")
                    .and_then(Value::as_str)
                    .unwrap_or("latest");
                let updated = document_record(address, history, version, created_seq, None);
                sqlx::query("UPDATE documents SET record = $2 WHERE id = $1")
                    .bind(document_id)
                    .bind(&updated)
                    .execute(&mut *connection)
                    .await?;
                insert_revision(
                    connection,
                    document_id,
                    seq,
                    "delta",
                    version,
                    &ops_revision(ops, &value),
                )
                .await?;
            } else {
                let document_id = alloc_id(connection).await?;
                let value = apply_delta(&Value::Null, ops);
                sqlx::query(
                    "INSERT INTO documents
                       (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8)",
                )
                .bind(document_id)
                .bind(&columns.kind)
                .bind(columns.family)
                .bind(&columns.key)
                .bind(columns.scope_kind)
                .bind(columns.owner_id)
                .bind(now)
                .bind(document_record(address, "latest", 1, seq, None))
                .execute(&mut *connection)
                .await?;
                insert_revision(
                    connection,
                    document_id,
                    seq,
                    "base",
                    1,
                    &value_revision(&value),
                )
                .await?;
            }
        }
        DocumentWrite::Retire { .. } => {
            sqlx::query(
                "UPDATE documents
                 SET retired_at = $1,
                     record = jsonb_set(record, '{retired_seq}', to_jsonb($2::bigint))
                 WHERE kind = $3 AND scope_kind = $4 AND owner_id = $5
                   AND key_value = $6 AND retired_at IS NULL",
            )
            .bind(now)
            .bind(seq)
            .bind(&columns.kind)
            .bind(columns.scope_kind)
            .bind(columns.owner_id)
            .bind(&columns.key)
            .execute(&mut *connection)
            .await?;
        }
    }
    Ok(())
}

async fn insert_revision(
    connection: &mut PgConnection,
    document_id: i64,
    seq: Seq,
    kind: &str,
    version: u32,
    content: &Value,
) -> Result<(), ApplyError> {
    sqlx::query(
        "INSERT INTO document_revisions (document_id, seq, kind, version, content)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (document_id, seq) DO UPDATE SET
           kind = EXCLUDED.kind, version = EXCLUDED.version, content = EXCLUDED.content",
    )
    .bind(document_id)
    .bind(seq)
    .bind(kind)
    .bind(version as i32)
    .bind(content)
    .execute(connection)
    .await?;
    Ok(())
}

async fn revision_value_connection(
    connection: &mut PgConnection,
    document_id: i64,
    at: DocumentPoint,
) -> Result<Option<(u32, Value)>, StorageError> {
    let rows: Vec<(String, i32, Value)> = match at {
        DocumentPoint::Current => {
            sqlx::query_as(
                "SELECT kind, version, content FROM document_revisions
             WHERE document_id = $1 ORDER BY seq ASC",
            )
            .bind(document_id)
            .fetch_all(&mut *connection)
            .await
        }
        DocumentPoint::Seq(seq) => {
            sqlx::query_as(
                "SELECT kind, version, content FROM document_revisions
             WHERE document_id = $1 AND seq <= $2 ORDER BY seq ASC",
            )
            .bind(document_id)
            .bind(seq)
            .fetch_all(&mut *connection)
            .await
        }
    }
    .map_err(map_error)?;

    let mut current: Option<Value> = None;
    let mut version = 0u32;
    let mut any = false;
    for (kind, revision_version, content) in rows {
        any = true;
        version = revision_version as u32;
        if let Some(value) = content.get("value") {
            current = Some(value.clone());
        } else if kind == "ops" {
            let ops = content.get("ops").cloned().unwrap_or(Value::Null);
            let base = current.take().unwrap_or(Value::Null);
            current = Some(apply_delta(&base, &ops));
        } else {
            current = Some(content);
        }
    }
    if !any {
        return Ok(None);
    }
    Ok(current.map(|value| (version, value)))
}

fn operation_status_str(status: rusty_bot_core::ToolOperationStatus) -> &'static str {
    use rusty_bot_core::ToolOperationStatus::*;
    match status {
        Intent => "intent",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Interrupted => "interrupted",
        Uncertain => "uncertain",
    }
}

fn parse_operation_status(value: &str) -> Result<rusty_bot_core::ToolOperationStatus, ApplyError> {
    use rusty_bot_core::ToolOperationStatus::*;
    match value {
        "intent" => Ok(Intent),
        "running" => Ok(Running),
        "succeeded" => Ok(Succeeded),
        "failed" => Ok(Failed),
        "interrupted" => Ok(Interrupted),
        "uncertain" => Ok(Uncertain),
        other => Err(ApplyError::Failure(DbFailure::Fatal(
            StorageError::corrupt(format!("unknown tool operation status {other}")),
        ))),
    }
}

fn submission_kind_str(kind: rusty_bot_core::SubmissionKind) -> &'static str {
    use rusty_bot_core::SubmissionKind::*;
    match kind {
        Input => "input",
        Write => "write",
        Continue => "continue",
    }
}

fn submission_status_str(status: rusty_bot_core::SubmissionStatus) -> &'static str {
    use rusty_bot_core::SubmissionStatus::*;
    match status {
        Queued => "queued",
        Placed => "placed",
        Done => "done",
        Unanswered => "unanswered",
    }
}

#[async_trait]
impl Storage for PgStorage {
    async fn mint_id(&self) -> Result<Id, StorageError> {
        self.with_read_retry(|| async {
            sqlx::query_scalar::<_, i64>(
                "UPDATE rusty_bot_meta SET next_id = next_id + 1 WHERE singleton RETURNING next_id - 1",
            )
            .fetch_one(&self.pool)
            .await
        })
        .await
    }

    async fn commit(
        &self,
        batches: Vec<CommitBatch>,
        ctx: &CommitContext,
    ) -> Result<CommitReceipt, StorageError> {
        let batch_count = batches.len();
        let mut receipt = CommitReceipt::default();
        for (index, batch) in batches.iter().enumerate() {
            match self.commit_one(batch, ctx, index, batch_count).await? {
                BatchOutcome::Committed { seq, event_count } => receipt.batches.push(StoredBatch {
                    index,
                    seq,
                    event_count,
                }),
                BatchOutcome::Refused(reason) => {
                    receipt.refused = Some(BatchRefusal {
                        batch_index: index,
                        reason,
                    });
                    break;
                }
            }
        }
        Ok(receipt)
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, StorageError> {
        let row: Option<(Value, Seq)> =
            sqlx::query_as("SELECT record, commit_seq FROM conversations WHERE id = $1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredConversation {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("conversation record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn get_entry(
        &self,
        conv: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let row: Option<(Value, Seq)> = sqlx::query_as(
            "SELECT record, commit_seq FROM entries WHERE conversation_id = $1 AND id = $2",
        )
        .bind(conv.0)
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredEntry {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("entry record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn scan_entries(
        &self,
        query: EntryQuery,
        cursor: Option<EntryCursor>,
        limit: usize,
    ) -> Result<Page<StoredEntry, EntryCursor>, StorageError> {
        let limit = cap(limit);
        let (rows, descending): (Vec<(Value, Seq)>, bool) = if query.ascending {
            (
                sqlx::query_as(
                    "SELECT record, commit_seq FROM entries
                     WHERE conversation_id = $1
                       AND ($2::text IS NULL OR kind = $2)
                       AND ($3::bigint IS NULL OR (record->>'by_task_id')::bigint = $3)
                       AND ($4::bigint IS NULL OR id > $4)
                     ORDER BY id ASC LIMIT $5",
                )
                .bind(query.conversation_id.0)
                .bind(&query.kind)
                .bind(query.by_task_id.map(|task| task.0))
                .bind(cursor.map(|cursor| cursor.0))
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
                .map_err(map_error)?,
                false,
            )
        } else {
            (
                sqlx::query_as(
                    "SELECT record, commit_seq FROM entries
                     WHERE conversation_id = $1
                       AND ($2::text IS NULL OR kind = $2)
                       AND ($3::bigint IS NULL OR (record->>'by_task_id')::bigint = $3)
                       AND ($4::bigint IS NULL OR id < $4)
                     ORDER BY id DESC LIMIT $5",
                )
                .bind(query.conversation_id.0)
                .bind(&query.kind)
                .bind(query.by_task_id.map(|task| task.0))
                .bind(cursor.map(|cursor| cursor.0))
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
                .map_err(map_error)?,
                true,
            )
        };
        let has_more = rows.len() as i64 > limit;
        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        for (record, commit_seq) in rows.into_iter().take(limit as usize) {
            items.push(StoredEntry {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("entry record", error))?,
                commit_seq,
            });
        }
        let next = if has_more {
            items.last().map(|entry| entry.record.id)
        } else {
            None
        };
        let _ = descending;
        Ok(Page { items, next })
    }

    async fn latest_head_marker(
        &self,
        conv: ConversationId,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let row: Option<(Value, Seq)> = sqlx::query_as(
            "SELECT record, commit_seq FROM entries
             WHERE conversation_id = $1 AND head IS NOT NULL
             ORDER BY id DESC LIMIT 1",
        )
        .bind(conv.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredEntry {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("entry record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn latest_entry_id(&self, conv: ConversationId) -> Result<Option<EntryId>, StorageError> {
        let id: Option<i64> =
            sqlx::query_scalar("SELECT MAX(id) FROM entries WHERE conversation_id = $1")
                .bind(conv.0)
                .fetch_one(&self.pool)
                .await
                .map_err(map_error)?;
        Ok(id.map(EntryId))
    }

    async fn find_entry_by_task(
        &self,
        conv: ConversationId,
        task: TaskId,
        kind: &str,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let row: Option<(Value, Seq)> = sqlx::query_as(
            "SELECT record, commit_seq FROM entries
             WHERE conversation_id = $1
               AND kind = $2
               AND (record->>'by_task_id')::bigint = $3
             ORDER BY id DESC LIMIT 1",
        )
        .bind(conv.0)
        .bind(kind)
        .bind(task.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredEntry {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("entry record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<StoredTask>, StorageError> {
        let row: Option<(Value, Seq)> =
            sqlx::query_as("SELECT record, commit_seq FROM tasks WHERE id = $1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredTask {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("task record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn scan_tasks(&self, query: TaskQuery) -> Result<Vec<StoredTask>, StorageError> {
        let limit = cap(query.limit);
        let rows: Vec<(Value, Seq)> = sqlx::query_as(
            "SELECT record, commit_seq FROM tasks
             WHERE ($1::bigint IS NULL OR conversation_id = $1)
               AND ($2::text IS NULL OR status = $2)
               AND ($3::text IS NULL OR kind = $3)
             ORDER BY id ASC LIMIT $4",
        )
        .bind(query.conversation_id.map(|id| id.0))
        .bind(&query.status)
        .bind(&query.kind)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_error)?;
        rows.into_iter()
            .map(|(record, commit_seq)| {
                Ok(StoredTask {
                    record: serde_json::from_value(record)
                        .map_err(|error| corrupt("task record", error))?,
                    commit_seq,
                })
            })
            .collect()
    }

    async fn get_submission(
        &self,
        id: SubmissionId,
    ) -> Result<Option<StoredSubmission>, StorageError> {
        let row: Option<(Value, Seq)> =
            sqlx::query_as("SELECT record, commit_seq FROM submissions WHERE id = $1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredSubmission {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("submission record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn find_submission_by_request(
        &self,
        conv: ConversationId,
        request_id: &str,
    ) -> Result<Option<StoredSubmission>, StorageError> {
        let row: Option<(Value, Seq)> = sqlx::query_as(
            "SELECT record, commit_seq FROM submissions
             WHERE conversation_id = $1 AND request_id = $2 LIMIT 1",
        )
        .bind(conv.0)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        row.map(|(record, commit_seq)| {
            Ok(StoredSubmission {
                record: serde_json::from_value(record)
                    .map_err(|error| corrupt("submission record", error))?,
                commit_seq,
            })
        })
        .transpose()
    }

    async fn get_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentValue>, StorageError> {
        self.read_document(address, at).await
    }

    async fn scan_documents(
        &self,
        query: DocumentQuery,
    ) -> Result<Vec<DocumentValue>, StorageError> {
        let limit = cap(query.limit);
        let (scope_kind, owner_id) = match &query.scope {
            Some(DocumentScope::Session(id)) => (Some("session"), Some(id.0.as_u128() as i64)),
            Some(DocumentScope::Conversation(id)) => (Some("conversation"), Some(id.0)),
            Some(DocumentScope::Task(id)) => (Some("task"), Some(id.0)),
            None => (None, None),
        };
        let rows: Vec<(i64, Value, i64, Option<i64>)> = match query.at {
            DocumentPoint::Current => sqlx::query_as(
                "SELECT id, record, created_at, retired_at FROM documents
                 WHERE retired_at IS NULL
                   AND ($1::text IS NULL OR kind = $1)
                   AND ($2::text IS NULL OR (scope_kind = $2 AND owner_id = $3))
                   AND ($4::text IS NULL OR key_value = $4)
                 ORDER BY (record->>'created_seq')::bigint ASC, id ASC LIMIT $5",
            )
            .bind(&query.kind)
            .bind(scope_kind)
            .bind(owner_id)
            .bind(&query.key)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(map_error)?,
            DocumentPoint::Seq(seq) => sqlx::query_as(
                "SELECT id, record, created_at, retired_at FROM documents
                 WHERE (record->>'created_seq')::bigint <= $5
                   AND (record->>'retired_seq' IS NULL OR (record->>'retired_seq')::bigint > $5)
                   AND ($1::text IS NULL OR kind = $1)
                   AND ($2::text IS NULL OR (scope_kind = $2 AND owner_id = $3))
                   AND ($4::text IS NULL OR key_value = $4)
                 ORDER BY (record->>'created_seq')::bigint ASC, id ASC LIMIT $6",
            )
            .bind(&query.kind)
            .bind(scope_kind)
            .bind(owner_id)
            .bind(&query.key)
            .bind(seq)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(map_error)?,
        };

        let mut values = Vec::new();
        for (id, record, created_at, retired_at) in rows {
            let Some((version, value)) = self.revision_value(id, query.at).await? else {
                continue;
            };
            let address: DocumentAddress =
                serde_json::from_value(record.get("address").cloned().unwrap_or(Value::Null))
                    .map_err(|error| corrupt("document address", error))?;
            values.push(DocumentValue {
                id: rusty_bot_core::DocumentId(id),
                address,
                version,
                value,
                created_at,
                retired_at,
            });
        }
        Ok(values)
    }

    async fn get_operation(
        &self,
        task: TaskId,
        key: &str,
    ) -> Result<Option<ToolOperationRecord>, StorageError> {
        let row: Option<(String, String, String, Value, Option<Value>, i64)> = sqlx::query_as(
            "SELECT idempotency_key, tool_name, status, request, result, updated_at
             FROM tool_operations WHERE task_id = $1 AND idempotency_key = $2",
        )
        .bind(task.0)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        row.map(
            |(idempotency_key, tool_name, status, request, result, updated_at)| {
                Ok(ToolOperationRecord {
                    task_id: task,
                    idempotency_key,
                    tool_name,
                    status: parse_operation_status(&status)
                        .map_err(|_| corrupt("operation status", status.clone()))?,
                    request,
                    result,
                    updated_at,
                })
            },
        )
        .transpose()
    }

    async fn scan_events(
        &self,
        session: SessionId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StorageError> {
        let limit = cap(limit);
        let rows: Vec<(i64, i32, uuid::Uuid, Option<i64>, String, Value, i64)> = sqlx::query_as(
            "SELECT seq, ordinal, session_id, conversation_id, kind, payload,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::bigint
             FROM session_events
             WHERE session_id = $1 AND (seq, ordinal) > ($2, $3)
             ORDER BY seq, ordinal LIMIT $4",
        )
        .bind(session.0)
        .bind(after.seq)
        .bind(after.ordinal as i32)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(map_error)?;

        let has_more = rows.len() as i64 > limit;
        let events: Vec<StoredEvent> = rows
            .into_iter()
            .take(limit as usize)
            .map(
                |(seq, ordinal, session_id, conversation_id, kind, payload, created_at_ms)| {
                    StoredEvent {
                        seq,
                        ordinal: ordinal as u32,
                        session_id: SessionId(session_id),
                        conversation_id: conversation_id.map(ConversationId),
                        kind,
                        payload,
                        created_at_ms,
                    }
                },
            )
            .collect();
        let next = if has_more {
            events.last().map(|event| EventCursor {
                seq: event.seq,
                ordinal: event.ordinal,
            })
        } else {
            None
        };
        Ok(EventPage { events, next })
    }

    async fn last_event_cursor(
        &self,
        session: SessionId,
    ) -> Result<Option<EventCursor>, StorageError> {
        let row: Option<(i64, i32)> = sqlx::query_as(
            "SELECT seq, ordinal FROM session_events
             WHERE session_id = $1 ORDER BY seq DESC, ordinal DESC LIMIT 1",
        )
        .bind(session.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        Ok(row.map(|(seq, ordinal)| EventCursor {
            seq,
            ordinal: ordinal as u32,
        }))
    }

    async fn get_session(&self, id: SessionId) -> Result<Option<SessionRecord>, StorageError> {
        let row: Option<(
            uuid::Uuid,
            Option<i64>,
            String,
            Value,
            Value,
            String,
            Value,
            String,
            String,
            i64,
            i64,
        )> = sqlx::query_as(
            "SELECT session_id, conversation_id, workspace_key, workspace, config, registry_version,
                    extension_versions, host_id, status,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::bigint,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint
             FROM sessions WHERE session_id = $1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_error)?;
        Ok(row.map(
            |(
                session_id,
                conversation_id,
                workspace_key,
                workspace,
                config,
                registry_version,
                extension_versions,
                host_id,
                status,
                created_at_ms,
                updated_at_ms,
            )| SessionRecord {
                session_id: SessionId(session_id),
                conversation_id: conversation_id.map(ConversationId),
                workspace_key,
                workspace,
                config,
                registry_version,
                extension_versions,
                host_id,
                status,
                created_at_ms,
                updated_at_ms,
            },
        ))
    }

    async fn put_session(&self, record: SessionRecord) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO sessions
               (session_id, workspace_key, workspace, config, registry_version,
                extension_versions, host_id, status, created_at, updated_at, conversation_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                     to_timestamp($9::double precision / 1000.0),
                     to_timestamp($10::double precision / 1000.0), $11)
             ON CONFLICT (session_id) DO UPDATE SET
               workspace_key = EXCLUDED.workspace_key,
               workspace = EXCLUDED.workspace,
               config = EXCLUDED.config,
               registry_version = EXCLUDED.registry_version,
               extension_versions = EXCLUDED.extension_versions,
               host_id = EXCLUDED.host_id,
               status = EXCLUDED.status,
               updated_at = EXCLUDED.updated_at",
        )
        .bind(record.session_id.0)
        .bind(&record.workspace_key)
        .bind(&record.workspace)
        .bind(&record.config)
        .bind(&record.registry_version)
        .bind(&record.extension_versions)
        .bind(&record.host_id)
        .bind(&record.status)
        .bind(record.created_at_ms)
        .bind(record.updated_at_ms)
        .bind(record.conversation_id.map(|id| id.0))
        .execute(&self.pool)
        .await
        .map_err(map_error)?;
        Ok(())
    }

    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>, StorageError> {
        let limit = cap(limit);
        let rows: Vec<(
            uuid::Uuid,
            Option<i64>,
            String,
            Value,
            Value,
            String,
            Value,
            String,
            String,
            i64,
            i64,
        )> = sqlx::query_as(
            "SELECT session_id, conversation_id, workspace_key, workspace, config, registry_version,
                    extension_versions, host_id, status,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::bigint,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint
             FROM sessions ORDER BY created_at ASC, session_id ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_error)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    session_id,
                    conversation_id,
                    workspace_key,
                    workspace,
                    config,
                    registry_version,
                    extension_versions,
                    host_id,
                    status,
                    created_at_ms,
                    updated_at_ms,
                )| SessionRecord {
                    session_id: SessionId(session_id),
                    conversation_id: conversation_id.map(ConversationId),
                    workspace_key,
                    workspace,
                    config,
                    registry_version,
                    extension_versions,
                    host_id,
                    status,
                    created_at_ms,
                    updated_at_ms,
                },
            )
            .collect())
    }
}

// Keep the alias import used on all sqlx versions where the pool type is
// re-exported under different paths; it is part of the public constructor.
#[allow(dead_code)]
type PoolAlias = _PgPoolAlias;
