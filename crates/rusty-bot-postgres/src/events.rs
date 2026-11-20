//! Durable event access and transient live notifications for PostgreSQL.
//!
//! Notifications are latency hints. Callers that reconnect must re-query
//! `session_events` from their last acknowledged `(seq, ordinal)` cursor; rows
//! received while a listener was disconnected are not replayed by PostgreSQL.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::postgres::{PgListener, PgPool};

use rusty_bot_core::{
    EventCursor, EventPage, LiveFrame, LiveSink, SessionId, Storage, StorageError, StorageErrorKind,
};

use crate::storage::PgStorage;

/// Maximum payload size accepted by PostgreSQL `NOTIFY` (8000-byte hard limit
/// minus envelope headroom).
pub const MAX_LIVE_PAYLOAD_BYTES: usize = 7000;

/// Durable event reader. Safe to share across gateways; each call is a fresh
/// row query and does not depend on notification delivery.
#[derive(Clone)]
pub struct PgEventReader {
    pool: PgPool,
}

impl PgEventReader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn scan(
        &self,
        session: SessionId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StorageError> {
        PgStorage::new(self.pool.clone())
            .scan_events(session, after, limit)
            .await
    }

    /// Re-query after a listener reconnect. This is the contract for catch-up:
    /// notifications received while disconnected are dropped, rows are not.
    pub async fn catch_up(
        &self,
        session: SessionId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StorageError> {
        self.scan(session, after, limit).await
    }

    pub async fn last_event_cursor(
        &self,
        session: SessionId,
    ) -> Result<Option<EventCursor>, StorageError> {
        PgStorage::new(self.pool.clone())
            .last_event_cursor(session)
            .await
    }
}

/// A parsed durable-event notification payload.
#[derive(Clone, Debug, PartialEq)]
pub struct PgEventNotification {
    pub seq: i64,
    pub session_id: SessionId,
}

impl PgEventNotification {
    fn parse(payload: &str) -> Result<Self, StorageError> {
        let value: Value = serde_json::from_str(payload)
            .map_err(|error| StorageError::corrupt(format!("event notification: {error}")))?;
        let seq = value
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| StorageError::corrupt("event notification missing seq"))?;
        let session_id = value
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .map(SessionId)
            .ok_or_else(|| StorageError::corrupt("event notification missing session_id"))?;
        Ok(Self { seq, session_id })
    }
}

/// Reconnecting listener for `rusty_bot_events`.
pub struct PgEventListener {
    listener: PgListener,
}

impl PgEventListener {
    pub async fn connect(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen("rusty_bot_events").await?;
        Ok(Self { listener })
    }

    pub async fn recv(&mut self) -> Result<PgEventNotification, StorageError> {
        let notification =
            self.listener.recv().await.map_err(|error| {
                StorageError::new(StorageErrorKind::Unavailable, error.to_string())
            })?;
        PgEventNotification::parse(notification.payload())
    }

    pub async fn try_recv(&mut self) -> Result<Option<PgEventNotification>, StorageError> {
        let notification =
            self.listener.try_recv().await.map_err(|error| {
                StorageError::new(StorageErrorKind::Unavailable, error.to_string())
            })?;
        notification
            .map(|notification| PgEventNotification::parse(notification.payload()))
            .transpose()
    }
}

/// Transient live frame sink. `publish` never blocks on the database and never
/// panics: frames are queued to a background task that issues
/// `pg_notify('rusty_bot_live', ...)`.
pub struct PgLiveSink {
    sender: tokio::sync::mpsc::UnboundedSender<String>,
    dropped: Arc<AtomicU64>,
}

impl PgLiveSink {
    pub fn new(pool: PgPool) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Some(payload) = receiver.recv().await {
                if let Err(error) = sqlx::query("SELECT pg_notify('rusty_bot_live', $1)")
                    .bind(payload)
                    .execute(&pool)
                    .await
                {
                    tracing::debug!(%error, "pg_notify(rusty_bot_live) failed");
                }
            }
        });
        Self {
            sender,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of frames dropped because they could not be encoded under the
    /// PostgreSQL NOTIFY size limit.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl LiveSink for PgLiveSink {
    fn publish(&self, session_id: SessionId, frame: LiveFrame) {
        let payloads = live_payloads(session_id, &frame, MAX_LIVE_PAYLOAD_BYTES);
        if payloads.is_empty() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                session_id = %session_id,
                "live frame dropped: cannot split under NOTIFY size limit"
            );
            return;
        }
        for payload in payloads {
            // Unbounded channel: `send` only fails after the receiver is gone.
            let _ = self.sender.send(payload);
        }
    }
}

/// Encode a live frame as one or more NOTIFY payloads. Oversized string deltas
/// are split at UTF-8 boundaries. Non-string oversized data is dropped.
pub fn live_payloads(session_id: SessionId, frame: &LiveFrame, max: usize) -> Vec<String> {
    let base = json!({
        "session_id": session_id.0,
        "task_id": frame.task_id.map(|task| task.0),
        "kind": frame.kind,
        "data": frame.data,
    });
    let encoded = base.to_string();
    if encoded.len() <= max {
        return vec![encoded];
    }
    // Generation activities publish deltas as {"delta": "..."}, sometimes
    // with tool-call metadata. Preserve that shape and metadata on each chunk.
    let text = match &frame.data {
        Value::String(text) => text.as_str(),
        Value::Object(object) => match object.get("delta").and_then(Value::as_str) {
            Some(text) => text,
            None => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let encode = |chunk: &str, index: usize, total: usize| {
        let mut data = frame.data.clone();
        match &mut data {
            Value::String(value) => *value = chunk.to_string(),
            Value::Object(object) => {
                object.insert("delta".into(), Value::String(chunk.to_string()));
            }
            _ => unreachable!(),
        }
        json!({
            "session_id": session_id.0,
            "task_id": frame.task_id.map(|task| task.0),
            "kind": frame.kind,
            "data": data,
            "chunk": { "index": index, "total": total },
        })
        .to_string()
    };

    // Account for JSON escaping and for the largest possible chunk counters.
    // Testing the serialized payload avoids silently dropping quote/control
    // characters that expand when encoded as JSON.
    let boundaries: Vec<usize> = text
        .char_indices()
        .map(|(i, _)| i)
        .chain([text.len()])
        .collect();
    let mut chunks = Vec::new();
    let mut start = 0;
    while start + 1 < boundaries.len() {
        let mut low = start + 1;
        let mut high = boundaries.len() - 1;
        let mut best = start;
        while low <= high {
            let mid = low + (high - low) / 2;
            if encode(
                &text[boundaries[start]..boundaries[mid]],
                usize::MAX,
                usize::MAX,
            )
            .len()
                <= max
            {
                best = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        if best == start {
            return Vec::new();
        }
        chunks.push(&text[boundaries[start]..boundaries[best]]);
        start = best;
    }
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| encode(chunk, index, total))
        .collect()
}
