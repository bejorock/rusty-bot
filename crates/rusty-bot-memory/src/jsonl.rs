//! One append-only JSONL journal per adapter, with an exclusive Unix file lock.
//! State is indexed in memory; the journal is the durable source of truth.
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use crate::{ApplyError, InMemoryStorage, Inner};
use rusty_bot_core::{
    CommitBatch, CommitContext, ConversationId, Id, SessionId, SessionRecord, StorageError, TaskId,
};
use serde::{Deserialize, Serialize};

/// File-backed session storage. Construct with `open`; `new` (inherited from
/// the shared implementation) intentionally creates an ephemeral adapter.
pub type FileSessionAdapter = InMemoryStorage;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum LogRecord {
    Mint {
        id: Id,
    },
    Batch {
        writes: Vec<rusty_bot_core::Write>,
        session_id: SessionId,
        conversation_id: Option<ConversationId>,
        task_id: Option<TaskId>,
        now: i64,
    },
    Session {
        record: SessionRecord,
    },
    Frame {
        record: rusty_bot_core::storage::SessionFrame,
    },
}

pub(super) struct Journal {
    file: File,
    failed: bool,
}

fn io_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::new(
        rusty_bot_core::StorageErrorKind::Unavailable,
        error.to_string(),
    )
}

impl Journal {
    pub(super) fn append(&mut self, record: &LogRecord) -> Result<(), StorageError> {
        if self.failed {
            return Err(io_error("journal requires reopen after a failed write"));
        }
        let mut bytes =
            serde_json::to_vec(record).map_err(|e| StorageError::internal(e.to_string()))?;
        bytes.push(b'\n');
        // Once a write begins, any failure poisons the writer. Do not append
        // past an uncertain/torn record or publish unpersisted state.
        self.failed = true;
        self.file.write_all(&bytes).map_err(io_error)?;
        self.file.sync_data().map_err(io_error)?;
        self.failed = false;
        Ok(())
    }
}

impl InMemoryStorage {
    /// Open/create a private JSONL journal. Parent directories must exist.
    /// Only one adapter may own this file at once; share it via Arc.
    /// A crash-truncated final line is discarded. Complete corrupt records
    /// are errors, never skipped. All mutations sync before becoming visible.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(io_error)?;
        // File locks are released on drop, including failed opens.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io_error(format!(
                "session journal already in use: {}",
                std::io::Error::last_os_error()
            )));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error)?;
        let valid = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        let mut inner = Inner::new();
        let records = if valid == 0 {
            &bytes[..0]
        } else {
            &bytes[..valid - 1]
        };
        for (line, data) in records.split(|b| *b == b'\n').enumerate() {
            if valid == 0 {
                break;
            }
            let record: LogRecord = serde_json::from_slice(data)
                .map_err(|e| StorageError::corrupt(format!("JSONL line {}: {e}", line + 1)))?;
            match record {
                LogRecord::Mint { id } => {
                    if id != inner.next_id {
                        return Err(StorageError::corrupt("non-monotonic ID allocation"));
                    }
                    inner.mint_id();
                }
                LogRecord::Batch {
                    writes,
                    session_id,
                    conversation_id,
                    task_id,
                    now,
                } => {
                    inner
                        .apply_batch_at(
                            &CommitBatch { writes },
                            &CommitContext {
                                session_id,
                                conversation_id,
                                task_id,
                            },
                            now,
                        )
                        .map_err(|e| match e {
                            ApplyError::Fatal(e) => e,
                            ApplyError::Refusal(e) => {
                                StorageError::corrupt(format!("invalid journal batch: {e}"))
                            }
                        })?;
                }
                LogRecord::Session { mut record } => {
                    if let Some(old) = inner.sessions.get(&record.session_id) {
                        record.created_at_ms = old.created_at_ms;
                    }
                    inner.sessions.insert(record.session_id, record);
                }
                LogRecord::Frame { record } => {
                    if record.cursor != inner.frames.last().map_or(1, |f| f.cursor + 1) {
                        return Err(StorageError::corrupt("non-monotonic progress cursor"));
                    }
                    inner.frames.push(record);
                }
            }
        }
        if valid != bytes.len() {
            file.set_len(valid as u64).map_err(io_error)?;
            file.sync_data().map_err(io_error)?;
        }
        file.seek(SeekFrom::End(0)).map_err(io_error)?;
        Ok(Self {
            inner: Mutex::new(inner),
            journal: Some(Mutex::new(Journal {
                file,
                failed: false,
            })),
        })
    }
}
