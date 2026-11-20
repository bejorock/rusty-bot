//! Commit builder used by the engine.
//!
//! A transaction stages writes into batches. Every batch becomes exactly one
//! storage transaction with its own `seq`; a refusal at batch `i` keeps the
//! durable prefix and refuses the rest. Table reads are only allowed before
//! the first table write (`ReadAfterWrite`); document reads stay valid.

use rusty_bot_core::{
    CommitBatch, CommitContext, CommitReceipt, ConversationId, DocumentAddress, DocumentPoint,
    DocumentValue, DocumentWrite, EntryId, Storage, StorageError, StoredEntry, StoredSubmission,
    StoredTask, SubmissionRecord, Write,
};

use crate::deferred::is_deferred_eligible;

#[cfg(test)]
#[path = "transaction_tests.rs"]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum TxError {
    /// A table was read after a table write was staged. Build batches from
    /// already-fetched reads instead of reading through a dirty transaction.
    #[error("read after write on table {table}")]
    ReadAfterWrite { table: &'static str },
    /// Only latest-document updates may be deferred.
    #[error("document write is not eligible for deferred commit")]
    IneligibleDeferredWrite,
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

pub struct Transaction<'a> {
    storage: &'a dyn Storage,
    ctx: CommitContext,
    batches: Vec<CommitBatch>,
    current: Vec<Write>,
    table_write_started: bool,
    deferred: Vec<DocumentWrite>,
}

impl<'a> Transaction<'a> {
    pub fn new(storage: &'a dyn Storage, ctx: CommitContext) -> Self {
        Self {
            storage,
            ctx,
            batches: Vec::new(),
            current: Vec::new(),
            table_write_started: false,
            deferred: Vec::new(),
        }
    }

    pub fn context(&self) -> &CommitContext {
        &self.ctx
    }

    async fn guard_read(&self, table: &'static str) -> Result<(), TxError> {
        if self.table_write_started {
            Err(TxError::ReadAfterWrite { table })
        } else {
            Ok(())
        }
    }

    pub async fn read_entry(
        &mut self,
        conv: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>, TxError> {
        self.guard_read("entries").await?;
        Ok(self.storage.get_entry(conv, id).await?)
    }

    pub async fn read_task(
        &mut self,
        id: rusty_bot_core::TaskId,
    ) -> Result<Option<StoredTask>, TxError> {
        self.guard_read("tasks").await?;
        Ok(self.storage.get_task(id).await?)
    }

    pub async fn read_submission_by_request(
        &mut self,
        conv: ConversationId,
        request_id: &str,
    ) -> Result<Option<StoredSubmission>, TxError> {
        self.guard_read("submissions").await?;
        Ok(self
            .storage
            .find_submission_by_request(conv, request_id)
            .await?)
    }

    pub async fn read_submission(
        &mut self,
        id: rusty_bot_core::SubmissionId,
    ) -> Result<Option<StoredSubmission>, TxError> {
        self.guard_read("submissions").await?;
        Ok(self.storage.get_submission(id).await?)
    }

    /// Document reads remain valid before and after table writes.
    pub async fn read_document(
        &mut self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentValue>, TxError> {
        Ok(self.storage.get_document(address, at).await?)
    }

    pub fn write(&mut self, write: Write) -> Result<(), TxError> {
        if write.is_table_write() {
            self.table_write_started = true;
        }
        self.current.push(write);
        Ok(())
    }

    /// End the current batch. The next write receives a new `seq`.
    pub fn boundary(&mut self) {
        if !self.current.is_empty() {
            self.batches.push(CommitBatch {
                writes: std::mem::take(&mut self.current),
            });
        }
    }

    pub fn defer_document(&mut self, write: DocumentWrite) -> Result<(), TxError> {
        if !is_deferred_eligible(&write) {
            return Err(TxError::IneligibleDeferredWrite);
        }
        self.deferred.push(write);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() && self.current.is_empty() && self.deferred.is_empty()
    }

    pub fn staged_batches(&self) -> &[CommitBatch] {
        &self.batches
    }

    /// Stage a submission for callers that already validated it.
    pub fn write_submission(&mut self, record: SubmissionRecord) -> Result<(), TxError> {
        self.write(Write::Submission(record))
    }

    pub async fn commit(mut self) -> Result<CommitReceipt, StorageError> {
        // Deferred document writes ride along with the final batch of the next
        // normal commit.
        for write in self.deferred.drain(..) {
            self.current.push(Write::Document(write));
        }
        if !self.current.is_empty() {
            self.batches.push(CommitBatch {
                writes: self.current,
            });
        }
        if self.batches.is_empty() {
            return Ok(CommitReceipt::default());
        }
        self.storage.commit(self.batches, &self.ctx).await
    }
}
