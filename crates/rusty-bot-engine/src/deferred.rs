//! In-memory deferred document writes for transient live partials.
//!
//! Only `PutLatest`/`Delta` writes are deferrable. Deferred writes are flushed
//! by the next normal commit, at their deadline, or dropped on session close;
//! durable state is never derived solely from a deferred document.

use rusty_bot_core::{DocumentAddress, DocumentWrite};

use crate::transaction::TxError;

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;

#[derive(Clone, Debug)]
struct DeferredEntry {
    address: DocumentAddress,
    write: DocumentWrite,
    first_at_ms: i64,
}

#[derive(Clone, Debug, Default)]
pub struct DeferredBuffer {
    entries: Vec<DeferredEntry>,
}

/// A deferred write may only replace the value of a singleton/latest document.
/// Rewindable writes and retirements change durable history and must go through
/// a normal commit.
pub fn is_deferred_eligible(write: &DocumentWrite) -> bool {
    matches!(
        write,
        DocumentWrite::PutLatest { .. } | DocumentWrite::Delta { .. }
    )
}

impl DeferredBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a write, coalescing by document address. The deadline stays
    /// anchored to the first staged write so repeated partials cannot postpone
    /// persistence forever.
    pub fn defer(&mut self, write: DocumentWrite, now_ms: i64) -> Result<(), TxError> {
        if !is_deferred_eligible(&write) {
            return Err(TxError::IneligibleDeferredWrite);
        }
        let address = write.address().clone();
        if let Some(entry) = self.entries.iter_mut().find(|e| e.address == address) {
            entry.write = write;
        } else {
            self.entries.push(DeferredEntry {
                address,
                write,
                first_at_ms: now_ms,
            });
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, address: &DocumentAddress) -> bool {
        self.entries.iter().any(|entry| &entry.address == address)
    }

    pub fn write_for(&self, address: &DocumentAddress) -> Option<&DocumentWrite> {
        self.entries
            .iter()
            .find(|entry| &entry.address == address)
            .map(|entry| &entry.write)
    }

    /// Remove and return writes whose deadline has been reached.
    pub fn take_due(&mut self, now_ms: i64, persist_within_ms: u64) -> Vec<DocumentWrite> {
        let persist = persist_within_ms as i64;
        let mut due = Vec::new();
        let mut keep = Vec::new();
        for entry in self.entries.drain(..) {
            if now_ms.saturating_sub(entry.first_at_ms) >= persist {
                due.push(entry.write);
            } else {
                keep.push(entry);
            }
        }
        self.entries = keep;
        due
    }

    /// Remove and return every staged write (session close / next commit).
    pub fn drain(&mut self) -> Vec<DocumentWrite> {
        self.entries
            .drain(..)
            .map(|entry| entry.write)
            .collect::<Vec<_>>()
    }
}
