//! Submission recovery.
//!
//! The storage trait exposes `get_submission`/`find_submission_by_request` but
//! no queued-submission scan. Durable events are the recovery path: every
//! `submission.queued`/`placed`/`done` event carries the submission id, so a
//! reopened engine can rebuild its pending queue from committed rows.

use std::collections::BTreeMap;

use rusty_bot_core::{event_kind, EventCursor, SessionError, SessionId, Storage, SubmissionId};
use serde_json::Value;

pub async fn recover_queued(
    storage: &dyn Storage,
    session: SessionId,
) -> Result<Vec<SubmissionId>, SessionError> {
    // id -> still queued
    let mut status: BTreeMap<i64, bool> = BTreeMap::new();
    let mut cursor = EventCursor::start();
    loop {
        let page = storage.scan_events(session, cursor, 500).await?;
        let count = page.events.len();
        let next = page.next;
        for event in &page.events {
            let Some(id) = event.payload.get("submission_id").and_then(Value::as_i64) else {
                continue;
            };
            match event.kind.as_str() {
                event_kind::SUBMISSION_QUEUED => {
                    status.insert(id, true);
                }
                event_kind::SUBMISSION_PLACED | event_kind::SUBMISSION_DONE => {
                    status.insert(id, false);
                }
                _ => {}
            }
        }
        match next {
            Some(next) if count > 0 && next != cursor => cursor = next,
            _ => break,
        }
    }
    Ok(status
        .into_iter()
        .filter(|(_, queued)| *queued)
        .map(|(id, _)| SubmissionId(id))
        .collect())
}
