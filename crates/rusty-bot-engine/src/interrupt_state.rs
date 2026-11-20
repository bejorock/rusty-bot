//! Dormant persisted interrupt primitives. Payloads and answers are sensitive.
//! These constructors do not authorize execution: a caller must commit the
//! returned batch, verify its receipt, and reload on OCC refusal.
use rusty_bot_core::{
    CommitBatch, DocumentAddress, DocumentPoint, DocumentQuery, DocumentScope, DocumentWrite,
    EntryId, ExecutionMode, InterruptId, PendingInterrupt, ReplayPolicy, SessionError, SessionId,
    Storage, SubmissionId, TaskRecord, TaskState, ToolCall, ToolError, ToolOutput, ToolSpec, Write,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::review_state::fingerprint;

const VERSION: u32 = 1;
pub(crate) const LEDGER: &str = "rusty-bot.interrupt.round";
pub(crate) const RECEIPT: &str = "rusty-bot.interrupt.receipt";
pub(crate) const PREPARED_NOT_STARTED: &str = "preparedNotStarted";

/// The pause transaction attests that this exact prepared effect was never polled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedNotStarted {
    version: u32,
    pub binding: Binding,
    digest: String,
}

impl PreparedNotStarted {
    pub(crate) fn new(binding: Binding) -> Result<Self, SessionError> {
        let digest = Self::digest(&binding)?;
        Ok(Self {
            version: 1,
            binding,
            digest,
        })
    }

    fn digest(binding: &Binding) -> Result<String, SessionError> {
        fingerprint(&serde_json::json!({
            "domain": "rusty-bot.interrupt.prepared-not-started.v1",
            "version": 1,
            "binding": binding,
        }))
    }

    pub(crate) fn validate(&self) -> Result<(), SessionError> {
        if self.version != 1
            || self.binding.pending.id.0.is_empty()
            || self.binding.pending.index != 0
            || !self.binding.pending.payload.is_null()
            || self.digest != Self::digest(&self.binding)?
        {
            return Err(protocol());
        }
        Ok(())
    }
}

/// Until an interrupt-capable driver owns recovery, no existing driver may
/// interpret interrupt documents as idle or resume their associated effects.
/// Even malformed and completed-looking receipts fail closed: they cannot be
/// authenticated or safely retired by a driver that does not implement the
/// protocol. A nonempty scan suffices; there is no reliance on scan pagination.
pub(crate) async fn guard_unsupported_host(
    storage: &dyn Storage,
    session: SessionId,
) -> Result<(), SessionError> {
    let ledger = storage
        .get_document(
            &DocumentAddress::new(LEDGER, DocumentScope::Session(session), ""),
            DocumentPoint::Current,
        )
        .await?;
    let receipts = storage
        .scan_documents(DocumentQuery {
            kind: Some(RECEIPT.into()),
            scope: Some(DocumentScope::Session(session)),
            key: None,
            at: DocumentPoint::Current,
            limit: 1,
        })
        .await?;
    if ledger.is_some() || !receipts.is_empty() {
        return Err(SessionError::InterruptUnsupported);
    }
    Ok(())
}

/// Metadata must be captured from trusted engine configuration, not the resume caller.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub pending: PendingInterrupt,
    pub submission: SubmissionId,
    pub assistant_entry: EntryId,
    pub call: ToolCall,
    pub final_arguments: Value,
    pub spec: ToolSpec,
    pub replay: ReplayPolicy,
    pub mode: ExecutionMode,
    pub extensions: Value,
    pub hooks: Value,
    pub workspace: Value,
    pub environment: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Candidate {
    pub binding: Binding,
    pub digest: String,
    pub answers: Vec<Value>,
}

impl Candidate {
    pub(crate) fn new(binding: Binding, answers: Vec<Value>) -> Result<Self, SessionError> {
        let digest = candidate_digest(&binding, &answers)?;
        let candidate = Self {
            binding,
            digest,
            answers,
        };
        candidate.validate()?;
        Ok(candidate)
    }
    fn validate(&self) -> Result<(), SessionError> {
        if self.binding.pending.id.0.is_empty()
            || usize::try_from(self.binding.pending.index).ok() != Some(self.answers.len())
            || candidate_digest(&self.binding, &self.answers)? != self.digest
        {
            return Err(protocol());
        }
        Ok(())
    }
}

fn candidate_digest(binding: &Binding, answers: &[Value]) -> Result<String, SessionError> {
    fingerprint(
        &serde_json::json!({"domain": "rusty-bot.interrupt.candidate.v1", "binding": binding, "answers": answers}),
    )
}

/// Raw peer effects are not transcript entries and have not passed after_tool.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct HeldOutcome {
    pub ordinal: usize,
    pub operation_key: String,
    pub original_call: ToolCall,
    pub final_arguments: Value,
    pub outcome: Result<ToolOutput, ToolError>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Authorization {
    Waiting,
    ResumeReady { id: InterruptId, generation: u64 },
    ReplayInFlight { id: InterruptId, generation: u64 },
    Settling,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub version: u32,
    pub candidates: Vec<Candidate>,
    pub held: Vec<HeldOutcome>,
    pub authorization: Authorization,
    pub digest: String,
}

/// A completed replay either exposes the next candidate or returns every raw
/// outcome to the caller for later ordered settlement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Remaining {
    Waiting(Ledger),
    Complete(Vec<HeldOutcome>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Disposition {
    AcceptedInProgress,
    CompletedOrAdvanced,
    TerminalFailure,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Receipt {
    pub version: u32,
    pub candidate: Candidate,
    pub value: Value,
    pub generation: u64,
    pub disposition: Disposition,
    pub digest: String,
}

impl Receipt {
    fn digest(&self) -> Result<String, SessionError> {
        fingerprint(
            &serde_json::json!({"domain": "rusty-bot.interrupt.receipt.v1", "candidate": self.candidate, "value": self.value, "generation": self.generation, "disposition": self.disposition}),
        )
    }
    pub(crate) fn resolve(&mut self, disposition: Disposition) -> Result<(), SessionError> {
        if self.disposition != Disposition::AcceptedInProgress
            || disposition == Disposition::AcceptedInProgress
            || self.digest != self.digest()?
        {
            return Err(protocol());
        }
        self.disposition = disposition;
        self.digest = self.digest()?;
        Ok(())
    }
    pub(crate) fn validate_integrity(&self, id: &InterruptId) -> Result<(), SessionError> {
        self.candidate.validate()?;
        if self.version != VERSION
            || self.digest != self.digest()?
            || &self.candidate.binding.pending.id != id
        {
            return Err(SessionError::InterruptProtocol(
                "receipt binding or integrity mismatch".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn validate(
        &self,
        id: &InterruptId,
        value: &Value,
        binding: &Binding,
    ) -> Result<(), SessionError> {
        self.validate_integrity(id)?;
        if &self.candidate.binding != binding {
            return Err(SessionError::InterruptProtocol(
                "receipt binding mismatch".into(),
            ));
        }
        if &self.value != value {
            return Err(SessionError::InterruptConflict);
        }
        if self.disposition != Disposition::AcceptedInProgress {
            return Err(SessionError::InterruptAlreadyResolved);
        }
        Ok(())
    }
}

impl Ledger {
    pub(crate) fn waiting(
        candidates: Vec<Candidate>,
        held: Vec<HeldOutcome>,
    ) -> Result<Self, SessionError> {
        let mut ledger = Self {
            version: VERSION,
            candidates,
            held,
            authorization: Authorization::Waiting,
            digest: String::new(),
        };
        ledger.reseal()?;
        ledger.validate()?;
        Ok(ledger)
    }
    pub(crate) fn settling(held: Vec<HeldOutcome>) -> Result<Self, SessionError> {
        let mut ledger = Self {
            version: VERSION,
            candidates: vec![],
            held,
            authorization: Authorization::Settling,
            digest: String::new(),
        };
        ledger.reseal()?;
        ledger.validate()?;
        Ok(ledger)
    }

    pub(crate) fn remove_first_held(&self, call: &ToolCall) -> Result<Option<Self>, SessionError> {
        self.validate()?;
        if !matches!(self.authorization, Authorization::Settling)
            || self.held.first().map(|h| &h.original_call) != Some(call)
        {
            return Err(protocol());
        }
        if self.held.len() == 1 {
            return Ok(None);
        }
        Ok(Some(Self::settling(self.held[1..].to_vec())?))
    }

    fn integrity(&self) -> Result<String, SessionError> {
        fingerprint(
            &serde_json::json!({"domain": "rusty-bot.interrupt.ledger.v1", "version": self.version, "candidates": self.candidates, "held": self.held, "authorization": self.authorization}),
        )
    }
    fn reseal(&mut self) -> Result<(), SessionError> {
        self.digest = self.integrity()?;
        Ok(())
    }
    pub(crate) fn validate(&self) -> Result<(), SessionError> {
        if self.version != VERSION || self.digest != self.integrity()? {
            return Err(protocol());
        }
        if self.candidates.is_empty() {
            if !matches!(self.authorization, Authorization::Settling) || self.held.is_empty() {
                return Err(protocol());
            }
            let mut previous = None;
            for h in &self.held {
                if h.operation_key
                    != crate::tool_task::idempotency_key(&h.original_call.name, &h.original_call.id)
                    || previous.is_some_and(|ordinal| ordinal >= h.ordinal)
                {
                    return Err(protocol());
                }
                previous = Some(h.ordinal);
            }
            return Ok(());
        }
        if matches!(self.authorization, Authorization::Settling) {
            return Err(protocol());
        }
        let first = &self.candidates[0].binding.pending;
        let mut previous = None;
        let mut ids = std::collections::HashSet::new();
        let first_binding = &self.candidates[0].binding;
        for c in &self.candidates {
            c.validate()?;
            let p = &c.binding.pending;
            if p.session_id != first.session_id
                || p.conversation_id != first.conversation_id
                || p.task_id != first.task_id
                || p.round != first.round
                || c.binding.submission != first_binding.submission
                || c.binding.assistant_entry != first_binding.assistant_entry
                || !ids.insert(&p.id)
                || previous.is_some_and(|v| v >= p.ordinal)
            {
                return Err(protocol());
            }
            previous = Some(p.ordinal);
        }
        let mut previous_held = None;
        for h in &self.held {
            if h.operation_key.is_empty()
                || previous_held.is_some_and(|ordinal| ordinal >= h.ordinal)
                || self
                    .candidates
                    .iter()
                    .any(|c| c.binding.pending.ordinal == h.ordinal)
            {
                return Err(protocol());
            }
            previous_held = Some(h.ordinal);
        }
        match &self.authorization {
            Authorization::Waiting => {}
            Authorization::ResumeReady { id, .. } | Authorization::ReplayInFlight { id, .. }
                if *id == first.id => {}
            _ => return Err(protocol()),
        }
        Ok(())
    }
    pub(crate) fn accept(
        &self,
        id: &InterruptId,
        value: Value,
        generation: u64,
        expected: &Binding,
    ) -> Result<(Self, Receipt), SessionError> {
        self.validate()?;
        let c = self.candidates.first().ok_or_else(protocol)?;
        if !matches!(self.authorization, Authorization::Waiting)
            || &c.binding.pending.id != id
            || &c.binding != expected
        {
            return Err(SessionError::InvalidInterrupt(
                "not the exposed interrupt".into(),
            ));
        }
        let mut receipt = Receipt {
            version: VERSION,
            candidate: c.clone(),
            value,
            generation,
            disposition: Disposition::AcceptedInProgress,
            digest: String::new(),
        };
        receipt.digest = receipt.digest()?;
        let mut next = self.clone();
        next.authorization = Authorization::ResumeReady {
            id: id.clone(),
            generation,
        };
        next.reseal()?;
        Ok((next, receipt))
    }
    /// Only a committed return batch from this transition permits one invocation.
    pub(crate) fn begin_replay(&self, receipt: &Receipt) -> Result<Self, SessionError> {
        self.validate()?;
        let (id, generation) = match &self.authorization {
            Authorization::ResumeReady { id, generation } => (id, *generation),
            _ => return Err(protocol()),
        };
        receipt.validate(id, &receipt.value, &self.candidates[0].binding)?;
        if receipt.candidate != self.candidates[0] || receipt.generation != generation {
            return Err(protocol());
        }
        let mut next = self.clone();
        next.authorization = Authorization::ReplayInFlight {
            id: id.clone(),
            generation,
        };
        next.reseal()?;
        Ok(next)
    }
    pub(crate) fn answers_for_replay(&self, receipt: &Receipt) -> Result<Vec<Value>, SessionError> {
        self.validate()?;
        let (id, generation) = match &self.authorization {
            Authorization::ReplayInFlight { id, generation } => (id, *generation),
            _ => return Err(protocol()),
        };
        receipt.validate(id, &receipt.value, &self.candidates[0].binding)?;
        if receipt.generation != generation || receipt.candidate != self.candidates[0] {
            return Err(protocol());
        }
        let mut answers = receipt.candidate.answers.clone();
        answers.push(receipt.value.clone());
        Ok(answers)
    }
    /// Replace only the exposed in-flight candidate with its next interruption.
    /// The receipt and returned ledger must be persisted atomically by the caller.
    pub(crate) fn repeat_first(
        &self,
        receipt: &Receipt,
        candidate: Candidate,
    ) -> Result<(Receipt, Self), SessionError> {
        let answers = self.answers_for_replay(receipt)?;
        candidate.validate()?;
        let old = &self.candidates[0];
        let mut expected_binding = old.binding.clone();
        expected_binding.pending.id = candidate.binding.pending.id.clone();
        expected_binding.pending.index = candidate.binding.pending.index;
        expected_binding.pending.payload = candidate.binding.pending.payload.clone();
        if candidate.binding != expected_binding
            || candidate.binding.pending.id == old.binding.pending.id
            || candidate.answers != answers
        {
            return Err(protocol());
        }
        let mut next = self.clone();
        next.candidates[0] = candidate;
        next.authorization = Authorization::Waiting;
        next.reseal()?;
        next.validate()?;
        let mut resolved = receipt.clone();
        resolved.resolve(Disposition::CompletedOrAdvanced)?;
        Ok((resolved, next))
    }

    /// Preserve the completed raw outcome without settling it or discarding peers.
    /// The receipt and returned state must be persisted atomically by the caller.
    pub(crate) fn complete_first(
        &self,
        receipt: &Receipt,
        outcome: HeldOutcome,
    ) -> Result<(Receipt, Remaining), SessionError> {
        self.answers_for_replay(receipt)?;
        let binding = &self.candidates[0].binding;
        if outcome.ordinal != binding.pending.ordinal
            || outcome.original_call != binding.call
            || outcome.final_arguments != binding.final_arguments
            || outcome.operation_key
                != crate::tool_task::idempotency_key(&binding.call.name, &binding.call.id)
        {
            return Err(protocol());
        }
        let mut next = self.clone();
        next.candidates.remove(0);
        let position = next
            .held
            .partition_point(|held| held.ordinal < outcome.ordinal);
        next.held.insert(position, outcome);
        let mut resolved = receipt.clone();
        resolved.resolve(Disposition::CompletedOrAdvanced)?;
        if next.candidates.is_empty() {
            Ok((resolved, Remaining::Complete(next.held)))
        } else {
            next.authorization = Authorization::Waiting;
            next.reseal()?;
            next.validate()?;
            Ok((resolved, Remaining::Waiting(next)))
        }
    }

    /// Construct one atomic OCC batch. The task's existing version must be incremented.
    pub(crate) fn batch(
        &self,
        previous: &TaskRecord,
        next: TaskRecord,
        receipt: Option<&Receipt>,
        event: &str,
    ) -> Result<CommitBatch, SessionError> {
        self.validate()?;
        let p = &self
            .candidates
            .first()
            .ok_or_else(protocol)?
            .binding
            .pending;
        if previous.id != p.task_id
            || previous.conversation_id != p.conversation_id
            || next.id != previous.id
            || next.conversation_id != previous.conversation_id
            || previous.version.checked_add(1) != Some(next.version)
        {
            return Err(protocol());
        }
        let valid_transition = matches!(
            (&self.authorization, &previous.state, &next.state),
            (
                Authorization::Waiting,
                TaskState::Running { .. },
                TaskState::Waiting { .. }
            ) | (
                Authorization::ResumeReady { .. },
                TaskState::Waiting { .. },
                TaskState::Running { .. }
            ) | (
                Authorization::ReplayInFlight { .. },
                TaskState::Running { .. },
                TaskState::Running { .. }
            )
        );
        if !valid_transition {
            return Err(protocol());
        }
        let waiting_state = match &self.authorization {
            Authorization::Waiting => &next.state,
            Authorization::ResumeReady { .. } => &previous.state,
            Authorization::ReplayInFlight { .. } => &previous.state,
            Authorization::Settling => return Err(protocol()),
        };
        if !matches!(self.authorization, Authorization::ReplayInFlight { .. })
            && !matches!(waiting_state, TaskState::Waiting { on, policy: rusty_bot_core::WaitPolicy::Any, .. } if *on == vec![format!("interrupt:{}", p.id.0)])
        {
            return Err(protocol());
        }
        match (&self.authorization, receipt) {
            (Authorization::Waiting, None) => {}
            (
                Authorization::ResumeReady { id, generation }
                | Authorization::ReplayInFlight { id, generation },
                Some(r),
            ) => {
                r.validate(id, &r.value, &self.candidates[0].binding)?;
                if r.candidate != self.candidates[0] || r.generation != *generation {
                    return Err(protocol());
                }
            }
            _ => return Err(protocol()),
        }
        let session = p.session_id;
        let mut writes = vec![
            Write::Task(next),
            Write::Document(DocumentWrite::PutLatest {
                address: DocumentAddress::new(LEDGER, DocumentScope::Session(session), ""),
                value: serde_json::to_value(self).map_err(|_| protocol())?,
            }),
        ];
        if let Some(r) = receipt {
            writes.push(Write::Document(DocumentWrite::PutLatest {
                address: DocumentAddress::new(
                    RECEIPT,
                    DocumentScope::Session(session),
                    &r.candidate.binding.pending.id.0,
                ),
                value: serde_json::to_value(r).map_err(|_| protocol())?,
            }));
        }
        writes.push(Write::Event(rusty_bot_core::SessionEventDraft {
            kind: event.into(),
            payload: serde_json::json!({}),
        }));
        Ok(CommitBatch { writes })
    }
}

fn protocol() -> SessionError {
    SessionError::InterruptProtocol("invalid interrupt ledger or transition".into())
}
