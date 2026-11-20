//! Interrupt-capable driver with bounded, ordered tool rounds.
use super::*;
use crate::interrupt_state::{
    self, Authorization, Binding, Candidate, Disposition, HeldOutcome, Ledger, Receipt, Remaining,
};
use rusty_bot_core::{DocumentPoint, InterruptId, PendingInterrupt, WaitPolicy};

fn unsafe_peer_result(result: &InvocationResult) -> bool {
    match result {
        InvocationResult::Returned(Err(error)) => {
            matches!(error.code.as_str(), "aborted" | "timeout")
        }
        InvocationResult::Returned(Ok(output)) => output
            .details
            .as_ref()
            .and_then(|details| details.get("code"))
            .and_then(Value::as_str)
            .is_some_and(|code| matches!(code, "aborted" | "timeout")),
        _ => false,
    }
}

pub struct SdkInterruptDriver(SessionEngine);

#[derive(Clone, Debug, PartialEq)]
pub enum InterruptDriveOutcome {
    Interrupted(PendingInterrupt),
    Settled(RunOutcome),
}

impl SessionEngine {
    /// Select the interrupt-capable driver. Review composition is not supported.
    pub fn into_sdk_interrupt_driver(mut self) -> Result<SdkInterruptDriver, SessionError> {
        if self.spec.review_enabled {
            return Err(SessionError::InterruptUnsupported);
        }
        self.interrupt_effect_driver = true;
        Ok(SdkInterruptDriver(self))
    }

    fn interrupt_ledger_address(&self) -> DocumentAddress {
        DocumentAddress::new(
            interrupt_state::LEDGER,
            DocumentScope::Session(self.spec.session_id),
            "",
        )
    }
    fn interrupt_receipt_address(&self, id: &InterruptId) -> DocumentAddress {
        DocumentAddress::new(
            interrupt_state::RECEIPT,
            DocumentScope::Session(self.spec.session_id),
            &id.0,
        )
    }
    async fn interrupt_ledger(&self) -> Result<Option<Ledger>, SessionError> {
        let doc = self
            .deps
            .storage
            .get_document(&self.interrupt_ledger_address(), DocumentPoint::Current)
            .await?;
        let Some(doc) = doc else { return Ok(None) };
        let ledger: Ledger = serde_json::from_value(doc.value)
            .map_err(|_| SessionError::InterruptProtocol("invalid interrupt ledger".into()))?;
        ledger.validate()?;
        Ok(Some(ledger))
    }
    async fn interrupt_receipt(&self, id: &InterruptId) -> Result<Receipt, SessionError> {
        let doc = self
            .deps
            .storage
            .get_document(&self.interrupt_receipt_address(id), DocumentPoint::Current)
            .await?
            .ok_or_else(|| SessionError::InvalidInterrupt("unknown interrupt".into()))?;
        let receipt: Receipt = serde_json::from_value(doc.value)
            .map_err(|_| SessionError::InterruptProtocol("invalid interrupt receipt".into()))?;
        receipt.validate_integrity(id)?;
        let binding = &receipt.candidate.binding;
        let tool = self
            .tools
            .get(&binding.call.name)
            .ok_or_else(|| SessionError::InterruptProtocol("receipt tool missing".into()))?;
        let task = self.interrupt_task(binding.pending.task_id).await?;
        if binding.pending.session_id != self.spec.session_id
            || binding.pending.conversation_id != self.conversation_id
            || task.record.conversation_id != self.conversation_id
            || submission_id_of(&task.record) != Some(binding.submission)
            || binding.spec != tool.spec()
            || binding.replay != tool.replay()
            || binding.mode != tool.execution_mode()
            || binding.extensions
                != json!({"registry": self.spec.registry_version, "versions": self.spec.extension_versions, "config": self.config})
            || binding.hooks != json!(self.spec.hook_descriptors)
            || binding.workspace
                != json!({"key": self.deps.workspace.key().0, "root": self.deps.workspace.root(), "host_id": self.deps.workspace.host_id()})
            || binding.environment != json!(self.deps.workspace.execution_env_id())
        {
            return Err(SessionError::InterruptProtocol(
                "receipt binding changed".into(),
            ));
        }
        Ok(receipt)
    }
    async fn interrupt_batch(&self, batch: CommitBatch, version: u32) -> Result<(), SessionError> {
        let receipt = self
            .deps
            .storage
            .commit(
                vec![batch],
                &CommitContext::conversation(self.spec.session_id, self.conversation_id),
            )
            .await?;
        self.handle_task_receipt(receipt, version).await
    }
    async fn interrupt_try_batch(
        &self,
        batch: CommitBatch,
        version: u32,
    ) -> Result<bool, SessionError> {
        let receipt = self
            .deps
            .storage
            .commit(
                vec![batch],
                &CommitContext::conversation(self.spec.session_id, self.conversation_id),
            )
            .await?;
        if matches!(
            receipt.refused.as_ref().map(|r| &r.reason),
            Some(rusty_bot_core::RefusalReason::VersionConflict { .. })
        ) {
            return Ok(false);
        }
        self.handle_task_receipt(receipt, version).await?;
        Ok(true)
    }
    async fn interrupt_task(&self, id: TaskId) -> Result<StoredTask, SessionError> {
        self.deps
            .storage
            .get_task(id)
            .await?
            .ok_or_else(|| SessionError::InterruptProtocol("missing interrupt task".into()))
    }
    async fn interrupt_binding(
        &self,
        task: &StoredTask,
        call: &ToolCall,
        arguments: Value,
        latched: crate::interrupt_port::LatchedInterrupt,
    ) -> Result<Binding, SessionError> {
        let round = task
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistant = entries
            .iter()
            .rev()
            .find(|e| {
                e.record.by_task_id == Some(task.record.id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == round
            })
            .ok_or_else(|| SessionError::InterruptProtocol("missing assistant".into()))?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| SessionError::InterruptProtocol("missing calls".into()))?,
        )
        .map_err(|_| SessionError::InterruptProtocol("invalid calls".into()))?;
        let ordinal = calls
            .iter()
            .position(|c| c == call)
            .ok_or_else(|| SessionError::InterruptProtocol("call missing from round".into()))?;
        if validate_round_prefix(&entries, task.record.id, round, &calls)? > ordinal {
            return Err(SessionError::InterruptProtocol(
                "call already settled".into(),
            ));
        }
        let tool = self
            .tools
            .get(&call.name)
            .ok_or(SessionError::InterruptUnsupported)?;
        let submission = submission_id_of(&task.record)
            .ok_or_else(|| SessionError::InterruptProtocol("missing submission".into()))?;
        Ok(Binding {
            pending: PendingInterrupt {
                id: InterruptId(uuid::Uuid::new_v4().to_string()),
                session_id: self.spec.session_id,
                conversation_id: self.conversation_id,
                task_id: task.record.id,
                round,
                ordinal,
                index: latched.index,
                payload: latched.payload,
            },
            submission,
            assistant_entry: assistant.record.id,
            call: call.clone(),
            final_arguments: arguments,
            spec: tool.spec(),
            replay: tool.replay(),
            mode: tool.execution_mode(),
            extensions: json!({"registry": self.spec.registry_version, "versions": self.spec.extension_versions, "config": self.config}),
            hooks: json!(self.spec.hook_descriptors),
            workspace: json!({"key": self.deps.workspace.key().0, "root": self.deps.workspace.root(), "host_id": self.deps.workspace.host_id()}),
            environment: json!(self.deps.workspace.execution_env_id()),
        })
    }
    /// Consume only the explicit pause-transaction attestation, never a generic intent.
    pub(super) async fn prepare_deferred_effect(
        &mut self,
        task_id: TaskId,
        call: &ToolCall,
    ) -> Result<Option<PreparedTool>, SessionError> {
        let key = tool_task::idempotency_key(&call.name, &call.id);
        let Some(mut op) = self.deps.storage.get_operation(task_id, &key).await? else {
            return Ok(None);
        };
        let Some(marker) = op
            .result
            .as_ref()
            .and_then(|value| value.get(interrupt_state::PREPARED_NOT_STARTED))
        else {
            return Ok(None);
        };
        if !self.interrupt_effect_driver {
            return Err(SessionError::InterruptUnsupported);
        }
        let marker: interrupt_state::PreparedNotStarted = serde_json::from_value(marker.clone())
            .map_err(|_| SessionError::InterruptProtocol("invalid prepared marker".into()))?;
        marker.validate()?;
        let b = &marker.binding;
        if b.pending.task_id != task_id
            || &b.call != call
            || !self.tools.model_visible.contains(&call.name)
        {
            return Err(SessionError::InterruptProtocol(
                "prepared call mismatch".into(),
            ));
        }
        let task = self.interrupt_task(task_id).await?;
        if !matches!(task.record.state, TaskState::Running { .. }) {
            return Err(SessionError::InterruptProtocol(
                "prepared task not running".into(),
            ));
        }
        let mut expected = self
            .interrupt_binding(
                &task,
                call,
                b.final_arguments.clone(),
                crate::interrupt_port::LatchedInterrupt {
                    index: b.pending.index,
                    payload: b.pending.payload.clone(),
                },
            )
            .await?;
        expected.pending.id = b.pending.id.clone();
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistant = entries
            .iter()
            .find(|entry| entry.record.id == b.assistant_entry)
            .ok_or_else(|| SessionError::InterruptProtocol("missing prepared assistant".into()))?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| SessionError::InterruptProtocol("missing prepared calls".into()))?,
        )
        .map_err(|_| SessionError::InterruptProtocol("invalid prepared calls".into()))?;
        let tool = self
            .tools
            .get(&call.name)
            .ok_or(SessionError::InterruptUnsupported)?;
        if expected != *b
            || validate_round_prefix(&entries, task_id, b.pending.round, &calls)?
                != b.pending.ordinal
            || calls.get(b.pending.ordinal) != Some(call)
            || op.task_id != task_id
            || op.idempotency_key != key
            || op.tool_name != call.name
            || op.status != ToolOperationStatus::Intent
            || op.request != b.final_arguments
            || tool_task::recorded_replay(&op) != Ok(Some(b.replay))
            || op
                .result
                .as_ref()
                .and_then(|v| v.get("rusty_bot_parallel_intent"))
                .and_then(|v| v.get("replayed"))
                .and_then(Value::as_bool)
                != Some(false)
            || validate_arguments(&tool, &b.final_arguments).is_err()
        {
            return Err(SessionError::InterruptProtocol(
                "invalid prepared intent".into(),
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(SessionError::Cancelled);
        }
        let mut next = task.record.clone();
        next.version += 1;
        next.state = TaskState::Running {
            checkpoint: Some(ToolCheckpoint::Execute {
                arguments: b.final_arguments.clone(),
                replay: b.replay,
                wake: None,
                checkpoint: None,
            }),
            wake: None,
        };
        op.status = ToolOperationStatus::Running;
        op.result = Some(tool_task::parallel_intent(b.replay, true));
        op.updated_at = self.deps.clock.now_ms();
        let mut tx = self.conversation_tx();
        tx.write(Write::Task(next))?;
        tx.write(Write::ToolOperation(op))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TOOL_STARTED.into(),
            payload: json!({"task_id": task_id.0, "tool_call_id": call.id, "tool_name": call.name, "idempotency_key": key, "replayed": true}),
        }))?;
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, task.record.version)
            .await?;
        Ok(Some(PreparedTool::Effect(PreparedToolEffect {
            identity: ToolEffectIdentity {
                stored: task,
                call: call.clone(),
                round: b.pending.round,
                key,
                _replay: b.replay,
            },
            tool: Some(tool),
            arguments: b.final_arguments.clone(),
            context: ToolContext {
                session_id: self.spec.session_id,
                conversation_id: self.conversation_id,
                task_id,
                files: self.deps.workspace.files(),
                shell: self.deps.workspace.execution(),
                live: self.deps.live.clone(),
                cancel: self.cancel.clone(),
                now_ms: self.deps.clock.now_ms(),
                image_limits: self.deps.provider.image_limits(),
                interrupt_port: None,
            },
            interrupt_answers: Some(Vec::new()),
        })))
    }

    async fn validate_interrupt_binding(
        &self,
        candidate: &Candidate,
        allow_uncertain: bool,
    ) -> Result<StoredTask, SessionError> {
        let b = &candidate.binding;
        if b.pending.session_id != self.spec.session_id
            || b.pending.conversation_id != self.conversation_id
        {
            return Err(SessionError::InterruptProtocol(
                "interrupt identity mismatch".into(),
            ));
        }
        let task = self.interrupt_task(b.pending.task_id).await?;
        let recomputed = self
            .interrupt_binding(
                &task,
                &b.call,
                b.final_arguments.clone(),
                crate::interrupt_port::LatchedInterrupt {
                    index: b.pending.index,
                    payload: b.pending.payload.clone(),
                },
            )
            .await?;
        let mut expected = recomputed;
        expected.pending.id = b.pending.id.clone();
        if &expected != b {
            return Err(SessionError::InterruptProtocol(
                "interrupt binding changed".into(),
            ));
        }
        let op = self
            .deps
            .storage
            .get_operation(
                task.record.id,
                &tool_task::idempotency_key(&b.call.name, &b.call.id),
            )
            .await?
            .ok_or_else(|| SessionError::InterruptProtocol("missing interrupt intent".into()))?;
        if op.task_id != task.record.id
            || op.idempotency_key != tool_task::idempotency_key(&b.call.name, &b.call.id)
            || op.tool_name != b.call.name
            || op.request != b.final_arguments
            || tool_task::recorded_replay(&op)
                .map_err(|_| SessionError::InterruptProtocol("invalid operation metadata".into()))?
                .is_some_and(|replay| replay != b.replay)
            || !(matches!(
                op.status,
                ToolOperationStatus::Intent | ToolOperationStatus::Running
            ) || (allow_uncertain && matches!(op.status, ToolOperationStatus::Uncertain)))
        {
            return Err(SessionError::InterruptProtocol(
                "invalid interrupt intent".into(),
            ));
        }
        Ok(task)
    }
    pub(super) async fn interrupt_completion_writes(
        &self,
        task_id: TaskId,
        call_id: &str,
    ) -> Result<Vec<Write>, SessionError> {
        let Some(ledger) = self.interrupt_ledger().await? else {
            return Ok(Vec::new());
        };
        if matches!(ledger.authorization, Authorization::Settling)
            || (matches!(ledger.authorization, Authorization::Waiting)
                && ledger.held.first().is_some_and(|held| {
                    held.ordinal < ledger.candidates[0].binding.pending.ordinal
                }))
        {
            let task = self.interrupt_task(task_id).await?;
            self.validate_held(&task, &ledger.held[0]).await?;
            if ledger.held[0].original_call.id != call_id {
                return Err(SessionError::InterruptProtocol(
                    "completion call mismatch".into(),
                ));
            }
            let next = if matches!(ledger.authorization, Authorization::Settling) {
                ledger.remove_first_held(&ledger.held[0].original_call)?
            } else {
                Some(Ledger::waiting(
                    ledger.candidates.clone(),
                    ledger.held[1..].to_vec(),
                )?)
            };
            return Ok(vec![Write::Document(match next {
                Some(next) => DocumentWrite::PutLatest {
                    address: self.interrupt_ledger_address(),
                    value: serde_json::to_value(next)
                        .map_err(|_| SessionError::InterruptProtocol("invalid ledger".into()))?,
                },
                None => DocumentWrite::Retire {
                    address: self.interrupt_ledger_address(),
                },
            })]);
        }
        let Authorization::ReplayInFlight { id, .. } = &ledger.authorization else {
            return Err(SessionError::InterruptProtocol(
                "completion without replay authorization".into(),
            ));
        };
        self.validate_interrupt_binding(&ledger.candidates[0], false)
            .await?;
        if ledger.candidates[0].binding.pending.task_id != task_id
            || ledger.candidates[0].binding.call.id != call_id
            || ledger.candidates.len() != 1
            || !ledger.held.is_empty()
        {
            return Err(SessionError::InterruptProtocol(
                "completion task mismatch".into(),
            ));
        }
        let mut receipt = self.interrupt_receipt(id).await?;
        ledger.answers_for_replay(&receipt)?;
        receipt.resolve(Disposition::CompletedOrAdvanced)?;
        Ok(vec![
            Write::Document(DocumentWrite::Retire {
                address: self.interrupt_ledger_address(),
            }),
            Write::Document(DocumentWrite::PutLatest {
                address: self.interrupt_receipt_address(id),
                value: serde_json::to_value(receipt)
                    .map_err(|_| SessionError::InterruptProtocol("invalid receipt".into()))?,
            }),
        ])
    }
    async fn validate_held(
        &self,
        task: &StoredTask,
        held: &HeldOutcome,
    ) -> Result<(), SessionError> {
        let round = task
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistant = entries
            .iter()
            .rev()
            .find(|e| {
                e.record.by_task_id == Some(task.record.id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == round
            })
            .ok_or_else(|| SessionError::InterruptProtocol("missing held assistant".into()))?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| SessionError::InterruptProtocol("missing held calls".into()))?,
        )
        .map_err(|_| SessionError::InterruptProtocol("invalid held calls".into()))?;
        if calls.get(held.ordinal) != Some(&held.original_call)
            || validate_round_prefix(&entries, task.record.id, round, &calls)? > held.ordinal
            || held.operation_key
                != tool_task::idempotency_key(&held.original_call.name, &held.original_call.id)
        {
            return Err(SessionError::InterruptProtocol(
                "invalid held binding".into(),
            ));
        }
        let tool = self
            .tools
            .get(&held.original_call.name)
            .ok_or_else(|| SessionError::InterruptProtocol("held tool missing".into()))?;
        let op = self
            .deps
            .storage
            .get_operation(task.record.id, &held.operation_key)
            .await?
            .ok_or_else(|| SessionError::InterruptProtocol("missing held intent".into()))?;
        if op.result.as_ref().and_then(|v| v.get("rawHeldOutcome"))
            != Some(
                &serde_json::to_value(held)
                    .map_err(|_| SessionError::InterruptProtocol("invalid held outcome".into()))?,
            )
            || op.task_id != task.record.id
            || op.idempotency_key != held.operation_key
            || op.tool_name != held.original_call.name
            || op.request != held.final_arguments
            || !matches!(
                op.status,
                ToolOperationStatus::Intent | ToolOperationStatus::Running
            )
            || tool_task::recorded_replay(&op)
                .map_err(|_| SessionError::InterruptProtocol("invalid held metadata".into()))?
                .is_some_and(|replay| replay != tool.replay())
        {
            return Err(SessionError::InterruptProtocol(
                "invalid held intent".into(),
            ));
        }
        Ok(())
    }

    async fn validate_round_ledger(&self, ledger: &Ledger) -> Result<StoredTask, SessionError> {
        let task = if !ledger.candidates.is_empty() {
            let mut task = None;
            for (index, candidate) in ledger.candidates.iter().enumerate() {
                let checked = self
                    .validate_interrupt_binding(
                        candidate,
                        index == 0
                            && matches!(ledger.authorization, Authorization::ReplayInFlight { .. }),
                    )
                    .await?;
                task = Some(checked);
            }
            task.ok_or_else(|| SessionError::InterruptProtocol("missing candidate".into()))?
        } else {
            let tasks = self
                .deps
                .storage
                .scan_tasks(TaskQuery {
                    conversation_id: Some(self.conversation_id),
                    status: None,
                    kind: Some(TURN_TASK_KIND.into()),
                    limit: 1000,
                })
                .await?;
            let mut found = None;
            for task in tasks {
                if matches!(task.record.state, TaskState::Running { .. })
                    && self.validate_held(&task, &ledger.held[0]).await.is_ok()
                {
                    if found.is_some() {
                        return Err(SessionError::InterruptProtocol(
                            "ambiguous settling task".into(),
                        ));
                    }
                    found = Some(task);
                }
            }
            found.ok_or_else(|| SessionError::InterruptProtocol("missing settling task".into()))?
        };
        for held in &ledger.held {
            self.validate_held(&task, held).await?;
        }
        Ok(task)
    }

    async fn commit_round_completion(
        &mut self,
        identity: &ToolEffectIdentity,
        receipt: &Receipt,
        result: Result<ToolOutput, rusty_bot_core::ToolError>,
    ) -> Result<Remaining, SessionError> {
        let ledger = self
            .interrupt_ledger()
            .await?
            .ok_or_else(|| SessionError::InterruptProtocol("missing replay ledger".into()))?;
        let task = self.validate_round_ledger(&ledger).await?;
        let binding = &ledger.candidates[0].binding;
        if task.record.id != identity.stored.record.id || binding.call != identity.call {
            return Err(SessionError::InterruptProtocol(
                "completion identity mismatch".into(),
            ));
        }
        let outcome = HeldOutcome {
            ordinal: binding.pending.ordinal,
            operation_key: identity.key.clone(),
            original_call: identity.call.clone(),
            final_arguments: binding.final_arguments.clone(),
            outcome: result,
        };
        let (resolved, remaining) = ledger.complete_first(receipt, outcome.clone())?;
        // Raw outcomes preceding the next candidate must be settled before that
        // candidate is exposed. Keep the task running until that prefix drains.
        let settle_before_wait = matches!(&remaining, Remaining::Waiting(waiting)
            if waiting.held.first().is_some_and(|held| held.ordinal < waiting.candidates[0].binding.pending.ordinal));
        let mut next = task.record.clone();
        next.version += 1;
        let next_ledger = match &remaining {
            Remaining::Waiting(waiting) if settle_before_wait => {
                next.state = TaskState::Running {
                    checkpoint: task.record.state.checkpoint().cloned(),
                    wake: None,
                };
                waiting.clone()
            }
            Remaining::Waiting(waiting) => {
                next.state = TaskState::Waiting {
                    checkpoint: task.record.state.checkpoint().cloned(),
                    on: vec![format!(
                        "interrupt:{}",
                        waiting.candidates[0].binding.pending.id.0
                    )],
                    policy: WaitPolicy::Any,
                };
                waiting.clone()
            }
            Remaining::Complete(held) => {
                next.state = TaskState::Running {
                    checkpoint: task.record.state.checkpoint().cloned(),
                    wake: None,
                };
                Ledger::settling(held.clone())?
            }
        };
        let mut operation = self
            .deps
            .storage
            .get_operation(task.record.id, &outcome.operation_key)
            .await?
            .ok_or_else(|| SessionError::InterruptProtocol("missing replay intent".into()))?;
        let metadata = operation.result.get_or_insert_with(|| json!({}));
        metadata
            .as_object_mut()
            .ok_or_else(|| SessionError::InterruptProtocol("invalid intent metadata".into()))?
            .insert(
                "rawHeldOutcome".into(),
                serde_json::to_value(&outcome)
                    .map_err(|_| SessionError::InterruptProtocol("invalid held outcome".into()))?,
            );
        let batch = CommitBatch {
            writes: vec![
                Write::ToolOperation(operation),
                Write::Task(next),
                Write::Document(DocumentWrite::PutLatest {
                    address: self.interrupt_ledger_address(),
                    value: serde_json::to_value(next_ledger)
                        .map_err(|_| SessionError::InterruptProtocol("invalid ledger".into()))?,
                }),
                Write::Document(DocumentWrite::PutLatest {
                    address: self.interrupt_receipt_address(&receipt.candidate.binding.pending.id),
                    value: serde_json::to_value(resolved)
                        .map_err(|_| SessionError::InterruptProtocol("invalid receipt".into()))?,
                }),
                Write::Event(SessionEventDraft {
                    kind: "interrupt.advanced".into(),
                    payload: json!({}),
                }),
            ],
        };
        self.interrupt_batch(batch, task.record.version).await?;
        Ok(remaining)
    }

    async fn drain_before_candidate(&mut self) -> Result<(), SessionError> {
        loop {
            let ledger = self
                .interrupt_ledger()
                .await?
                .ok_or_else(|| SessionError::InterruptProtocol("missing waiting ledger".into()))?;
            if !matches!(ledger.authorization, Authorization::Waiting)
                || ledger.candidates.is_empty()
            {
                return Err(SessionError::InterruptProtocol(
                    "invalid waiting ledger".into(),
                ));
            }
            let task = self.validate_round_ledger(&ledger).await?;
            let candidate = &ledger.candidates[0];
            let Some(held) = ledger
                .held
                .first()
                .filter(|h| h.ordinal < candidate.binding.pending.ordinal)
            else {
                if matches!(task.record.state, TaskState::Waiting { .. }) {
                    return Ok(());
                }
                let mut next = task.record.clone();
                next.version += 1;
                next.state = TaskState::Waiting {
                    checkpoint: task.record.state.checkpoint().cloned(),
                    on: vec![format!("interrupt:{}", candidate.binding.pending.id.0)],
                    policy: WaitPolicy::Any,
                };
                let batch = ledger.batch(&task.record, next, None, "interrupt.paused")?;
                self.interrupt_batch(batch, task.record.version).await?;
                return Ok(());
            };
            if !matches!(task.record.state, TaskState::Running { .. }) {
                return Err(SessionError::InterruptProtocol(
                    "waiting task has unsettled prefix".into(),
                ));
            }
            let tool = self
                .tools
                .get(&held.original_call.name)
                .ok_or(SessionError::InterruptUnsupported)?;
            let identity = ToolEffectIdentity {
                stored: task,
                call: held.original_call.clone(),
                round: candidate.binding.pending.round,
                key: held.operation_key.clone(),
                _replay: tool.replay(),
            };
            self.settle_tool_effect(&identity, held.outcome.clone(), true)
                .await?;
        }
    }

    async fn drain_held(&mut self) -> Result<(), SessionError> {
        loop {
            let Some(ledger) = self.interrupt_ledger().await? else {
                return Ok(());
            };
            if !matches!(ledger.authorization, Authorization::Settling) {
                return Err(SessionError::InterruptProtocol("not settling".into()));
            }
            let task = self.validate_round_ledger(&ledger).await?;
            let held = &ledger.held[0];
            let round = task
                .record
                .memos
                .get("generation_round")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let entries =
                prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
            let calls: Vec<ToolCall> = entries
                .iter()
                .rev()
                .find(|e| {
                    e.record.by_task_id == Some(task.record.id)
                        && e.record.kind == entry_kind::ASSISTANT
                        && e.record
                            .data
                            .get("round")
                            .and_then(Value::as_u64)
                            .unwrap_or(0)
                            == round
                })
                .and_then(|e| e.record.data.get("tool_calls").cloned())
                .and_then(|v| serde_json::from_value(v).ok())
                .ok_or_else(|| SessionError::InterruptProtocol("missing held calls".into()))?;
            if validate_round_prefix(&entries, task.record.id, round, &calls)? != held.ordinal {
                return Err(SessionError::InterruptProtocol(
                    "held outcome is not next in prefix".into(),
                ));
            }
            let tool = self
                .tools
                .get(&held.original_call.name)
                .ok_or(SessionError::InterruptUnsupported)?;
            let identity = ToolEffectIdentity {
                stored: task.clone(),
                call: held.original_call.clone(),
                round: task
                    .record
                    .memos
                    .get("generation_round")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                key: held.operation_key.clone(),
                _replay: tool.replay(),
            };
            self.settle_tool_effect(&identity, held.outcome.clone(), true)
                .await?;
        }
    }

    async fn pause_interrupt(
        &mut self,
        identity: &ToolEffectIdentity,
        arguments: Value,
        answers: Vec<Value>,
        latched: crate::interrupt_port::LatchedInterrupt,
        old_receipt: Option<Receipt>,
    ) -> Result<PendingInterrupt, SessionError> {
        let task = self.interrupt_task(identity.stored.record.id).await?;
        let binding = self
            .interrupt_binding(&task, &identity.call, arguments, latched)
            .await?;
        let pending = binding.pending.clone();
        let candidate = Candidate::new(binding, answers)?;
        let (ledger, resolved) = if let Some(receipt) = old_receipt {
            let existing = self
                .interrupt_ledger()
                .await?
                .ok_or_else(|| SessionError::InterruptProtocol("missing replay ledger".into()))?;
            let (resolved, next) = existing.repeat_first(&receipt, candidate)?;
            (next, Some(resolved))
        } else {
            (Ledger::waiting(vec![candidate], vec![])?, None)
        };
        let mut next = task.record.clone();
        next.version += 1;
        next.state = TaskState::Waiting {
            checkpoint: task.record.state.checkpoint().cloned(),
            on: vec![format!("interrupt:{}", pending.id.0)],
            policy: WaitPolicy::Any,
        };
        let mut batch = ledger.batch(&task.record, next, None, "interrupt.paused")?;
        if let Some(receipt) = resolved {
            batch.writes.push(Write::Document(DocumentWrite::PutLatest {
                address: self.interrupt_receipt_address(&receipt.candidate.binding.pending.id),
                value: serde_json::to_value(receipt)
                    .map_err(|_| SessionError::InterruptProtocol("invalid receipt".into()))?,
            }));
        }
        self.interrupt_batch(batch, task.record.version).await?;
        Ok(pending)
    }
}

impl SdkInterruptDriver {
    async fn execute_interrupt_round(
        &mut self,
        task_id: TaskId,
        cancel: &CancellationToken,
    ) -> Result<Option<PendingInterrupt>, SessionError> {
        let task = self.0.interrupt_task(task_id).await?;
        let round = task
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let entries =
            prompt::load_entries(self.0.deps.storage.as_ref(), self.0.conversation_id).await?;
        let calls: Vec<ToolCall> = entries
            .iter()
            .rev()
            .find(|e| {
                e.record.by_task_id == Some(task_id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == round
            })
            .and_then(|e| e.record.data.get("tool_calls").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .ok_or_else(|| SessionError::InvalidState("missing round calls".into()))?;
        let prefix = validate_round_prefix(&entries, task_id, round, &calls)?;
        let round_cancel = cancel.child_token();
        self.0.cancel = round_cancel.clone();
        let mut active = FuturesUnordered::new();
        let mut outcomes = BTreeMap::new();
        let mut launch = prefix;
        let mut fence = false;
        let mut paused = false;
        let mut failure = None;
        let mut prepared_pending = None;
        let mut skipped_effect = None;
        loop {
            if let Some(Some((ordinal, identity, arguments, result))) = active.next().now_or_never()
            {
                match &result {
                    Ok(InvocationResult::Interrupted(_)) => paused = true,
                    Ok(result) if unsafe_peer_result(result) => {
                        failure = Some(SessionError::InterruptUncertain);
                        round_cancel.cancel();
                    }
                    Ok(InvocationResult::ProtocolViolation) | Err(_) => {
                        failure = Some(SessionError::InterruptProtocol(
                            "tool effect panicked or violated interrupt protocol".into(),
                        ));
                        round_cancel.cancel();
                    }
                    _ => {}
                }
                outcomes.insert(ordinal, (identity, arguments, result));
                if active.is_empty() {
                    fence = false;
                }
            }
            if cancel.is_cancelled() && failure.is_none() {
                failure = Some(SessionError::InterruptUncertain);
                round_cancel.cancel();
            }
            if failure.is_none()
                && !paused
                && prepared_pending.is_none()
                && launch < calls.len()
                && active.len() < self.0.max_parallel_tools.get()
            {
                let sequential = self
                    .0
                    .tools
                    .get(&calls[launch].name)
                    .map(|tool| tool.execution_mode() == rusty_bot_core::ExecutionMode::Sequential)
                    .unwrap_or(true);
                if !fence && (!sequential || active.is_empty()) {
                    let ordinal = launch;
                    let prepared = {
                        let preparation =
                            AssertUnwindSafe(self.0.prepare_tool_effect_with_metadata(
                                task_id,
                                calls[ordinal].clone(),
                                true,
                            ))
                            .catch_unwind();
                        tokio::pin!(preparation);
                        loop {
                            tokio::select! {
                                biased;
                                completed = active.next(), if !active.is_empty() => {
                                    if let Some((completed_ordinal, identity, arguments, result)) = completed {
                                        match &result {
                                            Ok(InvocationResult::Interrupted(_)) => paused = true,
                                            Ok(result) if unsafe_peer_result(result) => {
                                                failure.get_or_insert(SessionError::InterruptUncertain);
                                                round_cancel.cancel();
                                            }
                                            Ok(InvocationResult::ProtocolViolation) | Err(_) => {
                                                failure.get_or_insert(SessionError::InterruptProtocol(
                                                    "tool effect panicked or violated interrupt protocol".into(),
                                                ));
                                                round_cancel.cancel();
                                            }
                                            _ => {}
                                        }
                                        outcomes.insert(completed_ordinal, (identity, arguments, result));
                                    }
                                }
                                _ = cancel.cancelled(), if failure.is_none() => {
                                    failure = Some(SessionError::InterruptUncertain);
                                    round_cancel.cancel();
                                }
                                result = &mut preparation => break result,
                            }
                        }
                    };
                    match prepared {
                        Ok(Ok(PreparedTool::Effect(effect))) => {
                            launch += 1;
                            let identity = effect.identity.clone();
                            let arguments = effect.arguments.clone();
                            // A pause may have arrived while this preparation was
                            // in flight. Keep its durable intent, but do not start
                            // another effect after the pause has been observed.
                            if failure.is_none() && !paused {
                                active.push(async move {
                                    let result =
                                        AssertUnwindSafe(effect.execute()).catch_unwind().await;
                                    (
                                        ordinal,
                                        identity,
                                        arguments,
                                        result.map(|(_, result)| result),
                                    )
                                });
                                fence = sequential;
                            } else {
                                skipped_effect = Some((ordinal, identity, arguments));
                            }
                        }
                        Ok(Ok(other)) => {
                            launch += 1;
                            prepared_pending = Some((ordinal, other));
                        }
                        Ok(Err(error)) => {
                            launch += 1; // Preparation may already have written an intent.
                            failure.get_or_insert(error);
                            round_cancel.cancel();
                        }
                        Err(_) => {
                            launch += 1;
                            failure.get_or_insert(SessionError::Internal(
                                "tool preparation panicked".into(),
                            ));
                            round_cancel.cancel();
                        }
                    }
                    continue;
                }
            }
            if !active.is_empty() {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled(), if failure.is_none() => {
                        failure = Some(SessionError::InterruptUncertain);
                        round_cancel.cancel();
                    }
                    completed = active.next() => {
                        if let Some((ordinal, identity, arguments, result)) = completed {
                            match &result {
                                Ok(InvocationResult::Interrupted(_)) => paused = true,
                                Ok(result) if unsafe_peer_result(result) => {
                                    if failure.is_none() { failure = Some(SessionError::InterruptUncertain); }
                                    round_cancel.cancel();
                                }
                                Ok(InvocationResult::ProtocolViolation) | Err(_) => {
                                    if failure.is_none() { failure = Some(SessionError::InterruptProtocol("tool effect panicked or violated interrupt protocol".into())); }
                                    round_cancel.cancel();
                                }
                                _ => {}
                            }
                            outcomes.insert(ordinal, (identity, arguments, result));
                        }
                    }
                }
                if active.is_empty() {
                    fence = false;
                }
                continue;
            }
            if failure.is_some() || paused || prepared_pending.is_some() || launch == calls.len() {
                break;
            }
        }
        self.0.cancel = cancel.clone();
        // Retain the exact transaction identity even if commit returns an error:
        // a storage provider can commit atomically and lose the acknowledgement.
        let mut attempted_pause = None;
        let finalized = AssertUnwindSafe(async {
            if let Some(error) = failure {
                return Err(error);
            }
            if paused {
                let first = outcomes
                    .iter()
                    .find_map(|(ordinal, (_, _, result))| {
                        matches!(result, Ok(InvocationResult::Interrupted(_))).then_some(*ordinal)
                    })
                    .ok_or_else(|| SessionError::InterruptProtocol("missing interrupt".into()))?;
                if let Some((ordinal, prepared)) = prepared_pending {
                    if ordinal < first {
                        self.0.settle_prepared_tool(prepared, true).await?;
                    }
                }
                let mut candidates = Vec::new();
                let mut held = Vec::new();
                for (ordinal, (identity, arguments, result)) in outcomes {
                    match result {
                        Ok(InvocationResult::Returned(result)) if ordinal < first => {
                            self.0.settle_tool_effect(&identity, result, true).await?;
                        }
                        Ok(InvocationResult::Returned(result)) => held.push(HeldOutcome {
                            ordinal,
                            operation_key: identity.key,
                            original_call: identity.call,
                            final_arguments: arguments,
                            outcome: result,
                        }),
                        Ok(InvocationResult::Interrupted(latched)) => {
                            let task = self.0.interrupt_task(task_id).await?;
                            let binding = self
                                .0
                                .interrupt_binding(&task, &identity.call, arguments, latched)
                                .await?;
                            if binding.pending.ordinal != ordinal {
                                return Err(SessionError::InterruptProtocol(
                                    "ambiguous call ordinal".into(),
                                ));
                            }
                            candidates.push(Candidate::new(binding, vec![])?);
                        }
                        _ => {
                            return Err(SessionError::InterruptProtocol(
                                "invalid drained effect".into(),
                            ))
                        }
                    }
                }
                let ledger = Ledger::waiting(candidates, held)?;
                let pending = ledger.candidates[0].binding.pending.clone();
                let task = self.0.interrupt_task(task_id).await?;
                let mut next = task.record.clone();
                next.version += 1;
                next.state = TaskState::Waiting {
                    checkpoint: task.record.state.checkpoint().cloned(),
                    on: vec![format!("interrupt:{}", pending.id.0)],
                    policy: WaitPolicy::Any,
                };
                let mut batch = ledger.batch(&task.record, next, None, "interrupt.paused")?;
                if let Some((ordinal, identity, arguments)) = skipped_effect {
                    let binding = self
                        .0
                        .interrupt_binding(
                            &task,
                            &identity.call,
                            arguments,
                            crate::interrupt_port::LatchedInterrupt {
                                index: 0,
                                payload: Value::Null,
                            },
                        )
                        .await?;
                    if binding.pending.ordinal != ordinal {
                        return Err(SessionError::InterruptProtocol(
                            "ambiguous prepared ordinal".into(),
                        ));
                    }
                    let mut op = self
                        .0
                        .deps
                        .storage
                        .get_operation(task_id, &identity.key)
                        .await?
                        .ok_or_else(|| {
                            SessionError::InterruptProtocol("missing prepared intent".into())
                        })?;
                    if op.status != ToolOperationStatus::Intent
                        || op.request != binding.final_arguments
                        || op.task_id != task_id
                        || op.tool_name != binding.call.name
                        || op.idempotency_key != identity.key
                        || tool_task::recorded_replay(&op) != Ok(Some(binding.replay))
                        || op
                            .result
                            .as_ref()
                            .and_then(|v| v.get("rusty_bot_parallel_intent"))
                            .and_then(|v| v.get("replayed"))
                            .and_then(Value::as_bool)
                            != Some(false)
                    {
                        return Err(SessionError::InterruptProtocol(
                            "invalid skipped prepared intent".into(),
                        ));
                    }
                    let marker = interrupt_state::PreparedNotStarted::new(binding)?;
                    let metadata = op
                        .result
                        .as_mut()
                        .and_then(Value::as_object_mut)
                        .ok_or_else(|| {
                            SessionError::InterruptProtocol("invalid prepared metadata".into())
                        })?;
                    if metadata
                        .insert(
                            interrupt_state::PREPARED_NOT_STARTED.into(),
                            serde_json::to_value(marker).map_err(|_| {
                                SessionError::InterruptProtocol("invalid prepared marker".into())
                            })?,
                        )
                        .is_some()
                    {
                        return Err(SessionError::InterruptProtocol(
                            "duplicate prepared marker".into(),
                        ));
                    }
                    batch.writes.push(Write::ToolOperation(op));
                }
                for held in &ledger.held {
                    let mut op = self
                        .0
                        .deps
                        .storage
                        .get_operation(task_id, &held.operation_key)
                        .await?
                        .ok_or_else(|| {
                            SessionError::InterruptProtocol("missing held intent".into())
                        })?;
                    let metadata = op.result.get_or_insert_with(|| json!({}));
                    metadata
                        .as_object_mut()
                        .ok_or_else(|| {
                            SessionError::InterruptProtocol("invalid intent metadata".into())
                        })?
                        .insert(
                            "rawHeldOutcome".into(),
                            serde_json::to_value(held).map_err(|_| {
                                SessionError::InterruptProtocol("invalid held outcome".into())
                            })?,
                        );
                    batch.writes.push(Write::ToolOperation(op));
                }
                if cancel.is_cancelled() {
                    return Err(SessionError::InterruptUncertain);
                }
                let expected_task = batch
                    .writes
                    .iter()
                    .find_map(|write| match write {
                        Write::Task(record) => Some(record.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| SessionError::InterruptProtocol("missing pause task".into()))?;
                let expected_operations = batch
                    .writes
                    .iter()
                    .filter_map(|write| match write {
                        Write::ToolOperation(operation) => Some(operation.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                attempted_pause = Some((
                    ledger,
                    task.record.clone(),
                    expected_task,
                    expected_operations,
                ));
                self.0.interrupt_batch(batch, task.record.version).await?;
                return Ok(Some(pending));
            }
            for (_, (identity, _, result)) in outcomes {
                match result {
                    Ok(InvocationResult::Returned(result)) => {
                        self.0.settle_tool_effect(&identity, result, true).await?;
                    }
                    _ => {
                        return Err(SessionError::InterruptProtocol(
                            "invalid drained effect".into(),
                        ))
                    }
                }
            }
            if let Some((_, prepared)) = prepared_pending {
                self.0.settle_prepared_tool(prepared, true).await?;
            }
            Ok(None)
        })
        .catch_unwind()
        .await;
        let finalized = match finalized {
            Ok(result) => result,
            Err(_) => Err(SessionError::Internal("tool settlement panicked".into())),
        };
        let mut cleanup_safe = finalized.is_err();
        if cleanup_safe {
            if let Some((expected, previous_task, expected_task, operations)) = attempted_pause {
                // An inconclusive read or inconsistent state is not evidence that
                // the pause failed. Preserve potentially committed bindings.
                cleanup_safe = match AssertUnwindSafe(async {
                    let ledger = self.0.interrupt_ledger().await?;
                    let task = self.0.interrupt_task(task_id).await?;
                    if ledger.as_ref() == Some(&expected) && task.record == expected_task {
                        self.0.validate_round_ledger(&expected).await?;
                        for operation in operations {
                            if self
                                .0
                                .deps
                                .storage
                                .get_operation(task_id, &operation.idempotency_key)
                                .await?
                                .as_ref()
                                != Some(&operation)
                            {
                                return Ok::<bool, SessionError>(false);
                            }
                        }
                        // Confirmed committed: propagate the original error but
                        // leave the pending interrupt and all its bindings intact.
                        Ok(false)
                    } else {
                        // With an atomic commit and unchanged OCC task, this exact
                        // transition did not commit; an unrelated ledger is not proof.
                        Ok(task.record == previous_task)
                    }
                })
                .catch_unwind()
                .await
                {
                    Ok(Ok(safe)) => safe,
                    _ => false,
                };
            }
        }
        if cleanup_safe {
            // Cleanup is best-effort for every prepared call, including the one
            // whose preparation failed after writing an intent. Never overwrite
            // a successfully settled operation with uncertainty.
            for call in calls.iter().skip(prefix).take(launch - prefix) {
                let key = tool_task::idempotency_key(&call.name, &call.id);
                let _ = AssertUnwindSafe(async {
                    if let Ok(Some(op)) = self.0.deps.storage.get_operation(task_id, &key).await {
                        if matches!(
                            op.status,
                            ToolOperationStatus::Intent | ToolOperationStatus::Running
                        ) {
                            let _ = self.0.mark_effect_uncertain(task_id, &key).await;
                        }
                    }
                })
                .catch_unwind()
                .await;
            }
        }
        finalized
    }

    pub async fn submit(&mut self, request: SubmitRequest) -> Result<SubmitOutcome, SessionError> {
        if let Some(ledger) = self.0.interrupt_ledger().await? {
            self.0.validate_round_ledger(&ledger).await?;
            if !matches!(ledger.authorization, Authorization::Waiting) {
                return Err(SessionError::PendingInterrupt);
            }
            let expected = self.interrupt_request_id(&ledger.candidates[0]).await?;
            let (request_id, kind, payload) = match &request {
                SubmitRequest::Input { request_id, text } => {
                    (request_id, SubmissionKind::Input, json!({"text": text}))
                }
                SubmitRequest::Content {
                    request_id,
                    content,
                } => (
                    request_id,
                    SubmissionKind::Input,
                    json!({"content": content}),
                ),
                _ => return Err(SessionError::PendingInterrupt),
            };
            if *request_id != expected {
                return Err(SessionError::PendingInterrupt);
            }
            let existing = self
                .0
                .deps
                .storage
                .find_submission_by_request(self.0.conversation_id, request_id)
                .await?
                .ok_or_else(|| SessionError::InterruptProtocol("missing submission".into()))?;
            return match compare_submission(&existing.record, kind, &payload, request_id.clone())? {
                duplicate @ SubmitOutcome::Duplicate { .. } => Ok(duplicate),
                _ => Err(SessionError::PendingInterrupt),
            };
        }
        self.0.submit(request).await
    }
    pub async fn cancel_accepted_input(&mut self, request_id: &str) -> Result<bool, SessionError> {
        // A committed pause owns this submission even if cancellation raced
        // with the storage transaction. Never terminalize its continuation.
        if self.0.interrupt_ledger().await?.is_some() {
            return Ok(false);
        }
        self.0.cancel_accepted_input(request_id).await
    }
    pub async fn pending_interrupt(&self) -> Result<Option<PendingInterrupt>, SessionError> {
        let Some(ledger) = self.0.interrupt_ledger().await? else {
            return Ok(None);
        };
        let task = self.0.validate_round_ledger(&ledger).await?;
        if matches!(ledger.authorization, Authorization::Settling) {
            return Err(SessionError::PendingInterrupt);
        }
        if !matches!(ledger.authorization, Authorization::Waiting) {
            if !matches!(task.record.state, TaskState::Running { .. }) {
                return Err(SessionError::InterruptProtocol(
                    "invalid replay task".into(),
                ));
            }
            let id = &ledger.candidates[0].binding.pending.id;
            let receipt = self.0.interrupt_receipt(id).await?;
            match &ledger.authorization {
                Authorization::ResumeReady { generation, .. } => {
                    if *generation != u64::from(task.record.version) {
                        return Err(SessionError::InterruptProtocol(
                            "replay generation mismatch".into(),
                        ));
                    }
                    ledger.begin_replay(&receipt)?;
                }
                Authorization::ReplayInFlight { generation, .. } => {
                    if generation.checked_add(1) != Some(u64::from(task.record.version)) {
                        return Err(SessionError::InterruptProtocol(
                            "replay generation mismatch".into(),
                        ));
                    }
                    ledger.answers_for_replay(&receipt)?;
                }
                Authorization::Waiting | Authorization::Settling => {
                    return Err(SessionError::InterruptProtocol(
                        "invalid authorization".into(),
                    ))
                }
            }
            return Err(SessionError::PendingInterrupt);
        }
        let p = &ledger.candidates[0].binding.pending;
        if !matches!(&task.record.state, TaskState::Waiting { on, policy: WaitPolicy::Any, .. } if *on == vec![format!("interrupt:{}", p.id.0)])
        {
            return Err(SessionError::InterruptProtocol(
                "invalid waiting task".into(),
            ));
        }
        Ok(Some(p.clone()))
    }
    pub async fn run_until_idle_with_supervisor(
        &mut self,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<InterruptDriveOutcome, SessionError> {
        let active = Arc::new(());
        self.0.job_scope = Some((Arc::downgrade(&supervisor), Arc::downgrade(&active)));
        let result = self.run_until_idle(cancel).await;
        self.0.job_scope = None;
        result
    }

    pub async fn run_until_idle(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<InterruptDriveOutcome, SessionError> {
        self.0.cancel = cancel.clone();
        if let Some(ledger) = self.0.interrupt_ledger().await? {
            match ledger.authorization {
                Authorization::Waiting => {
                    self.0.drain_before_candidate().await?;
                    let p = self.pending_interrupt().await?.ok_or_else(|| {
                        SessionError::InterruptProtocol("missing pending interrupt".into())
                    })?;
                    return Ok(InterruptDriveOutcome::Interrupted(p));
                }
                _ => return self.redrive_interrupt(cancel).await,
            }
        }
        let mut turns = 0;
        for _ in 0..1024 {
            if cancel.is_cancelled() {
                return Ok(InterruptDriveOutcome::Settled(RunOutcome {
                    turns_completed: turns,
                    idled: false,
                }));
            }
            match self.0.plan_next().await? {
                WorkItem::Generate {
                    task_id, request, ..
                } => self.0.generate(task_id, request, &cancel).await?,
                WorkItem::ExecuteTool { task_id, .. } => {
                    if let Some(pending) = self.execute_interrupt_round(task_id, &cancel).await? {
                        // Cancellation cannot undo an in-flight commit. Keep the
                        // committed pause, but report cancellation for this run.
                        if cancel.is_cancelled() {
                            return Ok(InterruptDriveOutcome::Settled(RunOutcome {
                                turns_completed: turns,
                                idled: false,
                            }));
                        }
                        return Ok(InterruptDriveOutcome::Interrupted(pending));
                    }
                }
                WorkItem::FinishTurn { task_id } => {
                    self.0.finish_turn(task_id).await?;
                    turns += 1;
                }
                WorkItem::Idle | WorkItem::Sleep { .. } => {
                    self.0.flush_due(self.0.deps.clock.now_ms()).await?;
                    return Ok(InterruptDriveOutcome::Settled(RunOutcome {
                        turns_completed: turns,
                        idled: true,
                    }));
                }
            }
        }
        Err(SessionError::InvalidState(
            "interrupt driver iteration limit".into(),
        ))
    }
    /// Compatibility wrapper. New callers should pass the run's cancellation token.
    pub async fn resume_interrupt(
        &mut self,
        id: InterruptId,
        value: Value,
    ) -> Result<InterruptDriveOutcome, SessionError> {
        self.resume_interrupt_with_cancel(id, value, CancellationToken::new())
            .await
    }

    pub async fn resume_interrupt_with_cancel(
        &mut self,
        id: InterruptId,
        value: Value,
        cancel: CancellationToken,
    ) -> Result<InterruptDriveOutcome, SessionError> {
        self.admit_interrupt(id, value, cancel.clone()).await?;
        self.redrive_interrupt(cancel).await
    }

    /// Commit the answer without starting replay; cancellation leaves a redrivable receipt.
    pub async fn admit_interrupt(
        &mut self,
        id: InterruptId,
        value: Value,
        cancel: CancellationToken,
    ) -> Result<String, SessionError> {
        self.0.cancel = cancel;
        // Reload after an OCC loss: only a committed ready -> in-flight winner
        // can invoke the effect. A receipt is checked before any new admission.
        loop {
            let ledger = self.0.interrupt_ledger().await?;
            let existing = match self.0.interrupt_receipt(&id).await {
                Ok(receipt) => Some(receipt),
                Err(SessionError::InvalidInterrupt(_)) => None,
                Err(error) => return Err(error),
            };
            if let Some(receipt) = existing {
                receipt.validate(&id, &value, &receipt.candidate.binding)?;
                let Some(ledger) = ledger else {
                    return Err(SessionError::InterruptProtocol(
                        "active receipt without ledger".into(),
                    ));
                };
                self.0.validate_round_ledger(&ledger).await?;
                if ledger.candidates.first() != Some(&receipt.candidate) {
                    return Err(SessionError::InterruptProtocol(
                        "active receipt does not match ledger".into(),
                    ));
                }
                return self.interrupt_request_id(&ledger.candidates[0]).await;
            }
            let ledger =
                ledger.ok_or_else(|| SessionError::InvalidInterrupt("unknown interrupt".into()))?;
            let task = self.0.validate_round_ledger(&ledger).await?;
            let candidate = ledger
                .candidates
                .first()
                .ok_or_else(|| SessionError::InvalidInterrupt("unknown interrupt".into()))?;
            if candidate.binding.pending.id != id {
                return Err(SessionError::InvalidInterrupt("unknown interrupt".into()));
            }
            let (ready, receipt) = ledger.accept(
                &id,
                value.clone(),
                u64::from(task.record.version) + 1,
                &candidate.binding,
            )?;
            let mut running = task.record.clone();
            running.version += 1;
            running.state = TaskState::Running {
                checkpoint: task.record.state.checkpoint().cloned(),
                wake: None,
            };
            let batch = ready.batch(&task.record, running, Some(&receipt), "interrupt.answered")?;
            if self
                .0
                .interrupt_try_batch(batch, task.record.version)
                .await?
            {
                return self.interrupt_request_id(candidate).await;
            }
        }
    }

    async fn interrupt_request_id(&self, candidate: &Candidate) -> Result<String, SessionError> {
        self.0
            .review_task_request_id(candidate.binding.pending.task_id)
            .await
    }

    async fn redrive_interrupt(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<InterruptDriveOutcome, SessionError> {
        self.0.cancel = cancel.clone();
        loop {
            let ledger =
                self.0.interrupt_ledger().await?.ok_or_else(|| {
                    SessionError::InterruptProtocol("missing replay ledger".into())
                })?;
            if matches!(ledger.authorization, Authorization::Settling) {
                self.0.drain_held().await?;
                return Box::pin(self.run_until_idle(cancel)).await;
            }
            let task = self.0.validate_round_ledger(&ledger).await?;
            let candidate = &ledger.candidates[0];
            if !matches!(task.record.state, TaskState::Running { .. }) {
                return Err(SessionError::InterruptProtocol(
                    "invalid replay task".into(),
                ));
            }
            let receipt = self
                .0
                .interrupt_receipt(&candidate.binding.pending.id)
                .await?;
            match ledger.authorization {
                Authorization::ReplayInFlight { generation, .. } => {
                    if generation.checked_add(1) != Some(u64::from(task.record.version)) {
                        return Err(SessionError::InterruptProtocol(
                            "replay generation mismatch".into(),
                        ));
                    }
                    ledger.answers_for_replay(&receipt)?;
                    return Err(SessionError::InterruptUncertain);
                }
                Authorization::Waiting | Authorization::Settling => {
                    return Err(SessionError::InterruptProtocol(
                        "replay not admitted".into(),
                    ))
                }
                Authorization::ResumeReady { generation, .. } => {
                    if generation != u64::from(task.record.version) {
                        return Err(SessionError::InterruptProtocol(
                            "replay generation mismatch".into(),
                        ));
                    }
                }
            }
            let inflight = ledger.begin_replay(&receipt)?;
            if cancel.is_cancelled() {
                return Err(SessionError::Cancelled);
            }
            let mut next = task.record.clone();
            next.version += 1;
            let batch =
                inflight.batch(&task.record, next, Some(&receipt), "interrupt.replaying")?;
            if !self
                .0
                .interrupt_try_batch(batch, task.record.version)
                .await?
            {
                continue;
            }
            let answers = inflight.answers_for_replay(&receipt)?;
            let b = &candidate.binding;
            let tool = self
                .0
                .tools
                .get(&b.call.name)
                .ok_or(SessionError::InterruptUnsupported)?;
            let identity = ToolEffectIdentity {
                stored: task,
                call: b.call.clone(),
                round: b.pending.round,
                key: tool_task::idempotency_key(&b.call.name, &b.call.id),
                _replay: b.replay,
            };
            let context = ToolContext {
                session_id: self.0.spec.session_id,
                conversation_id: self.0.conversation_id,
                task_id: b.pending.task_id,
                files: self.0.deps.workspace.files(),
                shell: self.0.deps.workspace.execution(),
                live: self.0.deps.live.clone(),
                cancel: self.0.cancel.clone(),
                now_ms: self.0.deps.clock.now_ms(),
                image_limits: self.0.deps.provider.image_limits(),
                interrupt_port: None,
            };
            let effect = PreparedToolEffect {
                identity,
                tool: Some(tool),
                arguments: b.final_arguments.clone(),
                context,
                interrupt_answers: Some(answers.clone()),
            };
            let identity_for_cancel = effect.identity.clone();
            let started = std::sync::atomic::AtomicBool::new(false);
            let future = async {
                started.store(true, std::sync::atomic::Ordering::Relaxed);
                AssertUnwindSafe(effect.execute()).catch_unwind().await
            };
            tokio::pin!(future);
            let (identity, result) = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    if started.load(std::sync::atomic::Ordering::Relaxed) { let _ = future.await; }
                    self.0.mark_effect_uncertain(b.pending.task_id, &identity_for_cancel.key).await?;
                    return Err(SessionError::InterruptUncertain);
                }
                result = &mut future => match result {
                    Ok(result) => result,
                    Err(_) => {
                        self.0.mark_effect_uncertain(b.pending.task_id, &identity_for_cancel.key).await?;
                        return Err(SessionError::InterruptProtocol("tool effect panicked".into()));
                    }
                },
            };
            if cancel.is_cancelled() {
                self.0
                    .mark_effect_uncertain(b.pending.task_id, &identity.key)
                    .await?;
                return Err(SessionError::InterruptUncertain);
            }
            return match result {
                InvocationResult::Interrupted(latched) => Ok(InterruptDriveOutcome::Interrupted(
                    self.0
                        .pause_interrupt(
                            &identity,
                            b.final_arguments.clone(),
                            answers,
                            latched,
                            Some(receipt),
                        )
                        .await?,
                )),
                InvocationResult::Returned(result) => {
                    match self
                        .0
                        .commit_round_completion(&identity, &receipt, result)
                        .await?
                    {
                        Remaining::Waiting(next) => {
                            self.0.drain_before_candidate().await?;
                            Ok(InterruptDriveOutcome::Interrupted(
                                next.candidates[0].binding.pending.clone(),
                            ))
                        }
                        Remaining::Complete(_) => {
                            self.0.drain_held().await?;
                            Box::pin(self.run_until_idle(cancel)).await
                        }
                    }
                }
                InvocationResult::ProtocolViolation => {
                    self.0
                        .mark_effect_uncertain(b.pending.task_id, &identity.key)
                        .await?;
                    Err(SessionError::InterruptProtocol(
                        "tool did not propagate interrupt or replay diverged".into(),
                    ))
                }
            };
        }
    }
}
