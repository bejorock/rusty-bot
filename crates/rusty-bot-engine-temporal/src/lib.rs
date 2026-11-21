//! Minimal, text-only agent loop using the official Temporal SDK.
//! Storage is injected by the caller; all external I/O runs in Activities.

use std::{sync::Arc, time::Duration};

use rusty_bot_core::{
    ChatRequest, ContentPart, Message, ModelError, ModelProvider, ProviderMessage, StopReason,
    ToolChoice,
};
use serde::{Deserialize, Serialize};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokio_util::sync::CancellationToken;

mod client;
pub use client::{Engine, EngineError, EngineOptions, EngineResult, ExecutionStatus, Session};
mod streaming;
use streaming::ProgressTarget;
pub use streaming::{ActivityInput, PartialReply};

pub use rusty_bot_session::{
    BlockStart, ChatHistory, ChatMessage, ChatPromptTemplate, ContentAnnotation, ContentBlock,
    ConversationMessage, FinishReason, GenerationMetadata, HistoryEntry, HistoryError, MediaSource,
    MessageAccumulator, MessageStatus, MessageStreamEvent, MessageStreamPayload, PromptError,
    PromptMessage, PromptRole, PromptTemplate, PromptVariables, ReplayState, SessionMessage,
    SessionSnapshot, SessionStorage, TokenUsage, ToolInvocation,
};
mod persistence;
pub use persistence::{ChangeInput, SessionActivities};
use rusty_bot_session::{SessionChange, StoredSession};

/// Between-turn safety thresholds; the server's suggestion is always honored.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloverPolicy {
    pub max_turns: u64,
    pub max_history_events: u32,
}
impl Default for RolloverPolicy {
    fn default() -> Self {
        Self {
            max_turns: 100,
            max_history_events: 8_000,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionInput {
    pub session_id: String,
    pub idle_timeout_ms: u64,
    #[serde(default)]
    pub rollover: RolloverPolicy,
    /// Absolute Workflow time, carried across rollover without renewing inactivity.
    #[serde(default)]
    pub idle_deadline_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnInput {
    pub request_id: String,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnResult {
    /// Internal admission response: retry the same request on the successor run.
    RollingOver,
    Completed(String),
    Rejected(String),
    ModelFailed(ModelError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenerateInput {
    pub model: String,
    pub messages: Vec<ChatMessage>,
}

/// Provider clients (and credentials) stay in the Activity worker, not Workflow state.
pub struct LlmActivities {
    provider: Arc<dyn ModelProvider>,
    storage: Option<Arc<dyn SessionStorage>>,
}

impl LlmActivities {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        Self {
            provider,
            storage: None,
        }
    }

    pub fn with_storage(
        provider: Arc<dyn ModelProvider>,
        storage: Arc<dyn SessionStorage>,
    ) -> Self {
        Self {
            provider,
            storage: Some(storage),
        }
    }

    #[cfg(test)]
    async fn generate_text(
        &self,
        input: GenerateInput,
        cancel: CancellationToken,
    ) -> Result<String, ModelError> {
        self.generate_with_progress(input, cancel, None).await
    }

    async fn generate_with_progress(
        &self,
        input: GenerateInput,
        cancel: CancellationToken,
        progress: Option<ProgressTarget>,
    ) -> Result<String, ModelError> {
        let request = ChatRequest {
            model: input.model,
            messages: input
                .messages
                .into_iter()
                .map(|message| match message {
                    ChatMessage::User(content) => ProviderMessage::User { content },
                    ChatMessage::Assistant(content) => ProviderMessage::Assistant {
                        content: Some(content),
                        tool_calls: vec![],
                    },
                })
                .collect(),
            tools: vec![],
            tool_choice: ToolChoice::None,
            temperature: None,
            max_tokens: None,
            metadata: serde_json::Value::Null,
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let generation = self.provider.stream(request, tx, cancel.clone());
        let drain = async {
            if let Some(progress) = progress {
                progress.forward(rx).await
            } else {
                while rx.recv().await.is_some() {}
                Ok(())
            }
        };
        let turn = match tokio::try_join!(generation, drain) {
            Ok((turn, ())) => turn,
            Err(error) => {
                cancel.cancel();
                return Err(error);
            }
        };
        match turn.message {
            Message::Assistant {
                content,
                tool_calls,
                ..
            } if tool_calls.is_empty()
                && turn.tool_calls.is_empty()
                && matches!(turn.stop, StopReason::Stop | StopReason::Length)
                && content
                    .iter()
                    .all(|p| matches!(p, ContentPart::Text { .. })) =>
            {
                Ok(Message::text_content(&content))
            }
            _ => Err(ModelError::unsupported(
                "Temporal prototype supports text replies only",
            )),
        }
    }
}

#[activities]
impl LlmActivities {
    #[activity]
    pub async fn generate(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: ActivityInput,
    ) -> Result<Result<String, ModelError>, ActivityError> {
        let info = ctx.info();
        let progress = ProgressTarget {
            client: ctx.client(),
            execution: temporalio_client::WorkflowExecutionInfo::builder()
                .namespace(info.namespace.clone())
                .workflow_id(info.workflow_id.clone().expect("Workflow Activity"))
                .run_id(info.workflow_run_id.clone().expect("Workflow run"))
                .build(),
            turn_id: input.turn_id,
            storage: self.storage.clone().expect("session storage registered"),
            session_id: input.session_id,
            run_id: info.workflow_run_id.clone().expect("Workflow run"),
        };
        let cancel = CancellationToken::new();
        tokio::select! {
            result = self.generate_with_progress(input.request, cancel.clone(), Some(progress)) => Ok(result),
            _ = ctx.cancelled() => {
                cancel.cancel();
                Err(ActivityError::Cancelled { details: None })
            }
        }
    }
}

#[workflow]
pub struct ChatSessionWorkflow {
    input: SessionInput,
    model: String,
    ready: bool,
    generation: u64,
    snapshot: SessionSnapshot,
    partial_sequence: u64,
    rolling_over: bool,
    settled_turns: u64,
    has_initial_history: bool,
}

#[workflow_methods]
impl ChatSessionWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, input: SessionInput) -> Self {
        Self {
            input,
            model: String::new(),
            ready: false,
            generation: 0,
            snapshot: SessionSnapshot::default(),
            partial_sequence: 0,
            rolling_over: false,
            settled_turns: 0,
            has_initial_history: false,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<SessionSnapshot> {
        use futures::FutureExt;
        let mut input = ctx.state(|s| s.input.clone());
        let loaded = ctx
            .execute_activity(
                SessionActivities::load,
                input.session_id.clone(),
                storage_options(),
            )
            .await?;
        ctx.state_mut(|s| {
            s.model = loaded.config.model.clone();
            s.has_initial_history = loaded.requires_history_translation();
            let closing = s.snapshot.closing;
            s.snapshot = loaded.snapshot();
            s.snapshot.closing |= closing;
            s.ready = true;
        });
        let mut observed_generation = ctx.state(|s| s.generation);
        let mut deadline = input
            .idle_deadline_ms
            .unwrap_or_else(|| workflow_ms(ctx).saturating_add(input.idle_timeout_ms));
        loop {
            if ctx.state(|s| s.snapshot.closing) {
                break;
            }
            ctx.wait_condition(|s| !s.snapshot.generating || s.snapshot.closing)
                .await?;
            if ctx.state(|s| s.snapshot.closing) {
                break;
            }
            let generation = ctx.state(|s| s.generation);
            if generation != observed_generation {
                deadline = workflow_ms(ctx).saturating_add(input.idle_timeout_ms);
                observed_generation = generation;
            }
            // Do not turn a history rollover into an inactivity extension.
            if workflow_ms(ctx) >= deadline {
                ctx.state_mut(|s| s.snapshot.closing = true);
                break;
            }
            if ctx.state(|s| {
                s.settled_turns > 0
                    && (ctx.continue_as_new_suggested()
                        || s.settled_turns >= input.rollover.max_turns
                        || ctx.history_length() >= input.rollover.max_history_events)
            }) {
                ctx.state_mut(|s| s.rolling_over = true);
                // New/queued Updates are rejected for retry; accepted work is never abandoned.
                ctx.wait_condition(|_| ctx.all_handlers_finished()).await?;
                if !ctx.state(|s| s.snapshot.closing) {
                    input.idle_deadline_ms = Some(deadline);
                    match ctx.continue_as_new(input, Default::default())? {}
                }
                break;
            }
            let expired = temporalio_workflow::select! {
                _ = ctx.wait_condition(move |s| s.snapshot.closing || s.generation != generation).fuse() => false,
                _ = ctx.timer(Duration::from_millis(deadline.saturating_sub(workflow_ms(ctx)))) => true,
            };
            if expired && ctx.state(|s| !s.snapshot.generating && s.generation == generation) {
                ctx.state_mut(|s| s.snapshot.closing = true);
            }
        }
        ctx.wait_condition(|_| ctx.all_handlers_finished()).await?;
        let closed = ctx
            .execute_activity(
                SessionActivities::change,
                ChangeInput {
                    session_id: input.session_id,
                    change: SessionChange::Close {
                        run_id: ctx.info().run_id().into(),
                    },
                },
                storage_options(),
            )
            .await?;
        Ok(closed.snapshot())
    }

    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> SessionSnapshot {
        self.snapshot.clone()
    }

    #[signal]
    pub fn close(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.snapshot.closing = true;
    }

    #[signal]
    pub fn partial_reply(&mut self, _ctx: &mut SyncWorkflowContext<Self>, partial: PartialReply) {
        if partial.sequence <= self.partial_sequence {
            return;
        }
        if let Some(message) = self.snapshot.messages.last_mut()
            && message.turn_id == partial.turn_id
            && message.status == MessageStatus::Generating
        {
            message.message = ChatMessage::Assistant(partial.text);
            self.partial_sequence = partial.sequence;
        }
    }

    #[update]
    pub async fn send_message(
        ctx: &mut WorkflowContext<Self>,
        text: String,
    ) -> Result<TurnResult, Box<dyn std::error::Error + Send + Sync>> {
        let request_id = ctx.uuid4();
        Self::admit_request(ctx, TurnInput { request_id, text }).await
    }

    #[update]
    pub async fn send_request(
        ctx: &mut WorkflowContext<Self>,
        request: TurnInput,
    ) -> Result<TurnResult, Box<dyn std::error::Error + Send + Sync>> {
        Self::admit_request(ctx, request).await
    }

    async fn admit_request(
        ctx: &mut WorkflowContext<Self>,
        request: TurnInput,
    ) -> Result<TurnResult, Box<dyn std::error::Error + Send + Sync>> {
        if request.text.trim().is_empty() || request.request_id.trim().is_empty() {
            return Ok(TurnResult::Rejected(
                "message and request ID must not be blank".into(),
            ));
        }
        ctx.wait_condition(|s| s.ready && !s.snapshot.generating)
            .await?;
        if ctx.state(|s| s.rolling_over && !s.snapshot.closing) {
            return Ok(TurnResult::RollingOver);
        }
        if ctx.state(|s| s.snapshot.closing) {
            return Ok(TurnResult::Rejected(
                "session is closing; resume to start a new execution".into(),
            ));
        }
        if ctx.state(|s| s.has_initial_history) {
            return Ok(TurnResult::Rejected("provider-neutral history is stored, but provider translation is not implemented yet".into()));
        }
        ctx.state_mut(|s| {
            s.snapshot.generating = true;
            s.generation += 1;
        });
        let outcome = Self::perform_turn(ctx, request).await;
        ctx.state_mut(|s| {
            s.snapshot.generating = false;
            s.generation += 1;
            s.settled_turns += 1;
        });
        outcome
    }

    async fn perform_turn(
        ctx: &mut WorkflowContext<Self>,
        request: TurnInput,
    ) -> Result<TurnResult, Box<dyn std::error::Error + Send + Sync>> {
        let session_id = ctx.state(|s| s.input.session_id.clone());
        let run_id = ctx.info().run_id().to_string();
        let stored = ctx
            .execute_activity(
                SessionActivities::change,
                ChangeInput {
                    session_id: session_id.clone(),
                    change: SessionChange::Begin {
                        run_id: run_id.clone(),
                        request_id: request.request_id.clone(),
                        text: request.text.clone(),
                    },
                },
                storage_options(),
            )
            .await?;
        let turn = stored
            .turns
            .iter()
            .find(|t| t.request_id == request.request_id)
            .expect("persisted request");
        if let Some(result) = &turn.result {
            return Ok(match result {
                Ok(reply) => TurnResult::Completed(reply.clone()),
                Err(error) => TurnResult::Rejected(error.clone()),
            });
        }
        let turn_id = turn.id;
        let mut messages = stored.completed_context();
        messages.push(ChatMessage::User(request.text));
        ctx.state_mut(|s| {
            let closing = s.snapshot.closing;
            s.snapshot = stored.snapshot();
            s.snapshot.closing |= closing;
            s.partial_sequence = 0;
        });
        let result = ctx
            .execute_activity(
                LlmActivities::generate,
                ActivityInput {
                    session_id: session_id.clone(),
                    turn_id,
                    request: GenerateInput {
                        model: ctx.state(|s| s.model.clone()),
                        messages,
                    },
                },
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(60))
                    .retry_policy(
                        temporalio_common::RetryPolicy::builder()
                            .maximum_attempts(1)
                            .build(),
                    )
                    .build(),
            )
            .await;
        let persisted_result = match &result {
            Ok(Ok(reply)) => Ok(reply.clone()),
            Ok(Err(error)) => Err(error.message.clone()),
            Err(error) => Err(format!("model Activity failed: {error}")),
        };
        let saved: StoredSession = ctx
            .execute_activity(
                SessionActivities::change,
                ChangeInput {
                    session_id,
                    change: SessionChange::Finish {
                        run_id,
                        turn_id,
                        result: persisted_result,
                    },
                },
                storage_options(),
            )
            .await?;
        ctx.state_mut(|s| {
            let closing = s.snapshot.closing;
            s.snapshot = saved.snapshot();
            s.snapshot.closing |= closing;
        });
        match result? {
            Ok(reply) => Ok(TurnResult::Completed(reply)),
            Err(error) => Ok(TurnResult::ModelFailed(error)),
        }
    }
}

fn workflow_ms(ctx: &WorkflowContext<ChatSessionWorkflow>) -> u64 {
    ctx.workflow_time()
        .expect("Workflow time")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch Workflow time")
        .as_millis()
        .try_into()
        .expect("representable Workflow time")
}

fn storage_options() -> temporalio_sdk::ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(15))
        .retry_policy(
            // Persistence is an idempotent commit boundary, not a disposable attempt.
            // Keep the recorded model result pending through backend outages. Conflict
            // and NotFound are explicitly non-retryable in SessionActivities.
            temporalio_common::RetryPolicy::builder()
                .initial_interval(Duration::from_millis(250))
                .maximum_interval(Duration::from_secs(5))
                .maximum_attempts(0)
                .build(),
        )
        .build()
}

#[cfg(test)]
mod closing_tests;
#[cfg(test)]
mod concurrency_tests;
#[cfg(test)]
mod disconnect_tests;
#[cfg(test)]
mod draft_persistence_tests;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod persistence_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod response_history_tests;
#[cfg(test)]
mod rollover_boundary_tests;
#[cfg(test)]
mod rollover_postgres_tests;
#[cfg(test)]
mod rollover_tests;
#[cfg(test)]
mod streaming_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
