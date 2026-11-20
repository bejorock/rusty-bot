//! Session engine: the only code that writes session state.
//!
//! A turn is advanced by committing durable intent, performing external
//! effects outside any transaction, then committing the outcome. See
//! `.pi-workflow/contracts/veronica-session-engine-contract.md`.

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};

use rusty_bot_core::error::redact_secrets;
use rusty_bot_core::{
    entry_kind, event_kind, Clock, CommitBatch, CommitContext, CommitReceipt, ConversationId,
    ConversationRecord, DocumentAddress, DocumentScope, DocumentWrite, EntryId, EntryRecord,
    LiveFrame, LiveKind, LiveSink, Message, ModelError, ModelErrorKind, ModelProvider,
    ProviderMessage, ReplayPolicy, SessionError, SessionEventDraft, SessionId, SessionRecord,
    Storage, StoredTask, SubmissionId, SubmissionKind, SubmissionRecord, SubmissionStatus, TaskId,
    TaskOutcome, TaskQuery, TaskRecord, TaskState, Tool, ToolCall, ToolCheckpoint, ToolChoice,
    ToolContext, ToolOperationRecord, ToolOperationStatus, ToolOutput, Workspace, Write,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::deferred::DeferredBuffer;
use crate::interrupt_port::{InvocationInterruptPort, InvocationResult};
use crate::prompt;
use crate::registry::{Registry, ResolvedExtensions, ResolvedToolSet};
use crate::scheduler::TURN_TASK_KIND;
use crate::tool_task::{self, RecoveryDecision};
use crate::transaction::{Transaction, TxError};

#[path = "session/interrupt_driver.rs"]
mod interrupt_driver;
pub use interrupt_driver::{InterruptDriveOutcome, SdkInterruptDriver};

impl From<TxError> for SessionError {
    fn from(error: TxError) -> Self {
        match error {
            TxError::Storage(error) => SessionError::Storage(error),
            other => SessionError::Internal(other.to_string()),
        }
    }
}

fn ask_user_spec() -> rusty_bot_core::ToolSpec {
    rusty_bot_core::ToolSpec {
        name: "ask_user".into(),
        description: "Ask the user a question".into(),
        parameters: json!({"type":"object", "properties":{"question":{"type":"string", "minLength":1, "maxLength":4096}}, "required":["question"], "additionalProperties":false}),
    }
}

fn valid_question(arguments: &Value) -> bool {
    jsonschema::validator_for(&ask_user_spec().parameters).is_ok_and(|v| v.is_valid(arguments))
        && arguments
            .get("question")
            .and_then(Value::as_str)
            .is_some_and(|q| !q.trim().is_empty() && q.len() <= 4096)
}

impl SdkReviewDriver {
    pub async fn submit(&mut self, request: SubmitRequest) -> Result<SubmitOutcome, SessionError> {
        self.0.check_current_review_policy().await?;
        self.0.submit(request).await
    }

    pub async fn cancel_accepted_input(&mut self, request_id: &str) -> Result<bool, SessionError> {
        self.0.cancel_accepted_input(request_id).await
    }

    pub async fn pending_review(
        &self,
    ) -> Result<Option<rusty_bot_core::PendingReview>, SessionError> {
        self.0.check_current_review_policy().await?;
        self.0.pending_review().await
    }

    /// Original submission associated with a pending or accepted review.
    pub async fn review_request_id(
        &self,
        id: &rusty_bot_core::ReviewId,
    ) -> Result<String, SessionError> {
        let pending = if let Some(pending) = self.0.pending_review().await? {
            if &pending.id == id {
                Some(pending)
            } else {
                None
            }
        } else {
            None
        };
        let pending = match pending {
            Some(pending) => pending,
            None => {
                let doc = self
                    .0
                    .deps
                    .storage
                    .get_document(
                        &crate::review_state::receipt_address(self.0.spec.session_id, id),
                        rusty_bot_core::DocumentPoint::Current,
                    )
                    .await?
                    .ok_or_else(|| SessionError::InvalidReview("unknown review ID".into()))?;
                let receipt: crate::review_state::ReceiptRecord = serde_json::from_value(doc.value)
                    .map_err(|_| SessionError::InvalidState("invalid review receipt".into()))?;
                if receipt.version != 1
                    || &receipt.answer.id != id
                    || &receipt.pending.pending.id != id
                {
                    return Err(SessionError::InvalidState("invalid review receipt".into()));
                }
                receipt.check_integrity()?;
                receipt.pending.pending
            }
        };
        self.0.review_task_request_id(pending.task_id).await
    }

    pub async fn run_until_idle_with_supervisor(
        &mut self,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.0.check_current_review_policy().await?;
        if let Some(pending) = self.0.pending_review().await? {
            return Ok(ReviewDriveOutcome::Paused(pending));
        }
        if self.0.active_review_receipt().await?.is_some() {
            return Err(SessionError::PendingReview);
        }
        self.drive_with_supervisor(cancel, supervisor).await
    }

    pub async fn admit_review(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
    ) -> Result<(), SessionError> {
        self.0.check_current_review_policy().await?;
        self.0.admit_review(answer).await
    }

    pub async fn admit_review_with_cancel(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
        cancel: CancellationToken,
    ) -> Result<(), SessionError> {
        self.0.set_cancel(cancel);
        self.admit_review(answer).await
    }

    pub async fn admit_review_with_supervisor(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<(), SessionError> {
        let active = Arc::new(());
        self.0.job_scope = Some((Arc::downgrade(&supervisor), Arc::downgrade(&active)));
        let result = self.admit_review_with_cancel(answer, cancel).await;
        drop(active);
        self.0.job_scope = None;
        result
    }

    pub async fn continue_review_with_supervisor(
        &mut self,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.drive_with_supervisor(cancel, supervisor).await
    }

    pub async fn resume_review_with_supervisor(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.admit_review_with_supervisor(answer, cancel.clone(), supervisor.clone())
            .await?;
        self.drive_with_supervisor(cancel, supervisor).await
    }

    async fn drive_with_supervisor(
        &mut self,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        let result = self
            .0
            .run_until_idle_with_supervisor(cancel, supervisor)
            .await?;
        if let Some(pending) = self.0.paused_review.take() {
            Ok(ReviewDriveOutcome::Paused(pending))
        } else {
            Ok(ReviewDriveOutcome::Settled(result))
        }
    }

    pub async fn run_until_idle(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.0.check_current_review_policy().await?;
        if let Some(pending) = self.0.pending_review().await? {
            return Ok(ReviewDriveOutcome::Paused(pending));
        }
        if self.0.active_review_receipt().await?.is_some() {
            return Err(SessionError::InvalidState(
                "accepted review requires identical answer to continue".into(),
            ));
        }
        self.drive(cancel).await
    }

    /// Admit an answer and continue the original task, never submitting a new prompt.
    pub async fn resume_review(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.resume_review_with_cancel(answer, CancellationToken::new())
            .await
    }

    pub async fn resume_review_with_cancel(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
        cancel: CancellationToken,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        self.admit_review_with_cancel(answer, cancel.clone())
            .await?;
        self.drive(cancel).await
    }

    async fn drive(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<ReviewDriveOutcome, SessionError> {
        let result = self.0.run_until_idle(cancel).await?;
        if let Some(pending) = self.0.paused_review.take() {
            Ok(ReviewDriveOutcome::Paused(pending))
        } else {
            Ok(ReviewDriveOutcome::Settled(result))
        }
    }
}

fn validate_round_prefix(
    entries: &[rusty_bot_core::StoredEntry],
    task_id: TaskId,
    round: u64,
    calls: &[ToolCall],
) -> Result<usize, SessionError> {
    let mut ids = HashSet::new();
    if calls
        .iter()
        .any(|call| call.id.is_empty() || call.name.is_empty() || !ids.insert(&call.id))
    {
        return Err(SessionError::InvalidState(
            "invalid or duplicate round call ID".into(),
        ));
    }
    let results: Vec<_> = entries
        .iter()
        .filter(|entry| {
            entry.record.by_task_id == Some(task_id)
                && entry.record.kind == entry_kind::TOOL_RESULT
                && entry
                    .record
                    .data
                    .get("round")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    == round
        })
        .collect();
    if results.len() > calls.len()
        || results.iter().zip(calls).any(|(entry, call)| {
            entry
                .record
                .data
                .get("tool_call_id")
                .and_then(Value::as_str)
                != Some(call.id.as_str())
                || entry.record.data.get("tool_name").and_then(Value::as_str)
                    != Some(call.name.as_str())
        })
    {
        return Err(SessionError::InvalidState(
            "tool results are not a valid round prefix".into(),
        ));
    }
    Ok(results.len())
}

fn finalize_interrupted_task(record: &mut TaskRecord, message: &str, now: i64) {
    record.state = TaskState::Terminal {
        outcome: TaskOutcome::Failed {
            error: message.to_string(),
            result: None,
        },
    };
    record.ended_at = Some(now);
}

// An effect owns everything it needs and never borrows the session engine.
// The caller is responsible for polling it to completion before settling.
#[derive(Clone)]
struct ToolEffectIdentity {
    stored: StoredTask,
    call: ToolCall,
    round: u64,
    key: String,
    _replay: ReplayPolicy,
}

struct ReviewCandidate {
    ordinal: usize,
    call: ToolCall,
    final_call: ToolCall,
    allowed: std::collections::BTreeSet<rusty_bot_core::DecisionKind>,
}

struct PreparedToolEffect {
    identity: ToolEffectIdentity,
    tool: Option<Arc<dyn Tool>>,
    arguments: Value,
    context: ToolContext,
    // None means an unsupported host; Some holds positional replay answers.
    interrupt_answers: Option<Vec<Value>>,
}

impl PreparedToolEffect {
    async fn execute(mut self) -> (ToolEffectIdentity, InvocationResult) {
        let port = self
            .interrupt_answers
            .map(|answers| Arc::new(InvocationInterruptPort::new(answers)));
        if let Some(port) = &port {
            self.context.interrupt_port = Some(port.clone());
        }
        let result = match self.tool {
            Some(tool) => tool.execute(self.arguments, &self.context).await,
            None => Ok(ToolOutput::error(
                format!("unknown tool: {}", self.identity.call.name),
                "unknown_tool",
            )),
        };
        let result = match port {
            Some(port) => port.finish(result),
            None => InvocationResult::Returned(result),
        };
        (self.identity, result)
    }
}

enum RoundResult {
    Prepared(PreparedTool),
    Effect(ToolEffectIdentity, InvocationResult),
    Panicked(ToolEffectIdentity),
}

impl RoundResult {
    // Until a pause ledger exists, even a valid candidate must fail closed.
    fn failure(&self) -> Option<SessionError> {
        match self {
            Self::Panicked(_) => Some(SessionError::Internal("tool effect panicked".into())),
            Self::Effect(_, InvocationResult::Interrupted(_)) => {
                Some(SessionError::InterruptUnsupported)
            }
            Self::Effect(_, InvocationResult::ProtocolViolation) => {
                Some(SessionError::InterruptProtocol(
                    "tool did not propagate interrupt or replay diverged".into(),
                ))
            }
            _ => None,
        }
    }
}

enum PreparedTool {
    Effect(PreparedToolEffect),
    Synthetic {
        stored: StoredTask,
        call: ToolCall,
        round: u64,
        output: ToolOutput,
    },
    Reused {
        stored: StoredTask,
        call: ToolCall,
        round: u64,
        output: ToolOutput,
        status: ToolOperationStatus,
    },
    Interrupted {
        stored: StoredTask,
        call: ToolCall,
        round: u64,
        message: String,
    },
    Review {
        call: ToolCall,
        final_call: ToolCall,
        allowed: std::collections::BTreeSet<rusty_bot_core::DecisionKind>,
    },
    AlreadySettled(ToolRunOutcome),
}

#[derive(Clone)]
pub struct SessionDeps {
    pub storage: Arc<dyn Storage>,
    pub workspace: Arc<dyn Workspace>,
    pub provider: Arc<dyn ModelProvider>,
    pub registry: Arc<Registry>,
    pub clock: Arc<dyn Clock>,
    pub live: Arc<dyn LiveSink>,
    pub progress: ProgressConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionConfig {
    pub config_version: u32,
    pub model: ModelSelection,
    pub thinking_level: ThinkingLevel,
    pub instructions: Option<String>,
    pub tools: ToolFilter,
    pub model_tools: ToolFilter,
    pub extensions: ExtensionSelection,
    pub compaction: CompactionConfig,
    pub progress: ProgressConfig,
    pub limits: RunLimits,
}

impl SessionConfig {
    /// Validate all selections against the explicitly injected provider and registry.
    pub fn validate_for_provider(
        &self,
        registry: &Registry,
        provider_id: &str,
    ) -> Result<(), String> {
        if self.config_version != 1
            || provider_id.trim().is_empty()
            || self.model.provider != provider_id
            || self.model.model_id.trim().is_empty()
        {
            return Err("unsupported model or configuration".into());
        }
        let extensions = registry.extension_names();
        let names = match &self.extensions {
            ExtensionSelection::All => Vec::new(),
            ExtensionSelection::Only(names) => names.iter().collect(),
            ExtensionSelection::AddRemove { add, remove } => add.iter().chain(remove).collect(),
        };
        if names.iter().any(|name| !extensions.contains(name)) {
            return Err("unknown extension".into());
        }
        let resolved = registry
            .resolve(&self.extensions)
            .map_err(|_| "invalid extensions")?;
        // AddRemove may add tools from any installed extension. Filters may
        // refer to installed but currently disabled tools.
        let all = registry
            .resolve(&ExtensionSelection::All)
            .map_err(|_| "invalid extensions")?;
        let tool_names: Vec<String> = all.tools.iter().map(|tool| tool.spec().name).collect();
        for filter in [&self.tools, &self.model_tools] {
            let names: Vec<&String> = match filter {
                ToolFilter::All => Vec::new(),
                ToolFilter::Only(names) => names.iter().collect(),
                ToolFilter::AddRemove { add, remove } => add.iter().chain(remove).collect(),
            };
            if names.iter().any(|name| !tool_names.contains(name)) {
                return Err("unknown tool".into());
            }
        }
        let _ = resolved;
        Ok(())
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            config_version: 1,
            model: ModelSelection::default(),
            thinking_level: ThinkingLevel::Off,
            instructions: None,
            tools: ToolFilter::All,
            model_tools: ToolFilter::All,
            extensions: ExtensionSelection::All,
            compaction: CompactionConfig::default(),
            progress: ProgressConfig::default(),
            limits: RunLimits::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelSelection {
    pub provider: String,
    pub model_id: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    #[default]
    Off,
    Minimal,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolFilter {
    #[default]
    All,
    Only(Vec<String>),
    AddRemove {
        add: Vec<String>,
        remove: Vec<String>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionSelection {
    #[default]
    All,
    Only(Vec<String>),
    AddRemove {
        add: Vec<String>,
        remove: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CompactionConfig {
    pub reserve_tokens: u32,
    pub keep_recent_tokens: u32,
    pub background_tokens: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            reserve_tokens: 16_384,
            keep_recent_tokens: 20_000,
            background_tokens: 32_768,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProgressConfig {
    pub partial_interval_ms: u64,
    pub output_interval_ms: u64,
    pub persist_within_ms: u64,
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            partial_interval_ms: 100,
            output_interval_ms: 100,
            persist_within_ms: 1_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunLimits {
    pub max_restarts: u32,
    pub max_tool_rounds: u32,
    pub turn_timeout_ms: Option<u64>,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            max_tool_rounds: 32,
            turn_timeout_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HookSnapshot {
    pub version: u32,
    pub descriptors: Vec<rusty_bot_core::HookDescriptor>,
    pub yield_limit: u32,
}

fn hook_identities_match(
    saved: &[rusty_bot_core::HookDescriptor],
    installed: &[crate::registry::ResolvedHook],
) -> bool {
    saved.len() == installed.len()
        && saved.iter().zip(installed).all(|(a, b)| {
            let b = &b.descriptor;
            a.extension == b.extension
                && a.extension_version == b.extension_version
                && a.hook == b.hook
                && a.hook_version == b.hook_version
                && a.points == b.points
        })
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionSpec {
    pub session_id: SessionId,
    pub config: SessionConfig,
    pub registry_version: String,
    pub extension_versions: BTreeMap<String, String>,
    pub conversation_id: Option<ConversationId>,
    /// Session-local selection captured at creation; never read from shared mutable hooks.
    pub hook_descriptors: Vec<rusty_bot_core::HookDescriptor>,
    pub hook_yield_limit: u32,
    /// The SDK may inspect hook-bearing sessions; execution remains disabled until dispatch is implemented.
    pub allow_hook_snapshot: bool,
    /// Persist review policy for a future authorized driver; manual execution remains blocked.
    pub review_enabled: bool,
}

impl SessionSpec {
    pub fn new(session_id: SessionId, config: SessionConfig) -> Self {
        Self {
            session_id,
            config,
            registry_version: String::new(),
            extension_versions: BTreeMap::new(),
            conversation_id: None,
            hook_descriptors: Vec::new(),
            hook_yield_limit: 8,
            allow_hook_snapshot: false,
            review_enabled: false,
        }
    }

    /// Capture the registry version/selection at open time.
    pub fn for_registry(
        session_id: SessionId,
        config: SessionConfig,
        registry: &Registry,
    ) -> Result<Self, SessionError> {
        let resolved = registry.resolve(&config.extensions)?;
        Ok(Self {
            session_id,
            config,
            registry_version: registry.version().to_string(),
            extension_versions: resolved.versions,
            conversation_id: None,
            hook_descriptors: resolved
                .hooks
                .into_iter()
                .map(|hook| hook.descriptor)
                .collect(),
            hook_yield_limit: 8,
            allow_hook_snapshot: false,
            review_enabled: false,
        })
    }

    pub fn with_conversation(mut self, conversation_id: ConversationId) -> Self {
        self.conversation_id = Some(conversation_id);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubmitRequest {
    Input {
        request_id: String,
        text: String,
    },
    Content {
        request_id: String,
        content: Vec<rusty_bot_core::ContentPart>,
    },
    Write {
        request_id: String,
        data: Value,
    },
    Continue {
        request_id: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubmitOutcome {
    Accepted { request_id: String },
    Duplicate { request_id: String },
    Rejected { reason: RejectReason },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    RequestIdReusedForDifferentKind,
    RequestIdReusedForDifferentPayload,
    EmptyInput,
    BusySteeringUnsupported,
    InvalidRequest(String),
}

#[derive(Clone, Debug)]
pub enum WorkItem {
    Generate {
        task_id: TaskId,
        request: rusty_bot_core::HistoryRequest,
        attempt: u32,
    },
    ExecuteTool {
        task_id: TaskId,
        call: ToolCall,
        tool_name: String,
        replay: ReplayPolicy,
        idempotency_key: String,
        attempt: u32,
    },
    FinishTurn {
        task_id: TaskId,
    },
    Sleep {
        task_id: TaskId,
        until_ms: i64,
    },
    Idle,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunOutcome {
    pub turns_completed: u32,
    pub idled: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ToolRunOutcome {
    pub is_error: bool,
    pub interrupted: bool,
}

pub struct SessionEngine {
    deps: SessionDeps,
    spec: SessionSpec,
    config: SessionConfig,
    conversation_id: ConversationId,
    resolved: ResolvedExtensions,
    tools: ResolvedToolSet,
    deferred: DeferredBuffer,
    queued: Vec<SubmissionId>,
    cancel: CancellationToken,
    reporter: Option<Arc<dyn rusty_bot_core::HookReporter>>,
    compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>,
    max_parallel_tools: NonZeroUsize,
    job_policy: rusty_bot_core::HookJobPolicy,
    job_scope: Option<(
        std::sync::Weak<rusty_bot_core::HookJobSupervisor>,
        std::sync::Weak<()>,
    )>,
    pending_text: String,
    pending_thinking: String,
    last_partial_at: Option<i64>,
    engine_attested_task: Option<TaskId>,
    sdk_review_driver: bool,
    // Private seam for a future driver that can durably account for pauses.
    interrupt_effect_driver: bool,
    paused_review: Option<rusty_bot_core::PendingReview>,
}

/// Explicitly marked trusted SDK review-driver boundary.
pub struct SdkReviewDriver(SessionEngine);

#[derive(Clone, Debug, PartialEq)]
pub enum ReviewDriveOutcome {
    Settled(RunOutcome),
    Paused(rusty_bot_core::PendingReview),
}

impl SessionEngine {
    pub async fn open(deps: SessionDeps, mut spec: SessionSpec) -> Result<Self, SessionError> {
        if spec.config.config_version != 1 {
            return Err(SessionError::UnsupportedConfigVersion(
                spec.config.config_version,
            ));
        }
        if spec.registry_version.is_empty() {
            return Err(SessionError::InvalidConfig(
                "SessionSpec.registry_version is required; build the spec with SessionSpec::for_registry"
                    .into(),
            ));
        }
        if spec.registry_version != deps.registry.version() {
            return Err(SessionError::InvalidConfig(format!(
                "SessionSpec.registry_version {} does not match registry {}",
                spec.registry_version,
                deps.registry.version()
            )));
        }

        let existing = deps.storage.get_session(spec.session_id).await?;
        let reopening = existing.is_some();
        let (config, conversation_id, _hooks_selected) = match existing {
            Some(record) => {
                // The session's workspace is caller-selected and durable. A
                // worker must not silently continue a session's history against
                // a different directory or host.
                let expected = format!("{}@{}", record.workspace_key, record.host_id);
                let actual = format!("{}@{}", deps.workspace.key().0, deps.workspace.host_id());
                if record.workspace_key != deps.workspace.key().0
                    || record.host_id != deps.workspace.host_id()
                {
                    return Err(SessionError::WorkspaceMismatch { expected, actual });
                }
                if let Some(stored_root) = record.workspace.get("root").and_then(Value::as_str) {
                    if std::path::Path::new(stored_root) != deps.workspace.root() {
                        return Err(SessionError::WorkspaceMismatch {
                            expected: format!("{expected} root {stored_root}"),
                            actual: format!("{actual} root {}", deps.workspace.root().display()),
                        });
                    }
                }
                let mut stored_config = record.config.clone();
                let stored_version = stored_config.get("config_version").and_then(Value::as_u64);
                let snapshot = if stored_version == Some(2) {
                    let snapshot: HookSnapshot = serde_json::from_value(
                        stored_config.get("hook_snapshot").cloned().ok_or_else(|| {
                            SessionError::InvalidConfig("missing hook snapshot".into())
                        })?,
                    )
                    .map_err(|_| SessionError::InvalidConfig("invalid hook snapshot".into()))?;
                    if snapshot.version != 1 || snapshot.descriptors.is_empty() {
                        return Err(SessionError::InvalidConfig(
                            "invalid hook snapshot version or empty descriptors".into(),
                        ));
                    }
                    stored_config["config_version"] = json!(1);
                    Some(snapshot)
                } else {
                    None
                };
                if snapshot.is_none() {
                    spec.hook_descriptors.clear();
                }
                if snapshot.is_some() && !spec.allow_hook_snapshot {
                    return Err(SessionError::InvalidConfig(
                        "hook-bearing sessions are unsupported by this host".into(),
                    ));
                }
                let config: SessionConfig =
                    serde_json::from_value(stored_config).map_err(|error| {
                        SessionError::InvalidConfig(format!("stored session config: {error}"))
                    })?;
                if config.config_version != 1 {
                    return Err(SessionError::UnsupportedConfigVersion(
                        config.config_version,
                    ));
                }
                // A hook-bearing record uses a registry envelope as well as config v2:
                // older hosts' config-replacement path checks registry_version but does
                // not validate config_version before overwriting it.
                let expected_registry = if snapshot.is_some() {
                    format!("hooks:v2:{}", deps.registry.version())
                } else {
                    deps.registry.version().to_owned()
                };
                // An enabled marker in the session record prevents a missing policy
                // document from being mistaken for a historical disabled session.
                // It also makes older hosts reject the unfamiliar registry envelope.
                let marked_review = record.registry_version.starts_with("review:v1:");
                if marked_review != spec.review_enabled {
                    return Err(SessionError::ReviewConfigMismatch);
                }
                let expected_registry = if marked_review {
                    format!("review:v1:{expected_registry}")
                } else {
                    expected_registry
                };
                if record.registry_version != expected_registry {
                    return Err(SessionError::RegistryDrift {
                        expected: record.registry_version.clone(),
                        actual: expected_registry,
                    });
                }
                let resolved = deps.registry.resolve(&config.extensions)?;
                if let Some(snapshot) = &snapshot {
                    if config.model.provider != deps.provider.id() {
                        return Err(SessionError::InvalidConfig(
                            "hook session provider mismatch".into(),
                        ));
                    }
                    if !hook_identities_match(&snapshot.descriptors, &resolved.hooks)
                        || snapshot.descriptors != spec.hook_descriptors
                        || snapshot.yield_limit != spec.hook_yield_limit
                    {
                        return Err(SessionError::InvalidConfig(
                            "hook identity or yield limit mismatch".into(),
                        ));
                    }
                }
                let recorded: BTreeMap<String, String> =
                    serde_json::from_value(record.extension_versions.clone()).unwrap_or_default();
                if recorded != resolved.versions {
                    return Err(SessionError::RegistryDrift {
                        expected: record.registry_version,
                        actual: deps.registry.version().to_string(),
                    });
                }
                let conversation_id = record.conversation_id.ok_or_else(|| {
                    SessionError::InvalidConfig(
                        "session has no unambiguous root conversation; manual reconciliation required".into(),
                    )
                })?;
                if spec
                    .conversation_id
                    .is_some_and(|requested| requested != conversation_id)
                {
                    return Err(SessionError::InvalidConfig(
                        "session conversation mismatch".into(),
                    ));
                }
                if deps
                    .storage
                    .get_conversation(conversation_id)
                    .await?
                    .is_none()
                {
                    return Err(SessionError::InvalidConfig(format!(
                        "conversation {} does not exist",
                        conversation_id.0
                    )));
                }
                (config, conversation_id, snapshot.is_some())
            }
            None => {
                let selected = deps.registry.resolve(&spec.config.extensions)?;
                if !hook_identities_match(&spec.hook_descriptors, &selected.hooks) {
                    return Err(SessionError::InvalidConfig("hook identity mismatch".into()));
                }
                if !spec.hook_descriptors.is_empty() && !spec.allow_hook_snapshot {
                    return Err(SessionError::InvalidConfig(
                        "hook-bearing sessions are unsupported by this host".into(),
                    ));
                }
                let conversation_id = match spec.conversation_id {
                    Some(conversation_id) => conversation_id,
                    None => ConversationId(deps.storage.mint_id().await?),
                };
                let now = deps.clock.now_ms();
                let workspace = json!({"key": deps.workspace.key().0, "root": deps.workspace.root(), "host_id": deps.workspace.host_id(), "env_id": deps.workspace.execution_env_id()});
                let policy = crate::review_state::PolicySnapshot::new(
                    spec.review_enabled,
                    &spec.config,
                    &selected,
                    &deps.registry,
                    deps.provider.id(),
                    workspace,
                    spec.hook_descriptors.clone(),
                )?;
                // The conversation, session association, policy and event share one batch.
                let mut tx = Transaction::new(
                    deps.storage.as_ref(),
                    CommitContext::conversation(spec.session_id, conversation_id),
                );
                tx.write(Write::Conversation(ConversationRecord {
                    id: conversation_id,
                    parent: None,
                    owner: None,
                }))?;
                tx.write(Write::Session(SessionRecord {
                    session_id: spec.session_id,
                    conversation_id: Some(conversation_id),
                    workspace_key: deps.workspace.key().0.clone(),
                    workspace: match deps.workspace.workspace_spec() {
                        Some(workspace) => serde_json::to_value(workspace)
                            .map_err(|error| SessionError::Internal(error.to_string()))?,
                        None => json!({
                            "key": deps.workspace.key().0,
                            "root": deps.workspace.root(),
                            "env_id": deps.workspace.execution_env_id(),
                            "host_id": deps.workspace.host_id(),
                        }),
                    },
                    config: {
                        let mut value = serde_json::to_value(&spec.config)
                            .map_err(|error| SessionError::Internal(error.to_string()))?;
                        if !spec.hook_descriptors.is_empty() {
                            value["config_version"] = json!(2);
                            value["hook_snapshot"] = serde_json::to_value(HookSnapshot {
                                version: 1,
                                descriptors: spec.hook_descriptors.clone(),
                                yield_limit: spec.hook_yield_limit,
                            })
                            .map_err(|error| SessionError::Internal(error.to_string()))?;
                        }
                        value
                    },
                    registry_version: {
                        let base = if spec.hook_descriptors.is_empty() {
                            deps.registry.version().to_owned()
                        } else {
                            format!("hooks:v2:{}", deps.registry.version())
                        };
                        if spec.review_enabled {
                            format!("review:v1:{base}")
                        } else {
                            base
                        }
                    },
                    extension_versions: serde_json::to_value(&spec.extension_versions)
                        .map_err(|error| SessionError::Internal(error.to_string()))?,
                    host_id: deps.workspace.host_id().to_string(),
                    status: "active".into(),
                    created_at_ms: now,
                    updated_at_ms: now,
                }))?;
                tx.write(Write::Document(DocumentWrite::PutLatest {
                    address: crate::review_state::address(
                        crate::review_state::POLICY,
                        spec.session_id,
                    ),
                    value: serde_json::to_value(policy)
                        .map_err(|error| SessionError::Internal(error.to_string()))?,
                }))?;
                tx.write(Write::Event(SessionEventDraft {
                    kind: event_kind::SESSION_OPENED.into(),
                    payload: json!({
                        "session_id": spec.session_id.0,
                        "conversation_id": conversation_id.0,
                        "registry_version": deps.registry.version(),
                    }),
                }))?;
                ensure_committed(tx.commit().await?)?;
                (
                    spec.config.clone(),
                    conversation_id,
                    !spec.hook_descriptors.is_empty(),
                )
            }
        };

        let resolved = deps.registry.resolve(&config.extensions)?;
        if reopening {
            if spec.review_enabled && spec.config != config {
                return Err(SessionError::InvalidConfig(
                    "review session configuration mismatch".into(),
                ));
            }
            let workspace = json!({"key": deps.workspace.key().0, "root": deps.workspace.root(), "host_id": deps.workspace.host_id(), "env_id": deps.workspace.execution_env_id()});
            let policy = crate::review_state::PolicySnapshot::new(
                spec.review_enabled,
                &config,
                &resolved,
                &deps.registry,
                deps.provider.id(),
                workspace,
                spec.hook_descriptors.clone(),
            )?;
            crate::review_state::check_policy(deps.storage.as_ref(), spec.session_id, &policy)
                .await?;
        }
        let tools = deps
            .registry
            .resolve_tool_set(&resolved, &config.tools, &config.model_tools);
        let queued =
            crate::submission::recover_queued(deps.storage.as_ref(), spec.session_id).await?;
        Ok(Self {
            deps,
            spec,
            config,
            conversation_id,
            resolved,
            tools,
            deferred: DeferredBuffer::new(),
            queued,
            cancel: CancellationToken::new(),
            compactor: None,
            max_parallel_tools: NonZeroUsize::new(4).expect("nonzero"),
            reporter: None,
            job_policy: rusty_bot_core::HookJobPolicy::default(),
            job_scope: None,
            pending_text: String::new(),
            pending_thinking: String::new(),
            last_partial_at: None,
            engine_attested_task: None,
            sdk_review_driver: false,
            interrupt_effect_driver: false,
            paused_review: None,
        })
    }

    async fn guard_review_host(&self) -> Result<(), SessionError> {
        if !self.interrupt_effect_driver {
            crate::interrupt_state::guard_unsupported_host(
                self.deps.storage.as_ref(),
                self.spec.session_id,
            )
            .await?;
        }
        if self.sdk_review_driver {
            return Ok(());
        }
        crate::review_state::guard_manual(
            self.deps.storage.as_ref(),
            self.spec.session_id,
            self.spec.review_enabled,
        )
        .await
    }

    /// Consume the engine into the explicitly designated review driver.
    /// This is a deliberate trusted-host opt-in, not an authorization or
    /// cryptographic boundary. Manual hosts must not use this boundary;
    /// review resumption is available only through this driver.
    pub fn into_sdk_review_driver(mut self) -> Result<SdkReviewDriver, SessionError> {
        if !self.spec.review_enabled {
            return Err(SessionError::ReviewUnsupported);
        }
        self.sdk_review_driver = true;
        Ok(SdkReviewDriver(self))
    }

    async fn check_current_review_policy(&self) -> Result<(), SessionError> {
        crate::interrupt_state::guard_unsupported_host(
            self.deps.storage.as_ref(),
            self.spec.session_id,
        )
        .await?;
        let expected = crate::review_state::PolicySnapshot::new(
            self.spec.review_enabled,
            &self.config,
            &self.resolved,
            &self.deps.registry,
            self.deps.provider.id(),
            json!({"key": self.deps.workspace.key().0, "root": self.deps.workspace.root(), "host_id": self.deps.workspace.host_id(), "env_id": self.deps.workspace.execution_env_id()}),
            self.spec.hook_descriptors.clone(),
        )?;
        crate::review_state::check_policy(
            self.deps.storage.as_ref(),
            self.spec.session_id,
            &expected,
        )
        .await
    }

    async fn review_task_request_id(&self, task_id: TaskId) -> Result<String, SessionError> {
        let task = self
            .deps
            .storage
            .get_task(task_id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review task".into()))?;
        let id = submission_id_of(&task.record)
            .ok_or_else(|| SessionError::InvalidState("missing review submission ID".into()))?;
        let submission = self
            .deps
            .storage
            .get_submission(id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review submission".into()))?;
        submission
            .record
            .request_id
            .ok_or_else(|| SessionError::InvalidState("missing review request ID".into()))
    }

    async fn pending_review(&self) -> Result<Option<rusty_bot_core::PendingReview>, SessionError> {
        let Some(doc) = self
            .deps
            .storage
            .get_document(
                &crate::review_state::address(crate::review_state::PENDING, self.spec.session_id),
                rusty_bot_core::DocumentPoint::Current,
            )
            .await?
        else {
            return Ok(None);
        };
        let record: crate::review_state::PendingRecord = serde_json::from_value(doc.value)
            .map_err(|_| SessionError::InvalidState("invalid pending review".into()))?;
        let p = &record.pending;
        if record.version != 1
            || p.id.0.is_empty()
            || p.session_id != self.spec.session_id
            || p.conversation_id != self.conversation_id
            || crate::review_state::fingerprint(
                &serde_json::to_value(&record.binding)
                    .map_err(|_| SessionError::InvalidState("invalid review binding".into()))?,
            )? != record.fingerprint
        {
            return Err(SessionError::InvalidState(
                "invalid pending review binding".into(),
            ));
        }
        let task = self
            .deps
            .storage
            .get_task(p.task_id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review task".into()))?;
        if !matches!(&task.record.state, TaskState::Waiting { on, policy: rusty_bot_core::WaitPolicy::Any, .. } if *on == vec![format!("review:{}", p.id.0)])
            || task
                .record
                .memos
                .get("generation_round")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                != p.round
        {
            return Err(SessionError::InvalidState("invalid review task".into()));
        }
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistant = entries
            .iter()
            .find(|e| {
                e.record.id == record.assistant_entry
                    && e.record.by_task_id == Some(p.task_id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == p.round
            })
            .ok_or_else(|| SessionError::InvalidState("missing review assistant".into()))?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| SessionError::InvalidState("missing review calls".into()))?,
        )
        .map_err(|_| SessionError::InvalidState("invalid review calls".into()))?;
        let call = calls
            .get(p.ordinal)
            .ok_or_else(|| SessionError::InvalidState("invalid review ordinal".into()))?;
        let policy = self
            .deps
            .storage
            .get_document(
                &crate::review_state::address(crate::review_state::POLICY, self.spec.session_id),
                rusty_bot_core::DocumentPoint::Current,
            )
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review policy".into()))?;
        self.check_current_review_policy().await?;
        let tool = if call.name == "ask_user" && self.ask_user_enabled() {
            crate::review_state::ToolSnapshot {
                spec: ask_user_spec(),
                replay: ReplayPolicy::Unsafe,
                mode: rusty_bot_core::ExecutionMode::Sequential,
            }
        } else {
            let t = self
                .tools
                .get(&call.name)
                .filter(|_| self.tools.model_visible.contains(&call.name))
                .ok_or_else(|| SessionError::InvalidState("missing reviewed tool".into()))?;
            crate::review_state::ToolSnapshot {
                spec: t.spec(),
                replay: t.replay(),
                mode: t.execution_mode(),
            }
        };
        let stored_policy: crate::review_state::PolicySnapshot =
            serde_json::from_value(policy.value)
                .map_err(|_| SessionError::InvalidState("invalid review policy".into()))?;
        let action_valid = match &p.action {
            rusty_bot_core::ReviewAction::Tool { pub_call } => {
                call.name != "ask_user"
                    && self.tools.get(&call.name).is_some_and(|tool| {
                        validate_arguments(&tool, &record.final_arguments).is_ok()
                    })
                    && pub_call.id == call.id
                    && pub_call.name == call.name
                    && pub_call.arguments == record.final_arguments
                    && !p.allowed_decisions.is_empty()
                    && p.allowed_decisions.iter().all(|d| {
                        matches!(
                            d,
                            rusty_bot_core::DecisionKind::Approve
                                | rusty_bot_core::DecisionKind::Edit
                                | rusty_bot_core::DecisionKind::Reject
                        )
                    })
            }
            rusty_bot_core::ReviewAction::Question { question } => {
                call.name == "ask_user"
                    && valid_question(&record.final_arguments)
                    && record
                        .final_arguments
                        .get("question")
                        .and_then(Value::as_str)
                        == Some(question)
                    && p.allowed_decisions
                        == [
                            rusty_bot_core::DecisionKind::Respond,
                            rusty_bot_core::DecisionKind::Reject,
                        ]
                        .into_iter()
                        .collect()
            }
        };
        if !action_valid
            || record.binding.policy != stored_policy
            || record.binding.tool != tool
            || validate_round_prefix(&entries, p.task_id, p.round, &calls)? != p.ordinal
            || call.arguments != record.original_arguments
            || record.binding.pending != *p
            || record.binding.assistant_entry != record.assistant_entry
            || record.binding.original_call != *call
            || record.binding.final_arguments != record.final_arguments
            || self
                .deps
                .storage
                .get_operation(p.task_id, &tool_task::idempotency_key(&call.name, &call.id))
                .await?
                .is_some()
        {
            return Err(SessionError::InvalidState(
                "invalid pending review state".into(),
            ));
        }
        Ok(Some(record.pending))
    }

    async fn active_review_receipt(
        &self,
    ) -> Result<Option<crate::review_state::ReceiptRecord>, SessionError> {
        let docs = self
            .deps
            .storage
            .scan_documents(rusty_bot_core::DocumentQuery {
                kind: Some(crate::review_state::RECEIPT.into()),
                scope: Some(DocumentScope::Session(self.spec.session_id)),
                key: None,
                at: rusty_bot_core::DocumentPoint::Current,
                limit: usize::MAX,
            })
            .await?;
        if docs.len() >= 1000 {
            return Err(SessionError::InvalidState(
                "review receipt scan is incomplete".into(),
            ));
        }
        let mut active = None;
        for doc in docs {
            let receipt: crate::review_state::ReceiptRecord = serde_json::from_value(doc.value)
                .map_err(|_| SessionError::InvalidState("invalid review receipt".into()))?;
            if receipt.version != 1
                || receipt.answer.id != receipt.pending.pending.id
                || doc.address
                    != crate::review_state::receipt_address(
                        self.spec.session_id,
                        &receipt.answer.id,
                    )
            {
                return Err(SessionError::InvalidState("invalid review receipt".into()));
            }
            receipt.check_integrity()?;
            if receipt.disposition == crate::review_state::ReceiptDisposition::AcceptedInProgress {
                self.validate_active_review_receipt(&receipt).await?;
                if active.replace(receipt).is_some() {
                    return Err(SessionError::InvalidState(
                        "multiple active review receipts".into(),
                    ));
                }
            }
        }
        Ok(active)
    }

    async fn validate_active_review_receipt(
        &self,
        receipt: &crate::review_state::ReceiptRecord,
    ) -> Result<(), SessionError> {
        use rusty_bot_core::{DecisionKind, ReviewAction, ReviewDecision};
        receipt.check_integrity()?;
        let r = &receipt.pending;
        let p = &r.pending;
        self.check_current_review_policy().await?;
        let policy = self
            .deps
            .storage
            .get_document(
                &crate::review_state::address(crate::review_state::POLICY, self.spec.session_id),
                rusty_bot_core::DocumentPoint::Current,
            )
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review policy".into()))?;
        let policy: crate::review_state::PolicySnapshot = serde_json::from_value(policy.value)
            .map_err(|_| SessionError::InvalidState("invalid review policy".into()))?;
        let call = &r.binding.original_call;
        let valid_decision = match (&p.action, &receipt.answer.decision) {
            (ReviewAction::Tool { pub_call }, decision)
                if call.name != "ask_user"
                    && pub_call.id == call.id
                    && pub_call.name == call.name
                    && pub_call.arguments == r.final_arguments =>
            {
                let tool = self
                    .tools
                    .get(&call.name)
                    .filter(|_| self.tools.model_visible.contains(&call.name));
                tool.is_some_and(|tool| {
                    let snapshot = crate::review_state::ToolSnapshot {
                        spec: tool.spec(),
                        replay: tool.replay(),
                        mode: tool.execution_mode(),
                    };
                    snapshot == r.binding.tool
                        && validate_arguments(&tool, &r.final_arguments).is_ok()
                        && !p.allowed_decisions.is_empty()
                        && p.allowed_decisions.iter().all(|d| {
                            matches!(
                                d,
                                DecisionKind::Approve | DecisionKind::Edit | DecisionKind::Reject
                            )
                        })
                        && match decision {
                            ReviewDecision::Approve => {
                                p.allowed_decisions.contains(&DecisionKind::Approve)
                            }
                            ReviewDecision::Edit { arguments } => {
                                p.allowed_decisions.contains(&DecisionKind::Edit)
                                    && validate_arguments(&tool, arguments).is_ok()
                            }
                            ReviewDecision::Reject { reason } => {
                                p.allowed_decisions.contains(&DecisionKind::Reject)
                                    && reason.len() <= 4096
                            }
                            _ => false,
                        }
                })
            }
            (ReviewAction::Question { question }, decision)
                if call.name == "ask_user" && self.ask_user_enabled() =>
            {
                r.binding.tool
                    == (crate::review_state::ToolSnapshot {
                        spec: ask_user_spec(),
                        replay: ReplayPolicy::Unsafe,
                        mode: rusty_bot_core::ExecutionMode::Sequential,
                    })
                    && valid_question(&r.final_arguments)
                    && r.final_arguments.get("question").and_then(Value::as_str) == Some(question)
                    && p.allowed_decisions
                        == [DecisionKind::Respond, DecisionKind::Reject]
                            .into_iter()
                            .collect()
                    && match decision {
                        ReviewDecision::Respond { text } => {
                            !text.trim().is_empty() && text.len() <= 16384
                        }
                        ReviewDecision::Reject { reason } => reason.len() <= 4096,
                        _ => false,
                    }
            }
            _ => false,
        };
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistant = entries
            .iter()
            .find(|e| {
                e.record.id == r.assistant_entry
                    && e.record.by_task_id == Some(p.task_id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == p.round
            })
            .ok_or_else(|| {
                SessionError::InvalidState("missing accepted review assistant".into())
            })?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| {
                    SessionError::InvalidState("missing accepted review calls".into())
                })?,
        )
        .map_err(|_| SessionError::InvalidState("invalid accepted review calls".into()))?;
        let prefix = validate_round_prefix(&entries, p.task_id, p.round, &calls)?;
        let no_effect = matches!(
            receipt.answer.decision,
            ReviewDecision::Reject { .. } | ReviewDecision::Respond { .. }
        );
        let result_valid =
            if no_effect {
                let expected = receipt.result_output.clone().unwrap_or_else(|| {
                    match &receipt.answer.decision {
                        ReviewDecision::Reject { reason } => {
                            ToolOutput::error(reason.clone(), "review_rejected")
                        }
                        ReviewDecision::Respond { text } => ToolOutput::success(text.clone()),
                        _ => unreachable!(),
                    }
                });
                entries
                    .iter()
                    .filter(|e| {
                        e.record.by_task_id == Some(p.task_id)
                            && e.record.kind == entry_kind::TOOL_RESULT
                            && e.record
                                .data
                                .get("round")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                                == p.round
                    })
                    .nth(p.ordinal)
                    .is_some_and(|e| {
                        e.record.model
                            == vec![Message::ToolResult {
                                tool_call_id: call.id.clone(),
                                tool_name: call.name.clone(),
                                content: expected.content,
                                is_error: expected.is_error,
                                details: expected.details,
                            }]
                    })
            } else {
                true
            };
        if r.version != 1
            || p.id.0.is_empty()
            || p.session_id != self.spec.session_id
            || p.conversation_id != self.conversation_id
            || r.binding.policy != policy
            || r.binding.pending != *p
            || r.binding.assistant_entry != r.assistant_entry
            || r.original_arguments != call.arguments
            || r.binding.final_arguments != r.final_arguments
            || calls.get(p.ordinal) != Some(call)
            || prefix < p.ordinal
            || (no_effect
                && (prefix <= p.ordinal
                    || self
                        .deps
                        .storage
                        .get_operation(p.task_id, &tool_task::idempotency_key(&call.name, &call.id))
                        .await?
                        .is_some()))
            || crate::review_state::fingerprint(
                &serde_json::to_value(&r.binding)
                    .map_err(|_| SessionError::InvalidState("invalid review binding".into()))?,
            )? != r.fingerprint
            || !valid_decision
            || receipt.result_output.is_some() != no_effect
            || !result_valid
        {
            return Err(SessionError::InvalidState(
                "invalid accepted review binding".into(),
            ));
        }
        Ok(())
    }

    async fn completed_review_write(
        &self,
        task_id: TaskId,
    ) -> Result<Option<DocumentWrite>, SessionError> {
        self.review_disposition_write(task_id, crate::review_state::ReceiptDisposition::Completed)
            .await
    }

    async fn review_disposition_write(
        &self,
        task_id: TaskId,
        disposition: crate::review_state::ReceiptDisposition,
    ) -> Result<Option<DocumentWrite>, SessionError> {
        let Some(mut receipt) = self.active_review_receipt().await? else {
            return Ok(None);
        };
        if receipt.pending.pending.task_id != task_id {
            return Err(SessionError::InvalidState(
                "review continuation task mismatch".into(),
            ));
        }
        receipt.disposition = disposition;
        Ok(Some(DocumentWrite::PutLatest {
            address: crate::review_state::receipt_address(self.spec.session_id, &receipt.answer.id),
            value: serde_json::to_value(receipt)
                .map_err(|_| SessionError::Internal("review serialization failed".into()))?,
        }))
    }

    async fn admit_review(
        &mut self,
        answer: rusty_bot_core::ReviewAnswer,
    ) -> Result<(), SessionError> {
        use rusty_bot_core::{DecisionKind, ReviewAction, ReviewDecision};
        let address = crate::review_state::receipt_address(self.spec.session_id, &answer.id);
        if let Some(doc) = self
            .deps
            .storage
            .get_document(&address, rusty_bot_core::DocumentPoint::Current)
            .await?
        {
            let receipt: crate::review_state::ReceiptRecord = serde_json::from_value(doc.value)
                .map_err(|_| SessionError::InvalidState("invalid review receipt".into()))?;
            if receipt.version != 1
                || receipt.answer.id != answer.id
                || receipt.pending.pending.id != answer.id
            {
                return Err(SessionError::InvalidState("invalid review receipt".into()));
            }
            receipt.check_integrity()?;
            if receipt.answer != answer {
                return Err(SessionError::ReviewConflict);
            }
            if receipt.disposition != crate::review_state::ReceiptDisposition::AcceptedInProgress {
                return Err(SessionError::ReviewAlreadyResolved);
            }
            if self.active_review_receipt().await?.as_ref() != Some(&receipt) {
                return Err(SessionError::InvalidState(
                    "invalid active review receipt".into(),
                ));
            }
            return Ok(());
        }
        let Some(pending) = self.pending_review().await? else {
            return Err(SessionError::InvalidReview("unknown review ID".into()));
        };
        if pending.id != answer.id {
            return Err(SessionError::InvalidReview("unknown review ID".into()));
        }
        if self.active_review_receipt().await?.is_some() {
            return Err(SessionError::InvalidState(
                "review continuation already active".into(),
            ));
        }
        let kind = match &answer.decision {
            ReviewDecision::Approve => DecisionKind::Approve,
            ReviewDecision::Edit { .. } => DecisionKind::Edit,
            ReviewDecision::Reject { .. } => DecisionKind::Reject,
            ReviewDecision::Respond { .. } => DecisionKind::Respond,
        };
        if !pending.allowed_decisions.contains(&kind) {
            return Err(SessionError::InvalidReview("decision not allowed".into()));
        }
        match (&pending.action, &answer.decision) {
            (ReviewAction::Tool { pub_call }, ReviewDecision::Approve) => {
                let _ = pub_call;
            }
            (ReviewAction::Tool { pub_call }, ReviewDecision::Edit { arguments }) => {
                let tool = self
                    .tools
                    .get(&pub_call.name)
                    .ok_or_else(|| SessionError::InvalidState("missing reviewed tool".into()))?;
                validate_arguments(&tool, arguments).map_err(SessionError::InvalidReview)?;
            }
            (
                ReviewAction::Tool { .. } | ReviewAction::Question { .. },
                ReviewDecision::Reject { reason },
            ) if reason.len() <= 4096 => {}
            (ReviewAction::Question { .. }, ReviewDecision::Respond { text })
                if !text.trim().is_empty() && text.len() <= 16384 => {}
            _ => {
                return Err(SessionError::InvalidReview(
                    "invalid decision or text".into(),
                ))
            }
        }
        let doc = self
            .deps
            .storage
            .get_document(
                &crate::review_state::address(crate::review_state::PENDING, self.spec.session_id),
                rusty_bot_core::DocumentPoint::Current,
            )
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review".into()))?;
        let record: crate::review_state::PendingRecord = serde_json::from_value(doc.value)
            .map_err(|_| SessionError::InvalidState("invalid pending review".into()))?;
        if record.pending != pending {
            return Err(SessionError::InvalidState("review changed".into()));
        }
        let task = self
            .deps
            .storage
            .get_task(pending.task_id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review task".into()))?;
        let mut updated = task.record.clone();
        if !matches!(&updated.state, TaskState::Waiting { on, .. } if *on == vec![format!("review:{}", pending.id.0)])
        {
            return Err(SessionError::InvalidState("review task changed".into()));
        }
        updated.version += 1;
        updated.state = TaskState::Running {
            checkpoint: task.record.state.checkpoint().cloned(),
            wake: None,
        };
        let result_output = match &answer.decision {
            ReviewDecision::Reject { reason } => {
                Some(ToolOutput::error(reason.clone(), "review_rejected"))
            }
            ReviewDecision::Respond { text } => Some(ToolOutput::success(text.clone())),
            _ => None,
        };
        let mut result_output = result_output;
        if let Some(output) = &mut result_output {
            self.hook_dispatcher(self.cancel.clone())
                .after_tool(pending.task_id, &record.binding.original_call, output)
                .await?;
            if let Err(reason) = validate_tool_images(output).await {
                *output = ToolOutput::error(reason, "invalid_image");
            }
        }
        let result_entry = if let Some(output) = &result_output {
            let entry_id = EntryId(self.deps.storage.mint_id().await?);
            let call = &record.binding.original_call;
            let has_more = self
                .has_more_calls(pending.task_id, pending.round, &call.id)
                .await?;
            updated.state = TaskState::Running {
                checkpoint: if has_more {
                    Some(ToolCheckpoint::Call)
                } else {
                    None
                },
                wake: None,
            };
            Some(tool_result_entry(
                entry_id,
                self.conversation_id,
                pending.task_id,
                call,
                output,
                pending.round,
                false,
            ))
        } else {
            None
        };
        let mut receipt = crate::review_state::ReceiptRecord {
            version: 1,
            answer: answer.clone(),
            pending: record,
            disposition: crate::review_state::ReceiptDisposition::AcceptedInProgress,
            result_output,
            integrity: None,
        };
        receipt.seal()?;
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(entry) = result_entry {
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::ENTRY_COMMITTED.into(),
                payload: json!({"entry_id": entry.id.0, "kind": entry_kind::TOOL_RESULT}),
            }))?;
            tx.write(Write::Entry(entry))?;
        }
        tx.write(Write::Task(updated.clone()))?;
        tx.write(Write::Document(DocumentWrite::PutLatest {
            address,
            value: serde_json::to_value(receipt)
                .map_err(|_| SessionError::Internal("review serialization failed".into()))?,
        }))?;
        tx.write(Write::Document(DocumentWrite::Retire {
            address: crate::review_state::address(
                crate::review_state::PENDING,
                self.spec.session_id,
            ),
        }))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&updated),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let result = tx.commit().await?;
        if matches!(
            result.refused.as_ref().map(|r| &r.reason),
            Some(rusty_bot_core::RefusalReason::VersionConflict { .. })
        ) {
            if let Some(doc) = self
                .deps
                .storage
                .get_document(
                    &crate::review_state::receipt_address(self.spec.session_id, &answer.id),
                    rusty_bot_core::DocumentPoint::Current,
                )
                .await?
            {
                let receipt: crate::review_state::ReceiptRecord = serde_json::from_value(doc.value)
                    .map_err(|_| SessionError::InvalidState("invalid review receipt".into()))?;
                if receipt.version != 1
                    || receipt.answer.id != answer.id
                    || receipt.pending.pending.id != answer.id
                {
                    return Err(SessionError::InvalidState("invalid review receipt".into()));
                }
                receipt.check_integrity()?;
                if receipt.answer != answer {
                    return Err(SessionError::ReviewConflict);
                }
                if receipt.disposition
                    != crate::review_state::ReceiptDisposition::AcceptedInProgress
                {
                    return Err(SessionError::ReviewAlreadyResolved);
                }
                if self.active_review_receipt().await?.as_ref() != Some(&receipt) {
                    return Err(SessionError::InvalidState(
                        "invalid active review receipt".into(),
                    ));
                }
                return Ok(());
            }
            return Err(SessionError::ReviewConflict);
        }
        self.handle_task_receipt(result, task.record.version).await
    }

    async fn commit_review(
        &mut self,
        task_id: TaskId,
        round: u64,
        assistant_entry: EntryId,
        calls: &[ToolCall],
        candidate: ReviewCandidate,
    ) -> Result<(), SessionError> {
        let ReviewCandidate {
            ordinal,
            call,
            final_call,
            allowed,
        } = candidate;
        if self.pending_review().await?.is_some() {
            return Err(SessionError::InvalidState("review already pending".into()));
        }
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        if validate_round_prefix(&entries, task_id, round, calls)? != ordinal
            || self
                .deps
                .storage
                .get_operation(task_id, &tool_task::idempotency_key(&call.name, &call.id))
                .await?
                .is_some()
        {
            return Err(SessionError::InvalidState(
                "invalid review prefix or operation".into(),
            ));
        }
        let task = self
            .deps
            .storage
            .get_task(task_id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review task".into()))?;
        if !matches!(task.record.state, TaskState::Running { .. }) {
            return Err(SessionError::InvalidState(
                "review task is not running".into(),
            ));
        }
        self.check_current_review_policy().await?;
        if self.cancel.is_cancelled() {
            return Err(SessionError::Cancelled);
        }
        let action = if call.name == "ask_user" {
            rusty_bot_core::ReviewAction::Question {
                question: final_call.arguments["question"]
                    .as_str()
                    .ok_or_else(|| SessionError::InvalidState("invalid question".into()))?
                    .to_owned(),
            }
        } else {
            rusty_bot_core::ReviewAction::Tool {
                pub_call: final_call.clone(),
            }
        };
        let pending = rusty_bot_core::PendingReview {
            id: rusty_bot_core::ReviewId(uuid::Uuid::new_v4().to_string()),
            session_id: self.spec.session_id,
            conversation_id: self.conversation_id,
            task_id,
            round,
            ordinal,
            action,
            allowed_decisions: allowed,
        };
        let policy = self
            .deps
            .storage
            .get_document(
                &crate::review_state::address(crate::review_state::POLICY, self.spec.session_id),
                rusty_bot_core::DocumentPoint::Current,
            )
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing review policy".into()))?;
        let tool = self.tools.get(&call.name);
        let tool_binding = tool
            .map(|t| crate::review_state::ToolSnapshot {
                spec: t.spec(),
                replay: t.replay(),
                mode: t.execution_mode(),
            })
            .unwrap_or_else(|| crate::review_state::ToolSnapshot {
                spec: ask_user_spec(),
                replay: ReplayPolicy::Unsafe,
                mode: rusty_bot_core::ExecutionMode::Sequential,
            });
        let binding = crate::review_state::ReviewBinding {
            pending: pending.clone(),
            assistant_entry,
            original_call: call.clone(),
            final_arguments: final_call.arguments.clone(),
            tool: tool_binding,
            policy: serde_json::from_value(policy.value)
                .map_err(|_| SessionError::InvalidState("invalid review policy".into()))?,
        };
        let binding_value = serde_json::to_value(&binding)
            .map_err(|_| SessionError::Internal("review serialization failed".into()))?;
        let record = crate::review_state::PendingRecord {
            version: 1,
            pending: pending.clone(),
            assistant_entry,
            original_arguments: call.arguments,
            final_arguments: final_call.arguments,
            fingerprint: crate::review_state::fingerprint(&binding_value)?,
            binding,
        };
        let mut updated = task.record.clone();
        updated.version += 1;
        updated.state = TaskState::Waiting {
            checkpoint: task.record.state.checkpoint().cloned(),
            on: vec![format!("review:{}", pending.id.0)],
            policy: rusty_bot_core::WaitPolicy::Any,
        };
        let completed_review = self.completed_review_write(task_id).await?;
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(write) = completed_review {
            tx.write(Write::Document(write))?;
        }
        tx.write(Write::Task(updated.clone()))?;
        tx.write(Write::Document(DocumentWrite::PutLatest {
            address: crate::review_state::address(
                crate::review_state::PENDING,
                self.spec.session_id,
            ),
            value: serde_json::to_value(record)
                .map_err(|_| SessionError::Internal("review serialization failed".into()))?,
        }))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&updated),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, task.record.version)
            .await?;
        self.paused_review = Some(pending);
        Ok(())
    }

    pub fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    pub fn session_id(&self) -> SessionId {
        self.spec.session_id
    }

    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    pub fn registry_version(&self) -> &str {
        self.deps.registry.version()
    }

    pub fn resolved_extensions(&self) -> &ResolvedExtensions {
        &self.resolved
    }

    /// Frozen per-session descriptors paired with registered hooks. Legacy records
    /// return an empty list even if this process now has hooks installed.
    /// Cloned values can be used without holding an engine lock across callbacks.
    pub fn selected_hooks(&self) -> Vec<crate::registry::ResolvedHook> {
        self.spec
            .hook_descriptors
            .iter()
            .zip(&self.resolved.hooks)
            .map(|(descriptor, hook)| crate::registry::ResolvedHook {
                descriptor: descriptor.clone(),
                hook: hook.hook.clone(),
            })
            .collect()
    }

    /// Install an optional observer without altering the source-compatible dependency bundle.
    /// Configure request-local compaction for engine-driven generation only.
    /// Manual `WorkItem::Generate` consumers must apply their own transformation.
    pub fn set_compactor(&mut self, compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>) {
        self.compactor = compactor;
    }

    pub fn set_hook_reporter(&mut self, reporter: Option<Arc<dyn rusty_bot_core::HookReporter>>) {
        self.reporter = reporter;
    }

    /// Configure process-local job limits before starting a run; never persisted.
    pub fn set_hook_job_policy(
        &mut self,
        policy: rusty_bot_core::HookJobPolicy,
    ) -> Result<(), SessionError> {
        if !policy.is_valid() {
            return Err(SessionError::InvalidConfig(
                "hook job limits must be nonzero".into(),
            ));
        }
        if self
            .job_scope
            .as_ref()
            .is_some_and(|(_, active)| active.upgrade().is_some())
        {
            return Err(SessionError::InvalidState("run already active".into()));
        }
        self.job_policy = policy;
        Ok(())
    }

    fn ensure_supported_hooks(&self) -> Result<(), SessionError> {
        if self
            .spec
            .hook_descriptors
            .iter()
            .flat_map(|d| &d.points)
            .any(|point| {
                !matches!(
                    point,
                    rusty_bot_core::HookPoint::BeforeRequest
                        | rusty_bot_core::HookPoint::BeforeProviderRequest
                        | rusty_bot_core::HookPoint::AfterResponse
                        | rusty_bot_core::HookPoint::BeforeTool
                        | rusty_bot_core::HookPoint::AfterTool
                        | rusty_bot_core::HookPoint::AfterTools
                        | rusty_bot_core::HookPoint::OnYield
                        | rusty_bot_core::HookPoint::AfterRun
                )
            })
        {
            return Err(SessionError::InvalidConfig(
                "selected hook point is not implemented".into(),
            ));
        }
        Ok(())
    }

    fn has_hook(&self, point: rusty_bot_core::HookPoint) -> bool {
        self.spec
            .hook_descriptors
            .iter()
            .any(|d| d.points.contains(&point))
    }

    fn hook_dispatcher(&self, cancel: CancellationToken) -> crate::hooks::Dispatcher {
        crate::hooks::Dispatcher::new(
            self.selected_hooks(),
            self.spec.session_id,
            self.conversation_id,
            self.deps.storage.clone(),
            self.reporter.clone(),
            cancel,
            self.job_scope
                .as_ref()
                .and_then(|(scope, active)| active.upgrade().and_then(|_| scope.upgrade())),
        )
    }

    pub fn resolved_tools(&self) -> &ResolvedToolSet {
        &self.tools
    }

    /// Drop the in-memory deferred buffer without persisting it. Used when a
    /// session closes; a crash has the same effect.
    pub fn drop_deferred(&mut self) {
        self.deferred = DeferredBuffer::new();
    }

    /// Install a cancellation token for granular step methods.
    ///
    /// `run_until_idle` installs one itself, but activities drive single steps
    /// (`execute_tool` in particular). Passing the transport's cancellation
    /// token here lets the sandboxed shell kill the child process group when
    /// the step is cancelled; the engine still commits the interrupted outcome
    /// itself, so no external effect is repeated on retry.
    /// Limit concurrent tool effects in engine-driven rounds (default four).
    pub fn set_max_parallel_tools(&mut self, max: NonZeroUsize) {
        self.max_parallel_tools = max;
    }

    pub fn set_cancel(&mut self, cancel: CancellationToken) {
        self.cancel = cancel;
    }

    /// Publish a throttled live partial and stage the durable live document.
    ///
    /// The frame is emitted at most once per `partial_interval_ms`; the full
    /// accumulated text is staged as a deferred `rusty-bot.live` document so the
    /// next normal commit (the assistant entry) persists it. Throttling never
    /// loses durable-visible progress.
    pub async fn publish_partial(
        &mut self,
        task_id: TaskId,
        kind: LiveKind,
        data: Value,
        text: &str,
        thinking: &str,
    ) -> Result<(), SessionError> {
        if self.partial_due() {
            self.emit_partial(task_id, kind, data, text, thinking)
                .await?;
        } else {
            // Throttling limits live frames, not the latest durable snapshot.
            self.stage_partial(task_id, text, thinking).await?;
        }
        Ok(())
    }

    /// Persist any staged progress whose deadline has elapsed. Callers driving
    /// a long-running provider must invoke this even when the provider stalls.
    pub async fn flush_due_progress(&mut self) -> Result<(), SessionError> {
        self.flush_due(self.deps.clock.now_ms()).await
    }

    pub async fn submit(&mut self, request: SubmitRequest) -> Result<SubmitOutcome, SessionError> {
        self.guard_review_host().await?;
        let pending = if self.sdk_review_driver {
            self.pending_review().await?
        } else {
            None
        };
        let accepted = self.sdk_review_driver && self.active_review_receipt().await?.is_some();
        self.ensure_supported_hooks()?;
        let (request_id, kind, payload, content) = match request {
            SubmitRequest::Input { request_id, text } => {
                let payload = json!({ "text": text });
                (
                    request_id,
                    SubmissionKind::Input,
                    payload,
                    Some(vec![rusty_bot_core::ContentPart::Text { text }]),
                )
            }
            SubmitRequest::Content {
                request_id,
                content,
            } => {
                let payload = json!({ "content": content });
                (request_id, SubmissionKind::Input, payload, Some(content))
            }
            SubmitRequest::Write { request_id, data } => {
                (request_id, SubmissionKind::Write, data, None)
            }
            SubmitRequest::Continue { request_id } => {
                (request_id, SubmissionKind::Continue, json!({}), None)
            }
        };
        if request_id.trim().is_empty() {
            return Ok(SubmitOutcome::Rejected {
                reason: RejectReason::InvalidRequest("request_id is required".into()),
            });
        }
        // A pending review permits only the exact original submission to be deduplicated.
        if accepted {
            return Err(SessionError::PendingReview);
        }
        if let Some(pending) = pending {
            if self.review_task_request_id(pending.task_id).await? != request_id {
                return Err(SessionError::PendingReview);
            }
            let existing = self
                .deps
                .storage
                .find_submission_by_request(self.conversation_id, &request_id)
                .await?
                .ok_or_else(|| SessionError::InvalidState("missing review submission".into()))?;
            return match compare_submission(&existing.record, kind, &payload, request_id)? {
                duplicate @ SubmitOutcome::Duplicate { .. } => Ok(duplicate),
                _ => Err(SessionError::PendingReview),
            };
        }
        // Dedupe happens before any write.
        if let Some(existing) = self
            .deps
            .storage
            .find_submission_by_request(self.conversation_id, &request_id)
            .await?
        {
            return compare_submission(&existing.record, kind, &payload, request_id);
        }
        if let Some(content) = &content {
            if content.is_empty() || content.iter().all(|part| matches!(part, rusty_bot_core::ContentPart::Text { text } if text.trim().is_empty())) {
                return Ok(SubmitOutcome::Rejected { reason: RejectReason::EmptyInput });
            }
            if content
                .iter()
                .any(|part| matches!(part, rusty_bot_core::ContentPart::Image { .. }))
            {
                let Some(limits) = self.deps.provider.image_limits() else {
                    return Ok(SubmitOutcome::Rejected {
                        reason: RejectReason::InvalidRequest(
                            "provider does not support images".into(),
                        ),
                    });
                };
                if let Err(error) =
                    rusty_bot_core::image_validation::validate_images(content, &limits)
                {
                    return Ok(SubmitOutcome::Rejected {
                        reason: RejectReason::InvalidRequest(error.into()),
                    });
                }
            }
        }

        let id = SubmissionId(self.deps.storage.mint_id().await?);
        let record = SubmissionRecord {
            id,
            conversation_id: self.conversation_id,
            request_id: Some(request_id.clone()),
            kind,
            status: SubmissionStatus::Queued,
            payload,
            entry_id: None,
            error: None,
        };
        let deferred = self.deferred.drain();
        let mut tx = Transaction::new(
            self.deps.storage.as_ref(),
            CommitContext::conversation(self.spec.session_id, self.conversation_id),
        );
        tx.write(Write::Submission(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::SUBMISSION_QUEUED.into(),
            payload: rusty_bot_core::storage::submission_event_payload(&record),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        if let Some(refusal) = &receipt.refused {
            if let rusty_bot_core::RefusalReason::DuplicateRequestId { .. } = &refusal.reason {
                let existing = self
                    .deps
                    .storage
                    .find_submission_by_request(self.conversation_id, &request_id)
                    .await?;
                let existing = existing.ok_or_else(|| {
                    SessionError::ContractViolation(
                        "duplicate submission missing after conflict".into(),
                    )
                })?;
                return compare_submission(&existing.record, kind, &record.payload, request_id);
            }
            return Err(SessionError::ContractViolation(format!(
                "submission commit refused: {}",
                refusal.reason
            )));
        }
        self.queued.push(id);
        Ok(SubmitOutcome::Accepted { request_id })
    }

    /// Cancel this engine's accepted input at a drive boundary, after all
    /// in-flight provider/tool calls have returned. This does not drive other
    /// queued submissions or invoke a provider or tool. The caller must be the
    /// exclusive driver and must not use it to recover an uncertain in-flight
    /// operation from another process.
    ///
    /// Returns true only if this call terminalized the submission. A finished
    /// submission is left untouched so completion and errors retain precedence.
    pub async fn cancel_accepted_input(&mut self, request_id: &str) -> Result<bool, SessionError> {
        self.guard_review_host().await?;
        let Some(stored) = self
            .deps
            .storage
            .find_submission_by_request(self.conversation_id, request_id)
            .await?
        else {
            return Err(SessionError::InvalidState(
                "accepted submission missing".into(),
            ));
        };
        if stored.record.kind != SubmissionKind::Input {
            return Err(SessionError::InvalidState(
                "accepted submission is not input".into(),
            ));
        }
        match stored.record.status {
            SubmissionStatus::Queued => {
                let error = ModelError::cancelled();
                let entry_id = EntryId(self.deps.storage.mint_id().await?);
                let entry = EntryRecord {
                    id: entry_id,
                    conversation_id: self.conversation_id,
                    kind: entry_kind::ERROR.into(),
                    model: Vec::new(),
                    data: json!({ "submission_id": stored.record.id.0, "error": {
                        "kind": error.kind, "message": error.message,
                        "retryable": error.retryable, "status": error.status,
                    }}),
                    head: None,
                    edits: Vec::new(),
                    by_task_id: None,
                };
                let mut settled = stored.record;
                settled.status = SubmissionStatus::Done;
                settled.error = Some(error.message);
                let deferred = self.deferred.drain();
                let mut tx = self.conversation_tx();
                tx.write(Write::Entry(entry))?;
                tx.write(Write::Event(SessionEventDraft {
                    kind: event_kind::ENTRY_COMMITTED.into(),
                    payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::ERROR }),
                }))?;
                tx.write(Write::Submission(settled.clone()))?;
                tx.write(Write::Event(SessionEventDraft {
                    kind: event_kind::SUBMISSION_DONE.into(),
                    payload: rusty_bot_core::storage::submission_event_payload(&settled),
                }))?;
                for write in deferred {
                    tx.write(Write::Document(write))?;
                }
                ensure_committed(tx.commit().await?)?;
                self.queued.retain(|id| *id != settled.id);
                Ok(true)
            }
            SubmissionStatus::Placed => {
                let entry_id = stored.record.entry_id.ok_or_else(|| {
                    SessionError::InvalidState("placed input entry missing".into())
                })?;
                let task_id = self
                    .deps
                    .storage
                    .get_entry(self.conversation_id, entry_id)
                    .await?
                    .and_then(|entry| entry.record.by_task_id)
                    .ok_or_else(|| {
                        SessionError::InvalidState("placed input task missing".into())
                    })?;
                let task = self.deps.storage.get_task(task_id).await?.ok_or_else(|| {
                    SessionError::InvalidState("placed submission task missing".into())
                })?;
                if submission_id_of(&task.record) != Some(stored.record.id)
                    || task.record.state.is_terminal()
                {
                    return Err(SessionError::InvalidState(
                        "placed submission task inconsistent".into(),
                    ));
                }
                // A committed final assistant response wins over cancellation,
                // even if the driver has not yet reached FinishTurn.
                if matches!(task.record.state, TaskState::Running { .. }) {
                    let round = task
                        .record
                        .memos
                        .get("generation_round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let entries =
                        prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id)
                            .await?;
                    let final_answer = entries
                        .iter()
                        .rev()
                        .find(|entry| {
                            entry.record.by_task_id == Some(task_id)
                                && entry.record.kind == entry_kind::ASSISTANT
                        })
                        .is_some_and(|entry| {
                            entry
                                .record
                                .data
                                .get("round")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                                == round
                                && entry
                                    .record
                                    .data
                                    .get("tool_calls")
                                    .and_then(Value::as_array)
                                    .is_some_and(Vec::is_empty)
                        });
                    if final_answer {
                        self.finish_turn(task_id).await?;
                        return Ok(false);
                    }
                }
                // Storage requires Pending -> Running -> Terminal. No model or
                // tool is invoked while advancing this state.
                if matches!(task.record.state, TaskState::Pending { .. }) {
                    self.begin_task(&task).await?;
                }
                self.fail_generation(task_id, ModelError::cancelled())
                    .await?;
                Ok(true)
            }
            SubmissionStatus::Done | SubmissionStatus::Unanswered => Ok(false),
        }
    }

    pub async fn plan_next(&mut self) -> Result<WorkItem, SessionError> {
        self.guard_review_host().await?;
        self.ensure_supported_hooks()?;
        // An accepted review owns the session until the original run pauses or
        // terminalizes. Do not place queued submissions or schedule other tasks.
        if self.sdk_review_driver {
            if let Some(receipt) = self.active_review_receipt().await? {
                let task = self
                    .deps
                    .storage
                    .get_task(receipt.pending.pending.task_id)
                    .await?
                    .ok_or_else(|| {
                        SessionError::InvalidState("missing review continuation task".into())
                    })?;
                if task.record.state.is_terminal()
                    || matches!(
                        task.record.state,
                        TaskState::Waiting { .. } | TaskState::Sleeping { .. }
                    )
                {
                    return Err(SessionError::InvalidState(
                        "invalid review continuation task state".into(),
                    ));
                }
                return match self.task_work(task).await? {
                    Some(work) => Ok(work),
                    None => self.plan_review_continuation().await,
                };
            }
        }
        // A settled run with pending observers holds the boundary, including on reopen.
        if self.has_hook(rusty_bot_core::HookPoint::AfterRun) {
            let tasks = self
                .deps
                .storage
                .scan_tasks(TaskQuery {
                    conversation_id: Some(self.conversation_id),
                    status: None,
                    kind: Some(TURN_TASK_KIND.into()),
                    limit: 1_000,
                })
                .await?;
            if let Some(task) = tasks.iter().find(|task| {
                matches!(task.record.state, TaskState::Completing { .. })
                    && task.record.memos.contains_key("after_run_pending")
            }) {
                self.finish_turn(task.record.id).await?;
            }
        }
        // Recover externally accepted work as well as this driver's local queue.
        if self.has_hook(rusty_bot_core::HookPoint::OnYield)
            || self.has_hook(rusty_bot_core::HookPoint::AfterRun)
            || self.has_hook(rusty_bot_core::HookPoint::AfterTools)
        {
            self.queued =
                crate::submission::recover_queued(self.deps.storage.as_ref(), self.spec.session_id)
                    .await?;
        }
        // Placement first: queued submissions become user entries and tasks.
        while let Some(id) = self.queued.first().copied() {
            let Some(stored) = self.deps.storage.get_submission(id).await? else {
                self.queued.remove(0);
                continue;
            };
            if stored.record.status != SubmissionStatus::Queued {
                self.queued.remove(0);
                continue;
            }
            self.place_submission(stored.record).await?;
            self.queued.remove(0);
        }

        for _ in 0..64 {
            let tasks = self
                .deps
                .storage
                .scan_tasks(TaskQuery {
                    conversation_id: Some(self.conversation_id),
                    status: None,
                    kind: Some(TURN_TASK_KIND.into()),
                    limit: 1_000,
                })
                .await?;
            let Some(task) = crate::scheduler::select_task(&tasks) else {
                let now = self.deps.clock.now_ms();
                self.flush_due(now).await?;
                return Ok(WorkItem::Idle);
            };
            let work = self.task_work(task.clone()).await?;
            if let Some(work) = work {
                return Ok(work);
            }
        }
        Err(SessionError::InvalidState(
            "plan_next could not reach a stable state".into(),
        ))
    }

    async fn plan_review_continuation(&mut self) -> Result<WorkItem, SessionError> {
        // task_work may finish an observer or a hook-driven termination. If the
        // receipt was completed in that transition, report idle rather than
        // scheduling unrelated queued submissions in this drive.
        if self.active_review_receipt().await?.is_none() {
            return Ok(WorkItem::Idle);
        }
        Err(SessionError::InvalidState(
            "review continuation did not produce work".into(),
        ))
    }

    async fn task_work(&mut self, task: StoredTask) -> Result<Option<WorkItem>, SessionError> {
        let task_id = task.record.id;
        match &task.record.state {
            TaskState::Pending { .. } => {
                self.begin_task(&task).await?;
                let request = self.build_request().await?;
                Ok(Some(WorkItem::Generate {
                    task_id,
                    request,
                    attempt: 0,
                }))
            }
            TaskState::Completing { .. } => {
                self.finish_turn(task_id).await?;
                Ok(None)
            }
            TaskState::Running { .. } => self.running_task_work(task).await,
            TaskState::Waiting { .. } | TaskState::Sleeping { .. } => Ok(None),
            TaskState::Terminal { .. } => Ok(None),
        }
    }

    async fn running_task_work(
        &mut self,
        task: StoredTask,
    ) -> Result<Option<WorkItem>, SessionError> {
        let task_id = task.record.id;
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let assistants: Vec<&EntryRecord> = entries
            .iter()
            .filter(|entry| {
                entry.record.by_task_id == Some(task_id)
                    && entry.record.kind == entry_kind::ASSISTANT
            })
            .map(|entry| &entry.record)
            .collect();
        let round = task
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(assistants.len() as u64);
        let latest = assistants.last().copied();
        if let Some(latest) = latest {
            let latest_round = latest
                .data
                .get("round")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if latest_round == round {
                let calls: Vec<ToolCall> = serde_json::from_value(
                    latest
                        .data
                        .get("tool_calls")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|_| SessionError::InvalidState("malformed round calls".into()))?;
                if calls.is_empty() {
                    return Ok(Some(WorkItem::FinishTurn { task_id }));
                }
                let executed = validate_round_prefix(&entries, task_id, round, &calls)?;
                if executed < calls.len() {
                    let call = calls[executed].clone();
                    let tool = self.tools.get(&call.name);
                    let replay = tool
                        .as_ref()
                        .map(|tool| tool.replay())
                        .unwrap_or(ReplayPolicy::Unsafe);
                    return Ok(Some(WorkItem::ExecuteTool {
                        idempotency_key: tool_task::idempotency_key(&call.name, &call.id),
                        task_id,
                        tool_name: call.name.clone(),
                        replay,
                        call,
                        attempt: 0,
                    }));
                }
                if self.has_hook(rusty_bot_core::HookPoint::AfterTools) {
                    let mut results = Vec::with_capacity(calls.len());
                    for call in &calls {
                        let result = entries
                            .iter()
                            .find(|entry| {
                                entry.record.by_task_id == Some(task_id)
                                    && entry.record.kind == entry_kind::TOOL_RESULT
                                    && entry
                                        .record
                                        .data
                                        .get("round")
                                        .and_then(Value::as_u64)
                                        .unwrap_or(0)
                                        == round
                                    && entry
                                        .record
                                        .data
                                        .get("tool_call_id")
                                        .and_then(Value::as_str)
                                        == Some(call.id.as_str())
                            })
                            .ok_or_else(|| {
                                SessionError::InvalidState("missing tool result for round".into())
                            })?;
                        results.push(result.record.id);
                    }
                    let terminate = self
                        .hook_dispatcher(self.cancel.clone())
                        .after_tools(task_id, latest.id, &results)
                        .await?;
                    if terminate {
                        let first = self
                            .first_run_entry(&task.record, &entries)
                            .unwrap_or(latest.id);
                        let last = results.last().copied().unwrap_or(latest.id);
                        let mut record = task.record.clone();
                        record.memos.insert(
                            "after_tools_decision".into(),
                            json!({"round": round, "terminate": true}),
                        );
                        self.settle_success(
                            record,
                            task.record.version,
                            rusty_bot_core::RunSummary {
                                reason: rusty_bot_core::RunReason::Terminate,
                                first,
                                answer: latest.id,
                                last,
                            },
                            None,
                        )
                        .await?;
                        return Ok(None);
                    }
                }
                let next_round = round + 1;
                if next_round > u64::from(self.config.limits.max_tool_rounds) {
                    self.fail_task_for_limit(task).await?;
                    return Ok(None);
                }
                let mut record = task.record.clone();
                record.version += 1;
                record
                    .memos
                    .insert("generation_round".into(), json!(next_round));
                if self.has_hook(rusty_bot_core::HookPoint::AfterTools) {
                    record.memos.insert(
                        "after_tools_decision".into(),
                        json!({"round": round, "terminate": false}),
                    );
                }
                let deferred = self.deferred.drain();
                let mut tx = self.conversation_tx();
                tx.write(Write::Task(record))?;
                for write in deferred {
                    tx.write(Write::Document(write))?;
                }
                ensure_committed(tx.commit().await?)?;
                let request = self.build_request().await?;
                return Ok(Some(WorkItem::Generate {
                    task_id,
                    request,
                    attempt: 0,
                }));
            }
        }
        let request = self.build_request().await?;
        Ok(Some(WorkItem::Generate {
            task_id,
            request,
            attempt: 0,
        }))
    }

    async fn begin_task(&mut self, task: &StoredTask) -> Result<(), SessionError> {
        let now = self.deps.clock.now_ms();
        let mut record = task.record.clone();
        record.version += 1;
        record.state = TaskState::Running {
            checkpoint: None,
            wake: None,
        };
        record.started_at = Some(now);
        record.memos.insert("generation_round".into(), json!(0));

        let system_entry = self.system_entry(task.record.id).await?;
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(entry) = system_entry {
            let entry_id = entry.id;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::ENTRY_COMMITTED.into(),
                payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::SYSTEM }),
            }))?;
            tx.write(Write::Entry(entry))?;
        }
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, task.record.version).await
    }

    /// Append a system entry only when the effective prompt changed.
    async fn system_entry(&mut self, task_id: TaskId) -> Result<Option<EntryRecord>, SessionError> {
        let sections = self
            .deps
            .registry
            .resolve_sections(&self.resolved, self.config.instructions.as_deref());
        let tool_names: Vec<String> = self
            .model_specs()
            .into_iter()
            .map(|spec| spec.name)
            .collect();
        let fingerprint = prompt::fingerprint(&sections, &tool_names);
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        if let Some(last) = prompt::last_system_entry(&entries) {
            if last.record.data.get("fingerprint").and_then(Value::as_str)
                == Some(fingerprint.as_str())
            {
                return Ok(None);
            }
        }
        let id = EntryId(self.deps.storage.mint_id().await?);
        let sections_json = serde_json::to_value(&sections)
            .map_err(|error| SessionError::Internal(error.to_string()))?;
        Ok(Some(EntryRecord {
            id,
            conversation_id: self.conversation_id,
            kind: entry_kind::SYSTEM.into(),
            model: vec![prompt::system_message(&sections, &tool_names)],
            data: json!({
                "sections": sections_json,
                "tools_added": tool_names,
                "fingerprint": fingerprint,
            }),
            head: None,
            edits: Vec::new(),
            by_task_id: Some(task_id),
        }))
    }

    fn ask_user_enabled(&self) -> bool {
        fn permits(filter: &ToolFilter) -> bool {
            match filter {
                ToolFilter::All => true,
                ToolFilter::Only(names) => names.iter().any(|n| n == "ask_user"),
                ToolFilter::AddRemove { remove, .. } => !remove.iter().any(|n| n == "ask_user"),
            }
        }
        self.spec.review_enabled && permits(&self.config.tools) && permits(&self.config.model_tools)
    }

    fn model_specs(&self) -> Vec<rusty_bot_core::ToolSpec> {
        let mut specs = self.tools.model_specs();
        if self.ask_user_enabled() {
            specs.push(ask_user_spec());
        }
        specs
    }

    async fn build_request(&mut self) -> Result<rusty_bot_core::HistoryRequest, SessionError> {
        let mut metadata = json!({
            "session_id": self.spec.session_id.0,
            "conversation_id": self.conversation_id.0,
        });
        if self.config.thinking_level != ThinkingLevel::Off {
            metadata["model_options"] = json!({"thinking_level": self.config.thinking_level});
        }
        let request = prompt::build_history_request(
            self.deps.storage.as_ref(),
            self.conversation_id,
            &self.config.model.model_id,
            self.model_specs(),
            metadata,
        )
        .await?;
        Ok(crate::native_context::load(
            self.deps.storage.as_ref(),
            self.conversation_id,
            self.deps.provider.id(),
            &request,
        )
        .await?
        .map_or(request, |(_, reconstructed)| reconstructed))
    }

    async fn place_submission(&mut self, submission: SubmissionRecord) -> Result<(), SessionError> {
        let entry_id = EntryId(self.deps.storage.mint_id().await?);
        let task_id = TaskId(self.deps.storage.mint_id().await?);
        let content = if submission.kind == SubmissionKind::Input {
            canonical_input(&submission.payload)?
        } else {
            Vec::new()
        };
        let text = Message::text_content(&content);
        let (entry, task) = match submission.kind {
            SubmissionKind::Input => {
                let entry = EntryRecord {
                    id: entry_id,
                    conversation_id: self.conversation_id,
                    kind: entry_kind::USER.into(),
                    model: vec![Message::User { content }],
                    data: json!({
                        "submission_id": submission.id.0,
                        "text": text,
                    }),
                    head: None,
                    edits: Vec::new(),
                    by_task_id: Some(task_id),
                };
                let task = TaskRecord {
                    id: task_id,
                    conversation_id: self.conversation_id,
                    kind: TURN_TASK_KIND.into(),
                    version: 0,
                    input: json!({ "submission_id": submission.id.0 }),
                    owner: None,
                    background: false,
                    abort_requested: false,
                    abort_reason: None,
                    abandon_on_restart: false,
                    started_at: None,
                    ended_at: None,
                    restarts: 0,
                    state: TaskState::Pending {
                        checkpoint: None,
                        wake: None,
                    },
                    memos: BTreeMap::new(),
                };
                (Some(entry), Some(task))
            }
            SubmissionKind::Write => {
                let entry = EntryRecord {
                    id: entry_id,
                    conversation_id: self.conversation_id,
                    kind: entry_kind::INBOX.into(),
                    model: Vec::new(),
                    data: json!({ "submission_id": submission.id.0, "payload": submission.payload }),
                    head: None,
                    edits: Vec::new(),
                    by_task_id: Some(task_id),
                };
                (Some(entry), None)
            }
            SubmissionKind::Continue => {
                let task = TaskRecord {
                    id: task_id,
                    conversation_id: self.conversation_id,
                    kind: TURN_TASK_KIND.into(),
                    version: 0,
                    input: json!({ "submission_id": submission.id.0 }),
                    owner: None,
                    background: false,
                    abort_requested: false,
                    abort_reason: None,
                    abandon_on_restart: false,
                    started_at: None,
                    ended_at: None,
                    restarts: 0,
                    state: TaskState::Pending {
                        checkpoint: None,
                        wake: None,
                    },
                    memos: BTreeMap::new(),
                };
                (None, Some(task))
            }
        };

        let mut placed = submission.clone();
        placed.status = if task.is_some() {
            SubmissionStatus::Placed
        } else {
            SubmissionStatus::Done
        };
        placed.entry_id = entry.as_ref().map(|entry| entry.id);

        let deferred = self.deferred.drain();

        let mut tx = self.conversation_tx();
        if let Some(entry) = entry {
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::ENTRY_COMMITTED.into(),
                payload: json!({ "entry_id": entry.id.0, "kind": entry.kind }),
            }))?;
            tx.write(Write::Entry(entry))?;
        }
        if let Some(task) = &task {
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::TASK_CHANGED.into(),
                payload: task_event_payload(task),
            }))?;
            tx.write(Write::Task(task.clone()))?;
        }
        tx.write(Write::Submission(placed.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::SUBMISSION_PLACED.into(),
            payload: rusty_bot_core::storage::submission_event_payload(&placed),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        ensure_committed(tx.commit().await?)?;
        Ok(())
    }

    pub async fn complete_generation(
        &mut self,
        task_id: TaskId,
        turn: rusty_bot_core::AssistantTurn,
    ) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        let turn = rusty_bot_core::history_turn_from_legacy(turn)
            .map_err(|e| SessionError::ContractViolation(e.to_string()))?;
        self.complete_history_generation(task_id, turn).await
    }

    pub async fn complete_history_generation(
        &mut self,
        task_id: TaskId,
        turn: rusty_bot_core::HistoryTurn,
    ) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        if self.engine_attested_task != Some(task_id) {
            let request = self.build_request().await?;
            if crate::native_context::load(
                self.deps.storage.as_ref(),
                self.conversation_id,
                self.deps.provider.id(),
                &request,
            )
            .await?
            .is_some()
            {
                return Err(SessionError::ContractViolation(
                    "manual completion unavailable with native context".into(),
                ));
            }
        }
        self.ensure_supported_hooks()?;
        let calls = rusty_bot_core::canonical_tool_calls(&turn.message)
            .map_err(|e| SessionError::ContractViolation(e.to_string()))?;
        if let Message::Rich { parts, .. } = &turn.message {
            for part in parts {
                let state = match part {
                    rusty_bot_core::HistoryPart::ProviderState { state } => Some(state),
                    rusty_bot_core::HistoryPart::Reasoning {
                        state: Some(state), ..
                    } => Some(state),
                    _ => None,
                };
                if state.is_some_and(|s| {
                    s.provider.trim().is_empty()
                        || s.format.trim().is_empty()
                        || s.model.as_deref().is_none_or(str::is_empty)
                }) {
                    return Err(SessionError::ContractViolation(
                        "invalid provider state".into(),
                    ));
                }
            }
            // Preserve the legacy rich-history boundary unless a selected tool
            // hook needs synthetic failures delivered through its pipeline.
            if !self.has_hook(rusty_bot_core::HookPoint::BeforeTool)
                && !self.has_hook(rusty_bot_core::HookPoint::AfterTool)
            {
                for call in &calls {
                    if call.name == "ask_user" && self.ask_user_enabled() {
                        if !valid_question(&call.arguments) {
                            return Err(SessionError::ContractViolation(
                                "invalid tool arguments".into(),
                            ));
                        }
                        continue;
                    }
                    if !self.tools.model_visible.contains(&call.name) {
                        return Err(SessionError::ContractViolation("undeclared tool".into()));
                    }
                    let tool = self
                        .tools
                        .get(&call.name)
                        .ok_or_else(|| SessionError::ContractViolation("undeclared tool".into()))?;
                    let spec = tool.spec();
                    let validator = jsonschema::validator_for(&spec.parameters).map_err(|_| {
                        SessionError::ContractViolation("invalid tool schema".into())
                    })?;
                    if !validator.is_valid(&call.arguments) {
                        return Err(SessionError::ContractViolation(
                            "invalid tool arguments".into(),
                        ));
                    }
                }
            }
        }
        let Some(stored) = self.deps.storage.get_task(task_id).await? else {
            return Err(SessionError::ContractViolation(format!(
                "unknown task {}",
                task_id.0
            )));
        };
        if stored.record.state.is_terminal() {
            return Ok(());
        }
        let round = stored
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let already = entries.iter().any(|entry| {
            entry.record.by_task_id == Some(task_id)
                && entry.record.kind == entry_kind::ASSISTANT
                && entry.record.data.get("round").and_then(Value::as_u64) == Some(round)
        });
        if already {
            return Ok(());
        }

        let entry_id = EntryId(self.deps.storage.mint_id().await?);
        let text = match &turn.message {
            Message::Assistant { content, .. } => Message::text_content(content),
            Message::Rich { parts, .. } => parts
                .iter()
                .filter_map(|part| match part {
                    rusty_bot_core::HistoryPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(""),
            _ => String::new(),
        };
        let entry = EntryRecord {
            id: entry_id,
            conversation_id: self.conversation_id,
            kind: entry_kind::ASSISTANT.into(),
            model: vec![turn.message.clone()],
            data: json!({
                "text": text,
                "tool_calls": calls,
                "usage": turn.usage,
                "stop": turn.stop,
                "round": round,
                "provider_metadata": turn.provider_metadata,
            }),
            head: None,
            edits: Vec::new(),
            by_task_id: Some(task_id),
        };
        if calls.is_empty()
            && (self.has_hook(rusty_bot_core::HookPoint::OnYield)
                || self.has_hook(rusty_bot_core::HookPoint::AfterRun)
                || self.has_hook(rusty_bot_core::HookPoint::AfterTools))
        {
            let continuation = self
                .hook_dispatcher(self.cancel.clone())
                .on_yield(task_id, &turn, self.deps.provider.image_limits().as_ref())
                .await?;
            // Named arbitration cut: only the committed queue snapshot here has priority.
            let snapshot =
                crate::submission::recover_queued(self.deps.storage.as_ref(), self.spec.session_id)
                    .await?;
            // plan_next may already have placed a queued user as a pending task.
            // Such an input is still pending at the arbitration boundary.
            let pending_tasks = self
                .deps
                .storage
                .scan_tasks(TaskQuery {
                    conversation_id: Some(self.conversation_id),
                    status: None,
                    kind: Some(TURN_TASK_KIND.into()),
                    limit: 1_000,
                })
                .await?;
            let mut user_waiting = false;
            for task in pending_tasks.iter().filter(|task| {
                task.record.id != task_id && matches!(task.record.state, TaskState::Pending { .. })
            }) {
                if let Some(id) = submission_id_of(&task.record) {
                    if let Some(item) = self.deps.storage.get_submission(id).await? {
                        if item.record.status == SubmissionStatus::Placed
                            && item.record.kind == SubmissionKind::Input
                        {
                            user_waiting = true;
                            break;
                        }
                    }
                }
            }
            for id in snapshot {
                if let Some(item) = self.deps.storage.get_submission(id).await? {
                    if item.record.status == SubmissionStatus::Queued
                        && item.record.kind == SubmissionKind::Input
                    {
                        user_waiting = true;
                        break;
                    }
                }
            }
            if !user_waiting {
                if let Some(content) = continuation {
                    let count = stored
                        .record
                        .memos
                        .get("hook_continuations")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if count >= u64::from(self.spec.hook_yield_limit) {
                        return self
                            .fail_generation(
                                task_id,
                                ModelError::new(
                                    ModelErrorKind::InvalidRequest,
                                    "hook continuation limit exceeded",
                                    false,
                                ),
                            )
                            .await;
                    }
                    let user_id = EntryId(self.deps.storage.mint_id().await?);
                    let user = EntryRecord {
                        id: user_id,
                        conversation_id: self.conversation_id,
                        kind: entry_kind::USER.into(),
                        model: vec![Message::User {
                            content: content.clone(),
                        }],
                        data: json!({"hook_continuation": true, "text": Message::text_content(&content)}),
                        head: None,
                        edits: Vec::new(),
                        by_task_id: Some(task_id),
                    };
                    let mut record = stored.record.clone();
                    record.version += 1;
                    record
                        .memos
                        .insert("hook_continuations".into(), json!(count + 1));
                    record
                        .memos
                        .insert("generation_round".into(), json!(round + 1));
                    record.state = TaskState::Running {
                        checkpoint: None,
                        wake: None,
                    };
                    let deferred = self.deferred.drain();
                    let mut tx = self.conversation_tx();
                    tx.write(Write::Entry(entry))?;
                    tx.write(Write::Event(SessionEventDraft {
                        kind: event_kind::ENTRY_COMMITTED.into(),
                        payload: json!({"entry_id": entry_id.0, "kind": entry_kind::ASSISTANT}),
                    }))?;
                    tx.write(Write::Entry(user))?;
                    tx.write(Write::Event(SessionEventDraft {
                        kind: event_kind::ENTRY_COMMITTED.into(),
                        payload: json!({"entry_id": user_id.0, "kind": entry_kind::USER}),
                    }))?;
                    tx.write(Write::Task(record))?;
                    for write in deferred {
                        tx.write(Write::Document(write))?;
                    }
                    let receipt = tx.commit().await?;
                    return self
                        .handle_task_receipt(receipt, stored.record.version)
                        .await;
                }
            }
            let first = self
                .first_run_entry(&stored.record, &entries)
                .unwrap_or(entry_id);
            let summary = rusty_bot_core::RunSummary {
                reason: rusty_bot_core::RunReason::Answer,
                first,
                answer: entry_id,
                last: entry_id,
            };
            return self
                .settle_success(
                    stored.record.clone(),
                    stored.record.version,
                    summary,
                    Some(entry),
                )
                .await;
        }
        let mut record = stored.record.clone();
        record.version += 1;
        record.state = TaskState::Running {
            checkpoint: if calls.is_empty() {
                None
            } else {
                Some(ToolCheckpoint::Call)
            },
            wake: None,
        };

        let deferred = self.deferred.drain();

        let mut tx = self.conversation_tx();
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::ENTRY_COMMITTED.into(),
            payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::ASSISTANT }),
        }))?;
        tx.write(Write::Entry(entry))?;
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        // The final partial document rides along with the assistant commit.
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, stored.record.version)
            .await
    }

    pub async fn fail_generation(
        &mut self,
        task_id: TaskId,
        error: ModelError,
    ) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        let Some(stored) = self.deps.storage.get_task(task_id).await? else {
            return Err(SessionError::ContractViolation(format!(
                "unknown task {}",
                task_id.0
            )));
        };
        if stored.record.state.is_terminal() {
            return Ok(());
        }
        let now = self.deps.clock.now_ms();
        let entry_id = EntryId(self.deps.storage.mint_id().await?);
        let message = redact_secrets(&error.message, &[]);
        let cancelled = error.kind == ModelErrorKind::Cancelled;
        let outcome = if cancelled {
            TaskOutcome::Aborted {
                reason: Some("cancelled".into()),
                result: None,
            }
        } else {
            TaskOutcome::Failed {
                error: message.clone(),
                result: None,
            }
        };
        let mut record = stored.record.clone();
        record.version += 1;
        record.state = TaskState::Terminal {
            outcome: outcome.clone(),
        };
        record.ended_at = Some(now);
        let round = record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let entry = EntryRecord {
            id: entry_id,
            conversation_id: self.conversation_id,
            kind: entry_kind::ERROR.into(),
            model: Vec::new(),
            data: json!({
                "error": {
                    "kind": error.kind,
                    "message": message,
                    "retryable": error.retryable,
                    "status": error.status,
                },
                "round": round,
            }),
            head: None,
            edits: Vec::new(),
            by_task_id: Some(task_id),
        };

        let completed_review = self
            .review_disposition_write(
                task_id,
                crate::review_state::ReceiptDisposition::TerminalFailure,
            )
            .await?;
        let deferred = self.deferred.drain();

        let mut tx = self.conversation_tx();
        if let Some(write) = completed_review {
            tx.write(Write::Document(write))?;
        }
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::ENTRY_COMMITTED.into(),
            payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::ERROR }),
        }))?;
        tx.write(Write::Entry(entry))?;
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        if let Some(submission) = self.settled_submission(&record, Some(message)).await? {
            tx.write(Write::Submission(submission.clone()))?;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::SUBMISSION_DONE.into(),
                payload: rusty_bot_core::storage::submission_event_payload(&submission),
            }))?;
        }
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, stored.record.version)
            .await
    }

    pub async fn execute_tool(
        &mut self,
        task_id: TaskId,
        call: ToolCall,
    ) -> Result<ToolRunOutcome, SessionError> {
        self.guard_review_host().await?;
        match self.prepare_tool_effect(task_id, call).await? {
            PreparedTool::Effect(effect) => {
                let (identity, result) = effect.execute().await;
                match result {
                    InvocationResult::Returned(result) => {
                        self.settle_tool_effect(&identity, result, false).await
                    }
                    InvocationResult::Interrupted(_) => {
                        self.mark_effect_uncertain(identity.stored.record.id, &identity.key)
                            .await?;
                        Err(SessionError::InterruptUnsupported)
                    }
                    InvocationResult::ProtocolViolation => {
                        self.mark_effect_uncertain(identity.stored.record.id, &identity.key)
                            .await?;
                        Err(SessionError::InterruptProtocol(
                            "tool did not propagate interrupt or replay diverged".into(),
                        ))
                    }
                }
            }
            prepared => self.settle_prepared_tool(prepared, false).await,
        }
    }

    // The round driver must drain active effects before finalizing the task.
    // This settles non-effect preparations in the caller's chosen order.
    async fn settle_prepared_tool(
        &mut self,
        prepared: PreparedTool,
        defer_terminal: bool,
    ) -> Result<ToolRunOutcome, SessionError> {
        match prepared {
            PreparedTool::Effect(_) => Err(SessionError::Internal(
                "cannot settle an unexecuted tool effect".into(),
            )),
            PreparedTool::Review { .. } => Err(SessionError::InvalidState(
                "review requires the round driver".into(),
            )),
            PreparedTool::Synthetic {
                stored,
                call,
                round,
                output,
            } => {
                let has_more = self
                    .has_more_calls(stored.record.id, round, &call.id)
                    .await?;
                self.commit_tool_outcome(
                    &stored,
                    &call,
                    round,
                    output,
                    ToolOperationStatus::Failed,
                    false,
                    has_more,
                )
                .await?;
                Ok(ToolRunOutcome {
                    is_error: true,
                    interrupted: false,
                })
            }
            PreparedTool::Reused {
                stored,
                call,
                round,
                output,
                status,
            } => {
                let has_more = self
                    .has_more_calls(stored.record.id, round, &call.id)
                    .await?;
                let is_error = self
                    .commit_tool_outcome(&stored, &call, round, output, status, false, has_more)
                    .await?;
                Ok(ToolRunOutcome {
                    is_error,
                    interrupted: false,
                })
            }
            PreparedTool::Interrupted {
                stored,
                call,
                round,
                message,
            } => {
                self.commit_interrupted(&stored, &call, round, &message, defer_terminal)
                    .await
            }
            PreparedTool::AlreadySettled(outcome) => Ok(outcome),
        }
    }

    async fn prepare_tool_effect(
        &mut self,
        task_id: TaskId,
        call: ToolCall,
    ) -> Result<PreparedTool, SessionError> {
        self.prepare_tool_effect_with_metadata(task_id, call, false)
            .await
    }

    async fn prepare_tool_effect_with_metadata(
        &mut self,
        task_id: TaskId,
        call: ToolCall,
        parallel: bool,
    ) -> Result<PreparedTool, SessionError> {
        self.ensure_supported_hooks()?;
        if let Some(prepared) = self.prepare_deferred_effect(task_id, &call).await? {
            return Ok(prepared);
        }
        let Some(stored) = self.deps.storage.get_task(task_id).await? else {
            return Err(SessionError::ContractViolation(format!(
                "unknown task {}",
                task_id.0
            )));
        };
        if stored.record.state.is_terminal() {
            return Ok(PreparedTool::AlreadySettled(ToolRunOutcome::default()));
        }
        let key = tool_task::idempotency_key(&call.name, &call.id);
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        // Idempotent retry: the tool result is already durable.
        if let Some(result) = entries.iter().find(|entry| {
            entry.record.by_task_id == Some(task_id)
                && entry.record.kind == entry_kind::TOOL_RESULT
                && entry
                    .record
                    .data
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    == Some(&call.id)
        }) {
            return Ok(PreparedTool::AlreadySettled(ToolRunOutcome {
                is_error: result
                    .record
                    .data
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                interrupted: result
                    .record
                    .data
                    .get("interrupted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }));
        }

        let round = stored
            .record
            .memos
            .get("generation_round")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let tool = if self.tools.model_visible.contains(&call.name) {
            self.tools.get(&call.name)
        } else {
            None
        };
        let tool_replay = tool
            .as_ref()
            .map(|tool| tool.replay())
            .unwrap_or(ReplayPolicy::Unsafe);
        let operation = self.deps.storage.get_operation(task_id, &key).await?;
        let checkpoint_replay = match operation
            .as_ref()
            .map(tool_task::recorded_replay)
            .transpose()
        {
            Ok(Some(Some(replay))) => replay,
            Ok(_) => stored
                .record
                .state
                .checkpoint()
                .and_then(|checkpoint| checkpoint.replay())
                .unwrap_or(tool_replay),
            Err(()) => {
                return Err(SessionError::ContractViolation(
                    "invalid parallel tool intent metadata".into(),
                ))
            }
        };

        let approved = if self.sdk_review_driver {
            if let Some(receipt) = self.active_review_receipt().await? {
                let p = &receipt.pending;
                if p.pending.task_id == task_id
                    && p.pending.round == round
                    && p.binding.original_call.id == call.id
                {
                    let assistant = entries
                        .iter()
                        .find(|entry| entry.record.id == p.assistant_entry);
                    let calls: Vec<ToolCall> = assistant
                        .and_then(|entry| entry.record.data.get("tool_calls"))
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .ok_or_else(|| {
                            SessionError::InvalidState("missing accepted review assistant".into())
                        })?;
                    if p.version != 1
                        || p.pending.session_id != self.spec.session_id
                        || p.pending.conversation_id != self.conversation_id
                        || p.binding.assistant_entry != p.assistant_entry
                        || p.binding.original_call != call
                        || p.original_arguments != call.arguments
                        || calls.get(p.pending.ordinal) != Some(&call)
                        || validate_round_prefix(&entries, task_id, round, &calls)?
                            != p.pending.ordinal
                        || !assistant.is_some_and(|entry| {
                            entry.record.by_task_id == Some(task_id)
                                && entry.record.kind == entry_kind::ASSISTANT
                                && entry
                                    .record
                                    .data
                                    .get("round")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0)
                                    == round
                        })
                        || !matches!(&p.pending.action, rusty_bot_core::ReviewAction::Tool { pub_call } if pub_call.id == call.id && pub_call.name == call.name && pub_call.arguments == p.final_arguments)
                        || !tool.as_ref().is_some_and(|t| {
                            p.binding.tool
                                == crate::review_state::ToolSnapshot {
                                    spec: t.spec(),
                                    replay: t.replay(),
                                    mode: t.execution_mode(),
                                }
                        })
                        || p.binding.pending != p.pending
                        || p.binding.final_arguments != p.final_arguments
                        || crate::review_state::fingerprint(
                            &serde_json::to_value(&p.binding).map_err(|_| {
                                SessionError::InvalidState("invalid review binding".into())
                            })?,
                        )? != p.fingerprint
                    {
                        return Err(SessionError::InvalidState(
                            "invalid accepted review binding".into(),
                        ));
                    }
                    Some(match &receipt.answer.decision {
                        rusty_bot_core::ReviewDecision::Approve
                            if p.pending
                                .allowed_decisions
                                .contains(&rusty_bot_core::DecisionKind::Approve) =>
                        {
                            p.final_arguments.clone()
                        }
                        rusty_bot_core::ReviewDecision::Edit { arguments }
                            if p.pending
                                .allowed_decisions
                                .contains(&rusty_bot_core::DecisionKind::Edit) =>
                        {
                            arguments.clone()
                        }
                        _ => {
                            return Err(SessionError::InvalidState(
                                "invalid accepted review decision".into(),
                            ))
                        }
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let reexecute = match tool_task::recovery_decision(
            operation.as_ref(),
            checkpoint_replay,
            tool_replay,
        ) {
            RecoveryDecision::Execute => false,
            RecoveryDecision::Reuse => {
                let operation = operation.expect("reuse requires an operation");
                let output = output_from_recorded(&operation)?;
                validate_tool_images(&output).await.map_err(|reason| {
                    SessionError::ContractViolation(format!(
                        "invalid recorded tool image: {reason}"
                    ))
                })?;
                return Ok(PreparedTool::Reused {
                    stored,
                    call,
                    round,
                    output,
                    status: operation.status,
                });
            }
            RecoveryDecision::Interrupt => {
                return Ok(PreparedTool::Interrupted {
                    stored,
                    call,
                    round,
                    message:
                        "tool execution interrupted: effect may have occurred and was not repeated"
                            .into(),
                });
            }
            RecoveryDecision::Reexecute => true,
        };

        if self.sdk_review_driver && self.spec.review_enabled && operation.is_some() && !reexecute {
            return Err(SessionError::InvalidState(
                "unexpected review operation".into(),
            ));
        }

        // Once intent exists, the journal is the source of truth for the
        // effect, not the original model-visible call. In particular a hook
        // may have rewritten the arguments before recording intent.
        let mut execution_arguments = if reexecute {
            operation
                .as_ref()
                .expect("reexecution requires an operation")
                .request
                .clone()
        } else {
            approved.clone().unwrap_or_else(|| call.arguments.clone())
        };
        let question = call.name == "ask_user" && self.ask_user_enabled();
        if tool.is_none() && !question && !reexecute {
            let output = ToolOutput::error(format!("unknown tool: {}", call.name), "unknown_tool");
            return Ok(PreparedTool::Synthetic {
                stored,
                call,
                round,
                output,
            });
        }
        if question && !valid_question(&execution_arguments) {
            return Ok(PreparedTool::Synthetic {
                stored,
                call,
                round,
                output: ToolOutput::error("invalid question", "invalid_arguments"),
            });
        }
        if let Some(tool) = &tool {
            if let Err(message) = validate_arguments(tool, &execution_arguments) {
                let output = ToolOutput {
                    content: prompt::text_content(&format!(
                        "invalid arguments for {}: {message}",
                        call.name
                    )),
                    details: Some(json!({ "code": "invalid_arguments" })),
                    is_error: true,
                };
                return Ok(PreparedTool::Synthetic {
                    stored,
                    call,
                    round,
                    output,
                });
            }
        }

        if !reexecute && approved.is_none() {
            let mut gated = call.clone();
            let blocked = self
                .hook_dispatcher(self.cancel.clone())
                .before_tool(task_id, &mut gated)
                .await?;
            let invalid = match &tool {
                Some(tool) => validate_arguments(tool, &gated.arguments).err(),
                None if question && !valid_question(&gated.arguments) => {
                    Some("invalid question".into())
                }
                None => None,
            };
            if let Some(message) = blocked.or(invalid) {
                let output = ToolOutput::error(message, "blocked_or_invalid_arguments");
                return Ok(PreparedTool::Synthetic {
                    stored,
                    call,
                    round,
                    output,
                });
            }
            if self.sdk_review_driver && self.spec.review_enabled {
                use rusty_bot_core::DecisionKind;
                let allowed = if question {
                    Some(
                        [DecisionKind::Respond, DecisionKind::Reject]
                            .into_iter()
                            .collect(),
                    )
                } else if let Some(selected) = self.resolved.selected_review_gate()? {
                    let ctx = rusty_bot_core::HookContext::new(
                        self.spec.session_id,
                        self.conversation_id,
                        task_id,
                        task_id,
                        selected.descriptor.config.clone(),
                        self.cancel.clone(),
                        self.deps.storage.clone(),
                    );
                    let request = AssertUnwindSafe(selected.gate.review(&gated, &ctx))
                        .catch_unwind()
                        .await
                        .map_err(|_| SessionError::InvalidState("review gate failed".into()))?
                        .map_err(|_| SessionError::InvalidState("review gate failed".into()))?;
                    request.map(|request| request.allowed_decisions)
                } else {
                    None
                };
                if let Some(allowed) = allowed {
                    if allowed.is_empty()
                        || (!question
                            && allowed.iter().any(|d| {
                                !matches!(
                                    d,
                                    DecisionKind::Approve
                                        | DecisionKind::Edit
                                        | DecisionKind::Reject
                                )
                            }))
                    {
                        return Err(SessionError::InvalidState("invalid review policy".into()));
                    }
                    return Ok(PreparedTool::Review {
                        call,
                        final_call: gated,
                        allowed,
                    });
                }
            }
            execution_arguments = gated.arguments;
        }
        let now = self.deps.clock.now_ms();
        let mut record = stored.record.clone();
        record.version += 1;
        record.state = TaskState::Running {
            checkpoint: Some(ToolCheckpoint::Execute {
                arguments: execution_arguments.clone(),
                replay: tool_replay,
                wake: None,
                checkpoint: None,
            }),
            wake: None,
        };
        let operation = ToolOperationRecord {
            task_id,
            idempotency_key: key.clone(),
            tool_name: call.name.clone(),
            status: if reexecute {
                ToolOperationStatus::Running
            } else {
                ToolOperationStatus::Intent
            },
            request: execution_arguments.clone(),
            result: if parallel
                || operation
                    .as_ref()
                    .is_some_and(|op| matches!(tool_task::recorded_replay(op), Ok(Some(_))))
            {
                Some(tool_task::parallel_intent(
                    if reexecute {
                        checkpoint_replay
                    } else {
                        tool_replay
                    },
                    reexecute,
                ))
            } else if reexecute {
                Some(json!({ "replayed": true }))
            } else {
                None
            },
            updated_at: now,
        };
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::ToolOperation(operation))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TOOL_STARTED.into(),
            payload: json!({
                "task_id": task_id.0,
                "tool_call_id": call.id,
                "tool_name": call.name,
                "idempotency_key": key,
                "replayed": reexecute,
            }),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, stored.record.version)
            .await?;

        if self.cancel.is_cancelled() {
            return Ok(PreparedTool::Interrupted {
                stored,
                call,
                round,
                message:
                    "tool execution interrupted: effect may have occurred and was not repeated"
                        .into(),
            });
        }

        let context = ToolContext {
            session_id: self.spec.session_id,
            conversation_id: self.conversation_id,
            task_id,
            files: self.deps.workspace.files(),
            shell: self.deps.workspace.execution(),
            live: Arc::clone(&self.deps.live),
            cancel: self.cancel.clone(),
            now_ms: self.deps.clock.now_ms(),
            image_limits: self.deps.provider.image_limits(),
            interrupt_port: None,
        };
        Ok(PreparedTool::Effect(PreparedToolEffect {
            identity: ToolEffectIdentity {
                stored,
                call,
                round,
                key,
                _replay: tool_replay,
            },
            tool,
            arguments: execution_arguments,
            context,
            interrupt_answers: self.interrupt_effect_driver.then(Vec::new),
        }))
    }

    async fn settle_tool_effect(
        &mut self,
        identity: &ToolEffectIdentity,
        result: Result<ToolOutput, rusty_bot_core::ToolError>,
        defer_terminal: bool,
    ) -> Result<ToolRunOutcome, SessionError> {
        let ToolEffectIdentity {
            stored,
            call,
            round,
            key,
            _replay: _,
        } = identity;
        debug_assert_eq!(*key, tool_task::idempotency_key(&call.name, &call.id));
        let round = *round;
        let task_id = stored.record.id;
        let output = match result {
            Ok(output) => output,
            Err(error) if error.code == "aborted" => {
                return self
                    .commit_interrupted(stored, call, round, &error.message, defer_terminal)
                    .await;
            }
            Err(error) => ToolOutput {
                content: prompt::text_content(&error.message),
                details: Some(json!({
                    "code": error.code,
                    "retryable": error.retryable,
                })),
                is_error: true,
            },
        };

        // The built-in shell preserves Pi-compatible output (including null
        // details) for aborts. Recognize that output only when cancellation was
        // actually requested, so the operation journal records interruption
        // instead of incorrectly treating a killed subprocess as a normal error.
        let builtin_shell_aborted = matches!(call.name.as_str(), "bash" | "powershell")
            && self.cancel.is_cancelled()
            && output.is_error
            && output.details.is_none()
            && output.text_content().ends_with("Command aborted");
        if builtin_shell_aborted
            || output
                .details
                .as_ref()
                .and_then(|details| details.get("code"))
                .and_then(Value::as_str)
                == Some("aborted")
        {
            return self
                .commit_interrupted(stored, call, round, &output.text_content(), defer_terminal)
                .await;
        }
        let uncertain = output
            .details
            .as_ref()
            .and_then(|details| details.get("code"))
            .and_then(Value::as_str)
            == Some("timeout");
        let status = if uncertain {
            ToolOperationStatus::Uncertain
        } else {
            tool_task::completion_status(&output)
        };
        let has_more = self.has_more_calls(task_id, round, &call.id).await?;
        let is_error = self
            .commit_tool_outcome(stored, call, round, output, status, false, has_more)
            .await?;
        Ok(ToolRunOutcome {
            is_error,
            interrupted: false,
        })
    }

    async fn has_more_calls(
        &self,
        task_id: TaskId,
        round: u64,
        executed_call_id: &str,
    ) -> Result<bool, SessionError> {
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let calls: Vec<ToolCall> = entries
            .iter()
            .rfind(|entry| {
                entry.record.by_task_id == Some(task_id)
                    && entry.record.kind == entry_kind::ASSISTANT
                    && entry.record.data.get("round").and_then(Value::as_u64) == Some(round)
            })
            .and_then(|entry| entry.record.data.get("tool_calls").cloned())
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default();
        let mut executed: Vec<String> = entries
            .iter()
            .filter(|entry| {
                entry.record.by_task_id == Some(task_id)
                    && entry.record.kind == entry_kind::TOOL_RESULT
                    && entry.record.data.get("round").and_then(Value::as_u64) == Some(round)
            })
            .filter_map(|entry| {
                entry
                    .record
                    .data
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
        if !executed.iter().any(|id| id == executed_call_id) {
            executed.push(executed_call_id.to_string());
        }
        Ok(executed.len() < calls.len())
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_tool_outcome(
        &mut self,
        stored: &StoredTask,
        call: &ToolCall,
        round: u64,
        mut output: ToolOutput,
        status: ToolOperationStatus,
        interrupted: bool,
        has_more: bool,
    ) -> Result<bool, SessionError> {
        let existing = self
            .deps
            .storage
            .get_operation(
                stored.record.id,
                &tool_task::idempotency_key(&call.name, &call.id),
            )
            .await?;
        if !existing.as_ref().is_some_and(|op| {
            matches!(
                op.status,
                ToolOperationStatus::Succeeded
                    | ToolOperationStatus::Failed
                    | ToolOperationStatus::Uncertain
            )
        }) {
            self.hook_dispatcher(self.cancel.clone())
                .after_tool(stored.record.id, call, &mut output)
                .await?;
        }
        if let Err(reason) = validate_tool_images(&output).await {
            output = ToolOutput::error(reason, "invalid_image");
        }
        let is_error = output.is_error;
        let current = self
            .deps
            .storage
            .get_task(stored.record.id)
            .await?
            .ok_or_else(|| {
                SessionError::ContractViolation(format!("unknown task {}", stored.record.id.0))
            })?;
        let now = self.deps.clock.now_ms();
        let entry_id = EntryId(self.deps.storage.mint_id().await?);
        let key = tool_task::idempotency_key(&call.name, &call.id);
        let entry = tool_result_entry(
            entry_id,
            self.conversation_id,
            stored.record.id,
            call,
            &output,
            round,
            interrupted,
        );
        let mut record = current.record.clone();
        record.version += 1;
        record.state = TaskState::Running {
            checkpoint: if has_more {
                Some(ToolCheckpoint::Call)
            } else {
                None
            },
            wake: None,
        };
        let request = self
            .deps
            .storage
            .get_operation(stored.record.id, &key)
            .await?
            .map(|operation| operation.request)
            .unwrap_or_else(|| call.arguments.clone());
        let operation = ToolOperationRecord {
            task_id: stored.record.id,
            idempotency_key: key.clone(),
            tool_name: call.name.clone(),
            status,
            request,
            result: Some(json!({
                "version": 2,
                "content": output.content,
                "details": output.details,
                "is_error": output.is_error,
            })),
            updated_at: now,
        };
        let deferred = self.deferred.drain();
        let interrupt_writes = if self.interrupt_effect_driver {
            self.interrupt_completion_writes(stored.record.id, &call.id)
                .await?
        } else {
            Vec::new()
        };
        let mut tx = self.conversation_tx();
        for write in interrupt_writes {
            tx.write(write)?;
        }
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::ENTRY_COMMITTED.into(),
            payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::TOOL_RESULT }),
        }))?;
        tx.write(Write::Entry(entry))?;
        tx.write(Write::ToolOperation(operation))?;
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TOOL_FINISHED.into(),
            payload: json!({
                "task_id": stored.record.id.0,
                "tool_call_id": call.id,
                "tool_name": call.name,
                "status": status,
            }),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, current.record.version)
            .await?;
        Ok(is_error)
    }

    // Do not fabricate a tool result for unsupported pauses or swallowed
    // control signals. Retain the intent metadata for conservative recovery.
    async fn mark_effect_uncertain(
        &mut self,
        task_id: TaskId,
        key: &str,
    ) -> Result<(), SessionError> {
        let mut operation = self
            .deps
            .storage
            .get_operation(task_id, key)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing tool intent".into()))?;
        operation.status = ToolOperationStatus::Uncertain;
        // An explicitly uncertain interrupt-driver effect must not be replayed
        // merely because its declared replay policy is Safe.
        if self.interrupt_effect_driver {
            if let Ok(Some(replay)) = tool_task::recorded_replay(&operation) {
                operation.result = Some(tool_task::parallel_intent(replay, true));
            } else {
                operation.result = Some(json!({"replayed": true}));
            }
        }
        operation.updated_at = self.deps.clock.now_ms();
        let mut tx = self.conversation_tx();
        tx.write(Write::ToolOperation(operation))?;
        tx.commit().await?;
        Ok(())
    }

    async fn commit_interrupted(
        &mut self,
        stored: &StoredTask,
        call: &ToolCall,
        round: u64,
        message: &str,
        defer_terminal: bool,
    ) -> Result<ToolRunOutcome, SessionError> {
        let current = self
            .deps
            .storage
            .get_task(stored.record.id)
            .await?
            .ok_or_else(|| {
                SessionError::ContractViolation(format!("unknown task {}", stored.record.id.0))
            })?;
        let now = self.deps.clock.now_ms();
        let entry_id = EntryId(self.deps.storage.mint_id().await?);
        let key = tool_task::idempotency_key(&call.name, &call.id);
        let mut output = ToolOutput::error(message.to_string(), "interrupted");
        let hook_suppressed = stored.record.restarts >= self.config.limits.max_restarts;
        if !self.cancel.is_cancelled() && !hook_suppressed {
            self.hook_dispatcher(self.cancel.clone())
                .after_tool(stored.record.id, call, &mut output)
                .await?;
        }
        if let Err(reason) = validate_tool_images(&output).await {
            output = ToolOutput::error(reason, "invalid_image");
        }
        let entry = tool_result_entry(
            entry_id,
            self.conversation_id,
            stored.record.id,
            call,
            &output,
            round,
            true,
        );
        let mut record = current.record.clone();
        record.version += 1;
        if defer_terminal {
            record.state = TaskState::Running {
                checkpoint: Some(ToolCheckpoint::Call),
                wake: None,
            };
        } else {
            finalize_interrupted_task(&mut record, message, now);
        }
        let prior = self
            .deps
            .storage
            .get_operation(stored.record.id, &key)
            .await?;
        let request = prior
            .as_ref()
            .map(|operation| operation.request.clone())
            .unwrap_or_else(|| call.arguments.clone());
        let mut result = json!({
            "version": 2,
            "content": output.content,
            "details": output.details,
            "is_error": output.is_error,
            "hook_suppressed": hook_suppressed,
        });
        if prior
            .as_ref()
            .and_then(|op| op.result.as_ref())
            .and_then(|value| value.get("replayed"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            result["replayed"] = json!(true);
        }
        if let Some(metadata) = prior
            .as_ref()
            .and_then(|op| op.result.as_ref())
            .and_then(|value| value.get("rusty_bot_parallel_intent"))
        {
            let mut metadata = metadata.clone();
            if self.cancel.is_cancelled() {
                // An observed cancellation or round failure must not make a
                // previously observed effect eligible for another execution.
                metadata["replayed"] = json!(true);
            }
            result["replayed"] = metadata.get("replayed").cloned().unwrap_or(json!(true));
            result["rusty_bot_parallel_intent"] = metadata;
        }
        let operation = ToolOperationRecord {
            task_id: stored.record.id,
            idempotency_key: key.clone(),
            tool_name: call.name.clone(),
            status: ToolOperationStatus::Interrupted,
            request,
            result: Some(result),
            updated_at: now,
        };
        let interrupt_writes = if self.interrupt_effect_driver {
            self.interrupt_completion_writes(stored.record.id, &call.id)
                .await?
        } else {
            Vec::new()
        };
        let failure_review = if !defer_terminal {
            self.review_disposition_write(
                stored.record.id,
                crate::review_state::ReceiptDisposition::TerminalFailure,
            )
            .await?
        } else {
            None
        };
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        for write in interrupt_writes {
            tx.write(write)?;
        }
        if let Some(write) = failure_review {
            tx.write(Write::Document(write))?;
        }
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::ENTRY_COMMITTED.into(),
            payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::TOOL_RESULT }),
        }))?;
        tx.write(Write::Entry(entry))?;
        tx.write(Write::ToolOperation(operation))?;
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TOOL_FINISHED.into(),
            payload: json!({
                "task_id": stored.record.id.0,
                "tool_call_id": call.id,
                "status": ToolOperationStatus::Interrupted,
            }),
        }))?;
        if !defer_terminal {
            if let Some(submission) = self
                .settled_submission(&record, Some(message.to_string()))
                .await?
            {
                tx.write(Write::Submission(submission.clone()))?;
                tx.write(Write::Event(SessionEventDraft {
                    kind: event_kind::SUBMISSION_DONE.into(),
                    payload: rusty_bot_core::storage::submission_event_payload(&submission),
                }))?;
            }
        }
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, current.record.version)
            .await?;
        Ok(ToolRunOutcome {
            is_error: true,
            interrupted: true,
        })
    }

    fn first_run_entry(
        &self,
        task: &TaskRecord,
        entries: &[rusty_bot_core::StoredEntry],
    ) -> Option<EntryId> {
        entries
            .iter()
            .find(|entry| {
                entry.record.by_task_id == Some(task.id) && entry.record.kind != entry_kind::SYSTEM
            })
            .map(|entry| entry.record.id)
    }

    async fn settle_success(
        &mut self,
        mut record: TaskRecord,
        version: u32,
        summary: rusty_bot_core::RunSummary,
        answer: Option<EntryRecord>,
    ) -> Result<(), SessionError> {
        let submission = self.settled_submission(&record, None).await?;
        record.version += 1;
        let outcome = TaskOutcome::Completed {
            result: json!({"submission_id": submission_id_of(&record).map(|id| id.0)}),
        };
        if self.has_hook(rusty_bot_core::HookPoint::AfterRun) {
            record.memos.insert("after_run_pending".into(), json!({"reason": if summary.reason == rusty_bot_core::RunReason::Answer {"answer"} else {"terminate"}, "first": summary.first.0, "answer": summary.answer.0, "last": summary.last.0}));
            record.state = TaskState::Completing { outcome };
        } else {
            record.state = TaskState::Terminal { outcome };
            record.ended_at = Some(self.deps.clock.now_ms());
        }
        let completed_review = if record.state.is_terminal() {
            self.completed_review_write(record.id).await?
        } else {
            None
        };
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(write) = completed_review {
            tx.write(Write::Document(write))?;
        }
        if let Some(entry) = answer {
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::ENTRY_COMMITTED.into(),
                payload: json!({"entry_id": entry.id.0, "kind": entry_kind::ASSISTANT}),
            }))?;
            tx.write(Write::Entry(entry))?;
        }
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        if let Some(submission) = submission {
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::SUBMISSION_DONE.into(),
                payload: rusty_bot_core::storage::submission_event_payload(&submission),
            }))?;
            tx.write(Write::Submission(submission))?;
        }
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, version).await?;
        if self.has_hook(rusty_bot_core::HookPoint::AfterRun) {
            self.finish_turn(record.id).await?;
        }
        Ok(())
    }

    pub async fn finish_turn(&mut self, task_id: TaskId) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        let Some(stored) = self.deps.storage.get_task(task_id).await? else {
            return Err(SessionError::ContractViolation(format!(
                "unknown task {}",
                task_id.0
            )));
        };
        if let Some(pending) = stored.record.memos.get("after_run_pending") {
            let get_id = |name: &str| {
                pending
                    .get(name)
                    .and_then(Value::as_i64)
                    .map(EntryId)
                    .ok_or_else(|| SessionError::InvalidState("invalid after_run marker".into()))
            };
            let reason = match pending.get("reason").and_then(Value::as_str) {
                Some("answer") => rusty_bot_core::RunReason::Answer,
                Some("terminate") => rusty_bot_core::RunReason::Terminate,
                _ => {
                    return Err(SessionError::InvalidState(
                        "invalid after_run marker".into(),
                    ))
                }
            };
            let summary = rusty_bot_core::RunSummary {
                reason,
                first: get_id("first")?,
                answer: get_id("answer")?,
                last: get_id("last")?,
            };
            self.hook_dispatcher(self.cancel.clone())
                .after_run(task_id, &summary)
                .await?;
            let mut record = stored.record.clone();
            let TaskState::Completing { outcome } = record.state.clone() else {
                return Err(SessionError::InvalidState(
                    "invalid after_run task state".into(),
                ));
            };
            record.version += 1;
            record.state = TaskState::Terminal { outcome };
            record.ended_at = Some(self.deps.clock.now_ms());
            record.memos.remove("after_run_pending");
            record.memos.insert("after_run_done".into(), json!(true));
            let completed_review = self.completed_review_write(task_id).await?;
            let mut tx = self.conversation_tx();
            if let Some(write) = completed_review {
                tx.write(Write::Document(write))?;
            }
            tx.write(Write::Task(record.clone()))?;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::TASK_CHANGED.into(),
                payload: task_event_payload(&record),
            }))?;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::TURN_FINISHED.into(),
                payload: json!({"task_id": task_id.0}),
            }))?;
            let receipt = tx.commit().await?;
            return self
                .handle_task_receipt(receipt, stored.record.version)
                .await;
        }
        let now = self.deps.clock.now_ms();
        let submission_id = submission_id_of(&stored.record);
        let submission = match submission_id {
            Some(id) => self.deps.storage.get_submission(id).await?,
            None => None,
        };
        let mut writes: Vec<Write> = Vec::new();
        let mut events: Vec<SessionEventDraft> = Vec::new();
        let mut record = stored.record.clone();
        if !record.state.is_terminal() {
            record.version += 1;
            record.state = TaskState::Terminal {
                outcome: TaskOutcome::Completed {
                    result: json!({ "submission_id": submission_id.map(|id| id.0) }),
                },
            };
            record.ended_at = Some(now);
            writes.push(Write::Task(record.clone()));
            events.push(SessionEventDraft {
                kind: event_kind::TASK_CHANGED.into(),
                payload: task_event_payload(&record),
            });
        }
        if let Some(submission) = submission {
            if submission.record.status != SubmissionStatus::Done {
                let mut settled = submission.record.clone();
                settled.status = SubmissionStatus::Done;
                settled.error = task_error(&record.state);
                writes.push(Write::Submission(settled.clone()));
                events.push(SessionEventDraft {
                    kind: event_kind::SUBMISSION_DONE.into(),
                    payload: rusty_bot_core::storage::submission_event_payload(&settled),
                });
            }
        }
        if writes.is_empty() {
            return Ok(());
        }
        let completed_review = if record.state.is_terminal() {
            self.completed_review_write(task_id).await?
        } else {
            None
        };
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(write) = completed_review {
            tx.write(Write::Document(write))?;
        }
        for write in writes {
            tx.write(write)?;
        }
        for event in events {
            tx.write(Write::Event(event))?;
        }
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TURN_FINISHED.into(),
            payload: json!({ "task_id": task_id.0 }),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, stored.record.version)
            .await
    }

    pub async fn request_abort(
        &mut self,
        task_id: Option<TaskId>,
        reason: &str,
    ) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        let task = match task_id {
            Some(id) => self.deps.storage.get_task(id).await?,
            None => {
                let tasks = self
                    .deps
                    .storage
                    .scan_tasks(TaskQuery {
                        conversation_id: Some(self.conversation_id),
                        status: None,
                        kind: Some(TURN_TASK_KIND.into()),
                        limit: 1_000,
                    })
                    .await?;
                crate::scheduler::select_task(&tasks).cloned()
            }
        };
        let Some(task) = task else {
            return Ok(());
        };
        if task.record.state.is_terminal() {
            // Aborting a finished turn is a no-op, never a state regression.
            return Ok(());
        }
        let mut record = task.record.clone();
        record.version += 1;
        record.abort_requested = true;
        record.abort_reason = Some(reason.to_string());
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_ABORT_REQUESTED.into(),
            payload: json!({ "task_id": record.id.0, "reason": reason }),
        }))?;
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, task.record.version).await
    }

    /// Terminalize an aborted turn without performing any tool effect.
    ///
    /// This is the abort path for an activity that cannot observe the running
    /// tool (for example after reopening a session). Unlike `fail_generation`,
    /// it never executes a tool. An unresolved tool intent is journaled as
    /// `Interrupted` (intent committed, effect not observed) or `Uncertain`
    /// (effect may have run), which is terminal for the idempotency key, so no
    /// retry can repeat the effect.
    pub async fn abort_task(&mut self, task_id: TaskId, reason: &str) -> Result<(), SessionError> {
        self.guard_review_host().await?;
        let Some(stored) = self.deps.storage.get_task(task_id).await? else {
            return Ok(());
        };
        if stored.record.state.is_terminal() {
            return Ok(());
        }

        let mut pending_tools = Vec::new();
        if matches!(&stored.record.state, TaskState::Running { .. }) {
            let round = stored
                .record
                .memos
                .get("generation_round")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let entries =
                prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
            if let Some(assistant) = entries.iter().rev().find(|entry| {
                entry.record.by_task_id == Some(task_id)
                    && entry.record.kind == entry_kind::ASSISTANT
                    && entry
                        .record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == round
            }) {
                let calls: Vec<ToolCall> = serde_json::from_value(
                    assistant
                        .record
                        .data
                        .get("tool_calls")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|_| SessionError::InvalidState("malformed round calls".into()))?;
                let prefix = validate_round_prefix(&entries, task_id, round, &calls)?;
                for call in calls.into_iter().skip(prefix) {
                    let key = tool_task::idempotency_key(&call.name, &call.id);
                    pending_tools.push((call, key));
                }
            }
        }

        let Some(current) = self.deps.storage.get_task(task_id).await? else {
            return Ok(());
        };
        if current.record.state.is_terminal() {
            return Ok(());
        }
        let now = self.deps.clock.now_ms();
        let mut record = current.record.clone();
        record.version += 1;
        record.abort_requested = true;
        record.abort_reason = Some(reason.to_string());
        record.state = TaskState::Terminal {
            outcome: TaskOutcome::Aborted {
                reason: Some(reason.to_string()),
                result: None,
            },
        };
        record.ended_at = Some(now);

        let failure_review = self
            .review_disposition_write(
                task_id,
                crate::review_state::ReceiptDisposition::TerminalFailure,
            )
            .await?;
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(write) = failure_review {
            tx.write(Write::Document(write))?;
        }
        for (call, key) in pending_tools {
            if let Some(operation) = self.deps.storage.get_operation(task_id, &key).await? {
                if matches!(
                    operation.status,
                    ToolOperationStatus::Intent | ToolOperationStatus::Running
                ) {
                    let status = if operation.status == ToolOperationStatus::Running {
                        ToolOperationStatus::Uncertain
                    } else {
                        ToolOperationStatus::Interrupted
                    };
                    let entry_id = EntryId(self.deps.storage.mint_id().await?);
                    let round = record
                        .memos
                        .get("generation_round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let output = ToolOutput::error(reason.to_string(), "interrupted");
                    let entry = tool_result_entry(
                        entry_id,
                        self.conversation_id,
                        task_id,
                        &call,
                        &output,
                        round,
                        true,
                    );
                    tx.write(Write::Event(SessionEventDraft {
                        kind: event_kind::ENTRY_COMMITTED.into(),
                        payload: json!({ "entry_id": entry_id.0, "kind": entry_kind::TOOL_RESULT }),
                    }))?;
                    tx.write(Write::Entry(entry))?;
                    tx.write(Write::ToolOperation(ToolOperationRecord {
                        task_id,
                        idempotency_key: key.clone(),
                        tool_name: call.name.clone(),
                        status,
                        request: operation.request,
                        result: Some(json!({
                            "content": reason,
                            "is_error": true,
                            "interrupted": true,
                        })),
                        updated_at: now,
                    }))?;
                    tx.write(Write::Event(SessionEventDraft {
                        kind: event_kind::TOOL_FINISHED.into(),
                        payload: json!({
                            "task_id": task_id.0,
                            "tool_call_id": call.id,
                            "status": status,
                        }),
                    }))?;
                }
            }
        }

        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        if let Some(submission) = self
            .settled_submission(&record, Some(reason.to_string()))
            .await?
        {
            tx.write(Write::Submission(submission.clone()))?;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::SUBMISSION_DONE.into(),
                payload: rusty_bot_core::storage::submission_event_payload(&submission),
            }))?;
        }
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, current.record.version)
            .await
    }

    pub async fn run_until_idle(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<RunOutcome, SessionError> {
        let supervisor = Arc::new(
            rusty_bot_core::HookJobSupervisor::new(self.job_policy, cancel.clone())
                .map_err(|_| SessionError::InvalidConfig("invalid hook job policy".into()))?,
        );
        let result = self
            .run_until_idle_with_supervisor(cancel.clone(), supervisor.clone())
            .await;
        if result.is_err() || cancel.is_cancelled() {
            supervisor.cancel_and_drain().await;
        } else {
            supervisor.seal_and_drain().await;
        }
        result
    }

    /// Drive a run using an externally owned supervisor. The caller must retain the
    /// owner and drain it after success, failure, cancellation, or driver panic.
    pub async fn run_until_idle_with_supervisor(
        &mut self,
        cancel: CancellationToken,
        supervisor: Arc<rusty_bot_core::HookJobSupervisor>,
    ) -> Result<RunOutcome, SessionError> {
        let active = Arc::new(());
        self.job_scope = Some((Arc::downgrade(&supervisor), Arc::downgrade(&active)));
        let result = self.run_until_idle_inner(cancel).await;
        drop(active);
        self.job_scope = None;
        result
    }

    async fn run_until_idle_inner(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<RunOutcome, SessionError> {
        self.ensure_supported_hooks()?;
        self.cancel = cancel.clone();
        let mut turns = 0u32;
        for _ in 0..1_024 {
            if cancel.is_cancelled() {
                return Ok(RunOutcome {
                    turns_completed: turns,
                    idled: false,
                });
            }
            let work = match self.plan_next().await {
                Err(SessionError::Cancelled) if cancel.is_cancelled() => {
                    return Ok(RunOutcome {
                        turns_completed: turns,
                        idled: false,
                    })
                }
                result => result?,
            };
            match work {
                WorkItem::Generate {
                    task_id, request, ..
                } => {
                    self.generate(task_id, request, &cancel).await?;
                }
                WorkItem::ExecuteTool { task_id, .. } => {
                    self.execute_tool_round(task_id, &cancel).await?;
                    if self.paused_review.is_some() {
                        return Ok(RunOutcome {
                            turns_completed: turns,
                            idled: false,
                        });
                    }
                }
                WorkItem::FinishTurn { task_id } => {
                    self.finish_turn(task_id).await?;
                    turns += 1;
                }
                WorkItem::Sleep { .. } | WorkItem::Idle => {
                    let now = self.deps.clock.now_ms();
                    self.flush_due(now).await?;
                    return Ok(RunOutcome {
                        turns_completed: turns,
                        idled: true,
                    });
                }
            }
        }
        Err(SessionError::InvalidState(
            "run_until_idle exceeded the iteration limit; the turn did not settle".into(),
        ))
    }

    async fn execute_tool_round(
        &mut self,
        task_id: TaskId,
        cancel: &CancellationToken,
    ) -> Result<(), SessionError> {
        let round_cancel = cancel.child_token();
        self.cancel = round_cancel.clone();
        let result = self.execute_tool_round_inner(task_id, &round_cancel).await;
        self.cancel = cancel.clone();
        result
    }

    async fn execute_tool_round_inner(
        &mut self,
        task_id: TaskId,
        cancel: &CancellationToken,
    ) -> Result<(), SessionError> {
        let task = self
            .deps
            .storage
            .get_task(task_id)
            .await?
            .ok_or_else(|| SessionError::InvalidState("missing round task".into()))?;
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
                e.record.by_task_id == Some(task_id)
                    && e.record.kind == entry_kind::ASSISTANT
                    && e.record
                        .data
                        .get("round")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        == round
            })
            .ok_or_else(|| SessionError::InvalidState("missing round assistant".into()))?;
        let calls: Vec<ToolCall> = serde_json::from_value(
            assistant
                .record
                .data
                .get("tool_calls")
                .cloned()
                .ok_or_else(|| SessionError::InvalidState("missing round calls".into()))?,
        )
        .map_err(|_| SessionError::InvalidState("malformed round calls".into()))?;
        let prefix = validate_round_prefix(&entries, task_id, round, &calls)?;
        let modes: Vec<_> = calls
            .iter()
            .map(|call| {
                self.tools
                    .model_visible
                    .contains(&call.name)
                    .then(|| self.tools.get(&call.name))
                    .flatten()
                    .map(|tool| tool.execution_mode())
                    .unwrap_or(rusty_bot_core::ExecutionMode::Sequential)
            })
            .collect();
        let mut active = FuturesUnordered::new();
        let mut outcomes = BTreeMap::new();
        let mut launch = prefix;
        let mut settle = prefix;
        let mut failure: Option<SessionError> = None;
        let mut interrupted = false;
        let mut fence = false;
        let mut review = None;
        loop {
            if failure.is_none() && !outcomes.contains_key(&settle) && !active.is_empty() {
                // Give already-started effects a poll between preparations. This
                // observes cancellation or panic before committing more intents.
                let completed: Option<Option<(usize, RoundResult)>> = active.next().now_or_never();
                if let Some(Some((ordinal, outcome))) = completed {
                    if let Some(error) = outcome.failure() {
                        failure.get_or_insert(error);
                        cancel.cancel();
                    }
                    outcomes.insert(ordinal, outcome);
                    if active.is_empty() {
                        fence = false;
                    }
                }
            }
            while failure.is_none()
                && !interrupted
                && !cancel.is_cancelled()
                && review.is_none()
                && launch < calls.len()
                && active.len() < self.max_parallel_tools.get()
            {
                let sequential = modes[launch] == rusty_bot_core::ExecutionMode::Sequential;
                if fence || (sequential && !active.is_empty()) {
                    break;
                }
                let ordinal = launch;
                let preparation = AssertUnwindSafe(self.prepare_tool_effect_with_metadata(
                    task_id,
                    calls[ordinal].clone(),
                    true,
                ))
                .catch_unwind();
                tokio::pin!(preparation);
                let prepared = loop {
                    tokio::select! {
                        result = &mut preparation => break result,
                        completed = active.next(), if !active.is_empty() => {
                            if let Some((completed_ordinal, outcome)) = completed {
                                if let Some(error) = outcome.failure() {
                                    failure.get_or_insert(error);
                                    cancel.cancel();
                                }
                                outcomes.insert(completed_ordinal, outcome);
                            }
                        }
                    }
                };
                let prepared = match prepared {
                    Ok(Ok(prepared)) => prepared,
                    Ok(Err(error)) => {
                        failure = Some(error);
                        cancel.cancel();
                        break;
                    }
                    Err(_) => {
                        failure = Some(SessionError::Internal("tool preparation panicked".into()));
                        cancel.cancel();
                        break;
                    }
                };
                launch += 1;
                match prepared {
                    PreparedTool::Review {
                        call,
                        final_call,
                        allowed,
                    } => {
                        review = Some(ReviewCandidate {
                            ordinal,
                            call,
                            final_call,
                            allowed,
                        });
                        break;
                    }
                    PreparedTool::Effect(effect) if cancel.is_cancelled() => {
                        outcomes.insert(
                            ordinal,
                            RoundResult::Prepared(PreparedTool::Interrupted {
                                stored: effect.identity.stored,
                                call: effect.identity.call,
                                round: effect.identity.round,
                                message: "tool execution interrupted".into(),
                            }),
                        );
                    }
                    PreparedTool::Effect(effect) => {
                        let identity = effect.identity.clone();
                        active.push(async move {
                            if effect.context.cancel.is_cancelled() {
                                return (
                                    ordinal,
                                    RoundResult::Prepared(PreparedTool::Interrupted {
                                        stored: identity.stored,
                                        call: identity.call,
                                        round: identity.round,
                                        message: "tool execution interrupted".into(),
                                    }),
                                );
                            }
                            let result = AssertUnwindSafe(effect.execute()).catch_unwind().await;
                            (
                                ordinal,
                                match result {
                                    Ok((id, result)) => RoundResult::Effect(id, result),
                                    Err(_) => RoundResult::Panicked(identity),
                                },
                            )
                        });
                        fence = sequential;
                        if sequential {
                            break;
                        }
                    }
                    other => {
                        outcomes.insert(ordinal, RoundResult::Prepared(other));
                        if sequential {
                            fence = true;
                            break;
                        }
                    }
                }
            }
            // Never invoke a settlement hook or commit while effects still need
            // draining after an infrastructure failure.
            while failure.is_none() || active.is_empty() {
                let Some(outcome) = outcomes.remove(&settle) else {
                    break;
                };
                if failure.is_some() {
                    settle += 1;
                    continue;
                }
                let settlement = AssertUnwindSafe(async {
                    match outcome {
                        RoundResult::Prepared(prepared) => {
                            self.settle_prepared_tool(prepared, true).await
                        }
                        RoundResult::Effect(identity, InvocationResult::Returned(result)) => {
                            self.settle_tool_effect(&identity, result, true).await
                        }
                        RoundResult::Effect(_, InvocationResult::Interrupted(_)) => {
                            Err(SessionError::InterruptUnsupported)
                        }
                        RoundResult::Effect(_, InvocationResult::ProtocolViolation) => {
                            Err(SessionError::InterruptProtocol(
                                "tool did not propagate interrupt or replay diverged".into(),
                            ))
                        }
                        RoundResult::Panicked(identity) => {
                            self.commit_interrupted(
                                &identity.stored,
                                &identity.call,
                                identity.round,
                                "tool effect panicked; effect may have occurred",
                                true,
                            )
                            .await
                        }
                    }
                })
                .catch_unwind();
                tokio::pin!(settlement);
                let result = loop {
                    tokio::select! {
                        result = &mut settlement => break result,
                        completed = active.next(), if !active.is_empty() => {
                            if let Some((ordinal, outcome)) = completed {
                                if let Some(error) = outcome.failure() {
                                    failure.get_or_insert(error);
                                    cancel.cancel();
                                }
                                outcomes.insert(ordinal, outcome);
                            }
                        }
                    }
                };
                match result {
                    Ok(Ok(result)) => {
                        if result.interrupted {
                            interrupted = true;
                            cancel.cancel();
                        }
                    }
                    Ok(Err(error)) => {
                        if failure.is_none() {
                            failure = Some(error);
                        }
                        cancel.cancel();
                    }
                    Err(_) => {
                        failure.get_or_insert_with(|| {
                            SessionError::Internal("tool settlement panicked".into())
                        });
                        cancel.cancel();
                    }
                }
                settle += 1;
            }
            if active.is_empty() {
                fence = false;
                if failure.is_some()
                    || interrupted
                    || cancel.is_cancelled()
                    || review.is_some()
                    || launch == calls.len()
                {
                    break;
                }
                continue;
            }
            if let Some((ordinal, outcome)) = active.next().await {
                if let Some(error) = outcome.failure() {
                    failure.get_or_insert(error);
                    cancel.cancel();
                }
                outcomes.insert(ordinal, outcome);
                if active.is_empty() {
                    fence = false;
                }
            }
        }
        if failure.is_none() && !interrupted && !cancel.is_cancelled() {
            if let Some(candidate) = review {
                if settle != candidate.ordinal {
                    return Err(SessionError::InvalidState(
                        "review prefix did not settle".into(),
                    ));
                }
                self.commit_review(task_id, round, assistant.record.id, &calls, candidate)
                    .await?;
                return Ok(());
            }
        }
        if let Some(error) = failure {
            let interrupt_failure = matches!(
                error,
                SessionError::InterruptUnsupported | SessionError::InterruptProtocol(_)
            );
            // Active effects are now drained. Reconcile journaled calls in
            // order without invoking hooks. If storage fails, retain the
            // existing intents for conservative recovery and the first error.
            let mut reconciled = true;
            for call in calls.iter().skip(prefix) {
                let cleanup = AssertUnwindSafe(async {
                    let entries =
                        prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id)
                            .await?;
                    if entries.iter().any(|entry| {
                        entry.record.by_task_id == Some(task_id)
                            && entry.record.kind == entry_kind::TOOL_RESULT
                            && entry
                                .record
                                .data
                                .get("round")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                                == round
                            && entry
                                .record
                                .data
                                .get("tool_call_id")
                                .and_then(Value::as_str)
                                == Some(call.id.as_str())
                    }) {
                        return Ok::<(), SessionError>(());
                    }
                    let key = tool_task::idempotency_key(&call.name, &call.id);
                    if self
                        .deps
                        .storage
                        .get_operation(task_id, &key)
                        .await?
                        .is_some()
                    {
                        let current =
                            self.deps.storage.get_task(task_id).await?.ok_or_else(|| {
                                SessionError::InvalidState("missing failed round task".into())
                            })?;
                        if interrupt_failure {
                            self.mark_effect_uncertain(task_id, &key).await?;
                        } else {
                            self.commit_interrupted(
                                &current,
                                call,
                                round,
                                "tool round failed; effect may have occurred",
                                true,
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
                .catch_unwind()
                .await;
                if !matches!(cleanup, Ok(Ok(()))) {
                    reconciled = false;
                    break;
                }
            }
            if reconciled {
                if let Ok(Some(current)) = self.deps.storage.get_task(task_id).await {
                    if !current.record.state.is_terminal() {
                        let _ = AssertUnwindSafe(async {
                            let mut record = current.record.clone();
                            record.version += 1;
                            let message = "tool round failed";
                            finalize_interrupted_task(
                                &mut record,
                                message,
                                self.deps.clock.now_ms(),
                            );
                            let submission = self
                                .settled_submission(&record, Some(message.into()))
                                .await?;
                            let failure_review = self
                                .review_disposition_write(
                                    task_id,
                                    crate::review_state::ReceiptDisposition::TerminalFailure,
                                )
                                .await?;
                            let mut tx = self.conversation_tx();
                            if let Some(write) = failure_review {
                                tx.write(Write::Document(write))?;
                            }
                            tx.write(Write::Task(record.clone()))?;
                            tx.write(Write::Event(SessionEventDraft {
                                kind: event_kind::TASK_CHANGED.into(),
                                payload: task_event_payload(&record),
                            }))?;
                            if let Some(submission) = submission {
                                tx.write(Write::Submission(submission.clone()))?;
                                tx.write(Write::Event(SessionEventDraft {
                                    kind: event_kind::SUBMISSION_DONE.into(),
                                    payload: rusty_bot_core::storage::submission_event_payload(
                                        &submission,
                                    ),
                                }))?;
                            }
                            let receipt = tx.commit().await?;
                            self.handle_task_receipt(receipt, current.record.version)
                                .await
                        })
                        .catch_unwind()
                        .await;
                    }
                }
            }
            return Err(error);
        }
        if interrupted {
            let current = self
                .deps
                .storage
                .get_task(task_id)
                .await?
                .ok_or_else(|| SessionError::InvalidState("missing interrupted task".into()))?;
            if !current.record.state.is_terminal() {
                let mut record = current.record.clone();
                record.version += 1;
                let message = "tool execution interrupted";
                finalize_interrupted_task(&mut record, message, self.deps.clock.now_ms());
                let submission = self
                    .settled_submission(&record, Some(message.into()))
                    .await?;
                let completed_review = self
                    .review_disposition_write(
                        task_id,
                        crate::review_state::ReceiptDisposition::TerminalFailure,
                    )
                    .await?;
                let mut tx = self.conversation_tx();
                if let Some(write) = completed_review {
                    tx.write(Write::Document(write))?;
                }
                tx.write(Write::Task(record.clone()))?;
                tx.write(Write::Event(SessionEventDraft {
                    kind: event_kind::TASK_CHANGED.into(),
                    payload: task_event_payload(&record),
                }))?;
                if let Some(submission) = submission {
                    tx.write(Write::Submission(submission.clone()))?;
                    tx.write(Write::Event(SessionEventDraft {
                        kind: event_kind::SUBMISSION_DONE.into(),
                        payload: rusty_bot_core::storage::submission_event_payload(&submission),
                    }))?;
                }
                let receipt = tx.commit().await?;
                self.handle_task_receipt(receipt, current.record.version)
                    .await?;
            }
        }
        Ok(())
    }

    // A last-user search is insufficient: hooks can introduce user messages.
    // Require the task's placed input submission and its source entry to agree.
    async fn is_external_input_boundary(
        &self,
        task_id: TaskId,
        request: &rusty_bot_core::HistoryRequest,
    ) -> Result<bool, SessionError> {
        let Some(task) = self.deps.storage.get_task(task_id).await? else {
            return Ok(false);
        };
        let Some(id) = submission_id_of(&task.record) else {
            return Ok(false);
        };
        let Some(submission) = self.deps.storage.get_submission(id).await? else {
            return Ok(false);
        };
        let submission = submission.record;
        if submission.conversation_id != self.conversation_id
            || submission.kind != SubmissionKind::Input
            || submission.status != SubmissionStatus::Placed
        {
            return Ok(false);
        }
        let Some(entry_id) = submission.entry_id else {
            return Ok(false);
        };
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let Some(entry) = entries.iter().rev().find(|e| !e.record.model.is_empty()) else {
            return Ok(false);
        };
        let expected = match canonical_input(&submission.payload) {
            Ok(content) => Message::User { content },
            Err(_) => return Ok(false),
        };
        Ok(entry.record.id == entry_id
            && entry.record.kind == entry_kind::USER
            && entry.record.by_task_id == Some(task_id)
            && entry
                .record
                .data
                .get("submission_id")
                .and_then(Value::as_i64)
                == Some(id.0)
            && entry.record.model.as_slice() == [expected.clone()]
            && request.messages.last() == Some(&expected))
    }

    async fn generate(
        &mut self,
        task_id: TaskId,
        mut request: rusty_bot_core::HistoryRequest,
        cancel: &CancellationToken,
    ) -> Result<(), SessionError> {
        let baseline = crate::native_context::load(
            self.deps.storage.as_ref(),
            self.conversation_id,
            self.deps.provider.id(),
            &request,
        )
        .await?;
        if let Some((_, reconstructed)) = &baseline {
            request = reconstructed.clone();
        }
        let entries =
            prompt::load_entries(self.deps.storage.as_ref(), self.conversation_id).await?;
        let frontier = entries
            .iter()
            .rev()
            .find(|e| !e.record.model.is_empty())
            .map(|e| e.record.id);
        let retry = baseline.as_ref().is_some_and(|(checkpoint, _)| {
            checkpoint.task_id == task_id && Some(checkpoint.frontier) == frontier
        });
        let continuation_prefix = baseline.as_ref().map(|_| request.messages.clone());
        let mut reset_original = None;
        let rich = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Rich { .. }));
        let defer = match &self.compactor {
            Some(compactor) if rich && compactor.defer_rich_until_new_submission() => {
                !self.is_external_input_boundary(task_id, &request).await?
            }
            _ => false,
        };
        if let Some(compactor) = self.compactor.as_ref().filter(|_| !defer && !retry) {
            let result = if compactor.drain_on_cancel() {
                compactor.propose(&request, cancel.clone()).await
            } else {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return self.fail_generation(task_id, ModelError::cancelled()).await,
                    result = compactor.propose(&request, cancel.clone()) => result,
                }
            };
            if cancel.is_cancelled() {
                return self.fail_generation(task_id, ModelError::cancelled()).await;
            }
            match result {
                Ok(rusty_bot_core::CompactionProposal::Replacement(candidate)) => {
                    if !crate::hooks::valid_replacement(&request.messages, &candidate)
                        || !valid_compaction_images(
                            &candidate,
                            self.deps.provider.image_limits().as_ref(),
                        )
                    {
                        return self
                            .fail_generation(
                                task_id,
                                ModelError::new(
                                    ModelErrorKind::InvalidRequest,
                                    "invalid request compaction",
                                    false,
                                ),
                            )
                            .await;
                    }
                    request.messages = candidate;
                }
                Ok(rusty_bot_core::CompactionProposal::Unchanged) => {}
                Ok(rusty_bot_core::CompactionProposal::CompletedPrefixSummary {
                    remove,
                    summary,
                }) => {
                    let candidate =
                        crate::compaction::structured_candidate(&request, remove, &summary);
                    match candidate {
                        Ok(candidate)
                            if valid_compaction_images(
                                &candidate.messages,
                                self.deps.provider.image_limits().as_ref(),
                            ) && self
                                .deps
                                .provider
                                .validate_compacted_history(&candidate)
                                .is_ok() =>
                        {
                            request = candidate
                        }
                        _ => {
                            return self
                                .fail_generation(
                                    task_id,
                                    ModelError::new(
                                        ModelErrorKind::InvalidRequest,
                                        "request compaction failed",
                                        false,
                                    ),
                                )
                                .await
                        }
                    }
                }
                Ok(rusty_bot_core::CompactionProposal::NativeResetSummary {
                    remove,
                    retain_recent_exchanges,
                    summary,
                }) => {
                    let candidate = crate::compaction::native_reset_candidate(
                        &request,
                        remove,
                        retain_recent_exchanges,
                        &summary,
                    );
                    if !self.is_external_input_boundary(task_id, &request).await? {
                        return self
                            .fail_generation(
                                task_id,
                                ModelError::new(
                                    ModelErrorKind::InvalidRequest,
                                    "request compaction failed",
                                    false,
                                ),
                            )
                            .await;
                    }
                    match candidate {
                        Ok(candidate)
                            if valid_compaction_images(
                                &candidate.messages,
                                self.deps.provider.image_limits().as_ref(),
                            ) && self
                                .deps
                                .provider
                                .validate_native_context_reset(&request, &candidate)
                                .is_ok() =>
                        {
                            reset_original = Some(request);
                            request = candidate;
                        }
                        _ => {
                            return self
                                .fail_generation(
                                    task_id,
                                    ModelError::new(
                                        ModelErrorKind::InvalidRequest,
                                        "request compaction failed",
                                        false,
                                    ),
                                )
                                .await
                        }
                    }
                }
                Err(_) => {
                    return self
                        .fail_generation(
                            task_id,
                            ModelError::new(
                                ModelErrorKind::InvalidRequest,
                                "request compaction failed",
                                false,
                            ),
                        )
                        .await
                }
            }
        }
        let dispatcher = self.hook_dispatcher(cancel.clone());
        if (baseline.is_some() || reset_original.is_some())
            && self.has_hook(rusty_bot_core::HookPoint::BeforeProviderRequest)
        {
            return self
                .fail_generation(
                    task_id,
                    ModelError::new(
                        ModelErrorKind::InvalidRequest,
                        "native context hook unavailable",
                        false,
                    ),
                )
                .await;
        }
        if !retry
            && dispatcher
                .before_request(task_id, &mut request.messages)
                .await
                .is_err()
        {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Ok(());
        }
        if let Some(original) = &reset_original {
            let valid = request.messages.first() == original.messages.first()
                && request.messages.last() == original.messages.last()
                && request
                    .messages
                    .iter()
                    .skip(1)
                    .all(|m| matches!(m, Message::User { .. }))
                && valid_compaction_images(
                    &request.messages,
                    self.deps.provider.image_limits().as_ref(),
                )
                && self
                    .deps
                    .provider
                    .validate_native_context_reset(original, &request)
                    .is_ok();
            if !valid {
                return self
                    .fail_generation(
                        task_id,
                        ModelError::new(
                            ModelErrorKind::InvalidRequest,
                            "invalid native context",
                            false,
                        ),
                    )
                    .await;
            }
        } else if let Some(prefix) = &continuation_prefix {
            if !request.messages.starts_with(prefix) {
                return self
                    .fail_generation(
                        task_id,
                        ModelError::new(
                            ModelErrorKind::InvalidRequest,
                            "invalid native context",
                            false,
                        ),
                    )
                    .await;
            }
        }
        if !retry && (baseline.is_some() || reset_original.is_some()) {
            let frontier = frontier.ok_or_else(|| {
                SessionError::ContractViolation("missing native context frontier".into())
            })?;
            let task = self
                .deps
                .storage
                .get_task(task_id)
                .await?
                .ok_or_else(|| SessionError::InvalidState("missing task".into()))?;
            crate::native_context::save(
                self.deps.storage.as_ref(),
                self.spec.session_id,
                self.conversation_id,
                &task.record,
                self.deps.provider.id(),
                &request,
                frontier,
            )
            .await?;
        }
        let (sender, mut receiver) = mpsc::channel(64);
        let provider = Arc::clone(&self.deps.provider);
        let provider_cancel = cancel.clone();
        let payload_hook = self.hook_dispatcher(cancel.clone()).payload_hook(task_id);
        let handle = tokio::spawn(async move {
            provider
                .stream_history_with_payload_hook(request, sender, provider_cancel, payload_hook)
                .await
        });
        let mut text = String::new();
        let mut thinking = String::new();
        self.pending_text.clear();
        self.pending_thinking.clear();
        self.last_partial_at = None;
        let mut progress_tick = tokio::time::interval(std::time::Duration::from_millis(
            (self.config.progress.persist_within_ms / 2).clamp(1, 100),
        ));
        loop {
            let event = tokio::select! {
                _ = progress_tick.tick() => {
                    self.flush_due_progress().await?;
                    continue;
                }
                event = receiver.recv() => match event {
                    Some(event) => event,
                    None => break,
                },
            };
            match event {
                rusty_bot_core::StreamEvent::TextDelta(delta) => {
                    text.push_str(&delta);
                    self.pending_text.push_str(&delta);
                    if self.partial_due() {
                        let frame = std::mem::take(&mut self.pending_text);
                        self.emit_partial(
                            task_id,
                            LiveKind::TextDelta,
                            json!({ "text": frame }),
                            &text,
                            &thinking,
                        )
                        .await?;
                    }
                }
                rusty_bot_core::StreamEvent::ThinkingDelta(delta) => {
                    thinking.push_str(&delta);
                    self.pending_thinking.push_str(&delta);
                    if self.partial_due() {
                        let frame = std::mem::take(&mut self.pending_thinking);
                        self.emit_partial(
                            task_id,
                            LiveKind::ThinkingDelta,
                            json!({ "text": frame }),
                            &text,
                            &thinking,
                        )
                        .await?;
                    }
                }
                rusty_bot_core::StreamEvent::ToolCallArgumentsDelta { delta, .. } => {
                    self.emit_partial(
                        task_id,
                        LiveKind::ToolDelta,
                        json!({ "delta": delta }),
                        &text,
                        &thinking,
                    )
                    .await?;
                }
                rusty_bot_core::StreamEvent::ToolCallStart { name, .. } => {
                    self.emit_partial(
                        task_id,
                        LiveKind::Progress,
                        json!({ "tool": name }),
                        &text,
                        &thinking,
                    )
                    .await?;
                }
                rusty_bot_core::StreamEvent::Start { .. }
                | rusty_bot_core::StreamEvent::Usage(_) => {}
            }
        }
        if !self.pending_text.is_empty() {
            let frame = std::mem::take(&mut self.pending_text);
            self.emit_partial(
                task_id,
                LiveKind::TextDelta,
                json!({ "text": frame }),
                &text,
                &thinking,
            )
            .await?;
        }
        if !self.pending_thinking.is_empty() {
            let frame = std::mem::take(&mut self.pending_thinking);
            self.emit_partial(
                task_id,
                LiveKind::ThinkingDelta,
                json!({ "text": frame }),
                &text,
                &thinking,
            )
            .await?;
        }
        let result = handle
            .await
            .map_err(|error| SessionError::Internal(format!("model task failed: {error}")))?;
        match result {
            Ok(turn) => {
                if dispatcher.after_response(task_id, &turn).await.is_err() {
                    return Ok(());
                }
                self.engine_attested_task = Some(task_id);
                let result = self.complete_history_generation(task_id, turn).await;
                self.engine_attested_task = None;
                match result {
                    Err(SessionError::Cancelled) if cancel.is_cancelled() => Ok(()),
                    result => result,
                }
            }
            Err(error) => self.fail_generation(task_id, error).await,
        }
    }

    /// Token partials are throttled to `partial_interval_ms`; the deferred
    /// live document always carries the full text so throttling never loses
    /// durable-visible progress.
    fn partial_due(&mut self) -> bool {
        let now = self.deps.clock.now_ms();
        let interval = self.config.progress.partial_interval_ms as i64;
        let due = match self.last_partial_at {
            None => true,
            Some(last) => interval == 0 || now.saturating_sub(last) >= interval,
        };
        if due {
            self.last_partial_at = Some(now);
        }
        due
    }

    async fn emit_partial(
        &mut self,
        task_id: TaskId,
        kind: LiveKind,
        data: Value,
        text: &str,
        thinking: &str,
    ) -> Result<(), SessionError> {
        self.deps.live.publish(
            self.spec.session_id,
            LiveFrame {
                task_id: Some(task_id),
                kind,
                data,
            },
        );
        self.stage_partial(task_id, text, thinking).await
    }

    async fn stage_partial(
        &mut self,
        task_id: TaskId,
        text: &str,
        thinking: &str,
    ) -> Result<(), SessionError> {
        let now = self.deps.clock.now_ms();
        let address = DocumentAddress::new(entry_kind::LIVE, DocumentScope::Task(task_id), "");
        self.deferred.defer(
            DocumentWrite::PutLatest {
                address,
                value: json!({
                    "task_id": task_id.0,
                    "text": text,
                    "thinking": thinking,
                    "updated_at_ms": now,
                }),
            },
            now,
        )?;
        self.flush_due(now).await
    }

    async fn flush_due(&mut self, now: i64) -> Result<(), SessionError> {
        let due = self
            .deferred
            .take_due(now, self.config.progress.persist_within_ms);
        if due.is_empty() {
            return Ok(());
        }
        let batches = vec![CommitBatch {
            writes: due.into_iter().map(Write::Document).collect(),
        }];
        let receipt = self
            .deps
            .storage
            .commit(
                batches,
                &CommitContext::conversation(self.spec.session_id, self.conversation_id),
            )
            .await?;
        ensure_committed(receipt)?;
        Ok(())
    }

    fn conversation_tx(&self) -> Transaction<'_> {
        Transaction::new(
            self.deps.storage.as_ref(),
            CommitContext::conversation(self.spec.session_id, self.conversation_id),
        )
    }

    async fn handle_task_receipt(
        &self,
        receipt: CommitReceipt,
        previous_version: u32,
    ) -> Result<(), SessionError> {
        if let Some(refusal) = receipt.refused {
            return match &refusal.reason {
                rusty_bot_core::RefusalReason::VersionConflict { expected, actual } => {
                    Err(SessionError::InvalidState(format!(
                        "task version conflict: expected {}, actual {} (previous {})",
                        expected, actual, previous_version
                    )))
                }
                rusty_bot_core::RefusalReason::InvalidTransition { from, to } => {
                    Err(SessionError::ContractViolation(format!(
                        "invalid task transition {from} -> {to}"
                    )))
                }
                other => Err(SessionError::ContractViolation(format!(
                    "commit refused: {other}"
                ))),
            };
        }
        Ok(())
    }

    async fn fail_task_for_limit(&mut self, task: StoredTask) -> Result<(), SessionError> {
        let now = self.deps.clock.now_ms();
        let mut record = task.record.clone();
        record.version += 1;
        record.state = TaskState::Terminal {
            outcome: TaskOutcome::Failed {
                error: "tool round limit".into(),
                result: None,
            },
        };
        record.ended_at = Some(now);
        let completed_review = self
            .review_disposition_write(
                record.id,
                crate::review_state::ReceiptDisposition::TerminalFailure,
            )
            .await?;
        let deferred = self.deferred.drain();
        let mut tx = self.conversation_tx();
        if let Some(write) = completed_review {
            tx.write(Write::Document(write))?;
        }
        tx.write(Write::Task(record.clone()))?;
        tx.write(Write::Event(SessionEventDraft {
            kind: event_kind::TASK_CHANGED.into(),
            payload: task_event_payload(&record),
        }))?;
        if let Some(submission) = self
            .settled_submission(&record, Some("tool round limit".to_string()))
            .await?
        {
            tx.write(Write::Submission(submission.clone()))?;
            tx.write(Write::Event(SessionEventDraft {
                kind: event_kind::SUBMISSION_DONE.into(),
                payload: rusty_bot_core::storage::submission_event_payload(&submission),
            }))?;
        }
        for write in deferred {
            tx.write(Write::Document(write))?;
        }
        let receipt = tx.commit().await?;
        self.handle_task_receipt(receipt, task.record.version).await
    }

    async fn settled_submission(
        &self,
        task: &TaskRecord,
        error: Option<String>,
    ) -> Result<Option<SubmissionRecord>, SessionError> {
        let Some(id) = submission_id_of(task) else {
            return Ok(None);
        };
        let Some(stored) = self.deps.storage.get_submission(id).await? else {
            return Ok(None);
        };
        // Preserve request_id/kind/payload/entry_id: only the settlement
        // columns change.
        let mut record = stored.record;
        record.status = SubmissionStatus::Done;
        record.error = error;
        Ok(Some(record))
    }
}

fn canonical_input(payload: &Value) -> Result<Vec<rusty_bot_core::ContentPart>, SessionError> {
    let object = payload
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or_else(|| SessionError::ContractViolation("invalid input payload".into()))?;
    if let Some(text) = object.get("text").and_then(Value::as_str) {
        return Ok(vec![rusty_bot_core::ContentPart::Text {
            text: text.to_owned(),
        }]);
    }
    object
        .get("content")
        .cloned()
        .ok_or_else(|| SessionError::ContractViolation("invalid input payload".into()))
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|_| SessionError::ContractViolation("invalid input payload".into()))
        })
}

fn compare_submission(
    existing: &SubmissionRecord,
    kind: SubmissionKind,
    payload: &Value,
    request_id: String,
) -> Result<SubmitOutcome, SessionError> {
    if existing.kind != kind {
        return Ok(SubmitOutcome::Rejected {
            reason: RejectReason::RequestIdReusedForDifferentKind,
        });
    }
    let equal = match kind {
        SubmissionKind::Input => canonical_input(&existing.payload)? == canonical_input(payload)?,
        SubmissionKind::Write => existing.payload == *payload,
        SubmissionKind::Continue => {
            if existing.payload != json!({}) || *payload != json!({}) {
                return Err(SessionError::ContractViolation(
                    "invalid continue payload".into(),
                ));
            }
            true
        }
    };
    Ok(if equal {
        SubmitOutcome::Duplicate { request_id }
    } else {
        SubmitOutcome::Rejected {
            reason: RejectReason::RequestIdReusedForDifferentPayload,
        }
    })
}

fn validate_arguments(tool: &Arc<dyn Tool>, arguments: &Value) -> Result<(), String> {
    if !arguments.is_object() {
        return Err("arguments must be an object".into());
    }
    let spec = tool.spec();
    let validator = match jsonschema::validator_for(&spec.parameters) {
        Ok(validator) => validator,
        Err(error) => {
            tracing::warn!(tool = %spec.name, %error, "tool JSON schema is invalid");
            return Err("invalid tool schema".into());
        }
    };
    let errors: Vec<String> = validator
        .iter_errors(arguments)
        .map(|error| error.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn submission_id_of(task: &TaskRecord) -> Option<SubmissionId> {
    task.input
        .get("submission_id")
        .and_then(Value::as_i64)
        .map(SubmissionId)
}

fn task_error(state: &TaskState) -> Option<String> {
    match state {
        TaskState::Terminal { outcome } => match outcome {
            TaskOutcome::Failed { error, .. } => Some(error.clone()),
            TaskOutcome::Aborted { reason, .. } => {
                Some(reason.clone().unwrap_or_else(|| "aborted".into()))
            }
            TaskOutcome::Orphaned { reason } => Some(reason.clone()),
            TaskOutcome::Faulted { error } => Some(error.clone()),
            TaskOutcome::Completed { .. } => None,
        },
        _ => None,
    }
}

fn task_event_payload(task: &TaskRecord) -> Value {
    json!({
        "task_id": task.id.0,
        "kind": task.kind,
        "version": task.version,
        "state": task.state,
        "abort_requested": task.abort_requested,
    })
}

fn tool_result_entry(
    id: EntryId,
    conversation_id: ConversationId,
    task_id: TaskId,
    call: &ToolCall,
    output: &ToolOutput,
    round: u64,
    interrupted: bool,
) -> EntryRecord {
    EntryRecord {
        id,
        conversation_id,
        kind: entry_kind::TOOL_RESULT.into(),
        model: vec![Message::ToolResult {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: output.content.clone(),
            is_error: output.is_error,
            details: output.details.clone(),
        }],
        data: json!({
            "tool_call_id": call.id,
            "tool_name": call.name,
            "is_error": output.is_error,
            "details": output.details,
            "round": round,
            "interrupted": interrupted,
        }),
        head: None,
        edits: Vec::new(),
        by_task_id: Some(task_id),
    }
}

// Rich provider state remains opaque and unchanged; its adapter validates it.
// Validate all directly represented images before dispatch, including retained ones.
fn valid_compaction_images(
    messages: &[Message],
    limits: Option<&rusty_bot_core::ImageLimits>,
) -> bool {
    use rusty_bot_core::{ContentPart, HistoryPart};
    let mut images = Vec::new();
    for message in messages {
        match message {
            Message::User { content }
            | Message::Assistant { content, .. }
            | Message::ToolResult { content, .. } => {
                images.extend(
                    content
                        .iter()
                        .filter(|part| matches!(part, ContentPart::Image { .. }))
                        .cloned(),
                );
            }
            Message::Rich { parts, .. } => {
                for part in parts {
                    match part {
                        HistoryPart::Image {
                            mime_type,
                            data_base64,
                        } => images.push(ContentPart::Image {
                            mime_type: mime_type.clone(),
                            data_base64: data_base64.clone(),
                        }),
                        HistoryPart::ToolResult { content, .. } => images.extend(
                            content
                                .iter()
                                .filter(|part| matches!(part, ContentPart::Image { .. }))
                                .cloned(),
                        ),
                        _ => {}
                    }
                }
            }
            Message::System { .. } => {}
        }
    }
    images.is_empty()
        || limits.is_some_and(|limits| {
            rusty_bot_core::image_validation::validate_images(&images, limits).is_ok()
        })
}

async fn validate_tool_images(output: &ToolOutput) -> Result<(), &'static str> {
    // Bound the complete persisted payload, including text and details, after hooks.
    if serde_json::to_vec(output)
        .map_err(|_| "invalid output")?
        .len()
        > 16 * 1024 * 1024
    {
        return Err("tool output exceeds size limit");
    }
    if !output
        .content
        .iter()
        .any(|part| matches!(part, rusty_bot_core::ContentPart::Image { .. }))
    {
        return Ok(());
    }
    let content = output.content.clone();
    tokio::task::spawn_blocking(move || {
        rusty_bot_core::image_validation::validate_images(
            &content,
            &rusty_bot_core::ImageLimits {
                max_bytes: rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES,
                max_width: 16_000,
                max_height: 16_000,
            },
        )
    })
    .await
    .map_err(|_| "image validation failed")?
}

fn output_from_recorded(operation: &ToolOperationRecord) -> Result<ToolOutput, SessionError> {
    let result = operation.result.clone().unwrap_or(Value::Null);
    serde_json::from_value(result)
        .map_err(|_| SessionError::ContractViolation("invalid recorded tool output".into()))
}

fn ensure_committed(receipt: CommitReceipt) -> Result<CommitReceipt, SessionError> {
    if let Some(refusal) = &receipt.refused {
        return Err(SessionError::ContractViolation(format!(
            "commit refused: {}",
            refusal.reason
        )));
    }
    Ok(receipt)
}

/// Re-exported convenience so provider message construction stays in one place.
pub fn provider_message_for(message: &Message) -> Option<ProviderMessage> {
    prompt::map_message(message)
}

/// Returned by [`SessionEngine::execute_tool`] and used by hosts to report a
/// tool-selected execution environment to the model.
pub fn tool_choice_for(tools: &[rusty_bot_core::ToolSpec]) -> ToolChoice {
    if tools.is_empty() {
        ToolChoice::None
    } else {
        ToolChoice::Auto
    }
}
