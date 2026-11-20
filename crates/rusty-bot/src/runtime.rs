use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use rusty_bot_core::{
    ConversationId, EntryCursor, EntryQuery, EventCursor, EventPage, Extension, HookJobPolicy,
    HookJobSupervisor, HookKey, HookReporter, InterruptId, LiveFrame, LiveSink, ModelProvider,
    Page, PendingInterrupt, PendingReview, PromptSection, ReviewAnswer, SessionError, SessionId,
    Storage, StorageError, StoredEntry, SystemClock, Tool, Workspace, WorkspaceError,
};
use rusty_bot_engine::{
    builtin_extension, InterruptDriveOutcome, Registry, RegistryBuilder, ReviewDriveOutcome,
    RunOutcome, SessionConfig, SessionDeps, SessionEngine, SessionSpec, SubmitOutcome,
    SubmitRequest,
};
use rusty_bot_memory::MemoryStorage;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("session is busy")]
    Busy,
    #[error("session not found")]
    NotFound,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("pending review blocks submission")]
    PendingReview,
    #[error("invalid review: {0}")]
    InvalidReview(String),
    #[error("review answer conflicts with the accepted answer")]
    ReviewConflict,
    #[error("review already resolved")]
    ReviewAlreadyResolved,
    #[error("review configuration mismatch")]
    ReviewConfigMismatch,
    #[error("review is not enabled")]
    ReviewUnsupported,
    #[error("pending interrupt blocks submission")]
    PendingInterrupt,
    #[error("invalid interrupt: {0}")]
    InvalidInterrupt(String),
    #[error("interrupt answer conflicts with the accepted answer")]
    InterruptConflict,
    #[error("interrupt already resolved")]
    InterruptAlreadyResolved,
    #[error("interrupts unsupported")]
    InterruptUnsupported,
    #[error("interrupt protocol violation: {0}")]
    InterruptProtocol(String),
    #[error(transparent)]
    Engine(SessionError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("runner failed: {0}")]
    RunnerFailed(String),
}

impl From<SessionError> for SdkError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::PendingReview => Self::PendingReview,
            SessionError::InvalidReview(message) => Self::InvalidReview(message),
            SessionError::ReviewConflict => Self::ReviewConflict,
            SessionError::ReviewAlreadyResolved => Self::ReviewAlreadyResolved,
            SessionError::ReviewConfigMismatch => Self::ReviewConfigMismatch,
            SessionError::ReviewUnsupported => Self::ReviewUnsupported,
            SessionError::PendingInterrupt => Self::PendingInterrupt,
            SessionError::InvalidInterrupt(message) => Self::InvalidInterrupt(message),
            SessionError::InterruptConflict => Self::InterruptConflict,
            SessionError::InterruptAlreadyResolved => Self::InterruptAlreadyResolved,
            SessionError::InterruptUnsupported => Self::InterruptUnsupported,
            SessionError::InterruptProtocol(message) => Self::InterruptProtocol(message),
            other => Self::Engine(other),
        }
    }
}

pub struct BotBuilder {
    provider: Arc<dyn ModelProvider>,
    storage: Arc<dyn Storage>,
    tools: Vec<Arc<dyn Tool>>,
    extensions: Vec<Arc<dyn Extension>>,
    reporter: Option<Arc<dyn HookReporter>>,
    compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>,
    hook_yield_limit: u32,
    job_policy: HookJobPolicy,
    max_parallel_tools: NonZeroUsize,
    capacity: usize,
    human_review: bool,
    tool_interrupts: bool,
}

struct CustomTools {
    tools: Vec<Arc<dyn Tool>>,
    version: String,
}
impl Extension for CustomTools {
    fn name(&self) -> &str {
        "rusty-bot.sdk-tools"
    }
    fn version(&self) -> &str {
        &self.version
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }
    fn sections(&self) -> Vec<PromptSection> {
        Vec::new()
    }
}

impl BotBuilder {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        Self {
            provider,
            storage: Arc::new(MemoryStorage::new()),
            tools: Vec::new(),
            extensions: Vec::new(),
            reporter: None,
            compactor: None,
            hook_yield_limit: 8,
            job_policy: HookJobPolicy::default(),
            max_parallel_tools: NonZeroUsize::new(4).expect("nonzero"),
            capacity: 64,
            human_review: false,
            tool_interrupts: false,
        }
    }
    /// Enable explicit human review for sessions created or reopened by this bot.
    pub fn human_review(mut self, enabled: bool) -> Self {
        self.human_review = enabled;
        self
    }
    /// Opt into the transitional single-tool interrupt driver (default: false).
    /// Multi-tool rounds and composition with human review are not yet supported.
    pub fn tool_interrupts(mut self, enabled: bool) -> Self {
        self.tool_interrupts = enabled;
        self
    }
    pub fn storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.storage = storage;
        self
    }
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }
    pub fn extension(mut self, extension: Arc<dyn Extension>) -> Self {
        self.extensions.push(extension);
        self
    }
    /// Inject a request-local strategy; no built-in compaction is enabled.
    pub fn compactor(mut self, compactor: Arc<dyn rusty_bot_core::RequestCompactor>) -> Self {
        self.compactor = Some(compactor);
        self
    }
    pub fn hook_reporter(mut self, reporter: Arc<dyn HookReporter>) -> Self {
        self.reporter = Some(reporter);
        self
    }
    pub fn hook_yield_limit(mut self, limit: u32) -> Self {
        self.hook_yield_limit = limit;
        self
    }
    pub fn hook_job_policy(mut self, policy: HookJobPolicy) -> Self {
        self.job_policy = policy;
        self
    }
    /// Bound concurrent effects in engine-driven tool rounds; one disables overlap.
    pub fn max_parallel_tools(mut self, max: NonZeroUsize) -> Self {
        self.max_parallel_tools = max;
        self
    }
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }
    pub fn build(self) -> Result<Bot, SdkError> {
        if self.provider.id().trim().is_empty() || self.capacity == 0 {
            return Err(SdkError::InvalidConfig(
                "provider ID and nonzero event capacity are required".into(),
            ));
        }
        if self.human_review && self.tool_interrupts {
            return Err(SdkError::InvalidConfig(
                "tool interrupts cannot yet be combined with human review".into(),
            ));
        }
        if !self.job_policy.is_valid() {
            return Err(SdkError::InvalidConfig(
                "hook job policy limits must be nonzero".into(),
            ));
        }
        let mut names: HashSet<String> = builtin_extension()
            .tools()
            .iter()
            .map(|tool| tool.spec().name)
            .collect();
        if self.human_review {
            names.insert("ask_user".into());
        }
        let mut descriptors = Vec::new();
        for extension in &self.extensions {
            for tool in extension.tools() {
                let spec = tool.spec();
                if spec.name.trim().is_empty() || !names.insert(spec.name.clone()) {
                    return Err(SdkError::InvalidConfig(format!(
                        "empty or duplicate tool name: {}",
                        spec.name
                    )));
                }
                jsonschema::validator_for(&spec.parameters)
                    .map_err(|e| SdkError::InvalidConfig(format!("invalid tool schema: {e}")))?;
            }
        }
        for tool in &self.tools {
            let spec = tool.spec();
            if spec.name.trim().is_empty() || !names.insert(spec.name.clone()) {
                return Err(SdkError::InvalidConfig(format!(
                    "empty or duplicate tool name: {}",
                    spec.name
                )));
            }
            jsonschema::validator_for(&spec.parameters)
                .map_err(|e| SdkError::InvalidConfig(format!("invalid tool schema: {e}")))?;
            descriptors.push(
                serde_json::to_string(&spec).map_err(|e| SdkError::InvalidConfig(e.to_string()))?,
            );
        }
        let mut builder = RegistryBuilder::new().add_builtin_tools();
        if !self.tools.is_empty() {
            // A deterministic declaration fingerprint detects changes in tool specs.
            let version = descriptors.join("|");
            builder = builder
                .add_extension(Arc::new(CustomTools {
                    tools: self.tools,
                    version,
                }))
                .map_err(|e| SdkError::InvalidConfig(e.to_string()))?;
        }
        for extension in self.extensions {
            builder = builder
                .add_extension(extension)
                .map_err(|e| SdkError::InvalidConfig(e.to_string()))?;
        }
        Ok(Bot {
            inner: Arc::new(BotInner {
                provider: self.provider,
                storage: self.storage,
                registry: Arc::new(builder.build()),
                capacity: self.capacity,
                reporter: self.reporter,
                compactor: self.compactor,
                hook_yield_limit: self.hook_yield_limit,
                job_policy: self.job_policy,
                max_parallel_tools: self.max_parallel_tools,
                human_review: self.human_review,
                tool_interrupts: self.tool_interrupts,
                slots: Mutex::new(HashMap::new()),
            }),
        })
    }
}

struct BotInner {
    provider: Arc<dyn ModelProvider>,
    storage: Arc<dyn Storage>,
    registry: Arc<Registry>,
    capacity: usize,
    reporter: Option<Arc<dyn HookReporter>>,
    compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>,
    hook_yield_limit: u32,
    job_policy: HookJobPolicy,
    max_parallel_tools: NonZeroUsize,
    human_review: bool,
    tool_interrupts: bool,
    slots: Mutex<HashMap<SessionId, Arc<SessionSlot>>>,
}
#[derive(Default)]
struct SessionSlot {
    gate: Arc<AsyncMutex<()>>,
    state: Mutex<SlotState>,
}
#[derive(Default)]
struct SlotState {
    active: Option<CancellationToken>,
    shutdowns: usize,
}
struct ShutdownReservation(Arc<SessionSlot>);
impl Drop for ShutdownReservation {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .shutdowns -= 1;
    }
}
#[derive(Clone)]
pub struct Bot {
    inner: Arc<BotInner>,
}
impl Bot {
    pub fn builder(provider: Arc<dyn ModelProvider>) -> BotBuilder {
        BotBuilder::new(provider)
    }
    fn slot(&self, id: SessionId) -> Arc<SessionSlot> {
        self.inner
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(id)
            .or_default()
            .clone()
    }
    fn deps(
        &self,
        workspace: Arc<dyn Workspace>,
        live: Arc<dyn LiveSink>,
        config: &SessionConfig,
    ) -> SessionDeps {
        SessionDeps {
            storage: self.inner.storage.clone(),
            workspace,
            provider: self.inner.provider.clone(),
            registry: self.inner.registry.clone(),
            clock: Arc::new(SystemClock),
            live,
            progress: config.progress.clone(),
        }
    }
    pub async fn create_session(
        &self,
        workspace: Arc<dyn Workspace>,
        config: SessionConfig,
    ) -> Result<Session, SdkError> {
        self.create_session_with_hooks(workspace, config, BTreeMap::new())
            .await
    }
    pub async fn create_session_with_hooks(
        &self,
        workspace: Arc<dyn Workspace>,
        config: SessionConfig,
        hook_configs: BTreeMap<HookKey, serde_json::Value>,
    ) -> Result<Session, SdkError> {
        config
            .validate_for_provider(&self.inner.registry, self.inner.provider.id())
            .map_err(SdkError::InvalidConfig)?;
        let id = SessionId::new();
        let mut spec = SessionSpec::for_registry(id, config, &self.inner.registry)?;
        for (key, value) in hook_configs {
            let descriptor = spec
                .hook_descriptors
                .iter_mut()
                .find(|d| d.extension == key.extension && d.hook == key.hook)
                .ok_or_else(|| {
                    SdkError::InvalidConfig("unknown or unselected hook configuration".into())
                })?;
            descriptor.config = value;
        }
        spec.allow_hook_snapshot = true;
        spec.review_enabled = self.inner.human_review;
        spec.hook_yield_limit = self.inner.hook_yield_limit;
        self.open(id, workspace, spec).await
    }
    pub async fn resume_session(
        &self,
        id: SessionId,
        workspace: Arc<dyn Workspace>,
    ) -> Result<Session, SdkError> {
        let slot = self.slot(id);
        let guard = slot
            .gate
            .clone()
            .try_lock_owned()
            .map_err(|_| SdkError::Busy)?;
        let record = self
            .inner
            .storage
            .get_session(id)
            .await?
            .ok_or(SdkError::NotFound)?;
        let mut stored = record.config;
        let snapshot = if stored
            .get("config_version")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
        {
            let snapshot: rusty_bot_engine::session::HookSnapshot = serde_json::from_value(
                stored
                    .get("hook_snapshot")
                    .cloned()
                    .ok_or_else(|| SdkError::InvalidConfig("missing hook snapshot".into()))?,
            )
            .map_err(|_| SdkError::InvalidConfig("invalid hook snapshot".into()))?;
            stored["config_version"] = serde_json::json!(1);
            Some(snapshot)
        } else {
            None
        };
        let config: SessionConfig =
            serde_json::from_value(stored).map_err(|e| SdkError::InvalidConfig(e.to_string()))?;
        config
            .validate_for_provider(&self.inner.registry, self.inner.provider.id())
            .map_err(SdkError::InvalidConfig)?;
        let mut spec = SessionSpec::for_registry(id, config, &self.inner.registry)?;
        spec.allow_hook_snapshot = true;
        spec.review_enabled = self.inner.human_review;
        spec.hook_yield_limit = self.inner.hook_yield_limit;
        // Historical records intentionally remain hook-free even if new hooks were installed.
        spec.hook_descriptors = snapshot.map_or_else(Vec::new, |s| s.descriptors);
        self.open_locked(id, workspace, spec, slot, guard).await
    }
    async fn open(
        &self,
        id: SessionId,
        workspace: Arc<dyn Workspace>,
        spec: SessionSpec,
    ) -> Result<Session, SdkError> {
        let slot = self.slot(id);
        let guard = slot
            .gate
            .clone()
            .try_lock_owned()
            .map_err(|_| SdkError::Busy)?;
        self.open_locked(id, workspace, spec, slot, guard).await
    }
    async fn open_locked(
        &self,
        id: SessionId,
        workspace: Arc<dyn Workspace>,
        spec: SessionSpec,
        slot: Arc<SessionSlot>,
        _guard: OwnedMutexGuard<()>,
    ) -> Result<Session, SdkError> {
        let config = &spec.config;
        let engine = SessionEngine::open(
            self.deps(
                workspace.clone(),
                Arc::new(rusty_bot_core::NullLiveSink),
                config,
            ),
            spec.clone(),
        )
        .await?;
        Ok(Session {
            bot: self.clone(),
            id,
            conversation: engine.conversation_id(),
            workspace,
            spec,
            slot,
        })
    }
}

#[derive(Clone)]
pub struct Session {
    bot: Bot,
    id: SessionId,
    conversation: ConversationId,
    workspace: Arc<dyn Workspace>,
    spec: SessionSpec,
    slot: Arc<SessionSlot>,
}
struct AdmissionCancellation(Option<CancellationToken>);

impl Drop for AdmissionCancellation {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}

enum InterruptStart {
    Prompt(SubmitRequest),
    Resume(InterruptId, serde_json::Value),
}

enum ReviewStart {
    Prompt(SubmitRequest),
    Resume(ReviewAnswer),
}
struct ChannelSink(mpsc::Sender<LiveFrame>);
impl LiveSink for ChannelSink {
    fn publish(&self, _session_id: SessionId, frame: LiveFrame) {
        let _ = self.0.try_send(frame);
    }
}
impl Session {
    pub fn id(&self) -> SessionId {
        self.id
    }
    pub fn conversation_id(&self) -> ConversationId {
        self.conversation
    }
    pub async fn prompt(&self, request_id: String, text: String) -> Result<RunHandle, SdkError> {
        self.submit_prompt(SubmitRequest::Input { request_id, text })
            .await
    }

    pub async fn prompt_content(
        &self,
        request_id: String,
        content: Vec<rusty_bot_core::ContentPart>,
    ) -> Result<RunHandle, SdkError> {
        self.submit_prompt(SubmitRequest::Content {
            request_id,
            content,
        })
        .await
    }

    fn validate_run_hooks(&self) -> Result<(), SdkError> {
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
            return Err(SdkError::InvalidConfig(
                "selected hook point is not implemented".into(),
            ));
        }
        Ok(())
    }

    async fn submit_prompt(&self, request: SubmitRequest) -> Result<RunHandle, SdkError> {
        self.validate_run_hooks()?;
        if self.spec.review_enabled {
            return self.start_review_run(ReviewStart::Prompt(request)).await;
        }
        if self.bot.inner.tool_interrupts {
            return self
                .start_interrupt_run(InterruptStart::Prompt(request))
                .await;
        }
        let cancel = CancellationToken::new();
        let guard = {
            let mut state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.shutdowns != 0 {
                return Err(SdkError::Busy);
            }
            let guard = self
                .slot
                .gate
                .clone()
                .try_lock_owned()
                .map_err(|_| SdkError::Busy)?;
            state.active = Some(cancel.clone());
            guard
        };
        let (sender, receiver) = mpsc::channel(self.bot.inner.capacity);
        let task_cancel = cancel.clone();
        let policy = self.bot.inner.job_policy;
        let slot = self.slot.clone();
        let deps = self.bot.deps(
            self.workspace.clone(),
            Arc::new(ChannelSink(sender)),
            &self.spec.config,
        );
        let spec = self.spec.clone();
        let storage = self.bot.inner.storage.clone();
        let conversation = self.conversation;
        let reporter = self.bot.inner.reporter.clone();
        let compactor = self.bot.inner.compactor.clone();
        let max_parallel_tools = self.bot.inner.max_parallel_tools;
        let (completion_tx, completion_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let supervisor = Arc::new(
                HookJobSupervisor::new(policy, task_cancel.clone())
                    .expect("validated hook job policy"),
            );
            let driver_supervisor = supervisor.clone();
            let driver_cancel = task_cancel.clone();
            let driver = tokio::spawn(async move {
                let task_cancel = driver_cancel;
                let mut engine = SessionEngine::open(deps, spec).await?;
                engine.set_hook_reporter(reporter);
                engine.set_compactor(compactor);
                engine.set_max_parallel_tools(max_parallel_tools);
                let submission = engine.submit(request).await?;
                let outcome = if matches!(submission, SubmitOutcome::Accepted { .. }) {
                    engine
                        .run_until_idle_with_supervisor(task_cancel.clone(), driver_supervisor)
                        .await?
                } else {
                    RunOutcome {
                        turns_completed: 0,
                        idled: true,
                    }
                };
                if let SubmitOutcome::Accepted { request_id } = &submission {
                    // The engine may return at a cancellation boundary before it
                    // has placed this input, or between its committed steps. Settle
                    // this submission only, without driving any unrelated work.
                    if !outcome.idled && task_cancel.is_cancelled() {
                        engine.cancel_accepted_input(request_id).await?;
                    }
                    if let Some(stored) = storage
                        .find_submission_by_request(conversation, request_id)
                        .await?
                    {
                        if let Some(error) = stored.record.error {
                            // Cancellation also commits a submission error. Inspect the typed
                            // generation or interrupted-tool entry, not its message: provider
                            // failures can race cancellation and must remain errors.
                            let cancelled = task_cancel.is_cancelled()
                                && submission_interrupted(
                                    &*storage,
                                    conversation,
                                    stored.record.id,
                                )
                                .await?;
                            if !cancelled {
                                return Err(SdkError::RunnerFailed(error));
                            }
                        }
                    }
                }
                let status = if outcome.idled {
                    RunStatus::Idle
                } else if task_cancel.is_cancelled() {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Idle
                };
                Ok(RunResult {
                    submission,
                    outcome,
                    status,
                })
            });
            let result = match driver.await {
                Ok(result) => result,
                Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
            };
            if result.is_err() || task_cancel.is_cancelled() {
                supervisor.cancel_and_drain().await;
            } else {
                supervisor.seal_and_drain().await;
            }
            slot.state.lock().unwrap_or_else(|e| e.into_inner()).active = None;
            drop(guard);
            let _ = completion_tx.send(());
            result
        });
        Ok(RunHandle {
            receiver,
            cancel,
            completion: Some(completion_rx),
            task: Some(task),
            completed: false,
        })
    }
    /// Read the committed interrupt while no run or shutdown owns the session.
    pub async fn pending_interrupt(&self) -> Result<Option<PendingInterrupt>, SdkError> {
        if !self.bot.inner.tool_interrupts {
            return Err(SdkError::InterruptUnsupported);
        }
        let _guard = self
            .slot
            .gate
            .clone()
            .try_lock_owned()
            .map_err(|_| SdkError::Busy)?;
        if self
            .slot
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .shutdowns
            != 0
        {
            return Err(SdkError::Busy);
        }
        let engine = SessionEngine::open(
            self.bot.deps(
                self.workspace.clone(),
                Arc::new(rusty_bot_core::NullLiveSink),
                &self.spec.config,
            ),
            self.spec.clone(),
        )
        .await?;
        Ok(engine
            .into_sdk_interrupt_driver()?
            .pending_interrupt()
            .await?)
    }

    pub async fn resume_interrupt(
        &self,
        id: InterruptId,
        value: serde_json::Value,
    ) -> Result<RunHandle, SdkError> {
        if !self.bot.inner.tool_interrupts {
            return Err(SdkError::InterruptUnsupported);
        }
        self.validate_run_hooks()?;
        self.start_interrupt_run(InterruptStart::Resume(id, value))
            .await
    }

    async fn start_interrupt_run(&self, start: InterruptStart) -> Result<RunHandle, SdkError> {
        let cancel = CancellationToken::new();
        let guard = {
            let mut state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.shutdowns != 0 {
                return Err(SdkError::Busy);
            }
            let guard = self
                .slot
                .gate
                .clone()
                .try_lock_owned()
                .map_err(|_| SdkError::Busy)?;
            state.active = Some(cancel.clone());
            guard
        };
        let (sender, receiver) = mpsc::channel(self.bot.inner.capacity);
        let deps = self.bot.deps(
            self.workspace.clone(),
            Arc::new(ChannelSink(sender)),
            &self.spec.config,
        );
        let spec = self.spec.clone();
        let reporter = self.bot.inner.reporter.clone();
        let compactor = self.bot.inner.compactor.clone();
        let max_parallel_tools = self.bot.inner.max_parallel_tools;
        let (admission_tx, admission_rx) = oneshot::channel();
        let (handoff_tx, handoff_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let slot = self.slot.clone();
        let task_cancel = cancel.clone();
        let policy = self.bot.inner.job_policy;
        let storage = self.bot.inner.storage.clone();
        let conversation = self.conversation;
        let task = tokio::spawn(async move {
            let supervisor = Arc::new(
                HookJobSupervisor::new(policy, task_cancel.clone())
                    .expect("validated hook job policy"),
            );
            let drive_supervisor = supervisor.clone();
            let drive_cancel = task_cancel.clone();
            let worker = tokio::spawn(async move {
                let admitted = async {
                    let mut engine = SessionEngine::open(deps, spec).await?;
                    engine.set_hook_reporter(reporter);
                    engine.set_compactor(compactor);
                    engine.set_max_parallel_tools(max_parallel_tools);
                    let mut driver = engine.into_sdk_interrupt_driver()?;
                    let (submission, resume) = match start {
                        InterruptStart::Prompt(request) => (driver.submit(request).await?, false),
                        InterruptStart::Resume(id, value) => {
                            let request_id = driver
                                .admit_interrupt(id, value, drive_cancel.clone())
                                .await?;
                            (SubmitOutcome::Accepted { request_id }, true)
                        }
                    };
                    Ok::<_, SdkError>((driver, submission, resume))
                }
                .await;
                let (mut driver, submission, resume) = match admitted {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = admission_tx.send(Err(error));
                        return Ok(None);
                    }
                };
                if admission_tx.send(Ok(())).is_err() || handoff_rx.await.is_err() {
                    drive_cancel.cancel();
                }
                let outcome = if matches!(submission, SubmitOutcome::Accepted { .. })
                    || (matches!(submission, SubmitOutcome::Duplicate { .. })
                        && driver.pending_interrupt().await?.is_some())
                {
                    match driver
                        .run_until_idle_with_supervisor(drive_cancel.clone(), drive_supervisor)
                        .await
                    {
                        Err(SessionError::Cancelled) if drive_cancel.is_cancelled() => {
                            InterruptDriveOutcome::Settled(RunOutcome {
                                turns_completed: 0,
                                idled: false,
                            })
                        }
                        result => result?,
                    }
                } else {
                    InterruptDriveOutcome::Settled(RunOutcome {
                        turns_completed: 0,
                        idled: true,
                    })
                };
                if let SubmitOutcome::Accepted { request_id } = &submission {
                    if !resume
                        && matches!(&outcome, InterruptDriveOutcome::Settled(result) if !result.idled)
                        && drive_cancel.is_cancelled()
                    {
                        driver.cancel_accepted_input(request_id).await?;
                    }
                    if let Some(stored) = storage
                        .find_submission_by_request(conversation, request_id)
                        .await?
                    {
                        if let Some(error) = stored.record.error {
                            let cancelled = drive_cancel.is_cancelled()
                                && submission_interrupted(
                                    &*storage,
                                    conversation,
                                    stored.record.id,
                                )
                                .await?;
                            if !cancelled {
                                return Err(SdkError::RunnerFailed(error));
                            }
                        }
                    }
                }
                let (outcome, status) = match outcome {
                    InterruptDriveOutcome::Interrupted(pending) => (
                        RunOutcome {
                            turns_completed: 0,
                            idled: false,
                        },
                        RunStatus::Interrupted(pending),
                    ),
                    InterruptDriveOutcome::Settled(outcome) => {
                        let status = if !outcome.idled && drive_cancel.is_cancelled() {
                            RunStatus::Cancelled
                        } else {
                            RunStatus::Idle
                        };
                        (outcome, status)
                    }
                };
                Ok::<_, SdkError>(Some(RunResult {
                    submission,
                    outcome,
                    status,
                }))
            });
            let result = match worker.await {
                Ok(Ok(Some(result))) => Ok(result),
                Ok(Ok(None)) => Err(SdkError::RunnerFailed("admission failed".into())),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
            };
            if result.is_err() || task_cancel.is_cancelled() {
                supervisor.cancel_and_drain().await;
            } else {
                supervisor.seal_and_drain().await;
            }
            slot.state.lock().unwrap_or_else(|e| e.into_inner()).active = None;
            drop(guard);
            let _ = completion_tx.send(());
            result
        });
        let mut admission_cancel = AdmissionCancellation(Some(cancel.clone()));
        match admission_rx.await {
            Ok(Ok(())) => {
                let _ = handoff_tx.send(());
                admission_cancel.0 = None;
                Ok(RunHandle {
                    receiver,
                    cancel,
                    task: Some(task),
                    completion: Some(completion_rx),
                    completed: false,
                })
            }
            Ok(Err(error)) => {
                let _ = completion_rx.await;
                Err(error)
            }
            Err(_) => {
                let _ = completion_rx.await;
                match task.await {
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
                    Ok(Ok(_)) => Err(SdkError::RunnerFailed("admission channel closed".into())),
                }
            }
        }
    }

    /// Read committed review state while holding the same exclusive session gate as runs.
    pub async fn pending_review(&self) -> Result<Option<PendingReview>, SdkError> {
        if !self.spec.review_enabled {
            return Err(SdkError::ReviewUnsupported);
        }
        let _guard = self
            .slot
            .gate
            .clone()
            .try_lock_owned()
            .map_err(|_| SdkError::Busy)?;
        if self
            .slot
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .shutdowns
            != 0
        {
            return Err(SdkError::Busy);
        }
        let engine = SessionEngine::open(
            self.bot.deps(
                self.workspace.clone(),
                Arc::new(rusty_bot_core::NullLiveSink),
                &self.spec.config,
            ),
            self.spec.clone(),
        )
        .await?;
        Ok(engine.into_sdk_review_driver()?.pending_review().await?)
    }

    pub async fn resume_review(&self, answer: ReviewAnswer) -> Result<RunHandle, SdkError> {
        if !self.spec.review_enabled {
            return Err(SdkError::ReviewUnsupported);
        }
        self.validate_run_hooks()?;
        self.start_review_run(ReviewStart::Resume(answer)).await
    }

    async fn start_review_run(&self, start: ReviewStart) -> Result<RunHandle, SdkError> {
        let cancel = CancellationToken::new();
        let guard = {
            let mut state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.shutdowns != 0 {
                return Err(SdkError::Busy);
            }
            let guard = self
                .slot
                .gate
                .clone()
                .try_lock_owned()
                .map_err(|_| SdkError::Busy)?;
            state.active = Some(cancel.clone());
            guard
        };
        let (sender, receiver) = mpsc::channel(self.bot.inner.capacity);
        let deps = self.bot.deps(
            self.workspace.clone(),
            Arc::new(ChannelSink(sender)),
            &self.spec.config,
        );
        let spec = self.spec.clone();
        let reporter = self.bot.inner.reporter.clone();
        let compactor = self.bot.inner.compactor.clone();
        let max_parallel_tools = self.bot.inner.max_parallel_tools;
        let (admission_tx, admission_rx) = oneshot::channel();
        let (handoff_tx, handoff_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let slot = self.slot.clone();
        let task_cancel = cancel.clone();
        let policy = self.bot.inner.job_policy;
        let storage = self.bot.inner.storage.clone();
        let conversation = self.conversation;
        let task = tokio::spawn(async move {
            let supervisor = Arc::new(
                HookJobSupervisor::new(policy, task_cancel.clone())
                    .expect("validated hook job policy"),
            );
            let drive_supervisor = supervisor.clone();
            let drive_cancel = task_cancel.clone();
            let worker = tokio::spawn(async move {
                let admitted = async {
                    let mut engine = SessionEngine::open(deps, spec).await?;
                    engine.set_hook_reporter(reporter);
                    engine.set_compactor(compactor);
                    engine.set_max_parallel_tools(max_parallel_tools);
                    let mut driver = engine.into_sdk_review_driver()?;
                    let (submission, resume) = match start {
                        ReviewStart::Prompt(request) => (driver.submit(request).await?, false),
                        ReviewStart::Resume(answer) => {
                            let request_id = driver.review_request_id(&answer.id).await?;
                            driver
                                .admit_review_with_supervisor(
                                    answer,
                                    drive_cancel.clone(),
                                    drive_supervisor.clone(),
                                )
                                .await?;
                            (SubmitOutcome::Accepted { request_id }, true)
                        }
                    };
                    Ok::<_, SdkError>((driver, submission, resume))
                }
                .await;
                let (mut driver, drive_submission, resume) = match admitted {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = admission_tx.send(Err(error));
                        return Ok(None);
                    }
                };
                if admission_tx.send(Ok(())).is_err() || handoff_rx.await.is_err() {
                    drive_cancel.cancel();
                }
                let outcome = if resume {
                    driver
                        .continue_review_with_supervisor(drive_cancel.clone(), drive_supervisor)
                        .await?
                } else if matches!(drive_submission, SubmitOutcome::Accepted { .. })
                    || (matches!(drive_submission, SubmitOutcome::Duplicate { .. })
                        && driver.pending_review().await?.is_some())
                {
                    driver
                        .run_until_idle_with_supervisor(drive_cancel.clone(), drive_supervisor)
                        .await?
                } else {
                    ReviewDriveOutcome::Settled(RunOutcome {
                        turns_completed: 0,
                        idled: true,
                    })
                };
                if let SubmitOutcome::Accepted { request_id } = &drive_submission {
                    if !resume
                        && matches!(&outcome, ReviewDriveOutcome::Settled(result) if !result.idled)
                        && drive_cancel.is_cancelled()
                    {
                        driver.cancel_accepted_input(request_id).await?;
                    }
                    if let Some(stored) = storage
                        .find_submission_by_request(conversation, request_id)
                        .await?
                    {
                        if let Some(error) = stored.record.error {
                            let cancelled = drive_cancel.is_cancelled()
                                && submission_interrupted(
                                    &*storage,
                                    conversation,
                                    stored.record.id,
                                )
                                .await?;
                            if !cancelled {
                                return Err(SdkError::RunnerFailed(error));
                            }
                        }
                    }
                }
                let (outcome, status) = match outcome {
                    ReviewDriveOutcome::Paused(pending) => (
                        RunOutcome {
                            turns_completed: 0,
                            idled: false,
                        },
                        RunStatus::Paused(pending),
                    ),
                    ReviewDriveOutcome::Settled(outcome) => {
                        let status = if !outcome.idled && drive_cancel.is_cancelled() {
                            RunStatus::Cancelled
                        } else {
                            RunStatus::Idle
                        };
                        (outcome, status)
                    }
                };
                Ok::<_, SdkError>(Some(RunResult {
                    submission: drive_submission,
                    outcome,
                    status,
                }))
            });
            let result = match worker.await {
                Ok(Ok(Some(result))) => Ok(result),
                Ok(Ok(None)) => Err(SdkError::RunnerFailed("admission failed".into())),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
            };
            if result.is_err() || task_cancel.is_cancelled() {
                supervisor.cancel_and_drain().await;
            } else {
                supervisor.seal_and_drain().await;
            }
            slot.state.lock().unwrap_or_else(|e| e.into_inner()).active = None;
            drop(guard);
            let _ = completion_tx.send(());
            result
        });
        // Cancellation is armed before the first admission await: dropping this
        // future cannot orphan a committed submission or answer.
        let mut admission_cancel = AdmissionCancellation(Some(cancel.clone()));
        match admission_rx.await {
            Ok(Ok(())) => {
                let _ = handoff_tx.send(());
                admission_cancel.0 = None;
                Ok(RunHandle {
                    receiver,
                    cancel,
                    task: Some(task),
                    completion: Some(completion_rx),
                    completed: false,
                })
            }
            Ok(Err(error)) => {
                let _ = completion_rx.await;
                Err(error)
            }
            Err(_) => {
                let _ = completion_rx.await;
                match task.await {
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
                    Ok(Ok(_)) => Err(SdkError::RunnerFailed("admission channel closed".into())),
                }
            }
        }
    }

    /// Cancel the active run and wait for its owned work to be reaped.
    pub async fn shutdown(&self) -> Result<(), SdkError> {
        {
            let mut state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            state.shutdowns += 1;
            if let Some(cancel) = &state.active {
                cancel.cancel();
            }
        }
        let _reservation = ShutdownReservation(self.slot.clone());
        let _guard = self.slot.gate.lock().await;
        Ok(())
    }
    pub async fn events(&self, after: EventCursor, limit: usize) -> Result<EventPage, SdkError> {
        if limit == 0 {
            return Err(SdkError::InvalidConfig("limit must be nonzero".into()));
        }
        Ok(self
            .bot
            .inner
            .storage
            .scan_events(self.id, after, limit)
            .await?)
    }
    pub async fn entries(
        &self,
        cursor: Option<EntryCursor>,
        limit: usize,
    ) -> Result<Page<StoredEntry, EntryCursor>, SdkError> {
        if limit == 0 {
            return Err(SdkError::InvalidConfig("limit must be nonzero".into()));
        }
        let mut cursor = cursor;
        let mut items = Vec::new();
        loop {
            let page = self
                .bot
                .inner
                .storage
                .scan_entries(
                    EntryQuery {
                        conversation_id: self.conversation,
                        kind: None,
                        by_task_id: None,
                        ascending: true,
                    },
                    cursor,
                    limit - items.len(),
                )
                .await?;
            let count = page.items.len();
            items.extend(
                page.items.into_iter().filter(|entry| {
                    entry.record.kind != "rusty-bot.internal.native-reset-marker.v1"
                }),
            );
            match page.next {
                Some(next) if count > 0 && items.len() < limit => cursor = Some(next),
                next => return Ok(Page { items, next }),
            }
        }
    }
}
async fn submission_interrupted(
    storage: &dyn Storage,
    conversation: ConversationId,
    submission: rusty_bot_core::SubmissionId,
) -> Result<bool, StorageError> {
    let mut cursor = None;
    loop {
        let page = storage
            .scan_entries(
                EntryQuery {
                    conversation_id: conversation,
                    kind: None,
                    by_task_id: None,
                    ascending: true,
                },
                cursor,
                100,
            )
            .await?;
        for entry in page.items {
            let cancelled_generation = entry.record.kind == rusty_bot_core::entry_kind::ERROR
                && entry.record.data["error"]["kind"] == "cancelled";
            let interrupted_tool = entry.record.kind == rusty_bot_core::entry_kind::TOOL_RESULT
                && entry.record.data["interrupted"] == true;
            if !cancelled_generation && !interrupted_tool {
                continue;
            }
            if cancelled_generation
                && entry.record.data["submission_id"].as_i64() == Some(submission.0)
            {
                return Ok(true);
            }
            if let Some(task_id) = entry.record.by_task_id {
                if let Some(task) = storage.get_task(task_id).await? {
                    if task.record.input["submission_id"].as_i64() == Some(submission.0) {
                        return Ok(true);
                    }
                }
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => return Ok(false),
        }
    }
}

#[derive(Debug)]
pub struct RunResult {
    pub submission: SubmitOutcome,
    pub outcome: RunOutcome,
    pub status: RunStatus,
}
#[derive(Clone, Debug, PartialEq)]
pub enum RunStatus {
    Idle,
    Cancelled,
    Paused(PendingReview),
    Interrupted(PendingInterrupt),
}
pub struct RunHandle {
    receiver: mpsc::Receiver<LiveFrame>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<RunResult, SdkError>>>,
    completion: Option<oneshot::Receiver<()>>,
    completed: bool,
}
impl RunHandle {
    pub async fn next_event(&mut self) -> Option<LiveFrame> {
        self.receiver.recv().await
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub async fn wait(mut self) -> Result<RunResult, SdkError> {
        self.completion
            .take()
            .expect("completion is present")
            .await
            .map_err(|_| SdkError::RunnerFailed("runner exited before releasing session".into()))?;
        let result = self
            .task
            .as_mut()
            .expect("task is present")
            .await
            .map_err(|e| SdkError::RunnerFailed(e.to_string()))?;
        self.completed = true;
        result
    }
}
impl Drop for RunHandle {
    fn drop(&mut self) {
        if !self.completed {
            self.cancel.cancel();
        }
    }
}
