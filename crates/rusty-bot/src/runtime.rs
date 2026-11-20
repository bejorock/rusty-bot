use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use rusty_bot_core::{
    ConversationId, EntryCursor, EntryQuery, EventCursor, EventPage, Extension, HookJobPolicy,
    HookJobSupervisor, HookKey, HookReporter, LiveFrame, LiveSink, ModelProvider, Page,
    PromptSection, SessionError, SessionId, Storage, StorageError, StoredEntry, SystemClock, Tool,
    Workspace, WorkspaceError,
};
use rusty_bot_engine::{
    builtin_extension, Registry, RegistryBuilder, RunOutcome, SessionConfig, SessionDeps,
    SessionEngine, SessionSpec, SubmitOutcome, SubmitRequest,
};
use rusty_bot_memory::FileSessionAdapter;
use tokio::sync::{oneshot, Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("session is busy")]
    Busy,
    #[error("session not found")]
    NotFound,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
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
        Self::Engine(error)
    }
}

pub struct BotBuilder {
    provider: Arc<dyn ModelProvider>,
    storage: Option<Arc<dyn Storage>>,
    tools: Vec<Arc<dyn Tool>>,
    extensions: Vec<Arc<dyn Extension>>,
    reporter: Option<Arc<dyn HookReporter>>,
    compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>,
    hook_yield_limit: u32,
    job_policy: HookJobPolicy,
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

#[cfg(test)]
#[path = "runtime_unit_tests.rs"]
mod tests;

impl BotBuilder {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        Self {
            provider,
            storage: None,
            tools: Vec::new(),
            extensions: Vec::new(),
            reporter: None,
            compactor: None,
            hook_yield_limit: 8,
            job_policy: HookJobPolicy::default(),
        }
    }
    pub fn storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.storage = Some(storage);
        self
    }
    /// Inject a session adapter instead of the default `.rusty-bot/sessions.jsonl` journal.
    pub fn session_adapter(self, adapter: Arc<dyn Storage>) -> Self {
        self.storage(adapter)
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
    pub fn build(self) -> Result<Bot, SdkError> {
        if self.provider.id().trim().is_empty() {
            return Err(SdkError::InvalidConfig("provider ID is required".into()));
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
        let storage = match self.storage {
            Some(adapter) => adapter,
            None => {
                let directory = std::path::Path::new(".rusty-bot");
                std::fs::create_dir_all(directory)
                    .map_err(|e| SdkError::InvalidConfig(e.to_string()))?;
                Arc::new(FileSessionAdapter::open(directory.join("sessions.jsonl"))?)
                    as Arc<dyn Storage>
            }
        };
        Ok(Bot {
            inner: Arc::new(BotInner {
                provider: self.provider,
                storage,
                registry: Arc::new(builder.build()),
                reporter: self.reporter,
                compactor: self.compactor,
                hook_yield_limit: self.hook_yield_limit,
                job_policy: self.job_policy,
                slots: Mutex::new(HashMap::new()),
            }),
        })
    }
}

struct BotInner {
    provider: Arc<dyn ModelProvider>,
    storage: Arc<dyn Storage>,
    registry: Arc<Registry>,
    reporter: Option<Arc<dyn HookReporter>>,
    compactor: Option<Arc<dyn rusty_bot_core::RequestCompactor>>,
    hook_yield_limit: u32,
    job_policy: HookJobPolicy,
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
struct AdapterSink {
    adapter: Arc<dyn Storage>,
    error: Arc<Mutex<Option<StorageError>>>,
    cancel: CancellationToken,
}
impl LiveSink for AdapterSink {
    fn publish(&self, session_id: SessionId, frame: LiveFrame) {
        let mut failure = self.error.lock().unwrap_or_else(|e| e.into_inner());
        if failure.is_none() {
            if let Err(error) = self.adapter.append_frame(session_id, frame) {
                *failure = Some(error);
                self.cancel.cancel();
            }
        }
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
        let cancel = CancellationToken::new();
        // Hold the session gate while capturing the start cursor; otherwise a
        // racing run could publish frames between the snapshot and admission.
        let guard = {
            let state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.shutdowns != 0 {
                return Err(SdkError::Busy);
            }
            self.slot
                .gate
                .clone()
                .try_lock_owned()
                .map_err(|_| SdkError::Busy)?
        };
        let cursor = self.bot.inner.storage.last_frame_cursor(self.id).await?;
        let stream_error = Arc::new(Mutex::new(None));
        let sink = Arc::new(AdapterSink {
            adapter: self.bot.inner.storage.clone(),
            error: stream_error.clone(),
            cancel: cancel.clone(),
        });
        {
            let mut state = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.shutdowns != 0 {
                return Err(SdkError::Busy);
            }
            state.active = Some(cancel.clone());
        }
        let task_cancel = cancel.clone();
        let policy = self.bot.inner.job_policy;
        let slot = self.slot.clone();
        let deps = self
            .bot
            .deps(self.workspace.clone(), sink, &self.spec.config);
        let spec = self.spec.clone();
        let storage = self.bot.inner.storage.clone();
        let conversation = self.conversation;
        let reporter = self.bot.inner.reporter.clone();
        let compactor = self.bot.inner.compactor.clone();
        let (completion_tx, completion_rx) = oneshot::channel();
        let end_cursor = Arc::new(Mutex::new(None));
        let task_end_cursor = end_cursor.clone();
        let frame_adapter = self.bot.inner.storage.clone();
        let session_id = self.id;
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
            let mut result = match driver.await {
                Ok(result) => result,
                Err(error) => Err(SdkError::RunnerFailed(error.to_string())),
            };
            if result.is_err() || task_cancel.is_cancelled() {
                supervisor.cancel_and_drain().await;
            } else {
                supervisor.seal_and_drain().await;
            }
            if let Some(error) = stream_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                result = Err(SdkError::Storage(error));
            }
            match frame_adapter.last_frame_cursor(session_id).await {
                Ok(cursor) => {
                    *task_end_cursor.lock().unwrap_or_else(|e| e.into_inner()) = Some(cursor)
                }
                Err(error) => {
                    result = Err(SdkError::Storage(error));
                }
            }
            slot.state.lock().unwrap_or_else(|e| e.into_inner()).active = None;
            drop(guard);
            let _ = completion_tx.send(());
            result
        });
        Ok(RunHandle {
            adapter: self.bot.inner.storage.clone(),
            session_id: self.id,
            cursor,
            end_cursor,
            cancel,
            completion: Some(completion_rx),
            task: Some(task),
            completed: false,
        })
    }
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
}
pub struct RunHandle {
    adapter: Arc<dyn Storage>,
    session_id: SessionId,
    cursor: u64,
    end_cursor: Arc<Mutex<Option<u64>>>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<RunResult, SdkError>>>,
    completion: Option<oneshot::Receiver<()>>,
    completed: bool,
}
impl RunHandle {
    /// Follow persisted progress, draining the final records after completion.
    pub async fn next_event(&mut self) -> Result<Option<LiveFrame>, SdkError> {
        loop {
            let finished = self.task.as_ref().is_none_or(|task| task.is_finished());
            let records = self
                .adapter
                .read_frames(self.session_id, self.cursor, 1)
                .await?;
            let end = *self.end_cursor.lock().unwrap_or_else(|e| e.into_inner());
            if end.is_some_and(|end| self.cursor >= end) {
                return Ok(None);
            }
            if let Some(record) = records.into_iter().next() {
                if end.is_some_and(|end| record.cursor > end) {
                    return Ok(None);
                }
                self.cursor = record.cursor;
                return Ok(Some(record.frame));
            }
            if finished {
                return Ok(None);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
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
