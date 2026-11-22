//! Embeddable host API. Configuration and storage are owned by the caller.
use super::*;
use rusty_bot_session::{SessionConfig, StoredSession};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, HistoryEventFilterType, Url,
    WorkflowDescribeOptions, WorkflowExecuteUpdateOptions, WorkflowExecutionInfo,
    WorkflowFetchHistoryOptions, WorkflowGetResultOptions, WorkflowHandle,
    WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

pub use temporalio_client::WorkflowExecutionStatus as ExecutionStatus;

pub type EngineError = Box<dyn std::error::Error + Send + Sync>;
pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub address: String,
    pub namespace: String,
    pub task_queue: String,
    /// Durable between-turn inactivity timer, not a hard session lifetime.
    pub idle_timeout: Duration,
}

pub struct Engine {
    runtime: Runtime,
    client: Client,
    provider: Arc<dyn rusty_bot_providers::Provider>,
    storage: Arc<dyn SessionStorage>,
    options: EngineOptions,
    rollover: RolloverPolicy,
    activity_timeout_ms: u64,
}
impl Engine {
    pub async fn connect(
        options: EngineOptions,
        provider: Arc<dyn rusty_bot_providers::Provider>,
        storage: Arc<dyn SessionStorage>,
    ) -> EngineResult<Self> {
        if options.namespace.trim().is_empty()
            || options.task_queue.trim().is_empty()
            || options.idle_timeout.as_millis() == 0
            || options.idle_timeout.as_millis() > u64::MAX as u128
        {
            return Err(
                "namespace, task queue and a positive representable idle timeout are required"
                    .into(),
            );
        }
        // A provider without an operation budget cannot be safely bounded by the Activity.
        let budget = provider
            .operation_timeout()
            .ok_or("provider must declare an operation timeout")?;
        if budget.is_zero() {
            return Err("provider operation timeout must be positive".into());
        }
        let activity_timeout_ms: u64 = budget
            .checked_add(Duration::from_secs(60))
            .ok_or("provider timeout too large")?
            .as_millis()
            .try_into()
            .map_err(|_| "provider timeout too large")?;
        let connection =
            Connection::connect(ConnectionOptions::new(Url::parse(&options.address)?).build())
                .await?;
        let client = Client::new(
            connection,
            ClientOptions::new(options.namespace.clone()).build(),
        )?;
        Ok(Self {
            runtime: Runtime::from_current_tokio(Default::default())?,
            client,
            provider,
            storage,
            options,
            rollover: RolloverPolicy::default(),
            activity_timeout_ms,
        })
    }
    /// Sets between-turn rollover thresholds for newly started executions.
    /// Existing runs retain their recorded policy across worker changes/rollovers.
    pub fn with_rollover_policy(mut self, policy: RolloverPolicy) -> EngineResult<Self> {
        if policy.max_turns == 0 || policy.max_history_events == 0 {
            return Err("rollover thresholds must be positive".into());
        }
        self.rollover = policy;
        Ok(self)
    }
    /// Stopping the host worker does not close or delete sessions.
    pub async fn run_until(
        &self,
        until: impl std::future::Future<Output = ()>,
    ) -> EngineResult<()> {
        let worker = Worker::new(
            &self.runtime,
            self.client.clone(),
            WorkerOptions::new(self.options.task_queue.clone())
                .register_activities(LlmActivities::with_storage(
                    self.provider.clone(),
                    self.storage.clone(),
                ))
                .register_activities(SessionActivities::new(self.storage.clone()))
                .register_workflow::<ChatSessionWorkflow>()?
                .build(),
        )?;
        worker.run_until(until).await?;
        Ok(())
    }
    /// Creates a logical session or resumes an existing one with identical configuration.
    /// Concurrent callers sharing the same adapter data attach to the same open execution.
    /// Session identity requires one shared storage backend/keyspace and Temporal namespace.
    /// After idle/explicit completion,
    /// a fresh run restores the transcript from the adapter; no fixed execution timeout.
    pub async fn start_session(&self, id: String, model: String) -> EngineResult<Session> {
        self.start_session_with_history(id, model, ChatHistory::default())
            .await
    }
    /// Stores immutable provider-neutral initial context. Declared provider models must
    /// match the session model. Callers must isolate differing providers and accounts by queue.
    /// To copy an exported session transcript, explicitly call
    /// [`ChatHistory::for_session_import`] first to rebase reserved turn entry IDs.
    /// Supplied IDs are otherwise left unchanged.
    pub async fn start_session_with_history(
        &self,
        id: String,
        model: String,
        initial_history: ChatHistory,
    ) -> EngineResult<Session> {
        validate_provider_model(self.provider.as_ref(), &model)?;
        let config = SessionConfig {
            id,
            model,
            namespace: self.options.namespace.clone(),
            task_queue: self.options.task_queue.clone(),
            idle_timeout_ms: self.options.idle_timeout.as_millis() as u64,
            initial_history,
        };
        config.validate()?;
        let stored = self
            .storage
            .create(StoredSession::new(
                config,
                format!("rusty-session-{}", uuid::Uuid::new_v4()),
            ))
            .await?;
        self.open(stored).await
    }
    /// Reopens a known session, checking a declared provider model against its stored label.
    pub async fn resume_session(&self, id: &str) -> EngineResult<Session> {
        self.open(self.storage.read(id).await?).await
    }
    async fn open(&self, stored: StoredSession) -> EngineResult<Session> {
        validate_provider_model(self.provider.as_ref(), &stored.config.model)?;
        if stored.config.namespace != self.options.namespace
            || stored.config.task_queue != self.options.task_queue
        {
            return Err("session namespace/task queue does not match this worker".into());
        }
        let handle = self
            .client
            .start_workflow(
                ChatSessionWorkflow::run,
                SessionInput {
                    session_id: stored.config.id.clone(),
                    idle_timeout_ms: stored.config.idle_timeout_ms,
                    rollover: self.rollover.clone(),
                    idle_deadline_ms: None,
                    activity_timeout_ms: self.activity_timeout_ms,
                },
                WorkflowStartOptions::new(
                    self.options.task_queue.clone(),
                    stored.workflow_id.clone(),
                )
                // The adapter's atomic create fixes one Workflow ID per logical session.
                // Temporal arbitrates concurrent opens across hosts: attach, never replace.
                .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
                // A closed run may be resumed, but never alongside an open run.
                .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
                .build(),
            )
            .await?;
        Ok(Session {
            id: stored.config.id,
            workflow_id: stored.workflow_id,
            handle,
            client: self.client.clone(),
            namespace: self.options.namespace.clone(),
            storage: self.storage.clone(),
        })
    }
}

/// A logical application session. `snapshot` does not require a live Workflow worker.
pub struct Session {
    id: String,
    workflow_id: String,
    handle: WorkflowHandle<Client, super::chat_session_workflow::Run>,
    client: Client,
    namespace: String,
    storage: Arc<dyn SessionStorage>,
}
impl Session {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }
    /// Run originally attached by this handle. Rollover successors are followed,
    /// but separately resumed executions after closure are never followed.
    pub fn run_id(&self) -> &str {
        self.handle.run_id().expect("started Workflow has a run ID")
    }
    /// Resolves only explicit Continue-As-New links, never the latest run by Workflow ID.
    async fn current_handle(
        &self,
    ) -> EngineResult<WorkflowHandle<Client, super::chat_session_workflow::Run>> {
        use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
        let mut handle = WorkflowHandle::<Client, super::chat_session_workflow::Run>::new(
            self.client.clone(),
            WorkflowExecutionInfo::builder()
                .namespace(self.namespace.clone())
                .workflow_id(self.workflow_id.clone())
                .run_id(self.run_id())
                .build(),
        );
        loop {
            if handle
                .describe(WorkflowDescribeOptions::default())
                .await?
                .status()
                != ExecutionStatus::ContinuedAsNew
            {
                return Ok(handle);
            }
            let events = handle
                .fetch_history(
                    WorkflowFetchHistoryOptions::builder()
                        .event_filter_type(HistoryEventFilterType::CloseEvent)
                        .build(),
                )
                .into_events()
                .await?;
            let next = events
                .into_iter()
                .find_map(|event| match event.attributes {
                    Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) => {
                        Some(attrs.new_execution_run_id)
                    }
                    _ => None,
                })
                .filter(|id| !id.is_empty())
                .ok_or("missing Continue-As-New successor")?;
            handle = WorkflowHandle::new(
                self.client.clone(),
                WorkflowExecutionInfo::builder()
                    .namespace(self.namespace.clone())
                    .workflow_id(self.workflow_id.clone())
                    .run_id(next)
                    .build(),
            );
        }
    }
    /// Current run within this handle's Continue-As-New chain.
    pub async fn current_run_id(&self) -> EngineResult<String> {
        Ok(self
            .current_handle()
            .await?
            .run_id()
            .expect("pinned run")
            .to_string())
    }
    /// Chain's current execution status, distinct from the adapter's closing flag.
    pub async fn execution_status(&self) -> EngineResult<ExecutionStatus> {
        Ok(self
            .current_handle()
            .await?
            .describe(WorkflowDescribeOptions::default())
            .await?
            .status())
    }
    /// Waits for this execution to finish without requesting closure.
    /// A worker must be available to process timers, Signals and pending Activities.
    /// Failed/cancelled executions return an error, not a successful close snapshot.
    pub async fn wait_closed(&self) -> EngineResult<SessionSnapshot> {
        Ok(self
            .handle
            .get_result(WorkflowGetResultOptions::default())
            .await?)
    }
    /// Convenience call. Use send_request with a stable ID to retry safely across reconnects.
    pub async fn send_message(&self, text: String) -> EngineResult<TurnResult> {
        self.send_request(uuid::Uuid::new_v4().to_string(), text)
            .await
    }
    pub async fn send_request(&self, request_id: String, text: String) -> EngineResult<TurnResult> {
        loop {
            let handle = self.current_handle().await?;
            let result = handle
                .execute_update(
                    ChatSessionWorkflow::send_request,
                    TurnInput {
                        request_id: request_id.clone(),
                        text: text.clone(),
                    },
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await;
            match result {
                Ok(TurnResult::RollingOver) => tokio::time::sleep(Duration::from_millis(20)).await,
                Ok(result) => return Ok(result),
                Err(error) => {
                    // Retry only a proven rollover race, with the same application request ID.
                    if self.current_handle().await?.run_id() == handle.run_id() {
                        return Err(error.into());
                    }
                }
            }
        }
    }
    pub async fn snapshot(&self) -> EngineResult<SessionSnapshot> {
        Ok(self.storage.read(&self.id).await?.snapshot())
    }
    /// Provider-neutral transcript, including initial context and draft/failure details.
    /// Reads the adapter directly; no Workflow worker is required.
    pub async fn history(&self) -> EngineResult<ChatHistory> {
        Ok(self.storage.read(&self.id).await?.history())
    }
    /// Completes this execution after active turns settle; safe to repeat after completion.
    /// The logical session remains readable and can be reopened with Engine::resume_session.
    /// An old handle never closes a newer run. This is graceful close, not cancellation.
    pub async fn close(&self) -> EngineResult<SessionSnapshot> {
        loop {
            let handle = self.current_handle().await?;
            match handle
                .describe(WorkflowDescribeOptions::default())
                .await?
                .status()
            {
                ExecutionStatus::ContinuedAsNew => continue,
                ExecutionStatus::Running => {}
                _ => return self.wait_closed().await,
            }
            match handle
                .signal(
                    ChatSessionWorkflow::close,
                    (),
                    WorkflowSignalOptions::default(),
                )
                .await
            {
                Ok(()) => return self.wait_closed().await,
                Err(error) => {
                    if self.current_handle().await?.run_id() != handle.run_id() {
                        continue;
                    }
                    if self.execution_status().await? != ExecutionStatus::Completed {
                        return Err(error.into());
                    }
                    return self.wait_closed().await;
                }
            }
        }
    }
}
