//! Synchronous hook decisions inspired by MIT-licensed Pi Durable (see docs/hooks-validation.md).
//! Callers must not put secrets in configuration; it is persisted with the session.
use crate::{
    ContentPart, ConversationId, EntryCursor, EntryId, EntryQuery, HistoryTurn, HookJob,
    HookJobError, HookJobSpawner, Message, Page, SessionId, Storage, StorageError, StoredEntry,
    TaskId, ToolCall, ToolOutput,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPoint {
    BeforeRequest,
    BeforeProviderRequest,
    AfterResponse,
    BeforeTool,
    AfterTool,
    AfterTools,
    OnYield,
    AfterRun,
}
pub type HookPoints = BTreeSet<HookPoint>;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HookKey {
    pub extension: String,
    pub hook: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HookDescriptor {
    pub extension: String,
    pub extension_version: String,
    pub hook: String,
    pub hook_version: String,
    pub points: HookPoints,
    pub config: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookReport {
    pub extension: String,
    pub hook: String,
    pub point: HookPoint,
    pub code: String,
}
pub trait HookReporter: Send + Sync {
    fn report(&self, report: HookReport);
}
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct HookError {
    pub code: String,
    pub message: String,
}
impl HookError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
/// Restricts error codes before handing them to reporters; never includes error text.
pub fn sanitize_hook_code(code: &str) -> &str {
    if !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        code
    } else {
        "hook_error"
    }
}
#[derive(Clone)]
pub struct HookContext {
    session: SessionId,
    conversation: ConversationId,
    task: TaskId,
    run: TaskId,
    config: serde_json::Value,
    cancel: CancellationToken,
    storage: Arc<dyn Storage>,
    jobs: Option<HookJobSpawner>,
}
impl HookContext {
    pub fn new(
        session: SessionId,
        conversation: ConversationId,
        task: TaskId,
        run: TaskId,
        config: serde_json::Value,
        cancel: CancellationToken,
        storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            session,
            conversation,
            task,
            run,
            config,
            cancel,
            storage,
            jobs: None,
        }
    }
    /// Attach the capability for one callback; its guard must outlive the callback.
    pub fn with_job_spawner(mut self, spawner: HookJobSpawner) -> Self {
        self.jobs = Some(spawner);
        self
    }
    pub fn spawn<T, F>(&self, future: F) -> Result<HookJob<T>, HookJobError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, HookError>> + Send + 'static,
    {
        self.jobs
            .as_ref()
            .ok_or(HookJobError::Unavailable)?
            .spawn(future)
    }
    pub fn session_id(&self) -> SessionId {
        self.session
    }
    pub fn conversation_id(&self) -> ConversationId {
        self.conversation
    }
    pub fn task_id(&self) -> TaskId {
        self.task
    }
    pub fn run_id(&self) -> TaskId {
        self.run
    }
    pub fn config(&self) -> &serde_json::Value {
        &self.config
    }
    pub fn cancel(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub async fn entries(
        &self,
        cursor: Option<EntryCursor>,
        limit: usize,
    ) -> Result<Page<StoredEntry, EntryCursor>, StorageError> {
        if !(1..=1000).contains(&limit) {
            return Err(StorageError::constraint(
                "entry page limit must be 1..=1000",
            ));
        }
        self.storage
            .scan_entries(
                EntryQuery {
                    conversation_id: self.conversation,
                    kind: None,
                    by_task_id: None,
                    ascending: true,
                },
                cursor,
                limit,
            )
            .await
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum BeforeTool {
    Continue,
    Arguments(serde_json::Value),
    Block(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterTools {
    Continue,
    Terminate,
}
#[derive(Clone, Debug, PartialEq)]
pub enum YieldDecision {
    Stand,
    Continue(Vec<ContentPart>),
}
pub struct AfterToolsJobs {
    pub direct: AfterTools,
    pub jobs: Vec<HookJob<AfterTools>>,
}
pub enum YieldJobs {
    Direct(YieldDecision),
    Jobs(Vec<HookJob<YieldDecision>>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunReason {
    Answer,
    Terminate,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub reason: RunReason,
    pub first: EntryId,
    pub answer: EntryId,
    pub last: EntryId,
}
#[async_trait::async_trait]
pub trait Hook: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> &str;
    fn points(&self) -> HookPoints {
        HookPoints::new()
    }
    async fn before_request(
        &self,
        _messages: &[Message],
        _ctx: &HookContext,
    ) -> Result<Option<Vec<Message>>, HookError> {
        Ok(None)
    }
    async fn before_provider_request(
        &self,
        _api: &str,
        _model: &str,
        _payload: &serde_json::Value,
        _ctx: &HookContext,
    ) -> Result<Option<serde_json::Value>, HookError> {
        Ok(None)
    }
    async fn after_response(
        &self,
        _turn: &HistoryTurn,
        _ctx: &HookContext,
    ) -> Result<(), HookError> {
        Ok(())
    }
    async fn before_tool(
        &self,
        _call: &ToolCall,
        _ctx: &HookContext,
    ) -> Result<BeforeTool, HookError> {
        Ok(BeforeTool::Continue)
    }
    async fn after_tool(
        &self,
        _call: &ToolCall,
        _result: &ToolOutput,
        _ctx: &HookContext,
    ) -> Result<Option<ToolOutput>, HookError> {
        Ok(None)
    }
    async fn after_tools(
        &self,
        _assistant: EntryId,
        _results: &[EntryId],
        _ctx: &HookContext,
    ) -> Result<AfterTools, HookError> {
        Ok(AfterTools::Continue)
    }
    async fn after_tools_jobs(
        &self,
        assistant: EntryId,
        results: &[EntryId],
        ctx: &HookContext,
    ) -> Result<AfterToolsJobs, HookError> {
        Ok(AfterToolsJobs {
            direct: self.after_tools(assistant, results, ctx).await?,
            jobs: vec![],
        })
    }
    async fn on_yield(
        &self,
        _answer: &HistoryTurn,
        _ctx: &HookContext,
    ) -> Result<YieldDecision, HookError> {
        Ok(YieldDecision::Stand)
    }
    async fn on_yield_jobs(
        &self,
        answer: &HistoryTurn,
        ctx: &HookContext,
    ) -> Result<YieldJobs, HookError> {
        Ok(YieldJobs::Direct(self.on_yield(answer, ctx).await?))
    }
    async fn after_run(&self, _summary: &RunSummary, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }
    async fn after_run_jobs(
        &self,
        summary: &RunSummary,
        ctx: &HookContext,
    ) -> Result<Vec<HookJob<()>>, HookError> {
        self.after_run(summary, ctx).await?;
        Ok(vec![])
    }
}
