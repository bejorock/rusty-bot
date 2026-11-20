//! Session-scoped hook dispatch. Callbacks run outside storage transactions.
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Weak},
};

use rusty_bot_core::{
    canonical_tool_calls, sanitize_hook_code, AfterTools, BeforeTool, ContentPart, ConversationId,
    EntryId, HistoryPart, HistoryTurn, HookContext, HookError, HookInvocationGuard, HookJob,
    HookJobSpawner, HookJobSupervisor, HookPoint, HookReport, HookReporter, Message, ModelError,
    ProviderPayloadHook, RichRole, RunSummary, SessionError, SessionId, Storage, TaskId, ToolCall,
    ToolOutput, YieldDecision,
};
use tokio_util::sync::CancellationToken;

use crate::registry::ResolvedHook;

#[cfg(test)]
#[path = "hooks_unit_tests.rs"]
mod unit_tests;

pub(crate) struct Dispatcher {
    hooks: Vec<ResolvedHook>,
    session: SessionId,
    conversation: ConversationId,
    storage: Arc<dyn Storage>,
    reporter: Option<Arc<dyn HookReporter>>,
    cancel: CancellationToken,
    supervisor: Option<Weak<HookJobSupervisor>>,
}

struct LogReporter;
impl HookReporter for LogReporter {
    fn report(&self, report: HookReport) {
        tracing::warn!(extension = %report.extension, hook = %report.hook, point = ?report.point, code = %report.code, "hook failed");
    }
}

struct PayloadDispatcher {
    dispatcher: Dispatcher,
    task: TaskId,
}

#[async_trait::async_trait]
impl ProviderPayloadHook for PayloadDispatcher {
    async fn apply(
        &self,
        api: &str,
        model: &str,
        original: serde_json::Value,
        cancel: CancellationToken,
    ) -> Result<serde_json::Value, ModelError> {
        rusty_bot_core::validate_provider_payload(api, &original, &original)?;
        let mut candidate = original.clone();
        for hook in &self.dispatcher.hooks {
            if !hook
                .descriptor
                .points
                .contains(&HookPoint::BeforeProviderRequest)
            {
                continue;
            }
            let (ctx, _guard, _) =
                self.dispatcher
                    .context(hook, self.task, HookPoint::BeforeProviderRequest);
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ModelError::cancelled()),
                _ = self.dispatcher.cancel.cancelled() => return Err(ModelError::cancelled()),
                result = hook.hook.before_provider_request(api, model, &candidate, &ctx) => result,
            };
            drop(_guard);
            if cancel.is_cancelled() || self.dispatcher.cancel.is_cancelled() {
                return Err(ModelError::cancelled());
            }
            match result {
                Ok(Some(next)) => {
                    rusty_bot_core::validate_provider_payload(api, &original, &next)?;
                    candidate = next;
                }
                Ok(None) => {}
                Err(error) => {
                    self.dispatcher
                        .report(hook, HookPoint::BeforeProviderRequest, &error.code)
                }
            }
        }
        Ok(candidate)
    }
}

impl Dispatcher {
    pub(crate) fn new(
        hooks: Vec<ResolvedHook>,
        session: SessionId,
        conversation: ConversationId,
        storage: Arc<dyn Storage>,
        reporter: Option<Arc<dyn HookReporter>>,
        cancel: CancellationToken,
        supervisor: Option<Arc<HookJobSupervisor>>,
    ) -> Self {
        Self {
            hooks,
            session,
            conversation,
            storage,
            reporter,
            cancel,
            supervisor: supervisor.as_ref().map(Arc::downgrade),
        }
    }

    async fn invoke<T>(
        &self,
        hook: &ResolvedHook,
        point: HookPoint,
        future: impl Future<Output = Result<T, HookError>>,
    ) -> Result<Option<T>, SessionError> {
        let result = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(SessionError::Cancelled),
            result = future => result,
        };
        if self.cancel.is_cancelled() {
            return Err(SessionError::Cancelled);
        }
        match result {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                self.report(hook, point, &error.code);
                Ok(None)
            }
        }
    }

    fn report(&self, hook: &ResolvedHook, point: HookPoint, code: &str) {
        let report = HookReport {
            extension: hook.descriptor.extension.clone(),
            hook: hook.descriptor.hook.clone(),
            point,
            code: sanitize_hook_code(code).to_owned(),
        };
        if let Some(reporter) = &self.reporter {
            reporter.report(report);
        } else {
            tracing::warn!(extension = %report.extension, hook = %report.hook, point = ?report.point, code = %report.code, "hook failed");
        }
    }

    fn context(
        &self,
        hook: &ResolvedHook,
        task: TaskId,
        point: HookPoint,
    ) -> (
        HookContext,
        Option<HookInvocationGuard>,
        Option<HookJobSpawner>,
    ) {
        let ctx = HookContext::new(
            self.session,
            self.conversation,
            task,
            task,
            hook.descriptor.config.clone(),
            self.cancel.clone(),
            self.storage.clone(),
        );
        if let Some(supervisor) = self.supervisor.as_ref().and_then(Weak::upgrade) {
            let reporter = self
                .reporter
                .clone()
                .unwrap_or_else(|| Arc::new(LogReporter));
            let (spawner, guard) = supervisor.invocation(
                hook.descriptor.extension.clone(),
                hook.descriptor.hook.clone(),
                point,
                reporter,
            );
            (
                ctx.with_job_spawner(spawner.clone()),
                Some(guard),
                Some(spawner),
            )
        } else {
            (ctx, None, None)
        }
    }

    fn register<T>(
        &self,
        hook: &ResolvedHook,
        point: HookPoint,
        spawner: Option<&HookJobSpawner>,
        jobs: Vec<HookJob<T>>,
    ) -> Vec<HookJob<T>> {
        let mut valid = Vec::new();
        for job in jobs {
            if spawner.is_some_and(|s| s.register(&job).is_ok()) {
                valid.push(job);
            } else {
                self.report(hook, point, "invalid_job");
            }
        }
        valid.sort_by_key(HookJob::sequence);
        valid
    }

    async fn settle<T>(&self, job: HookJob<T>) -> Result<Option<T>, SessionError> {
        tokio::select! { biased; _ = self.cancel.cancelled() => Err(SessionError::Cancelled), result = job.wait() => Ok(result.ok()) }
    }

    pub(crate) fn payload_hook(self, task: TaskId) -> Option<Arc<dyn ProviderPayloadHook>> {
        if self.hooks.iter().any(|hook| {
            hook.descriptor
                .points
                .contains(&HookPoint::BeforeProviderRequest)
        }) {
            Some(Arc::new(PayloadDispatcher {
                dispatcher: self,
                task,
            }))
        } else {
            None
        }
    }

    pub(crate) async fn after_tools(
        &self,
        task: TaskId,
        assistant: EntryId,
        results: &[EntryId],
    ) -> Result<bool, SessionError> {
        let mut terminate = false;
        let mut jobs = Vec::new();
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::AfterTools) {
                continue;
            }
            let (ctx, guard, spawner) = self.context(hook, task, HookPoint::AfterTools);
            let result = self
                .invoke(
                    hook,
                    HookPoint::AfterTools,
                    hook.hook.after_tools_jobs(assistant, results, &ctx),
                )
                .await;
            drop(guard);
            if let Some(result) = result? {
                terminate |= matches!(result.direct, AfterTools::Terminate);
                jobs.extend(self.register(
                    hook,
                    HookPoint::AfterTools,
                    spawner.as_ref(),
                    result.jobs,
                ));
            }
        }
        for job in jobs {
            if matches!(self.settle(job).await?, Some(AfterTools::Terminate)) {
                terminate = true;
            }
        }
        Ok(terminate)
    }

    fn valid_yield(
        &self,
        hook: &ResolvedHook,
        decision: YieldDecision,
        limits: Option<&rusty_bot_core::ImageLimits>,
    ) -> Option<Vec<ContentPart>> {
        let YieldDecision::Continue(content) = decision else {
            return None;
        };
        let nonempty = !content.is_empty()
            && content
                .iter()
                .any(|part| !matches!(part, ContentPart::Text { text } if text.trim().is_empty()));
        let images_ok = !content
            .iter()
            .any(|p| matches!(p, ContentPart::Image { .. }))
            || limits.is_some_and(|l| {
                rusty_bot_core::image_validation::validate_images(&content, l).is_ok()
            });
        if nonempty && images_ok {
            Some(content)
        } else {
            self.report(hook, HookPoint::OnYield, "invalid_content");
            None
        }
    }

    pub(crate) async fn on_yield(
        &self,
        task: TaskId,
        answer: &HistoryTurn,
        limits: Option<&rusty_bot_core::ImageLimits>,
    ) -> Result<Option<Vec<ContentPart>>, SessionError> {
        enum Candidate<'a> {
            Direct(Vec<ContentPart>),
            Job(&'a ResolvedHook, HookJob<YieldDecision>),
        }
        let mut candidates = Vec::new();
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::OnYield) {
                continue;
            }
            let (ctx, guard, spawner) = self.context(hook, task, HookPoint::OnYield);
            let result = self
                .invoke(
                    hook,
                    HookPoint::OnYield,
                    hook.hook.on_yield_jobs(answer, &ctx),
                )
                .await;
            drop(guard);
            match result? {
                Some(rusty_bot_core::YieldJobs::Direct(decision)) => {
                    if let Some(content) = self.valid_yield(hook, decision, limits) {
                        candidates.push(Candidate::Direct(content));
                        break;
                    }
                }
                Some(rusty_bot_core::YieldJobs::Jobs(jobs)) => {
                    candidates.extend(
                        self.register(hook, HookPoint::OnYield, spawner.as_ref(), jobs)
                            .into_iter()
                            .map(|job| Candidate::Job(hook, job)),
                    );
                }
                None => {}
            }
        }
        let mut selected = None;
        for candidate in candidates {
            let content = match candidate {
                Candidate::Direct(content) => Some(content),
                Candidate::Job(hook, job) => match self.settle(job).await? {
                    Some(decision) => self.valid_yield(hook, decision, limits),
                    None => None,
                },
            };
            if selected.is_none() {
                selected = content;
            }
        }
        Ok(selected)
    }

    pub(crate) async fn after_run(
        &self,
        task: TaskId,
        summary: &RunSummary,
    ) -> Result<(), SessionError> {
        let mut jobs = Vec::new();
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::AfterRun) {
                continue;
            }
            let (ctx, guard, spawner) = self.context(hook, task, HookPoint::AfterRun);
            let result = self
                .invoke(
                    hook,
                    HookPoint::AfterRun,
                    hook.hook.after_run_jobs(summary, &ctx),
                )
                .await;
            drop(guard);
            if let Some(returned) = result? {
                jobs.extend(self.register(hook, HookPoint::AfterRun, spawner.as_ref(), returned));
            }
        }
        for job in jobs {
            self.settle(job).await?;
        }
        Ok(())
    }

    pub(crate) async fn before_tool(
        &self,
        task: TaskId,
        call: &mut ToolCall,
    ) -> Result<Option<String>, SessionError> {
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::BeforeTool) {
                continue;
            }
            let (ctx, _guard, _) = self.context(hook, task, HookPoint::BeforeTool);
            match self
                .invoke(
                    hook,
                    HookPoint::BeforeTool,
                    hook.hook.before_tool(call, &ctx),
                )
                .await?
            {
                Some(BeforeTool::Continue) => {}
                Some(BeforeTool::Arguments(arguments)) => call.arguments = arguments,
                Some(BeforeTool::Block(message)) => return Ok(Some(message)),
                None => return Ok(Some("hook failed".into())),
            }
        }
        Ok(None)
    }

    pub(crate) async fn after_tool(
        &self,
        task: TaskId,
        call: &ToolCall,
        output: &mut ToolOutput,
    ) -> Result<(), SessionError> {
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::AfterTool) {
                continue;
            }
            let (ctx, _guard, _) = self.context(hook, task, HookPoint::AfterTool);
            if let Some(Some(replacement)) = self
                .invoke(
                    hook,
                    HookPoint::AfterTool,
                    hook.hook.after_tool(call, output, &ctx),
                )
                .await?
            {
                *output = replacement;
            }
        }
        Ok(())
    }

    pub(crate) async fn before_request(
        &self,
        task: TaskId,
        messages: &mut Vec<Message>,
    ) -> Result<(), SessionError> {
        let baseline = messages.clone();
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::BeforeRequest) {
                continue;
            }
            let (ctx, _guard, _) = self.context(hook, task, HookPoint::BeforeRequest);
            if let Some(Some(candidate)) = self
                .invoke(
                    hook,
                    HookPoint::BeforeRequest,
                    hook.hook.before_request(messages, &ctx),
                )
                .await?
            {
                if valid_replacement(&baseline, &candidate) {
                    *messages = candidate;
                } else {
                    self.report(hook, HookPoint::BeforeRequest, "invalid_messages");
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn after_response(
        &self,
        task: TaskId,
        turn: &HistoryTurn,
    ) -> Result<(), SessionError> {
        for hook in &self.hooks {
            if !hook.descriptor.points.contains(&HookPoint::AfterResponse) {
                continue;
            }
            let (ctx, _guard, _) = self.context(hook, task, HookPoint::AfterResponse);
            self.invoke(
                hook,
                HookPoint::AfterResponse,
                hook.hook.after_response(turn, &ctx),
            )
            .await?;
        }
        Ok(())
    }
}

// Provider replay payloads are opaque: retain rich messages byte-for-byte and in
// order rather than guessing a provider's replay protocol. Provider adapters
// still perform their own stronger provider-specific checks at dispatch.
pub(crate) fn valid_replacement(original: &[Message], candidate: &[Message]) -> bool {
    let rich = |messages: &[Message]| {
        messages
            .iter()
            .filter(|m| matches!(m, Message::Rich { .. }))
            .cloned()
            .collect::<Vec<_>>()
    };
    if rich(original) != rich(candidate) {
        return false;
    }
    let mut pending = HashMap::<String, String>::new();
    let mut seen = HashSet::new();
    for (index, message) in candidate.iter().enumerate() {
        match message {
            Message::System { .. } if index == 0 => {}
            Message::System { .. } => return false,
            Message::User { .. }
            | Message::Rich {
                rich_role: RichRole::User,
                ..
            } if pending.is_empty() => {}
            Message::Assistant { .. }
            | Message::Rich {
                rich_role: RichRole::Assistant,
                ..
            } if pending.is_empty() => {
                let Ok(calls) = canonical_tool_calls(message) else {
                    return false;
                };
                for call in calls {
                    if !seen.insert(call.id.clone()) {
                        return false;
                    }
                    pending.insert(call.id, call.name);
                }
            }
            Message::ToolResult {
                tool_call_id,
                tool_name,
                ..
            } => {
                if pending.remove(tool_call_id).as_ref() != Some(tool_name) {
                    return false;
                }
            }
            Message::Rich {
                rich_role: RichRole::Tool,
                parts,
            } => {
                if parts.is_empty() {
                    return false;
                }
                for part in parts {
                    if let HistoryPart::ToolResult {
                        call_id,
                        name,
                        content,
                        ..
                    } = part
                    {
                        if pending.remove(call_id).as_ref() != Some(name) || content.iter().any(|p| matches!(p, ContentPart::Image { mime_type, data_base64 } if mime_type.is_empty() || data_base64.is_empty())) { return false; }
                    } else {
                        return false;
                    }
                }
            }
            _ => return false,
        }
    }
    pending.is_empty()
}
