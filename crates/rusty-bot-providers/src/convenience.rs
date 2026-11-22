//! Non-streaming helpers and opt-in, content-free operation observation.
use crate::{DiscoveryRequest, GenerationRequest, ModelPage, OutputFormat, Provider};
use crate::{ModelError, ModelErrorKind};
use async_trait::async_trait;
use rusty_bot_messages::{
    ContentBlock, ConversationMessage, FinishReason, HistoryEntry, MessageStatus,
    MessageStreamEvent, MessageStreamPayload,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const EVENT_CAPACITY: usize = 32;

/// Drain provisional events while the provider runs, returning its authoritative entry.
/// The future owns its work: dropping it cancels its child token and drops the provider
/// future rather than leaving a detached task behind. The caller's token is not cancelled.
pub async fn generate(
    provider: &dyn Provider,
    request: &GenerationRequest,
    cancel: CancellationToken,
) -> Result<HistoryEntry, ModelError> {
    let child = cancel.child_token();
    let (sender, mut receiver) = mpsc::channel(EVENT_CAPACITY);
    let stream = provider.stream(request, sender, child.clone());
    tokio::pin!(stream);
    let _guard = child.clone().drop_guard();
    let mut completed: Option<HistoryEntry> = None;
    let mut invalid = false;
    let mut sequence = 0u64;
    let result = loop {
        tokio::select! {
            biased;
            _ = child.cancelled() => break Err(ModelError::cancelled()),
            result = &mut stream => break result,
            event = receiver.recv() => match event {
                Some(event) => record_terminal(event, &request.message_id, &mut sequence, &mut completed, &mut invalid),
                None => break tokio::select! {
                    biased;
                    _ = child.cancelled() => Err(ModelError::cancelled()),
                    result = &mut stream => result,
                },
            }
        }
    };
    // Do not wait for channel closure: a broken provider may retain a sender.
    while let Ok(event) = receiver.try_recv() {
        record_terminal(
            event,
            &request.message_id,
            &mut sequence,
            &mut completed,
            &mut invalid,
        );
    }
    let entry = result?;
    if invalid
        || completed.as_ref() != Some(&entry)
        || entry.id != request.message_id
        || entry.status != MessageStatus::Completed
        || entry.failure.is_some()
        || !matches!(entry.message, ConversationMessage::Assistant { .. })
        || entry.validate().is_err()
    {
        return Err(ModelError::new(
            ModelErrorKind::Protocol,
            "invalid provider completion",
            false,
        ));
    }
    Ok(entry)
}

fn record_terminal(
    event: MessageStreamEvent,
    message_id: &str,
    sequence: &mut u64,
    completed: &mut Option<HistoryEntry>,
    invalid: &mut bool,
) {
    if event.message_id != message_id || sequence.checked_add(1) != Some(event.sequence) {
        *invalid = true;
    }
    *sequence = event.sequence;
    if completed.is_some() {
        *invalid = true;
    }
    match event.event {
        MessageStreamPayload::Completed { response } => *completed = Some(*response),
        MessageStreamPayload::Failed { .. } => *invalid = true,
        _ => {}
    }
}

/// Parsed value together with the unmodified, authoritative provider response.
#[derive(Clone, Debug, PartialEq)]
pub struct StructuredResponse<T> {
    pub value: T,
    pub entry: HistoryEntry,
}

struct NoExternalSchemas;
impl jsonschema::Retrieve for NoExternalSchemas {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schemas are not supported".into())
    }
}

/// Generate and strictly parse JSON under the provider's configured output constraint.
/// No JSON is extracted from markdown, coerced, or taken from provisional events.
/// External schema retrieval is disabled, including when dependency features are unified.
pub async fn generate_structured<T: DeserializeOwned>(
    provider: &dyn Provider,
    request: &GenerationRequest,
    cancel: CancellationToken,
) -> Result<StructuredResponse<T>, ModelError> {
    let format = provider.output_format();
    let validator = match format {
        OutputFormat::Text => {
            return Err(ModelError::unsupported(
                "structured output requires a JSON output format",
            ));
        }
        OutputFormat::JsonObject => None,
        OutputFormat::JsonSchema { schema, .. } => Some(
            jsonschema::options()
                .with_retriever(NoExternalSchemas)
                .build(schema)
                .map_err(|_| {
                    ModelError::new(
                        ModelErrorKind::InvalidRequest,
                        "invalid or unsupported output schema",
                        false,
                    )
                })?,
        ),
    };
    let entry = generate(provider, request, cancel).await?;
    if !matches!(
        entry
            .generation
            .as_ref()
            .and_then(|g| g.finish_reason.as_ref()),
        Some(FinishReason::Stop)
    ) {
        return Err(structured_error());
    }
    let ConversationMessage::Assistant { content } = &entry.message else {
        return Err(structured_error());
    };
    let mut text = String::new();
    let mut found = false;
    for block in content {
        collect_text(block, &mut text, &mut found)?;
    }
    if !found {
        return Err(structured_error());
    }
    let json: Value = serde_json::from_str(&text).map_err(|_| structured_error())?;
    if matches!(format, OutputFormat::JsonObject) && !json.is_object() {
        return Err(structured_error());
    }
    if validator.as_ref().is_some_and(|v| !v.is_valid(&json)) {
        return Err(structured_error());
    }
    let value = serde_json::from_value(json).map_err(|_| structured_error())?;
    Ok(StructuredResponse { value, entry })
}

fn collect_text(
    block: &ContentBlock,
    text: &mut String,
    found: &mut bool,
) -> Result<(), ModelError> {
    match block {
        ContentBlock::Text { text: part } => {
            text.push_str(part);
            *found = true;
            Ok(())
        }
        ContentBlock::Reasoning { .. } | ContentBlock::Replay { .. } => Ok(()),
        ContentBlock::Annotated { block, .. } => collect_text(block, text, found),
        _ => Err(structured_error()),
    }
}
fn structured_error() -> ModelError {
    ModelError::new(
        ModelErrorKind::Protocol,
        "invalid structured provider response",
        false,
    )
}

fn observation_timeout() -> ModelError {
    ModelError::new(
        ModelErrorKind::Network,
        "provider operation timed out",
        false,
    )
}

/// A bounded operation label: never includes prompts, endpoints, credentials or output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedOperation {
    BuildRequest,
    ListModels,
    Stream,
}
/// Content-free observation of one operation. Retry attempts inside a provider are not visible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    Started {
        operation: ObservedOperation,
    },
    Completed {
        operation: ObservedOperation,
        elapsed: Duration,
        first_event: Option<Duration>,
        usage: Option<crate::usage::UsageStatistics>,
    },
    Failed {
        operation: ObservedOperation,
        elapsed: Duration,
        kind: ModelErrorKind,
    },
}
/// Callbacks must be fast and non-blocking. They receive no prompt, response, or error text.
pub trait Observer: Send + Sync {
    fn observe(&self, observation: Observation);
}

/// Observe a provider without changing its output or exposing content to the observer.
pub struct ObservedProvider {
    inner: Arc<dyn Provider>,
    observer: Arc<dyn Observer>,
}
impl ObservedProvider {
    pub fn new(inner: Arc<dyn Provider>, observer: Arc<dyn Observer>) -> Self {
        Self { inner, observer }
    }
    fn start(&self, operation: ObservedOperation) -> Instant {
        let start = Instant::now();
        self.observer.observe(Observation::Started { operation });
        start
    }
    fn finish<T>(
        &self,
        operation: ObservedOperation,
        start: Instant,
        result: &Result<T, ModelError>,
        first_event: Option<Duration>,
        usage: Option<crate::usage::UsageStatistics>,
    ) {
        let elapsed = start.elapsed();
        self.observer.observe(match result {
            Ok(_) => Observation::Completed {
                operation,
                elapsed,
                first_event,
                usage,
            },
            Err(error) => Observation::Failed {
                operation,
                elapsed,
                kind: error.kind,
            },
        });
    }
}
#[async_trait]
impl Provider for ObservedProvider {
    fn model_id(&self) -> Option<&str> {
        self.inner.model_id()
    }
    fn provider_id(&self) -> Option<&str> {
        self.inner.provider_id()
    }
    fn operation_timeout(&self) -> Option<Duration> {
        self.inner.operation_timeout()
    }
    fn output_format(&self) -> &OutputFormat {
        self.inner.output_format()
    }
    fn build_request(&self, request: &GenerationRequest) -> Result<Value, ModelError> {
        let op = ObservedOperation::BuildRequest;
        let start = self.start(op);
        let result = self.inner.build_request(request);
        self.finish(op, start, &result, None, None);
        result
    }
    async fn list_models(
        &self,
        request: DiscoveryRequest,
        cancel: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        let op = ObservedOperation::ListModels;
        let start = self.start(op);
        let result = self.inner.list_models(request, cancel).await;
        self.finish(op, start, &result, None, None);
        result
    }
    async fn stream(
        &self,
        request: &GenerationRequest,
        events: mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        let op = ObservedOperation::Stream;
        let start = self.start(op);
        let child = cancel.child_token();
        let deadline = self
            .operation_timeout()
            .map(|duration| {
                tokio::time::Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| {
                        ModelError::new(
                            ModelErrorKind::InvalidRequest,
                            "provider timeout is too large",
                            false,
                        )
                    })
            })
            .transpose()?;
        let expires = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(expires);
        let (tx, mut rx) = mpsc::channel(EVENT_CAPACITY);
        let stream = self.inner.stream(request, tx, child.clone());
        tokio::pin!(stream);
        let _guard = child.clone().drop_guard();
        let mut first_event = None;
        let mut timed_out = false;
        let mut terminal = None;
        let result = loop {
            tokio::select! {
                biased;
                _ = child.cancelled() => break Err(ModelError::cancelled()),
                _ = &mut expires => break Err(observation_timeout()),
                result = &mut stream => break result,
                event = rx.recv() => match event {
                    Some(event) => {
                        first_event.get_or_insert_with(|| start.elapsed());
                        if matches!(event.event, MessageStreamPayload::Completed { .. }) {
                            terminal = Some(event);
                            continue;
                        }
                        let sent = tokio::select! { biased;
                            _ = &mut expires => { timed_out = true; child.cancel(); false },
                            _ = child.cancelled() => false,
                            result = events.send(event) => result.is_ok(),
                        };
                        if !sent { break Err(ModelError::cancelled()); }
                    }
                    None => break tokio::select! {
                        biased;
                        _ = child.cancelled() => Err(ModelError::cancelled()),
                        _ = &mut expires => Err(observation_timeout()),
                        result = &mut stream => result,
                    },
                }
            }
        };
        let mut closed = false;
        while let Ok(event) = rx.try_recv() {
            first_event.get_or_insert_with(|| start.elapsed());
            if matches!(event.event, MessageStreamPayload::Completed { .. }) {
                terminal = Some(event);
                continue;
            }
            let sent = tokio::select! { biased;
                _ = &mut expires => { timed_out = true; child.cancel(); false },
                _ = child.cancelled() => false,
                result = events.send(event) => result.is_ok(),
            };
            if !sent {
                closed = true;
                break;
            }
        }
        // Hold Completed until the inner future has succeeded, so an error or
        // timeout after a provisional terminal send cannot escape as success.
        if result.is_ok() && !closed && !timed_out {
            if let Some(event) = terminal {
                let sent = tokio::select! { biased;
                    _ = &mut expires => { timed_out = true; false },
                    _ = child.cancelled() => false,
                    result = events.send(event) => result.is_ok(),
                };
                if !sent {
                    closed = true;
                }
            }
        }
        let result = if timed_out {
            Err(observation_timeout())
        } else if closed {
            Err(ModelError::cancelled())
        } else {
            result
        };
        let usage = result
            .as_ref()
            .ok()
            .and_then(|entry| entry.generation.as_ref())
            .and_then(|g| g.usage.as_ref())
            .map(crate::usage::statistics);
        self.finish(op, start, &result, first_event, usage);
        result
    }
}
