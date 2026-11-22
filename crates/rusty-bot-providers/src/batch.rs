//! Native asynchronous batch jobs. Handles are caller-owned persistence records.
use crate::{
    request, wire, AnthropicProvider, Api, Client, DeepseekProvider, GenerationRequest,
    OpenAIProvider,
};
use crate::{ModelError, ModelErrorKind};
use async_trait::async_trait;
use rusty_bot_messages::HistoryEntry;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use tokio_util::sync::CancellationToken;
use url::Url;

const LIMIT: usize = 16 * 1024 * 1024;
const BOUNDARY: &str = "rusty-bot-batch-boundary-7f48c431";

#[derive(Clone)]
pub struct BatchRequest {
    pub custom_id: String,
    pub request: GenerationRequest,
}

impl std::fmt::Debug for BatchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchRequest([REDACTED])")
    }
}

/// Persist this entire value securely to resume a job after a restart. It contains no credential.
#[derive(Clone, Serialize, Deserialize)]
pub struct BatchHandle {
    pub provider: String,
    pub protocol: String,
    pub model: String,
    pub endpoint: String,
    pub job_id: String,
    pub message_ids: BTreeMap<String, String>,
}
impl std::fmt::Debug for BatchHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchHandle([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchStatus {
    Pending,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Expired,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchCounts {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub expired: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchJob {
    pub status: BatchStatus,
    /// Counts are unavailable for some OpenAI jobs, particularly validation failures.
    pub counts: Option<BatchCounts>,
}
#[derive(Clone, Debug)]
pub struct BatchResults {
    pub job: BatchJob,
    pub items: Vec<BatchItemResult>,
    pub unreported_ids: Vec<String>,
}
#[derive(Clone)]
pub enum BatchOutcome {
    /// A decoded assistant history entry, boxed to keep outcomes compact.
    Success(Box<HistoryEntry>),
    Error(ModelError),
    Cancelled,
    Expired,
}
#[derive(Clone)]
pub struct BatchItemResult {
    pub custom_id: String,
    pub outcome: BatchOutcome,
}
impl std::fmt::Debug for BatchOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchOutcome([REDACTED])")
    }
}
impl std::fmt::Debug for BatchItemResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchItemResult([REDACTED])")
    }
}

/// Offline translation. OpenAI bodies are JSONL; Anthropic bodies are JSON.
pub trait BatchTranslator: Send + Sync {
    fn build_submission(&self, items: &[BatchRequest]) -> Result<Vec<u8>, ModelError>;
    fn decode_response(
        &self,
        message_id: &str,
        response: &Value,
    ) -> Result<HistoryEntry, ModelError>;
}
#[async_trait]
pub trait BatchProvider: Send + Sync {
    async fn submit(
        &self,
        items: &[BatchRequest],
        cancel: CancellationToken,
    ) -> Result<BatchHandle, ModelError>;
    async fn status(
        &self,
        handle: &BatchHandle,
        cancel: CancellationToken,
    ) -> Result<BatchJob, ModelError>;
    async fn results(
        &self,
        handle: &BatchHandle,
        cancel: CancellationToken,
    ) -> Result<BatchResults, ModelError>;
    async fn cancel(
        &self,
        handle: &BatchHandle,
        cancel: CancellationToken,
    ) -> Result<BatchJob, ModelError>;
}
fn invalid() -> ModelError {
    ModelError::new(
        ModelErrorKind::InvalidRequest,
        "invalid batch request or handle",
        false,
    )
}
fn bad() -> ModelError {
    ModelError::protocol("invalid or unsupported batch response")
}
fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 256
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}
fn protocol(api: Api) -> &'static str {
    match api {
        Api::Responses => "responses",
        Api::Chat => "chat_completions",
        Api::Anthropic => "messages",
        Api::Deepseek => "unsupported",
    }
}
fn batch_endpoint(client: &Client, tail: &[&str]) -> Result<Url, ModelError> {
    let mut url = client.transport.endpoint().clone();
    let suffix = match client.api {
        Api::Responses => "/responses",
        Api::Chat => "/chat/completions",
        Api::Anthropic => "/messages",
        Api::Deepseek => return Err(crate::unsupported()),
    };
    let prefix = url
        .path()
        .strip_suffix(suffix)
        .ok_or_else(invalid)?
        .to_owned();
    if tail.iter().any(|s| !safe_id(s)) {
        return Err(invalid());
    }
    let mut path = prefix;
    for part in tail {
        path.push('/');
        path.push_str(part);
    }
    url.set_path(&path);
    Ok(url)
}
fn validate_items(
    api: Api,
    items: &[BatchRequest],
) -> Result<BTreeMap<String, String>, ModelError> {
    if items.is_empty() || items.len() > 50_000 {
        return Err(invalid());
    }
    let mut map = BTreeMap::new();
    for item in items {
        if !safe_id(&item.custom_id)
            || (api == Api::Anthropic && item.custom_id.len() > 64)
            || item.request.message_id.trim().is_empty()
            || map
                .insert(item.custom_id.clone(), item.request.message_id.clone())
                .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(map)
}
fn build(client: &Client, items: &[BatchRequest]) -> Result<Vec<u8>, ModelError> {
    build_with(client.api, &client.config, items)
}
fn build_with(
    api: Api,
    config: &crate::ProviderConfig,
    items: &[BatchRequest],
) -> Result<Vec<u8>, ModelError> {
    if api == Api::Deepseek {
        return Err(crate::unsupported());
    }
    validate_items(api, items)?;
    // Bound serialization itself, including an individual oversized item.
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > LIMIT.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("batch size limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    use std::io::Write;
    let mut lines = Bounded(Vec::new());
    if api == Api::Anthropic {
        lines.write_all(b"{\"requests\":[").map_err(|_| invalid())?;
    }
    for (index, item) in items.iter().enumerate() {
        let mut body = request::build(api, config, &item.request)?;
        let obj = body.as_object_mut().ok_or_else(invalid)?;
        obj.remove("stream");
        obj.remove("stream_options");
        if api == Api::Anthropic {
            if index != 0 {
                lines.write_all(b",").map_err(|_| invalid())?;
            }
            serde_json::to_writer(
                &mut lines,
                &json!({"custom_id":item.custom_id,"params":body}),
            )
            .map_err(|_| invalid())?;
        } else {
            let endpoint = if api == Api::Responses {
                "/v1/responses"
            } else {
                "/v1/chat/completions"
            };
            serde_json::to_writer(
                &mut lines,
                &json!({"custom_id":item.custom_id,"method":"POST","url":endpoint,"body":body}),
            )
            .map_err(|_| invalid())?;
            lines.write_all(b"\n").map_err(|_| invalid())?;
        }
    }
    if api == Api::Anthropic {
        lines.write_all(b"]}").map_err(|_| invalid())?;
    }
    let lines = lines.0;
    Ok(lines)
}
fn validate_handle(client: &Client, h: &BatchHandle) -> Result<(), ModelError> {
    if client.api == Api::Deepseek {
        return Err(crate::unsupported());
    }
    if h.provider != client.api.provider()
        || h.protocol != protocol(client.api)
        || h.model != client.config.model
        || h.endpoint != client.transport.endpoint().as_str()
        || !safe_id(&h.job_id)
        || h.message_ids.is_empty()
        || h.message_ids.len() > 50_000
        || h.message_ids.iter().any(|(k, v)| {
            !safe_id(k) || (client.api == Api::Anthropic && k.len() > 64) || v.trim().is_empty()
        })
    {
        return Err(invalid());
    }
    Ok(())
}
struct SendContext<'a> {
    cancel: CancellationToken,
    budget: &'a mut usize,
    deadline: tokio::time::Instant,
}

async fn send(
    client: &Client,
    method: reqwest::Method,
    url: Url,
    body: Option<(&str, Vec<u8>)>,
    lines: bool,
    context: &mut SendContext<'_>,
) -> Result<Vec<Value>, ModelError> {
    check_operation(&context.cancel, context.deadline)?;
    let mut response = client
        .transport
        .request(
            client.api.provider(),
            &client.config.model,
            method,
            url,
            body,
            context.cancel.clone(),
        )
        .await
        .map_err(|e| e.error)?;
    response.check_value(&json!({"model":client.config.model}))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.next_chunk().await? {
        if chunk.len() > *context.budget {
            return Err(bad());
        }
        *context.budget -= chunk.len();
        bytes.extend(chunk);
    }
    let parse = |bytes: &[u8]| -> Result<Value, ModelError> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| bad())?;
        response.check_value(&value)?;
        Ok(value)
    };
    let parsed = if lines {
        bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
            .map(parse)
            .collect()
    } else {
        Ok(vec![parse(&bytes)?])
    };
    check_operation(&context.cancel, context.deadline)?;
    parsed
}
fn job(client: &Client, value: &Value, expected: &str) -> Result<BatchJob, ModelError> {
    if value["id"].as_str() != Some(expected) {
        return Err(bad());
    }
    let (status, counts) = if client.api == Api::Anthropic {
        let status = match value["processing_status"].as_str() {
            Some("in_progress") => BatchStatus::Running,
            Some("canceling") => BatchStatus::Cancelling,
            Some("ended") => BatchStatus::Completed,
            _ => return Err(bad()),
        };
        let c = &value["request_counts"];
        let count = |key: &str| c[key].as_u64().ok_or_else(bad);
        let processing = count("processing")?;
        let completed = count("succeeded")?;
        let failed = count("errored")?;
        let cancelled = count("canceled")?;
        let expired = count("expired")?;
        let total = processing
            .checked_add(completed)
            .and_then(|n| n.checked_add(failed))
            .and_then(|n| n.checked_add(cancelled))
            .and_then(|n| n.checked_add(expired))
            .ok_or_else(bad)?;
        if status == BatchStatus::Completed && processing != 0 {
            return Err(bad());
        }
        (
            status,
            Some(BatchCounts {
                total,
                completed,
                failed,
                cancelled,
                expired,
            }),
        )
    } else {
        let status = match value["status"].as_str() {
            Some("validating" | "in_progress" | "finalizing") => BatchStatus::Running,
            Some("cancelling") => BatchStatus::Cancelling,
            Some("completed") => BatchStatus::Completed,
            Some("failed") => BatchStatus::Failed,
            Some("cancelled") => BatchStatus::Cancelled,
            Some("expired") => BatchStatus::Expired,
            _ => return Err(bad()),
        };
        let c = &value["request_counts"];
        let counts = if c.is_null() {
            if status == BatchStatus::Completed {
                return Err(bad());
            }
            None
        } else {
            let total = c["total"].as_u64().ok_or_else(bad)?;
            let completed = c["completed"].as_u64().ok_or_else(bad)?;
            let failed = c["failed"].as_u64().ok_or_else(bad)?;
            if completed.checked_add(failed).is_none_or(|n| n > total) {
                return Err(bad());
            }
            Some(BatchCounts {
                total,
                completed,
                failed,
                ..Default::default()
            })
        };
        (status, counts)
    };
    Ok(BatchJob { status, counts })
}
fn timeout_error() -> ModelError {
    ModelError::new(
        ModelErrorKind::Network,
        "provider operation timed out",
        false,
    )
}
fn check_operation(
    cancel: &CancellationToken,
    deadline: tokio::time::Instant,
) -> Result<(), ModelError> {
    if cancel.is_cancelled() {
        return Err(ModelError::cancelled());
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(timeout_error());
    }
    Ok(())
}
async fn operation<T, F, Fut>(
    client: &Client,
    cancel: CancellationToken,
    f: F,
) -> Result<T, ModelError>
where
    F: FnOnce(tokio::time::Instant) -> Fut,
    Fut: std::future::Future<Output = Result<T, ModelError>>,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(client.config.timeout)
        .ok_or_else(invalid)?;
    check_operation(&cancel, deadline)?;
    tokio::select! { biased; _ = cancel.cancelled() => Err(ModelError::cancelled()), result = tokio::time::timeout_at(deadline, f(deadline)) => { let result = result.unwrap_or_else(|_| Err(timeout_error())); check_operation(&cancel, deadline)?; result } }
}
async fn submit(
    client: &Client,
    items: &[BatchRequest],
    cancel: CancellationToken,
) -> Result<BatchHandle, ModelError> {
    operation(client, cancel.clone(), |deadline| async move {
        let ids = validate_items(client.api, items)?;
        let owned = items.to_vec();
        check_operation(&cancel, deadline)?;
        let api = client.api;
        let config = client.config.clone();
        let body = tokio::task::spawn_blocking(move || build_with(api, &config, &owned))
            .await.map_err(|_| invalid())??;
        check_operation(&cancel, deadline)?;
        let mut budget = LIMIT;
        let mut context = SendContext { cancel: cancel.clone(), budget: &mut budget, deadline };
        let value = if client.api == Api::Anthropic {
            send(client, reqwest::Method::POST, batch_endpoint(client,&["messages","batches"])?, Some(("application/json",body)),false,&mut context).await?.remove(0)
        } else {
            if body.windows(BOUNDARY.len()).any(|w| w == BOUNDARY.as_bytes()) { return Err(invalid()); }
            let mut multipart = format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"purpose\"\r\n\r\nbatch\r\n--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"batch.jsonl\"\r\nContent-Type: application/jsonl\r\n\r\n").into_bytes(); multipart.extend(body); multipart.extend(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
            let ct = format!("multipart/form-data; boundary={BOUNDARY}");
            let upload = send(client, reqwest::Method::POST, batch_endpoint(client,&["files"])?, Some((&ct,multipart)),false,&mut context).await?.remove(0);
            let file = upload["id"].as_str().filter(|s| safe_id(s)).ok_or_else(bad)?;
            let endpoint = if client.api == Api::Responses { "/v1/responses" } else { "/v1/chat/completions" };
            let body = serde_json::to_vec(&json!({"input_file_id":file,"endpoint":endpoint,"completion_window":"24h"})).map_err(|_| invalid())?;
            send(client, reqwest::Method::POST, batch_endpoint(client,&["batches"])?,Some(("application/json",body)),false,&mut context).await?.remove(0)
        };
        let id = value["id"].as_str().filter(|s| safe_id(s)).ok_or_else(bad)?;
        Ok(BatchHandle { provider:client.api.provider().into(), protocol:protocol(client.api).into(),model:client.config.model.clone(), endpoint:client.transport.endpoint().as_str().to_owned(), job_id:id.into(),message_ids:ids })
    }).await
}
async fn fetch_job(
    client: &Client,
    h: &BatchHandle,
    cancel: CancellationToken,
    cancelling: bool,
    deadline: tokio::time::Instant,
) -> Result<Value, ModelError> {
    validate_handle(client, h)?;
    let mut tail = if client.api == Api::Anthropic {
        vec!["messages", "batches", h.job_id.as_str()]
    } else {
        vec!["batches", h.job_id.as_str()]
    };
    if cancelling {
        tail.push("cancel");
    }
    let method = if cancelling {
        reqwest::Method::POST
    } else {
        reqwest::Method::GET
    };
    let mut budget = LIMIT;
    let mut context = SendContext {
        cancel,
        budget: &mut budget,
        deadline,
    };
    Ok(send(
        client,
        method,
        batch_endpoint(client, &tail)?,
        None,
        false,
        &mut context,
    )
    .await?
    .remove(0))
}
async fn status(
    client: &Client,
    h: &BatchHandle,
    cancel: CancellationToken,
    cancelling: bool,
) -> Result<BatchJob, ModelError> {
    operation(client, cancel.clone(), |deadline| async move {
        let v = fetch_job(client, h, cancel.clone(), cancelling, deadline).await?;
        job(client, &v, &h.job_id)
    })
    .await
}
async fn results(
    client: &Client,
    h: &BatchHandle,
    cancel: CancellationToken,
) -> Result<BatchResults, ModelError> {
    operation(client, cancel.clone(), |deadline| async move {
        let value = fetch_job(client, h, cancel.clone(), false, deadline).await?;
        let state = job(client, &value, &h.job_id)?;
        if matches!(
            state.status,
            BatchStatus::Pending | BatchStatus::Running | BatchStatus::Cancelling
        ) {
            return Err(invalid());
        }
        if client.api == Api::Anthropic || state.status == BatchStatus::Completed {
            let counts = state.counts.as_ref().ok_or_else(bad)?;
            if counts.total != h.message_ids.len() as u64 {
                return Err(bad());
            }
        }
        let mut values = Vec::new();
        let mut budget = LIMIT;
        let mut context = SendContext {
            cancel: cancel.clone(),
            budget: &mut budget,
            deadline,
        };
        if client.api == Api::Anthropic {
            values.extend(
                send(
                    client,
                    reqwest::Method::GET,
                    batch_endpoint(client, &["messages", "batches", &h.job_id, "results"])?,
                    None,
                    true,
                    &mut context,
                )
                .await?,
            );
        } else {
            for key in ["output_file_id", "error_file_id"] {
                if let Some(v) = value.get(key).filter(|v| !v.is_null()) {
                    let id = v.as_str().filter(|s| safe_id(s)).ok_or_else(bad)?;
                    values.extend(
                        send(
                            client,
                            reqwest::Method::GET,
                            batch_endpoint(client, &["files", id, "content"])?,
                            None,
                            true,
                            &mut context,
                        )
                        .await?,
                    );
                }
            }
        }
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for v in values {
            let id = v["custom_id"].as_str().ok_or_else(bad)?;
            let message_id = h.message_ids.get(id).ok_or_else(bad)?;
            if !seen.insert(id.to_owned()) {
                return Err(bad());
            }
            let outcome = if client.api == Api::Anthropic {
                let r = &v["result"];
                match r["type"].as_str() {
                    Some("succeeded") => {
                        BatchOutcome::Success(Box::new(wire::Decoder::from_nonstream(
                            client.api,
                            &client.config.model,
                            message_id,
                            &r["message"],
                        )?))
                    }
                    Some("errored") => {
                        BatchOutcome::Error(crate::error::classify(None, Some(&r["error"]), &[]))
                    }
                    Some("canceled") => BatchOutcome::Cancelled,
                    Some("expired") => BatchOutcome::Expired,
                    _ => return Err(bad()),
                }
            } else if !v["error"].is_null() {
                match v["error"]["code"].as_str() {
                    Some("batch_expired") => BatchOutcome::Expired,
                    Some("batch_cancelled") => BatchOutcome::Cancelled,
                    _ => BatchOutcome::Error(crate::error::classify(None, Some(&v["error"]), &[])),
                }
            } else {
                let r = &v["response"];
                let status = r["status_code"]
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok())
                    .filter(|s| (100..=599).contains(s))
                    .ok_or_else(bad)?;
                if (200..300).contains(&status) {
                    BatchOutcome::Success(Box::new(wire::Decoder::from_nonstream(
                        client.api,
                        &client.config.model,
                        message_id,
                        &r["body"],
                    )?))
                } else {
                    BatchOutcome::Error(crate::error::classify(Some(status), Some(&r["body"]), &[]))
                }
            };
            out.push(BatchItemResult {
                custom_id: id.into(),
                outcome,
            });
        }
        check_operation(&cancel, deadline)?;
        let unreported_ids: Vec<_> = h
            .message_ids
            .keys()
            .filter(|id| !seen.contains(*id))
            .cloned()
            .collect();
        if client.api == Api::Anthropic || state.status == BatchStatus::Completed {
            if !unreported_ids.is_empty() {
                return Err(bad());
            }
            let mut actual = BatchCounts::default();
            for item in &out {
                match &item.outcome {
                    BatchOutcome::Success(_) => actual.completed += 1,
                    BatchOutcome::Error(_) => actual.failed += 1,
                    BatchOutcome::Cancelled => actual.cancelled += 1,
                    BatchOutcome::Expired => actual.expired += 1,
                }
            }
            let counts = state.counts.as_ref().ok_or_else(bad)?;
            if actual.completed != counts.completed
                || (if client.api == Api::Anthropic {
                    actual.failed != counts.failed
                        || actual.cancelled != counts.cancelled
                        || actual.expired != counts.expired
                } else {
                    actual.failed + actual.cancelled + actual.expired != counts.failed
                })
            {
                return Err(bad());
            }
        }
        Ok(BatchResults {
            job: state,
            items: out,
            unreported_ids,
        })
    })
    .await
}
macro_rules! implementations {
    ($ty:ty) => {
        impl BatchTranslator for $ty {
            fn build_submission(&self, items: &[BatchRequest]) -> Result<Vec<u8>, ModelError> {
                build(&self.0, items)
            }
            fn decode_response(
                &self,
                message_id: &str,
                response: &Value,
            ) -> Result<HistoryEntry, ModelError> {
                if self.0.api == Api::Deepseek {
                    return Err(crate::unsupported());
                }
                if message_id.trim().is_empty() {
                    return Err(invalid());
                }
                wire::Decoder::from_nonstream(
                    self.0.api,
                    &self.0.config.model,
                    message_id,
                    response,
                )
            }
        }
        #[async_trait]
        impl BatchProvider for $ty {
            async fn submit(
                &self,
                items: &[BatchRequest],
                cancel: CancellationToken,
            ) -> Result<BatchHandle, ModelError> {
                submit(&self.0, items, cancel).await
            }
            async fn status(
                &self,
                h: &BatchHandle,
                cancel: CancellationToken,
            ) -> Result<BatchJob, ModelError> {
                status(&self.0, h, cancel, false).await
            }
            async fn results(
                &self,
                h: &BatchHandle,
                cancel: CancellationToken,
            ) -> Result<BatchResults, ModelError> {
                results(&self.0, h, cancel).await
            }
            async fn cancel(
                &self,
                h: &BatchHandle,
                cancel: CancellationToken,
            ) -> Result<BatchJob, ModelError> {
                status(&self.0, h, cancel, true).await
            }
        }
    };
}
implementations!(OpenAIProvider);
implementations!(AnthropicProvider);
implementations!(DeepseekProvider);
