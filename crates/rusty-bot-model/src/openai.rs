use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use futures_util::StreamExt;
use rusty_bot_core::{
    AssistantTurn, ChatRequest, ContentPart, ImageLimits, Message, ModelError, ModelErrorKind,
    ModelProvider, ProviderMessage, ProviderToolCall, StopReason, StreamEvent, ToolCall,
    ToolChoice, ToolSpec, Usage,
};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{json, Value};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::sse::SseParser;

#[derive(Clone)]
pub struct OpenAiCompatConfig {
    /// Full chat-completions URL, e.g. `https://api.openai.com/v1/chat/completions`.
    pub endpoint: Url,
    pub api_key: Option<SecretString>,
    pub model: String,
    pub headers: Vec<(String, String)>,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub retry_jitter: bool,
    /// Opt in only after confirming this endpoint accepts portable user-image
    /// messages following tool replies. None is explicitly text-only.
    pub image_limits: Option<ImageLimits>,
}

impl OpenAiCompatConfig {
    pub fn new(endpoint: Url, model: impl Into<String>) -> Self {
        Self {
            endpoint,
            api_key: None,
            model: model.into(),
            headers: Vec::new(),
            request_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(60),
            retry_jitter: true,
            image_limits: None,
        }
    }
}

impl fmt::Debug for OpenAiCompatConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompatConfig")
            .field("endpoint", &self.endpoint.as_str())
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("model", &self.model)
            .field("headers", &self.headers.len())
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_retries", &self.max_retries)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("retry_jitter", &self.retry_jitter)
            .finish()
    }
}

#[derive(Clone)]
pub struct OpenAiCompatProvider {
    client: reqwest::Client,
    config: OpenAiCompatConfig,
}

impl fmt::Debug for OpenAiCompatProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompatProvider")
            .field("config", &self.config)
            .finish()
    }
}

impl OpenAiCompatProvider {
    pub fn new(config: OpenAiCompatConfig) -> Result<Self, ModelError> {
        if config.image_limits.as_ref().is_some_and(|limits| {
            limits.max_bytes == 0
                || limits.max_bytes > rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES
                || limits.max_width == 0
                || limits.max_height == 0
        }) {
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "invalid or excessive configured image limits",
                false,
            ));
        }
        if !matches!(config.endpoint.scheme(), "http" | "https") {
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "endpoint must be an http(s) URL",
                false,
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| {
                ModelError::new(
                    ModelErrorKind::Network,
                    format!("failed to build HTTP client: {error}"),
                    false,
                )
            })?;
        Ok(Self { client, config })
    }

    pub fn config(&self) -> &OpenAiCompatConfig {
        &self.config
    }

    async fn send(
        &self,
        body: &Value,
        cancel: &CancellationToken,
    ) -> Result<reqwest::Response, ModelError> {
        let mut request = self
            .client
            .post(self.config.endpoint.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(body);
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key.expose_secret());
        }
        tokio::select! {
            _ = cancel.cancelled() => Err(ModelError::cancelled()),
            result = request.send() => result.map_err(|error| {
                if error.is_timeout() {
                    ModelError::new(ModelErrorKind::Network, "model request timed out", true)
                } else {
                    ModelError::new(ModelErrorKind::Network, "model request failed", true)
                }
            }),
        }
    }

    async fn consume(
        &self,
        response: reqwest::Response,
        request: &ChatRequest,
        events: &Sender<StreamEvent>,
        cancel: &CancellationToken,
        emitted: &mut bool,
    ) -> Result<AssistantTurn, ModelError> {
        let mut stream = response.bytes_stream();
        let mut parser = SseParser::new();
        let mut text = String::new();
        let mut thinking = String::new();
        let mut calls: BTreeMap<u32, ToolCallAcc> = BTreeMap::new();
        let mut usage = Usage::default();
        let mut finish_reason: Option<String> = None;
        let mut done = false;

        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return Err(ModelError::cancelled()),
                next = stream.next() => next,
            };
            let Some(item) = next else {
                break;
            };
            let bytes = item.map_err(|error| {
                ModelError::new(
                    ModelErrorKind::Network,
                    format!("model stream disconnected: {}", error.without_url()),
                    !*emitted,
                )
            })?;
            for data in parser.push(&bytes) {
                if data.trim() == "[DONE]" {
                    done = true;
                    break;
                }
                let chunk: Value = serde_json::from_str(&data).map_err(|error| {
                    ModelError::new(
                        ModelErrorKind::Protocol,
                        format!("invalid SSE JSON: {error}"),
                        !*emitted,
                    )
                })?;
                self.handle_chunk(
                    &chunk,
                    &mut text,
                    &mut thinking,
                    &mut calls,
                    &mut usage,
                    &mut finish_reason,
                    events,
                    cancel,
                    emitted,
                )
                .await?;
            }
            if done {
                break;
            }
        }
        if let Some(data) = parser.finish() {
            if data.trim() == "[DONE]" {
                done = true;
            } else {
                let chunk: Value = serde_json::from_str(&data).map_err(|error| {
                    ModelError::new(
                        ModelErrorKind::Protocol,
                        format!("invalid SSE JSON: {error}"),
                        !*emitted,
                    )
                })?;
                self.handle_chunk(
                    &chunk,
                    &mut text,
                    &mut thinking,
                    &mut calls,
                    &mut usage,
                    &mut finish_reason,
                    events,
                    cancel,
                    emitted,
                )
                .await?;
            }
        }

        if !done && finish_reason.is_none() {
            return Err(ModelError::new(
                ModelErrorKind::Protocol,
                "model stream ended without [DONE] or a finish reason",
                !*emitted,
            ));
        }

        let stop = match finish_reason.as_deref() {
            Some("stop") => StopReason::Stop,
            Some("length") => StopReason::Length,
            Some("tool_calls") | Some("function_call") => StopReason::ToolUse,
            Some("content_filter") => StopReason::Error,
            None if done => StopReason::Stop,
            None => {
                return Err(ModelError::new(
                    ModelErrorKind::Protocol,
                    "model stream ended without a finish reason",
                    !*emitted,
                ))
            }
            Some(other) => {
                return Err(ModelError::new(
                    ModelErrorKind::Protocol,
                    format!("unknown finish reason: {other}"),
                    !*emitted,
                ))
            }
        };

        let mut tool_calls = Vec::new();
        for (index, acc) in calls {
            let id = acc.id.ok_or_else(|| {
                ModelError::new(
                    ModelErrorKind::Protocol,
                    format!("tool call {index} is missing an id"),
                    !*emitted,
                )
            })?;
            let name = acc.name.ok_or_else(|| {
                ModelError::new(
                    ModelErrorKind::Protocol,
                    format!("tool call {index} is missing a name"),
                    !*emitted,
                )
            })?;
            let arguments = if acc.arguments.trim().is_empty() {
                let required = request
                    .tools
                    .iter()
                    .find(|tool| tool.name == name)
                    .and_then(|tool| tool.parameters.get("required"))
                    .and_then(|required| required.as_array())
                    .map(|required| !required.is_empty())
                    .unwrap_or(false);
                if required {
                    return Err(ModelError::new(
                        ModelErrorKind::Protocol,
                        format!("tool call {name} has no arguments but requires parameters"),
                        !*emitted,
                    ));
                }
                json!({})
            } else {
                serde_json::from_str(&acc.arguments).map_err(|error| {
                    ModelError::new(
                        ModelErrorKind::Protocol,
                        format!("tool call {name} arguments are not valid JSON: {error}"),
                        !*emitted,
                    )
                })?
            };
            tool_calls.push(ToolCall {
                id,
                name,
                arguments,
            });
        }

        let content = if text.is_empty() {
            Vec::new()
        } else {
            vec![ContentPart::Text { text: text.clone() }]
        };
        let message = Message::Assistant {
            content,
            tool_calls: tool_calls.clone(),
            usage: Some(usage),
            stop: Some(stop),
        };
        Ok(AssistantTurn {
            message,
            tool_calls,
            usage,
            stop,
            provider_metadata: json!({
                "model": request.model,
                "finish_reason": finish_reason,
                "thinking": if thinking.is_empty() { Value::Null } else { Value::String(thinking) },
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_chunk(
        &self,
        chunk: &Value,
        text: &mut String,
        thinking: &mut String,
        calls: &mut BTreeMap<u32, ToolCallAcc>,
        usage: &mut Usage,
        finish_reason: &mut Option<String>,
        events: &Sender<StreamEvent>,
        cancel: &CancellationToken,
        emitted: &mut bool,
    ) -> Result<(), ModelError> {
        if let Some(value) = chunk
            .get("usage")
            .and_then(|usage| (!usage.is_null()).then_some(usage))
        {
            *usage = parse_usage(value);
            send_event(events, StreamEvent::Usage(*usage), cancel).await?;
        }

        let Some(choices) = chunk.get("choices").and_then(Value::as_array) else {
            return Ok(());
        };
        let Some(choice) = choices.first() else {
            return Ok(());
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            *finish_reason = Some(reason.to_string());
        }
        let Some(delta) = choice.get("delta") else {
            return Ok(());
        };

        if let Some(content) = delta.get("content").and_then(Value::as_str) {
            if !content.is_empty() {
                *emitted = true;
                text.push_str(content);
                send_event(events, StreamEvent::TextDelta(content.to_string()), cancel).await?;
            }
        }
        if let Some(content) = delta.get("reasoning_content").and_then(Value::as_str) {
            if !content.is_empty() {
                *emitted = true;
                thinking.push_str(content);
                send_event(
                    events,
                    StreamEvent::ThinkingDelta(content.to_string()),
                    cancel,
                )
                .await?;
            }
        }

        if let Some(deltas) = delta.get("tool_calls").and_then(Value::as_array) {
            for (position, call) in deltas.iter().enumerate() {
                let index = call
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|index| index as u32)
                    .unwrap_or(position as u32);
                let acc = calls.entry(index).or_default();
                let id = call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty());
                let name = call
                    .get("function")
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty());
                if let Some(id) = id {
                    acc.id = Some(id.to_string());
                }
                if let Some(name) = name {
                    acc.name = Some(name.to_string());
                }
                if let (Some(id), Some(name)) = (&acc.id, &acc.name) {
                    if !acc.started {
                        acc.started = true;
                        *emitted = true;
                        send_event(
                            events,
                            StreamEvent::ToolCallStart {
                                index,
                                id: id.clone(),
                                name: name.clone(),
                            },
                            cancel,
                        )
                        .await?;
                    }
                }
                if let Some(arguments) = call
                    .get("function")
                    .and_then(|function| function.get("arguments"))
                    .and_then(Value::as_str)
                {
                    acc.arguments.push_str(arguments);
                    *emitted = true;
                    send_event(
                        events,
                        StreamEvent::ToolCallArgumentsDelta {
                            index,
                            delta: arguments.to_string(),
                        },
                        cancel,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    fn build_body(&self, request: &ChatRequest) -> Result<Value, ModelError> {
        let mut body = json!({
            "model": request.model,
            "messages": self.wire_messages(&request.messages)?,
            "stream": true,
            "stream_options": { "include_usage": true },
            "tool_choice": tool_choice_json(&request.tool_choice),
        });
        if !request.tools.is_empty() {
            body["tools"] = Value::Array(request.tools.iter().map(tool_spec_json).collect());
        }
        if let Some(temperature) = request.temperature {
            // `ChatRequest::temperature` is an `f32`; converting it straight to
            // JSON widens it to `0.20000000298023224`. Round to the six decimal
            // places the OpenAI-compatible wire format accepts so the request
            // carries the value the caller configured.
            let widened = f64::from(temperature);
            let rounded = (widened * 1_000_000.0).round() / 1_000_000.0;
            body["temperature"] = json!(rounded);
        }
        if let Some(max_tokens) = request.max_tokens {
            body["max_tokens"] = json!(max_tokens);
        }
        Ok(body)
    }

    fn wire_messages(&self, messages: &[ProviderMessage]) -> Result<Vec<Value>, ModelError> {
        let mut wire = Vec::new();
        let mut image_parts = Vec::new();
        for message in messages {
            if !matches!(message, ProviderMessage::Tool { .. }) && !image_parts.is_empty() {
                wire.push(json!({"role":"user", "content": std::mem::take(&mut image_parts)}));
            }
            if let ProviderMessage::Tool {
                tool_call_id,
                content,
            } = message
            {
                let has_image = content
                    .iter()
                    .any(|p| matches!(p, ContentPart::Image { .. }));
                if has_image {
                    let limits = self.config.image_limits.as_ref().ok_or_else(|| {
                        ModelError::new(
                            ModelErrorKind::Unsupported,
                            "provider is configured as text-only",
                            false,
                        )
                    })?;
                    rusty_bot_core::image_validation::validate_images(content, limits).map_err(
                        |reason| ModelError::new(ModelErrorKind::InvalidRequest, reason, false),
                    )?;
                    image_parts.push(json!({"type":"text", "text":format!("Tool result images and content for call {tool_call_id}:")}));
                    let mut text = String::new();
                    for part in content {
                        match part {
                            ContentPart::Text { text: value } => {
                                text.push_str(value);
                                image_parts.push(json!({"type":"text", "text":value}));
                            }
                            ContentPart::Image {
                                mime_type,
                                data_base64,
                            } => {
                                text.push_str(&format!(
                                    "[image associated with tool call {tool_call_id}]"
                                ));
                                image_parts.push(json!({"type":"image_url", "image_url":{"url":format!("data:{mime_type};base64,{data_base64}")}}));
                            }
                        }
                    }
                    wire.push(json!({"role":"tool", "tool_call_id":tool_call_id, "content":text}));
                    continue;
                }
            }
            wire.push(provider_message_json(message));
        }
        if !image_parts.is_empty() {
            wire.push(json!({"role":"user", "content":image_parts}));
        }
        Ok(wire)
    }

    fn status_error(&self, status: reqwest::StatusCode) -> ModelError {
        let kind = match status.as_u16() {
            401 | 403 => ModelErrorKind::Auth,
            400 | 422 => ModelErrorKind::InvalidRequest,
            413 => ModelErrorKind::ContextOverflow,
            429 => ModelErrorKind::RateLimited,
            500..=599 => ModelErrorKind::Server,
            _ => ModelErrorKind::InvalidRequest,
        };
        let retryable = matches!(
            kind,
            ModelErrorKind::RateLimited | ModelErrorKind::Server | ModelErrorKind::Network
        );
        ModelError::new(
            kind,
            format!("model provider returned HTTP {}", status.as_u16()),
            retryable,
        )
        .with_status(status.as_u16())
    }

    fn backoff(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let base = retry_after.unwrap_or_else(|| {
            let exponential = self
                .config
                .base_delay
                .saturating_mul(1u32 << attempt.min(10));
            exponential.min(self.config.max_delay)
        });
        if self.config.retry_jitter && retry_after.is_none() {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.subsec_nanos())
                .unwrap_or(0);
            let millis = base.as_millis() as u64;
            if millis > 0 {
                return Duration::from_millis((nanos as u64) % millis);
            }
        }
        base
    }
}

#[async_trait::async_trait]
impl ModelProvider for OpenAiCompatProvider {
    fn validate_native_context_reset(
        &self,
        original: &rusty_bot_core::HistoryRequest,
        candidate: &rusty_bot_core::HistoryRequest,
    ) -> Result<(), ModelError> {
        crate::native_reset::same_options(original, candidate)?;
        self.build_body(&rusty_bot_core::project_history(original.clone())?)?;
        self.build_body(&rusty_bot_core::project_history(candidate.clone())?)?;
        Ok(())
    }
    fn image_limits(&self) -> Option<ImageLimits> {
        self.config.image_limits.clone()
    }
    fn id(&self) -> &str {
        "openai-compatible"
    }

    async fn stream(
        &self,
        request: ChatRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        self.stream_with_hook(request, events, cancel, None).await
    }

    async fn stream_history_with_payload_hook(
        &self,
        request: rusty_bot_core::HistoryRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
        payload_hook: Option<std::sync::Arc<dyn rusty_bot_core::ProviderPayloadHook>>,
    ) -> Result<rusty_bot_core::HistoryTurn, ModelError> {
        let request = rusty_bot_core::project_history(request)?;
        let turn = self
            .stream_with_hook(request, events, cancel, payload_hook)
            .await?;
        rusty_bot_core::history_turn_from_legacy(turn)
    }
}

impl OpenAiCompatProvider {
    async fn stream_with_hook(
        &self,
        request: ChatRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
        payload_hook: Option<std::sync::Arc<dyn rusty_bot_core::ProviderPayloadHook>>,
    ) -> Result<AssistantTurn, ModelError> {
        let provider = self.clone();
        let (request, body) = tokio::task::spawn_blocking(move || {
            let body = provider.build_body(&request);
            (request, body)
        })
        .await
        .map_err(|_| {
            ModelError::new(
                ModelErrorKind::InvalidRequest,
                "request validation failed",
                false,
            )
        })?;
        let body = body?;
        let mut emitted = false;
        let mut attempt = 0u32;
        loop {
            if cancel.is_cancelled() {
                return Err(ModelError::cancelled());
            }
            let candidate = crate::native_transport::apply_payload_hook(
                "openai.chat_completions.v1",
                &request.model,
                &body,
                payload_hook.as_ref(),
                &cancel,
            )
            .await?;
            let response = self.send(&candidate, &cancel).await?;
            let status = response.status();
            if status.is_success() {
                return self
                    .consume(response, &request, &events, &cancel, &mut emitted)
                    .await;
            }

            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            let error = self.status_error(status);
            tracing::warn!(
                status = status.as_u16(),
                model = %request.model,
                retryable = error.retryable,
                "model provider request rejected"
            );
            if error.retryable && !emitted && attempt < self.config.max_retries {
                let delay = self.backoff(attempt, retry_after);
                attempt += 1;
                tokio::select! {
                    _ = cancel.cancelled() => return Err(ModelError::cancelled()),
                    _ = tokio::time::sleep(delay) => {}
                }
                continue;
            }
            return Err(error);
        }
    }
}

#[derive(Default)]
struct ToolCallAcc {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
    started: bool,
}

async fn send_event(
    events: &Sender<StreamEvent>,
    event: StreamEvent,
    cancel: &CancellationToken,
) -> Result<(), ModelError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(ModelError::cancelled()),
        result = events.send(event) => result.map_err(|_| ModelError::cancelled()),
    }
}

fn parse_usage(value: &Value) -> Usage {
    Usage {
        input_tokens: value
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_tokens: value
            .get("prompt_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
        cache_write_tokens: None,
    }
}

fn tool_choice_json(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::Named(name) => json!({
            "type": "function",
            "function": { "name": name }
        }),
    }
}

fn tool_spec_json(tool: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        }
    })
}

fn provider_message_json(message: &ProviderMessage) -> Value {
    match message {
        ProviderMessage::System { content } => json!({ "role": "system", "content": content }),
        ProviderMessage::User { content } => json!({ "role": "user", "content": content }),
        ProviderMessage::Assistant {
            content,
            tool_calls,
        } => {
            // Null content is valid alongside actual tool calls. With no
            // calls, preserve an empty reply as a string rather than a null-only
            // assistant message, which some compatible providers reject.
            let wire_content = if tool_calls.is_empty() {
                Some(content.as_deref().unwrap_or(""))
            } else {
                content.as_deref()
            };
            let mut message = json!({ "role": "assistant", "content": wire_content });
            // OpenAI-compatible servers reject an explicitly empty tool_calls
            // array. Ordinary assistant replies must omit the optional field.
            if !tool_calls.is_empty() {
                message["tool_calls"] = json!(tool_calls
                    .iter()
                    .map(|call| json!({
                        "id": call.id,
                        "type": "function",
                        "function": { "name": call.name, "arguments": call.arguments },
                    }))
                    .collect::<Vec<_>>());
            }
            message
        }
        ProviderMessage::Tool {
            tool_call_id,
            content,
        } => json!({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": Message::text_content(content),
        }),
    }
}

/// Shared conversion used by engine prompt composition as well.
pub fn tool_calls_to_wire(calls: &[ToolCall]) -> Vec<ProviderToolCall> {
    calls
        .iter()
        .map(|call| ProviderToolCall {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.to_string(),
        })
        .collect()
}
