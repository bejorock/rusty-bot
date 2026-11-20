//! OpenAI Responses API adapter with stateless, provider-bound replay.
//!
//! Only text, images, client functions and encrypted reasoning are supported.
//! Server-side tools and unknown output items fail closed. Native output items
//! are sensitive and are stored in the transcript, not provider metadata.

use std::sync::Arc;

use rusty_bot_core::{
    AssistantTurn, ChatRequest, ContentPart, CredentialResolver, HistoryPart, HistoryRequest,
    HistoryTurn, Message, ModelDiscovery, ModelError, ModelErrorKind, ModelProvider,
    ProviderCredential, ProviderState, RichRole, StopReason, StreamEvent, ToolChoice, Usage,
};
use serde_json::{json, Value};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use crate::{
    native_transport::{NativeTransport, StaticCredentialResolver},
    SseParser,
};

const FORMAT: &str = "openai.responses.v1";

/// Native Responses provider. `identity` is a caller-chosen stable non-secret
/// namespace distinguishing accounts and endpoints; it is persisted in history.
pub struct OpenAiResponsesProvider {
    identity: String,
    transport: NativeTransport,
    image_limits: Option<rusty_bot_core::ImageLimits>,
}

impl std::fmt::Debug for OpenAiResponsesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenAiResponsesProvider([REDACTED])")
    }
}

impl OpenAiResponsesProvider {
    pub fn new(
        identity: impl Into<String>,
        endpoint: &str,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        let identity = identity.into();
        if identity.trim().is_empty()
            || identity.len() > 128
            || !identity
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
        {
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "invalid provider identity",
                false,
            ));
        }
        Ok(Self {
            identity,
            transport: NativeTransport::new(endpoint, resolver)?,
            image_limits: None,
        })
    }

    pub fn with_static_credential(
        identity: impl Into<String>,
        endpoint: &str,
        credential: ProviderCredential,
    ) -> Result<Self, ModelError> {
        Self::new(
            identity,
            endpoint,
            Arc::new(StaticCredentialResolver::new(credential)),
        )
    }

    /// Opt in to image admission with limits chosen for the caller's model.
    pub fn with_image_limits(mut self, limits: rusty_bot_core::ImageLimits) -> Self {
        self.image_limits = Some(limits);
        self
    }

    /// Responses model-list metadata does not reliably advertise capabilities.
    pub fn discover_models(&self) -> ModelDiscovery {
        ModelDiscovery::Unsupported
    }

    fn body(&self, request: &HistoryRequest) -> Result<Value, ModelError> {
        crate::native_images::validate_history_images(
            &request.messages,
            self.image_limits.as_ref(),
        )?;
        if request.model.trim().is_empty() {
            return Err(invalid("model is required"));
        }
        if request
            .temperature
            .is_some_and(|t| !t.is_finite() || !(0.0..=2.0).contains(&t))
            || request.max_tokens == Some(0)
        {
            return Err(invalid("invalid model options"));
        }
        if let Some(options) = request.metadata.get("model_options") {
            let obj = options
                .as_object()
                .ok_or_else(|| invalid("invalid model options"))?;
            if obj.keys().any(|key| key != "thinking_level") {
                return Err(ModelError::unsupported("unsupported model option"));
            }
            if let Some(thinking) = obj.get("thinking_level") {
                if thinking.as_str() != Some("off") {
                    return Err(ModelError::unsupported("thinking level is unsupported"));
                }
            }
        }
        let mut input = Vec::new();
        for message in &request.messages {
            self.input_message(message, &request.model, &mut input)?;
        }
        let tools: Vec<Value> = request.tools.iter().map(|tool| json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":false})).collect();
        let choice = match &request.tool_choice {
            ToolChoice::Auto => json!("auto"),
            ToolChoice::None => json!("none"),
            ToolChoice::Required => json!("required"),
            ToolChoice::Named(name) => json!({"type":"function","name":name}),
        };
        let mut body = json!({"model":request.model,"input":input,"stream":true,"store":false,"include":["reasoning.encrypted_content"],"tools":tools,"tool_choice":choice});
        if let Some(t) = request.temperature {
            body["temperature"] = json!(t);
        }
        if let Some(n) = request.max_tokens {
            body["max_output_tokens"] = json!(n);
        }
        Ok(body)
    }

    fn input_message(
        &self,
        message: &Message,
        model: &str,
        input: &mut Vec<Value>,
    ) -> Result<(), ModelError> {
        match message {
            Message::System { content, .. } => {
                input.push(json!({"role":"system","content":content}))
            }
            Message::User { content } => {
                input.push(json!({"role":"user","content":input_content(content)}))
            }
            Message::Assistant {
                content,
                tool_calls,
                ..
            } => {
                if !content.is_empty() {
                    input.push(json!({"role":"assistant","content":assistant_content(content)?}));
                }
                for call in tool_calls {
                    input.push(json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments.to_string()}));
                }
            }
            Message::ToolResult {
                tool_call_id,
                content,
                is_error,
                ..
            } => input.push(tool_output(tool_call_id, content, *is_error)),
            Message::Rich {
                rich_role: RichRole::Assistant,
                parts,
            } => {
                let Some(HistoryPart::ProviderState { state }) = parts.last() else {
                    return Err(invalid("missing native replay state"));
                };
                if state.provider != self.identity
                    || state.model.as_deref() != Some(model)
                    || state.format != FORMAT
                    || !state.required_for_replay
                {
                    return Err(invalid("invalid native replay state"));
                }
                let items = state
                    .payload
                    .get("output")
                    .and_then(Value::as_array)
                    .ok_or_else(|| invalid("invalid native replay state"))?;
                let replay_parts = parts_from_items(items)?;
                if replay_parts != parts[..parts.len() - 1] {
                    return Err(invalid("native state does not match transcript"));
                }
                input.extend(items.iter().cloned());
            }
            Message::Rich {
                rich_role: RichRole::User,
                parts,
            } => {
                let mut content = Vec::new();
                for part in parts {
                    match part {
                        HistoryPart::Text { text } => {
                            content.push(ContentPart::Text { text: text.clone() })
                        }
                        HistoryPart::Image {
                            mime_type,
                            data_base64,
                        } => content.push(ContentPart::Image {
                            mime_type: mime_type.clone(),
                            data_base64: data_base64.clone(),
                        }),
                        _ => return Err(ModelError::unsupported("unsupported user history part")),
                    }
                }
                input.push(json!({"role":"user","content":input_content(&content)}));
            }
            Message::Rich {
                rich_role: RichRole::Tool,
                parts,
            } => {
                for part in parts {
                    match part {
                        HistoryPart::ToolResult {
                            call_id,
                            content,
                            is_error,
                            ..
                        } => input.push(tool_output(call_id, content, *is_error)),
                        _ => return Err(ModelError::unsupported("unsupported tool history part")),
                    }
                }
            }
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> ModelError {
    ModelError::new(ModelErrorKind::InvalidRequest, message, false)
}
fn input_content(content: &[ContentPart]) -> Vec<Value> {
    content.iter().map(|part| match part {
        ContentPart::Text { text } => json!({"type":"input_text","text":text}),
        ContentPart::Image { mime_type, data_base64 } => json!({"type":"input_image","image_url":format!("data:{mime_type};base64,{data_base64}"),"detail":"auto"}),
    }).collect()
}
fn assistant_content(content: &[ContentPart]) -> Result<Vec<Value>, ModelError> {
    content
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => {
                Ok(json!({"type":"output_text","text":text,"annotations":[]}))
            }
            ContentPart::Image { .. } => Err(ModelError::unsupported(
                "assistant image history unsupported",
            )),
        })
        .collect()
}
fn tool_output(id: &str, content: &[ContentPart], is_error: bool) -> Value {
    let mut parts = input_content(content);
    if is_error {
        parts.insert(
            0,
            json!({"type":"input_text","text":"Tool execution failed."}),
        );
    }
    json!({"type":"function_call_output","call_id":id,"output":parts})
}

fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ModelError::protocol("malformed native output item"))
}

fn parts_from_items(items: &[Value]) -> Result<Vec<HistoryPart>, ModelError> {
    let mut parts = Vec::new();
    let mut calls = std::collections::HashSet::new();
    for item in items {
        match string(item, "type")? {
            "reasoning" => {
                string(item, "id")?;
                string(item, "encrypted_content")?;
                let summary = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .ok_or_else(|| ModelError::protocol("malformed reasoning item"))?;
                for part in summary {
                    if part.get("type").and_then(Value::as_str) != Some("summary_text")
                        || !part.get("text").is_some_and(Value::is_string)
                    {
                        return Err(ModelError::unsupported("unsupported reasoning summary"));
                    }
                }
                parts.push(HistoryPart::Reasoning {
                    text: None,
                    state: None,
                });
            }
            "message" => {
                string(item, "id")?;
                if item.get("role").and_then(Value::as_str) != Some("assistant")
                    || item.get("status").and_then(Value::as_str) != Some("completed")
                {
                    return Err(ModelError::protocol("malformed assistant item"));
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| ModelError::protocol("malformed assistant item"))?
                {
                    match string(part, "type")? {
                        "output_text" => parts.push(HistoryPart::Text {
                            text: part
                                .get("text")
                                .and_then(Value::as_str)
                                .ok_or_else(|| ModelError::protocol("malformed text item"))?
                                .to_owned(),
                        }),
                        _ => return Err(ModelError::unsupported("unsupported output content")),
                    }
                }
            }
            "function_call" => {
                string(item, "id")?;
                let id = string(item, "call_id")?;
                let name = string(item, "name")?;
                if item.get("namespace").is_some_and(|v| !v.is_null())
                    || item.get("async").and_then(Value::as_bool) == Some(true)
                    || item
                        .pointer("/caller/type")
                        .is_some_and(|v| v.as_str() != Some("direct"))
                {
                    return Err(ModelError::unsupported("unsupported function call mode"));
                }
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ModelError::protocol("malformed function call"))?;
                if !calls.insert(id.to_owned())
                    || !serde_json::from_str::<Value>(arguments).is_ok_and(|v| v.is_object())
                {
                    return Err(ModelError::protocol("malformed function call"));
                }
                if item.get("status").and_then(Value::as_str) != Some("completed") {
                    return Err(ModelError::protocol("incomplete function call"));
                }
                parts.push(HistoryPart::ToolCall {
                    id: id.to_owned(),
                    name: name.to_owned(),
                    arguments_json: arguments.to_owned(),
                });
            }
            _ => return Err(ModelError::unsupported("unsupported native output item")),
        }
    }
    Ok(parts)
}

async fn emit(
    events: &Sender<StreamEvent>,
    event: StreamEvent,
    cancel: &CancellationToken,
) -> Result<(), ModelError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(ModelError::cancelled()),
        result = events.send(event) => { let _ = result; Ok(()) },
    }
}

#[async_trait::async_trait]
impl ModelProvider for OpenAiResponsesProvider {
    fn validate_native_context_reset(
        &self,
        original: &HistoryRequest,
        candidate: &HistoryRequest,
    ) -> Result<(), ModelError> {
        crate::native_reset::same_options(original, candidate)?;
        self.body(original)?;
        self.body(candidate)?;
        Ok(())
    }
    fn id(&self) -> &str {
        &self.identity
    }

    fn image_limits(&self) -> Option<rusty_bot_core::ImageLimits> {
        self.image_limits.clone()
    }

    async fn stream(
        &self,
        request: ChatRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        let _ = (request, events, cancel);
        // Every native reply requires replay state which the legacy return
        // type cannot represent, so reject before any transport effects.
        Err(ModelError::unsupported(
            "native response cannot be represented by legacy turn",
        ))
    }

    async fn stream_history(
        &self,
        request: HistoryRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryTurn, ModelError> {
        self.stream_history_with_payload_hook(request, events, cancel, None)
            .await
    }

    async fn stream_history_with_payload_hook(
        &self,
        request: HistoryRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
        payload_hook: Option<Arc<dyn rusty_bot_core::ProviderPayloadHook>>,
    ) -> Result<HistoryTurn, ModelError> {
        let original = self.body(&request)?;
        let body = crate::native_transport::apply_payload_hook(
            FORMAT,
            &request.model,
            &original,
            payload_hook.as_ref(),
            &cancel,
        )
        .await?;
        let mut response = self
            .transport
            .post(&self.identity, &request.model, &body, cancel.clone())
            .await?;
        let mut metadata = json!({"http_status":response.status()});
        if let Some(id) = response.provider_request_id() {
            metadata["provider_request_id"] = json!(id);
        }
        emit(
            &events,
            StreamEvent::Start {
                provider_request_id: response.provider_request_id().map(str::to_owned),
            },
            &cancel,
        )
        .await?;
        let mut parser = SseParser::new();
        let mut completed = None;
        let mut stream = StreamState::default();
        let mut streamed_content = String::new();
        while let Some(chunk) = response.next_chunk().await? {
            for payload in parser.push(&chunk) {
                process_event(
                    &payload,
                    &response,
                    &events,
                    &cancel,
                    &mut stream,
                    &mut streamed_content,
                    &mut completed,
                )
                .await?;
            }
        }
        if let Some(payload) = parser.finish() {
            process_event(
                &payload,
                &response,
                &events,
                &cancel,
                &mut stream,
                &mut streamed_content,
                &mut completed,
            )
            .await?;
        }
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let final_response: Value =
            completed.ok_or_else(|| ModelError::protocol("missing completed response"))?;
        let output = final_response
            .get("output")
            .and_then(Value::as_array)
            .ok_or_else(|| ModelError::protocol("missing native output"))?;
        stream.reconcile(output)?;
        let mut parts = parts_from_items(output)?;
        let usage_value = final_response
            .get("usage")
            .ok_or_else(|| ModelError::protocol("missing native usage"))?;
        let usage = Usage {
            input_tokens: usage_value
                .get("input_tokens")
                .and_then(Value::as_u64)
                .ok_or_else(|| ModelError::protocol("malformed native usage"))?,
            output_tokens: usage_value
                .get("output_tokens")
                .and_then(Value::as_u64)
                .ok_or_else(|| ModelError::protocol("malformed native usage"))?,
            cache_read_tokens: usage_value
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(Value::as_u64),
            cache_write_tokens: None,
        };
        let stop = if parts
            .iter()
            .any(|p| matches!(p, HistoryPart::ToolCall { .. }))
        {
            StopReason::ToolUse
        } else {
            StopReason::Stop
        };
        response.check_value(&json!({"provider":self.identity,"model":request.model}))?;
        for part in &parts {
            if let HistoryPart::ToolCall { name, .. } = part {
                if !request.tools.iter().any(|tool| tool.name == *name) {
                    return Err(ModelError::protocol("undeclared native function call"));
                }
            }
        }
        parts.push(HistoryPart::ProviderState {
            state: ProviderState {
                provider: self.identity.clone(),
                model: Some(request.model),
                format: FORMAT.into(),
                payload: json!({"output":output}),
                required_for_replay: true,
            },
        });
        emit(&events, StreamEvent::Usage(usage), &cancel).await?;
        Ok(HistoryTurn {
            message: Message::Rich {
                rich_role: RichRole::Assistant,
                parts,
            },
            usage,
            stop,
            provider_metadata: metadata,
        })
    }
}

#[derive(Default)]
struct StreamState {
    items: std::collections::BTreeMap<u32, StreamItem>,
    response_id: Option<String>,
    created: bool,
    in_progress: bool,
    had_output: bool,
}
#[derive(Default)]
struct StreamItem {
    id: String,
    added: Option<Value>,
    done: Option<Value>,
    arguments: Option<String>,
    arguments_done: Option<String>,
    text: std::collections::BTreeMap<u32, StreamText>,
}
#[derive(Default)]
struct StreamText {
    delta: Option<String>,
    done: Option<String>,
    part_added: Option<Value>,
    part_done: Option<Value>,
}
fn event_index(event: &Value, key: &str) -> Result<u32, ModelError> {
    event
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| ModelError::protocol("invalid native event index"))
}
impl StreamState {
    fn item(&mut self, index: u32, id: &str) -> Result<&mut StreamItem, ModelError> {
        self.had_output = true;
        if self
            .items
            .iter()
            .any(|(other, item)| *other != index && item.id == id)
        {
            return Err(ModelError::protocol("duplicate native item id"));
        }
        let item = self.items.entry(index).or_default();
        if item.id.is_empty() {
            item.id = id.to_owned();
        }
        if item.id != id {
            return Err(ModelError::protocol("native item id mismatch"));
        }
        Ok(item)
    }
    fn reconcile(&self, output: &[Value]) -> Result<(), ModelError> {
        for (index, item) in &self.items {
            let final_item = output
                .get(*index as usize)
                .ok_or_else(|| ModelError::protocol("unknown streamed item"))?;
            if final_item.get("id").and_then(Value::as_str) != Some(&item.id) {
                return Err(ModelError::protocol("native item id mismatch"));
            }
            if let Some(added) = &item.added {
                for field in ["type", "id", "call_id", "name", "role"] {
                    if let Some(value) = added.get(field) {
                        if final_item.get(field) != Some(value) {
                            return Err(ModelError::protocol("native item changed"));
                        }
                    }
                }
            }
            if item.done.as_ref().is_some_and(|v| v != final_item) {
                return Err(ModelError::protocol("native item done mismatch"));
            }
            for args in [&item.arguments, &item.arguments_done]
                .into_iter()
                .flatten()
            {
                if final_item.get("type").and_then(Value::as_str) != Some("function_call")
                    || final_item.get("arguments").and_then(Value::as_str) != Some(args)
                {
                    return Err(ModelError::protocol("native arguments mismatch"));
                }
            }
            for (content_index, text) in &item.text {
                let part = final_item
                    .get("content")
                    .and_then(Value::as_array)
                    .and_then(|v| v.get(*content_index as usize))
                    .ok_or_else(|| ModelError::protocol("unknown streamed content"))?;
                if final_item.get("type").and_then(Value::as_str) != Some("message")
                    || part.get("type").and_then(Value::as_str) != Some("output_text")
                {
                    return Err(ModelError::protocol("invalid streamed content"));
                }
                for value in [&text.delta, &text.done].into_iter().flatten() {
                    if part.get("text").and_then(Value::as_str) != Some(value) {
                        return Err(ModelError::protocol("native text mismatch"));
                    }
                }
                if text.part_done.as_ref().is_some_and(|v| v != part) {
                    return Err(ModelError::protocol("native content done mismatch"));
                }
                if let Some(added) = &text.part_added {
                    if added.get("type") != part.get("type") {
                        return Err(ModelError::protocol("native content changed"));
                    }
                }
            }
        }
        Ok(())
    }
}

async fn process_event(
    payload: &str,
    response: &crate::native_transport::NativeResponse,
    events: &Sender<StreamEvent>,
    cancel: &CancellationToken,
    stream: &mut StreamState,
    streamed_content: &mut String,
    completed: &mut Option<Value>,
) -> Result<(), ModelError> {
    if payload == "[DONE]" {
        return Ok(());
    }
    let event: Value = serde_json::from_str(payload)
        .map_err(|_| ModelError::protocol("malformed native event"))?;
    response.check_value(&event)?;
    if completed.is_some() {
        return Err(ModelError::protocol("event after completed response"));
    }
    let kind = string(&event, "type")?;
    match kind {
        "response.output_text.delta" => {
            let index = event_index(&event, "output_index")?;
            let content_index = event_index(&event, "content_index")?;
            let id = string(&event, "item_id")?;
            let delta = event
                .get("delta")
                .and_then(Value::as_str)
                .ok_or_else(|| ModelError::protocol("malformed text delta"))?;
            let item = stream.item(index, id)?;
            if item.done.is_some() {
                return Err(ModelError::protocol("delta after item done"));
            }
            let text = item.text.entry(content_index).or_default();
            if text.done.is_some() || text.part_done.is_some() {
                return Err(ModelError::protocol("delta after text done"));
            }
            text.delta.get_or_insert_with(String::new).push_str(delta);
            response.check_delta(streamed_content, delta)?;
            emit(events, StreamEvent::TextDelta(delta.to_owned()), cancel).await?;
        }
        "response.output_item.added" => {
            let index = event_index(&event, "output_index")?;
            let added = event
                .get("item")
                .ok_or_else(|| ModelError::protocol("missing output item"))?;
            let id = string(added, "id")?;
            let item = stream.item(index, id)?;
            if item.added.is_some()
                || item.done.is_some()
                || !item.text.is_empty()
                || item.arguments.is_some()
                || item.arguments_done.is_some()
            {
                return Err(ModelError::protocol("duplicate or late output item"));
            }
            match string(added, "type")? {
                "function_call" => {
                    let call_id = string(added, "call_id")?.to_owned();
                    let name = string(added, "name")?.to_owned();
                    item.added = Some(added.clone());
                    emit(
                        events,
                        StreamEvent::ToolCallStart {
                            index,
                            id: call_id,
                            name,
                        },
                        cancel,
                    )
                    .await?;
                }
                "message" | "reasoning" => item.added = Some(added.clone()),
                _ => return Err(ModelError::unsupported("unsupported native output item")),
            }
        }
        "response.function_call_arguments.delta" => {
            let index = event_index(&event, "output_index")?;
            let id = string(&event, "item_id")?;
            let delta = event
                .get("delta")
                .and_then(Value::as_str)
                .ok_or_else(|| ModelError::protocol("malformed tool delta"))?;
            let item = stream.item(index, id)?;
            if item
                .added
                .as_ref()
                .and_then(|v| v.get("type"))
                .and_then(Value::as_str)
                != Some("function_call")
                || item.done.is_some()
                || item.arguments_done.is_some()
            {
                return Err(ModelError::protocol("invalid tool delta order"));
            }
            item.arguments
                .get_or_insert_with(String::new)
                .push_str(delta);
            response.check_delta(streamed_content, delta)?;
            emit(
                events,
                StreamEvent::ToolCallArgumentsDelta {
                    index,
                    delta: delta.to_owned(),
                },
                cancel,
            )
            .await?;
        }
        "response.output_item.done" => {
            let index = event_index(&event, "output_index")?;
            let done = event
                .get("item")
                .ok_or_else(|| ModelError::protocol("missing output item"))?;
            let item = stream.item(index, string(done, "id")?)?;
            if item.done.replace(done.clone()).is_some() {
                return Err(ModelError::protocol("duplicate item done"));
            }
        }
        "response.output_text.done"
        | "response.content_part.added"
        | "response.content_part.done" => {
            let index = event_index(&event, "output_index")?;
            let content_index = event_index(&event, "content_index")?;
            let item = stream.item(index, string(&event, "item_id")?)?;
            if item.done.is_some() {
                return Err(ModelError::protocol("content after item done"));
            }
            let text = item.text.entry(content_index).or_default();
            match kind {
                "response.output_text.done" => {
                    let done = event
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ModelError::protocol("malformed text done"))?;
                    if text.done.replace(done.to_owned()).is_some() || text.part_done.is_some() {
                        return Err(ModelError::protocol("invalid text done order"));
                    }
                }
                "response.content_part.added" => {
                    let part = event
                        .get("part")
                        .ok_or_else(|| ModelError::protocol("missing content part"))?;
                    if text.part_added.replace(part.clone()).is_some()
                        || text.delta.is_some()
                        || text.done.is_some()
                        || text.part_done.is_some()
                    {
                        return Err(ModelError::protocol("invalid content added order"));
                    }
                }
                _ => {
                    let part = event
                        .get("part")
                        .ok_or_else(|| ModelError::protocol("missing content part"))?;
                    if text.part_done.replace(part.clone()).is_some() {
                        return Err(ModelError::protocol("duplicate content done"));
                    }
                }
            }
        }
        "response.function_call_arguments.done" => {
            let index = event_index(&event, "output_index")?;
            let item = stream.item(index, string(&event, "item_id")?)?;
            if item.done.is_some()
                || item
                    .added
                    .as_ref()
                    .and_then(|v| v.get("type"))
                    .and_then(Value::as_str)
                    != Some("function_call")
            {
                return Err(ModelError::protocol("invalid tool done order"));
            }
            let args = event
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or_else(|| ModelError::protocol("malformed tool done"))?;
            if item.arguments_done.replace(args.to_owned()).is_some() {
                return Err(ModelError::protocol("duplicate tool done"));
            }
        }
        "response.created" | "response.in_progress" => {
            if stream.had_output
                || (kind == "response.created" && (stream.created || stream.in_progress))
                || (kind == "response.in_progress" && stream.in_progress)
            {
                return Err(ModelError::protocol("invalid response lifecycle"));
            }
            let response = event
                .get("response")
                .ok_or_else(|| ModelError::protocol("missing native response"))?;
            let id = string(response, "id")?;
            if stream
                .response_id
                .as_deref()
                .is_some_and(|previous| previous != id)
            {
                return Err(ModelError::protocol("native response id mismatch"));
            }
            stream.response_id = Some(id.to_owned());
            if kind == "response.created" {
                stream.created = true;
            } else {
                stream.in_progress = true;
            }
        }
        "response.completed" => {
            let final_response = event
                .get("response")
                .ok_or_else(|| ModelError::protocol("missing completed response"))?;
            if stream
                .response_id
                .as_deref()
                .is_some_and(|id| final_response.get("id").and_then(Value::as_str) != Some(id))
            {
                return Err(ModelError::protocol("native response id mismatch"));
            }
            if final_response.get("status").and_then(Value::as_str) != Some("completed") {
                return Err(ModelError::protocol("invalid completed response"));
            }
            *completed = Some(final_response.clone());
        }
        "response.failed" | "response.incomplete" | "error" => {
            return Err(ModelError::protocol("native response failed or incomplete"))
        }
        "response.reasoning_summary_part.added"
        | "response.reasoning_summary_part.done"
        | "response.reasoning_summary_text.delta"
        | "response.reasoning_summary_text.done" => {}
        _ => return Err(ModelError::unsupported("unsupported native stream event")),
    }
    Ok(())
}
