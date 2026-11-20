//! Anthropic Messages streaming and provider-bound replay. Transcript state is sensitive.
use crate::{
    native_transport::{NativeAuth, NativeResponse, NativeTransport, StaticCredentialResolver},
    SseParser,
};
use rusty_bot_core::{
    AssistantTurn, ChatRequest, ContentPart, CredentialResolver, HistoryPart, HistoryRequest,
    HistoryTurn, Message, ModelDiscovery, ModelError, ModelErrorKind, ModelProvider,
    ProviderCredential, ProviderState, RichRole, StopReason, StreamEvent, ToolChoice, Usage,
};
use serde_json::{json, Value};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "anthropic_replay_tests.rs"]
mod replay_tests;

#[cfg(test)]
#[path = "anthropic_tests.rs"]
mod tests;

const FORMAT: &str = "anthropic.messages.v1";

pub struct AnthropicMessagesProvider {
    identity: String,
    transport: NativeTransport,
    image_limits: Option<rusty_bot_core::ImageLimits>,
}
impl std::fmt::Debug for AnthropicMessagesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AnthropicMessagesProvider([REDACTED])")
    }
}
impl AnthropicMessagesProvider {
    pub fn new(
        identity: impl Into<String>,
        endpoint: &str,
        resolver: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ModelError> {
        let identity = identity.into();
        if identity.is_empty()
            || identity.len() > 128
            || !identity
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
        {
            return Err(invalid("invalid provider identity"));
        }
        Ok(Self {
            identity,
            transport: NativeTransport::with_auth(endpoint, resolver, NativeAuth::Anthropic)?,
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
    pub fn with_image_limits(mut self, limits: rusty_bot_core::ImageLimits) -> Self {
        self.image_limits = Some(limits);
        self
    }
    pub fn discover_models(&self) -> ModelDiscovery {
        ModelDiscovery::Unsupported
    }
    fn body(&self, request: &HistoryRequest) -> Result<Value, ModelError> {
        crate::native_images::validate_history_images(
            &request.messages,
            self.image_limits.as_ref(),
        )?;
        if request.model.trim().is_empty()
            || request.max_tokens == Some(0)
            || request
                .temperature
                .is_some_and(|t| !t.is_finite() || !(0.0..=1.0).contains(&t))
        {
            return Err(invalid("invalid model options"));
        }
        if let Some(options) = request.metadata.get("model_options") {
            let options = options
                .as_object()
                .ok_or_else(|| invalid("invalid model options"))?;
            if options.keys().any(|k| k != "thinking_level") {
                return Err(ModelError::unsupported("unsupported model option"));
            }
            if options
                .get("thinking_level")
                .is_some_and(|v| v.as_str() != Some("off"))
            {
                return Err(ModelError::unsupported("thinking level is unsupported"));
            }
        }
        let mut messages = Vec::new();
        let mut system = None;
        for message in &request.messages {
            match message {
                Message::System { content, .. } => {
                    if system.is_some() || !messages.is_empty() {
                        return Err(invalid("system must lead history"));
                    }
                    system = Some(content.clone());
                }
                Message::User { content } => {
                    messages.push(json!({"role":"user","content":content_blocks(content)}))
                }
                Message::ToolResult {
                    tool_call_id,
                    content,
                    is_error,
                    ..
                } => {
                    nonempty(tool_call_id)?;
                    messages.push(json!({"role":"user","content":[tool_result(tool_call_id, content, *is_error)]}));
                }
                Message::Assistant {
                    content,
                    tool_calls,
                    ..
                } => {
                    let mut blocks = Vec::new();
                    for part in content {
                        match part {
                            ContentPart::Text { text } => {
                                blocks.push(json!({"type":"text","text":text}))
                            }
                            _ => {
                                return Err(ModelError::unsupported("assistant image unsupported"))
                            }
                        }
                    }
                    let mut call_ids = HashSet::new();
                    for call in tool_calls {
                        if !call_ids.insert(&call.id) {
                            return Err(invalid("duplicate tool id"));
                        }
                        nonempty(&call.id)?;
                        nonempty(&call.name)?;
                        if !call.arguments.is_object() {
                            return Err(invalid("invalid tool arguments"));
                        }
                        blocks.push(json!({"type":"tool_use","id":call.id,"name":call.name,"input":call.arguments}));
                    }
                    messages.push(json!({"role":"assistant","content":blocks}));
                }
                Message::Rich {
                    rich_role: RichRole::Assistant,
                    parts,
                } => {
                    let Some(HistoryPart::ProviderState { state }) = parts.last() else {
                        return Err(invalid("missing native replay state"));
                    };
                    if state.provider != self.identity
                        || state.model.as_deref() != Some(&request.model)
                        || state.format != FORMAT
                        || !state.required_for_replay
                    {
                        return Err(invalid("invalid native replay state"));
                    }
                    let blocks = state
                        .payload
                        .get("content")
                        .and_then(Value::as_array)
                        .ok_or_else(|| invalid("invalid native replay state"))?;
                    let args = state
                        .payload
                        .get("arguments")
                        .and_then(Value::as_array)
                        .ok_or_else(|| invalid("invalid native replay state"))?;
                    if parts_from_blocks(blocks, args)? != parts[..parts.len() - 1] {
                        return Err(invalid("native state does not match transcript"));
                    }
                    messages.push(json!({"role":"assistant","content":blocks}));
                }
                Message::Rich {
                    rich_role: RichRole::User,
                    parts,
                } => {
                    let mut blocks = Vec::new();
                    for part in parts {
                        match part {
                            HistoryPart::Text { text } => {
                                blocks.push(json!({"type":"text","text":text}))
                            }
                            HistoryPart::Image {
                                mime_type,
                                data_base64,
                            } => blocks.push(image(mime_type, data_base64)),
                            _ => return Err(ModelError::unsupported("unsupported user history")),
                        }
                    }
                    messages.push(json!({"role":"user","content":blocks}));
                }
                Message::Rich {
                    rich_role: RichRole::Tool,
                    parts,
                } => {
                    let mut blocks = Vec::new();
                    for part in parts {
                        match part {
                            HistoryPart::ToolResult {
                                call_id,
                                content,
                                is_error,
                                ..
                            } => {
                                nonempty(call_id)?;
                                blocks.push(tool_result(call_id, content, *is_error));
                            }
                            _ => return Err(ModelError::unsupported("unsupported tool history")),
                        }
                    }
                    messages.push(json!({"role":"user","content":blocks}));
                }
            }
        }
        let mut seen = HashSet::new();
        for tool in &request.tools {
            nonempty(&tool.name)?;
            if !seen.insert(&tool.name) || !tool.parameters.is_object() {
                return Err(invalid("invalid tools"));
            }
        }
        let choice = match &request.tool_choice {
            ToolChoice::Auto => json!({"type":"auto"}),
            ToolChoice::None => json!({"type":"none"}),
            ToolChoice::Required => json!({"type":"any"}),
            ToolChoice::Named(name) => {
                if !seen.contains(name) {
                    return Err(invalid("unknown tool choice"));
                }
                json!({"type":"tool","name":name})
            }
        };
        let tools: Vec<_> = request
            .tools
            .iter()
            .map(|t| json!({"name":t.name,"description":t.description,"input_schema":t.parameters}))
            .collect();
        let mut body = json!({"model":request.model,"max_tokens":request.max_tokens.unwrap_or(4096),"stream":true,"messages":messages,"tools":tools,"tool_choice":choice});
        if let Some(system) = system {
            body["system"] = json!(system);
        }
        if let Some(t) = request.temperature {
            body["temperature"] = json!(t);
        }
        Ok(body)
    }
}
fn invalid(s: &'static str) -> ModelError {
    ModelError::new(ModelErrorKind::InvalidRequest, s, false)
}
fn nonempty(s: &str) -> Result<(), ModelError> {
    if s.trim().is_empty() {
        Err(invalid("empty identifier"))
    } else {
        Ok(())
    }
}
fn field<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ModelError::protocol("malformed native content"))
}
fn required<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    let s = field(v, key)?;
    if s.trim().is_empty() {
        return Err(ModelError::protocol("malformed native identifier"));
    }
    Ok(s)
}
fn image(mime: &str, data: &str) -> Value {
    json!({"type":"image","source":{"type":"base64","media_type":mime,"data":data}})
}
fn content_blocks(parts: &[ContentPart]) -> Vec<Value> {
    parts
        .iter()
        .map(|p| match p {
            ContentPart::Text { text } => json!({"type":"text","text":text}),
            ContentPart::Image {
                mime_type,
                data_base64,
            } => image(mime_type, data_base64),
        })
        .collect()
}
fn tool_result(id: &str, content: &[ContentPart], error: bool) -> Value {
    json!({"type":"tool_result","tool_use_id":id,"content":content_blocks(content),"is_error":error})
}
fn parts_from_blocks(blocks: &[Value], args: &[Value]) -> Result<Vec<HistoryPart>, ModelError> {
    if blocks.len() != args.len() {
        return Err(ModelError::protocol("invalid native replay state"));
    }
    let mut parts = Vec::new();
    let mut ids = HashSet::new();
    for (block, arg) in blocks.iter().zip(args) {
        let kind = required(block, "type")?;
        if kind != "tool_use" && !arg.is_null() {
            return Err(ModelError::protocol("invalid native replay arguments"));
        }
        parts.push(match kind {
            "text" => HistoryPart::Text {
                text: field(block, "text")?.into(),
            },
            "thinking" => {
                required(block, "signature")?;
                HistoryPart::Reasoning {
                    text: Some(field(block, "thinking")?.into()),
                    state: None,
                }
            }
            "redacted_thinking" => {
                required(block, "data")?;
                HistoryPart::Reasoning {
                    text: None,
                    state: None,
                }
            }
            "tool_use" => {
                let id = required(block, "id")?;
                let name = required(block, "name")?;
                let raw = arg
                    .as_str()
                    .ok_or_else(|| ModelError::protocol("missing tool arguments"))?;
                let parsed: Value = serde_json::from_str(raw)
                    .map_err(|_| ModelError::protocol("invalid tool arguments"))?;
                if !ids.insert(id.to_owned())
                    || !parsed.is_object()
                    || block.get("input") != Some(&parsed)
                {
                    return Err(ModelError::protocol("invalid tool arguments"));
                }
                HistoryPart::ToolCall {
                    id: id.into(),
                    name: name.into(),
                    arguments_json: raw.into(),
                }
            }
            _ => return Err(ModelError::unsupported("unsupported native content block")),
        });
    }
    Ok(parts)
}
async fn emit(
    tx: &Sender<StreamEvent>,
    event: StreamEvent,
    cancel: &CancellationToken,
) -> Result<(), ModelError> {
    tokio::select! { biased; _ = cancel.cancelled() => Err(ModelError::cancelled()), result = tx.send(event) => { let _ = result; Ok(()) } }
}

#[cfg(test)]
#[path = "anthropic_stream_tests.rs"]
mod stream_tests;

struct Accumulator {
    started: bool,
    stopped: bool,
    blocks: Vec<Value>,
    args: Vec<Value>,
    open: bool,
    raw: String,
    usage: Usage,
    stop: Option<StopReason>,
    suffix: String,
    signature_seen: bool,
    output_usage_seen: bool,
}
impl Accumulator {
    fn new() -> Self {
        Self {
            started: false,
            stopped: false,
            blocks: vec![],
            args: vec![],
            open: false,
            raw: String::new(),
            usage: Usage::default(),
            stop: None,
            suffix: String::new(),
            signature_seen: false,
            output_usage_seen: false,
        }
    }
    async fn event(
        &mut self,
        payload: &str,
        response: &NativeResponse,
        tx: &Sender<StreamEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), ModelError> {
        let v: Value = serde_json::from_str(payload)
            .map_err(|_| ModelError::protocol("malformed native event"))?;
        response.check_value(&v)?;
        let kind = required(&v, "type")?;
        if self.stopped {
            return Err(ModelError::protocol("event after message stop"));
        }
        match kind {
            "ping" => {}
            "message_start" => {
                if self.started {
                    return Err(ModelError::protocol("duplicate message start"));
                }
                let m = &v["message"];
                required(m, "id")?;
                if field(m, "type")? != "message"
                    || field(m, "role")? != "assistant"
                    || m.get("content")
                        .and_then(Value::as_array)
                        .is_none_or(|a| !a.is_empty())
                {
                    return Err(ModelError::protocol("invalid message start"));
                }
                self.usage.input_tokens = m
                    .pointer("/usage/input_tokens")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| ModelError::protocol("missing usage"))?;
                self.usage.cache_read_tokens = m
                    .pointer("/usage/cache_read_input_tokens")
                    .and_then(Value::as_u64);
                self.usage.cache_write_tokens = m
                    .pointer("/usage/cache_creation_input_tokens")
                    .and_then(Value::as_u64);
                self.started = true;
            }
            "content_block_start" => {
                if !self.started
                    || self.open
                    || self.stop.is_some()
                    || v.get("index").and_then(Value::as_u64) != Some(self.blocks.len() as u64)
                {
                    return Err(ModelError::protocol("invalid block start"));
                }
                let b = v
                    .get("content_block")
                    .ok_or_else(|| ModelError::protocol("missing block"))?
                    .clone();
                match required(&b, "type")? {
                    "text" => {
                        if !field(&b, "text")?.is_empty() {
                            return Err(ModelError::protocol("invalid initial text"));
                        }
                    }
                    "thinking" => {
                        if !field(&b, "thinking")?.is_empty() || !field(&b, "signature")?.is_empty()
                        {
                            return Err(ModelError::protocol("invalid initial thinking"));
                        }
                    }
                    "redacted_thinking" => {
                        required(&b, "data")?;
                    }
                    "tool_use" => {
                        let id = required(&b, "id")?;
                        let name = required(&b, "name")?;
                        if b.get("input") != Some(&json!({})) {
                            return Err(ModelError::protocol("invalid initial tool input"));
                        }
                        emit(
                            tx,
                            StreamEvent::ToolCallStart {
                                index: self.blocks.len() as u32,
                                id: id.into(),
                                name: name.into(),
                            },
                            cancel,
                        )
                        .await?;
                    }
                    _ => return Err(ModelError::unsupported("unsupported native content block")),
                }
                self.blocks.push(b);
                self.args.push(Value::Null);
                self.raw.clear();
                self.signature_seen = false;
                self.open = true;
            }
            "content_block_delta" => {
                if !self.open
                    || v.get("index").and_then(Value::as_u64)
                        != Some((self.blocks.len() - 1) as u64)
                {
                    return Err(ModelError::protocol("invalid block delta"));
                }
                let d = &v["delta"];
                let b = self.blocks.last_mut().unwrap();
                let typ = required(b, "type")?.to_owned();
                match (typ.as_str(), required(d, "type")?) {
                    ("text", "text_delta") => {
                        let s = field(d, "text")?;
                        response.check_delta(&mut self.suffix, s)?;
                        b["text"] = json!(format!("{}{s}", field(b, "text")?));
                        emit(tx, StreamEvent::TextDelta(s.into()), cancel).await?;
                    }
                    ("thinking", "thinking_delta") => {
                        if self.signature_seen {
                            return Err(ModelError::protocol("thinking after signature"));
                        }
                        let s = field(d, "thinking")?;
                        response.check_delta(&mut self.suffix, s)?;
                        b["thinking"] = json!(format!("{}{s}", field(b, "thinking")?));
                        emit(tx, StreamEvent::ThinkingDelta(s.into()), cancel).await?;
                    }
                    ("thinking", "signature_delta") => {
                        self.signature_seen = true;
                        let s = field(d, "signature")?;
                        b["signature"] = json!(format!("{}{s}", field(b, "signature")?));
                    }
                    ("tool_use", "input_json_delta") => {
                        let s = field(d, "partial_json")?;
                        response.check_delta(&mut self.suffix, s)?;
                        self.raw.push_str(s);
                        emit(
                            tx,
                            StreamEvent::ToolCallArgumentsDelta {
                                index: (self.blocks.len() - 1) as u32,
                                delta: s.into(),
                            },
                            cancel,
                        )
                        .await?;
                    }
                    _ => return Err(ModelError::unsupported("unsupported native content delta")),
                }
            }
            "content_block_stop" => {
                if !self.open
                    || v.get("index").and_then(Value::as_u64)
                        != Some((self.blocks.len() - 1) as u64)
                {
                    return Err(ModelError::protocol("invalid block stop"));
                }
                let b = self.blocks.last_mut().unwrap();
                if field(b, "type")? == "tool_use" {
                    let raw = if self.raw.is_empty() { "{}" } else { &self.raw };
                    let input: Value = serde_json::from_str(raw)
                        .map_err(|_| ModelError::protocol("invalid tool arguments"))?;
                    if !input.is_object() {
                        return Err(ModelError::protocol("invalid tool arguments"));
                    }
                    b["input"] = input;
                    *self.args.last_mut().unwrap() = json!(raw);
                }
                if field(b, "type")? == "thinking" {
                    required(b, "signature")?;
                }
                self.open = false;
            }
            "message_delta" => {
                if !self.started || self.open {
                    return Err(ModelError::protocol("invalid message delta"));
                }
                let delta = v
                    .get("delta")
                    .and_then(Value::as_object)
                    .ok_or_else(|| ModelError::protocol("invalid message delta"))?;
                if delta
                    .keys()
                    .any(|k| k != "stop_reason" && k != "stop_sequence")
                {
                    return Err(ModelError::unsupported("unsupported message delta"));
                }
                if let Some(reason) = v.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    let stop = match reason {
                        "end_turn" | "stop_sequence" => StopReason::Stop,
                        "max_tokens" => StopReason::Length,
                        "tool_use" => StopReason::ToolUse,
                        _ => return Err(ModelError::unsupported("unsupported stop reason")),
                    };
                    if self.stop.is_some_and(|old| old != stop) {
                        return Err(ModelError::protocol("conflicting stop reason"));
                    }
                    self.stop = Some(stop);
                }
                if let Some(n) = v.pointer("/usage/output_tokens").and_then(Value::as_u64) {
                    self.usage.output_tokens = n;
                    self.output_usage_seen = true;
                }
            }
            "message_stop" => {
                if !self.started || self.open || self.stop.is_none() || !self.output_usage_seen {
                    return Err(ModelError::protocol("invalid message stop"));
                }
                self.stopped = true;
            }
            "error" => return Err(ModelError::protocol("native stream failed")),
            _ => return Err(ModelError::unsupported("unsupported native stream event")),
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl ModelProvider for AnthropicMessagesProvider {
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
        _request: ChatRequest,
        _tx: Sender<StreamEvent>,
        _cancel: CancellationToken,
    ) -> Result<AssistantTurn, ModelError> {
        // Native replay state cannot be represented by a legacy turn. Reject
        // before resolving credentials, dispatching, or emitting events.
        Err(ModelError::unsupported(
            "native response cannot be represented by legacy turn",
        ))
    }
    async fn stream_history(
        &self,
        request: HistoryRequest,
        tx: Sender<StreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryTurn, ModelError> {
        self.stream_history_with_payload_hook(request, tx, cancel, None)
            .await
    }

    async fn stream_history_with_payload_hook(
        &self,
        request: HistoryRequest,
        events: Sender<StreamEvent>,
        cancel: CancellationToken,
        payload_hook: Option<Arc<dyn rusty_bot_core::ProviderPayloadHook>>,
    ) -> Result<HistoryTurn, ModelError> {
        let tx = events;
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
        response.check_value(&json!({"provider":self.identity,"model":request.model}))?;
        let mut metadata = json!({"http_status":response.status()});
        if let Some(id) = response.provider_request_id() {
            metadata["provider_request_id"] = json!(id);
        }
        emit(
            &tx,
            StreamEvent::Start {
                provider_request_id: response.provider_request_id().map(str::to_owned),
            },
            &cancel,
        )
        .await?;
        let mut parser = SseParser::new();
        let mut acc = Accumulator::new();
        while let Some(chunk) = response.next_chunk().await? {
            for event in parser.push(&chunk) {
                acc.event(&event, &response, &tx, &cancel).await?;
            }
        }
        if let Some(event) = parser.finish() {
            acc.event(&event, &response, &tx, &cancel).await?;
        }
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        if !acc.stopped {
            return Err(ModelError::protocol("missing message stop"));
        }
        let mut parts = parts_from_blocks(&acc.blocks, &acc.args)?;
        let has_tools = parts
            .iter()
            .any(|p| matches!(p, HistoryPart::ToolCall { .. }));
        let stop = acc
            .stop
            .ok_or_else(|| ModelError::protocol("missing stop reason"))?;
        if (stop == StopReason::ToolUse) != has_tools {
            return Err(ModelError::protocol("inconsistent stop reason"));
        }
        for part in &parts {
            if let HistoryPart::ToolCall { name, .. } = part {
                if !request.tools.iter().any(|t| t.name == *name) {
                    return Err(ModelError::protocol("undeclared native function call"));
                }
            }
        }
        response.check_value(&json!({"content":acc.blocks,"arguments":acc.args}))?;
        parts.push(HistoryPart::ProviderState {
            state: ProviderState {
                provider: self.identity.clone(),
                model: Some(request.model),
                format: FORMAT.into(),
                payload: json!({"content":acc.blocks,"arguments":acc.args}),
                required_for_replay: true,
            },
        });
        emit(&tx, StreamEvent::Usage(acc.usage), &cancel).await?;
        Ok(HistoryTurn {
            message: Message::Rich {
                rich_role: RichRole::Assistant,
                parts,
            },
            usage: acc.usage,
            stop,
            provider_metadata: metadata,
        })
    }
}
