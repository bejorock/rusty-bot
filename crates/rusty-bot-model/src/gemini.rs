//! Gemini generateContent streaming adapter. The endpoint is the complete
//! `.../models/{model}:streamGenerateContent` URL chosen by the caller. It is
//! not rewritten; the decoded endpoint model must equal the request model.
//! All model names, including aliases, require the first function-call signature.
//! The transport adds only `alt=sse`, and authenticates with x-goog-api-key.
//! Native transcript state is sensitive and bound to the supplied identity/model.
use crate::{
    native_transport::{NativeAuth, NativeTransport, StaticCredentialResolver},
    SseParser,
};
use rusty_bot_core::{
    AssistantTurn, ChatRequest, ContentPart, CredentialResolver, HistoryPart, HistoryRequest,
    HistoryTurn, ImageLimits, Message, ModelDiscovery, ModelError, ModelErrorKind, ModelProvider,
    ProviderCredential, ProviderState, RichRole, StopReason, StreamEvent, ToolChoice, Usage,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

const FORMAT: &str = "gemini.generate_content.v1";
fn invalid() -> ModelError {
    ModelError::new(
        ModelErrorKind::InvalidRequest,
        "invalid Gemini request or replay state",
        false,
    )
}
fn protocol() -> ModelError {
    ModelError::protocol("malformed Gemini response")
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(protocol)
}

#[cfg(test)]
#[path = "gemini_replay_tests.rs"]
mod replay_tests;

#[cfg(test)]
#[path = "gemini_tests.rs"]
mod tests;

pub struct GeminiGenerateContentProvider {
    identity: String,
    transport: NativeTransport,
    endpoint_model: String,
    image_limits: Option<ImageLimits>,
}
impl std::fmt::Debug for GeminiGenerateContentProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GeminiGenerateContentProvider([REDACTED])")
    }
}
impl GeminiGenerateContentProvider {
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
            return Err(invalid());
        }
        let transport = NativeTransport::with_auth(endpoint, resolver, NativeAuth::Gemini)?;
        let url = url::Url::parse(endpoint).map_err(|_| invalid())?;
        let segments: Vec<_> = url.path_segments().ok_or_else(invalid)?.collect();
        let [.., "models", selected] = segments.as_slice() else {
            return Err(invalid());
        };
        let encoded = selected
            .strip_suffix(":streamGenerateContent")
            .ok_or_else(invalid)?;
        // Invalid escapes must not be normalized or interpreted as a different model.
        let bytes = encoded.as_bytes();
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && (i + 2 >= bytes.len()
                    || !bytes[i + 1].is_ascii_hexdigit()
                    || !bytes[i + 2].is_ascii_hexdigit())
            {
                return Err(invalid());
            }
        }
        let endpoint_model = percent_encoding::percent_decode_str(encoded)
            .decode_utf8()
            .map_err(|_| invalid())?
            .into_owned();
        if endpoint_model.is_empty()
            || !endpoint_model
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        {
            return Err(invalid());
        }
        Ok(Self {
            identity,
            transport,
            endpoint_model,
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
    pub fn with_image_limits(mut self, limits: ImageLimits) -> Self {
        self.image_limits = Some(limits);
        self
    }
    pub fn discover_models(&self) -> ModelDiscovery {
        ModelDiscovery::Unsupported
    }
    fn image(&self, mime: &str, data: &str) -> Result<Value, ModelError> {
        if !matches!(mime, "image/png" | "image/jpeg" | "image/webp") {
            return Err(invalid());
        }
        let limits = self
            .image_limits
            .as_ref()
            .ok_or_else(|| ModelError::unsupported("image capability not configured"))?;
        if limits.max_bytes == 0
            || limits.max_width == 0
            || limits.max_height == 0
            || limits.max_bytes > rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES
        {
            return Err(invalid());
        }
        rusty_bot_core::image_validation::validate_images(
            &[ContentPart::Image {
                mime_type: mime.into(),
                data_base64: data.into(),
            }],
            limits,
        )
        .map_err(|_| invalid())?;
        Ok(json!({"mimeType":mime,"data":data}))
    }
    fn content(&self, parts: &[ContentPart]) -> Result<Vec<Value>, ModelError> {
        parts
            .iter()
            .map(|p| match p {
                ContentPart::Text { text } => Ok(json!({"text":text})),
                ContentPart::Image {
                    mime_type,
                    data_base64,
                } => Ok(json!({"inlineData":self.image(mime_type,data_base64)?})),
            })
            .collect()
    }
    #[allow(clippy::too_many_arguments)]
    fn result(
        &self,
        id: &str,
        name: &str,
        content: &[ContentPart],
        is_error: bool,
        details: &Option<Value>,
        pending: &mut HashMap<String, (String, bool)>,
        order: &mut Vec<String>,
    ) -> Result<Value, ModelError> {
        let (expected, native_id) = pending.remove(id).ok_or_else(invalid)?;
        if name != expected && !name.is_empty() {
            return Err(invalid());
        }
        if !native_id && order.first().map(String::as_str) != Some(id) {
            return Err(invalid());
        }
        order.retain(|v| v != id);
        let mut result_parts = Vec::new();
        let mut payload = Vec::new();
        for p in content {
            match p {
                ContentPart::Text { text } => payload.push(json!({"text":text})),
                ContentPart::Image {
                    mime_type,
                    data_base64,
                } => {
                    let mut data = self.image(mime_type, data_base64)?;
                    let label = format!("image_{}", result_parts.len());
                    data["displayName"] = json!(label);
                    payload.push(json!({"$ref":label}));
                    result_parts.push(json!({"inlineData":data}));
                }
            }
        }
        let mut response = json!({"content":payload});
        if let Some(details) = details {
            response["details"] = details.clone();
        }
        let mut call = json!({"name":expected,"response":if is_error { json!({"error":response}) } else { json!({"result":response}) }});
        if native_id {
            call["id"] = json!(id);
        }
        if !result_parts.is_empty() {
            call["parts"] = json!(result_parts);
        }
        Ok(json!({"functionResponse":call}))
    }
    fn body(&self, request: &HistoryRequest) -> Result<Value, ModelError> {
        crate::native_images::validate_history_images(
            &request.messages,
            self.image_limits.as_ref(),
        )?;
        if request.model != self.endpoint_model
            || request.max_tokens == Some(0)
            || request
                .temperature
                .is_some_and(|x| !x.is_finite() || !(0.0..=2.0).contains(&x))
        {
            return Err(invalid());
        }
        if let Some(options) = request.metadata.get("model_options") {
            let o = options.as_object().ok_or_else(invalid)?;
            if !o.is_empty() {
                return Err(ModelError::unsupported("unsupported Gemini model option"));
            }
        }
        let mut contents: Vec<Value> = Vec::new();
        let mut system = None;
        let mut pending = HashMap::new();
        let mut order = Vec::new();
        for (mi, message) in request.messages.iter().enumerate() {
            let (role, parts) = match message {
                Message::System { content, .. } => {
                    if system.is_some() || !contents.is_empty() {
                        return Err(invalid());
                    }
                    system = Some(content.clone());
                    continue;
                }
                Message::User { content } => ("user", self.content(content)?),
                Message::ToolResult {
                    tool_call_id,
                    tool_name,
                    content,
                    is_error,
                    details,
                } => (
                    "user",
                    vec![self.result(
                        tool_call_id,
                        tool_name,
                        content,
                        *is_error,
                        details,
                        &mut pending,
                        &mut order,
                    )?],
                ),
                Message::Assistant {
                    content,
                    tool_calls,
                    ..
                } => {
                    let p = self.content(content)?;
                    if content
                        .iter()
                        .any(|v| matches!(v, ContentPart::Image { .. }))
                    {
                        return Err(ModelError::unsupported("assistant images unsupported"));
                    }
                    // Legacy calls carry no native signature; do not invent one.
                    if !tool_calls.is_empty() {
                        return Err(ModelError::unsupported(
                            "unsigned legacy Gemini function calls",
                        ));
                    }
                    ("model", p)
                }
                Message::Rich {
                    rich_role: RichRole::Assistant,
                    parts,
                } => {
                    let Some(HistoryPart::ProviderState { state }) = parts.last() else {
                        return Err(invalid());
                    };
                    if state.provider != self.identity
                        || state.model.as_deref() != Some(&request.model)
                        || state.format != FORMAT
                        || !state.required_for_replay
                    {
                        return Err(invalid());
                    }
                    let raw = state
                        .payload
                        .get("parts")
                        .and_then(Value::as_array)
                        .ok_or_else(invalid)?;
                    let ids = state
                        .payload
                        .get("ids")
                        .and_then(Value::as_array)
                        .ok_or_else(invalid)?;
                    let signatures = state
                        .payload
                        .get("signatures")
                        .and_then(Value::as_array)
                        .ok_or_else(invalid)?;
                    if signatures.len() != raw.len()
                        || raw.iter().zip(signatures).any(|(part, signature)| {
                            part.get("thoughtSignature").unwrap_or(&Value::Null) != signature
                        })
                    {
                        return Err(invalid());
                    }
                    require_signature(raw).map_err(|_| invalid())?;
                    let projected = project(raw, ids, mi).map_err(|_| invalid())?;
                    if projected != parts[..parts.len() - 1] {
                        return Err(invalid());
                    }
                    for (part, id) in raw.iter().zip(ids) {
                        if let Some(call) = part.get("functionCall") {
                            let id = id.as_str().ok_or_else(invalid)?;
                            let name = text(call, "name").map_err(|_| invalid())?;
                            if pending
                                .insert(id.into(), (name.into(), call.get("id").is_some()))
                                .is_some()
                            {
                                return Err(invalid());
                            }
                            order.push(id.into());
                        }
                    }
                    ("model", raw.clone())
                }
                Message::Rich {
                    rich_role: RichRole::User,
                    parts,
                } => {
                    let mut p = Vec::new();
                    for part in parts {
                        match part {
                            HistoryPart::Text { text } => p.push(json!({"text":text})),
                            HistoryPart::Image {
                                mime_type,
                                data_base64,
                            } => p.push(json!({"inlineData":self.image(mime_type,data_base64)?})),
                            _ => {
                                return Err(ModelError::unsupported("unsupported Gemini user part"))
                            }
                        }
                    }
                    ("user", p)
                }
                Message::Rich {
                    rich_role: RichRole::Tool,
                    parts,
                } => {
                    let mut p = Vec::new();
                    for part in parts {
                        match part {
                            HistoryPart::ToolResult {
                                call_id,
                                name,
                                content,
                                is_error,
                                details,
                            } => p.push(self.result(
                                call_id,
                                name,
                                content,
                                *is_error,
                                details,
                                &mut pending,
                                &mut order,
                            )?),
                            _ => {
                                return Err(ModelError::unsupported("unsupported Gemini tool part"))
                            }
                        }
                    }
                    ("user", p)
                }
            };
            if parts.is_empty() {
                return Err(invalid());
            }
            if role == "model" && contents.last().is_some_and(|v| v["role"] == "model") {
                return Err(invalid());
            }
            if role == "user"
                && contents.last().is_some_and(|v| v["role"] == "user")
                && parts.iter().all(|p| p.get("functionResponse").is_some())
                && contents.last().unwrap()["parts"]
                    .as_array()
                    .is_some_and(|a| a.iter().all(|p| p.get("functionResponse").is_some()))
            {
                contents.last_mut().unwrap()["parts"]
                    .as_array_mut()
                    .unwrap()
                    .extend(parts);
            } else {
                contents.push(json!({"role":role,"parts":parts}));
            }
        }
        if !pending.is_empty() {
            return Err(invalid());
        }
        let mut names = HashSet::new();
        for tool in &request.tools {
            if tool.name.is_empty() || !tool.parameters.is_object() || !names.insert(&tool.name) {
                return Err(invalid());
            }
        }
        let (mode, allowed) = match &request.tool_choice {
            ToolChoice::Auto => ("AUTO", None),
            ToolChoice::None => ("NONE", None),
            ToolChoice::Required => ("ANY", None),
            ToolChoice::Named(n) => {
                if !names.contains(n) {
                    return Err(invalid());
                }
                ("ANY", Some(n))
            }
        };
        if request.tools.is_empty()
            && matches!(
                request.tool_choice,
                ToolChoice::Required | ToolChoice::Named(_)
            )
        {
            return Err(invalid());
        }
        let mut body = json!({"contents":contents});
        if let Some(s) = system {
            body["systemInstruction"] = json!({"parts":[{"text":s}]});
        }
        let mut generation = json!({});
        if let Some(n) = request.max_tokens {
            generation["maxOutputTokens"] = json!(n);
        }
        if let Some(t) = request.temperature {
            generation["temperature"] = json!(t);
        }
        if generation != json!({}) {
            body["generationConfig"] = generation;
        }
        if !request.tools.is_empty() {
            body["tools"] = json!([{"functionDeclarations":request.tools.iter().map(|t| json!({"name":t.name,"description":t.description,"parametersJsonSchema":t.parameters})).collect::<Vec<_>>()}]);
            let mut config = json!({"mode":mode});
            if let Some(n) = allowed {
                config["allowedFunctionNames"] = json!([n]);
            }
            body["toolConfig"] = json!({"functionCallingConfig":config});
        }
        Ok(body)
    }
}
// ids are parallel to native parts: null for non-calls. Synthetic ids are
// derived from the original message and part positions, and never sent on wire.
fn require_signature(parts: &[Value]) -> Result<(), ModelError> {
    if parts
        .iter()
        .find(|p| p.get("functionCall").is_some())
        .is_some_and(|p| {
            p.get("thoughtSignature")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        })
    {
        return Err(protocol());
    }
    Ok(())
}
fn project(raw: &[Value], ids: &[Value], mi: usize) -> Result<Vec<HistoryPart>, ModelError> {
    if raw.len() != ids.len() || raw.is_empty() {
        return Err(protocol());
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for (i, (part, id)) in raw.iter().zip(ids).enumerate() {
        let obj = part.as_object().ok_or_else(protocol)?;
        if obj.keys().any(|k| {
            !matches!(
                k.as_str(),
                "text" | "thought" | "thoughtSignature" | "functionCall" | "partMetadata"
            )
        }) {
            return Err(ModelError::unsupported("unsupported Gemini native part"));
        }
        if let Some(sig) = obj.get("thoughtSignature") {
            if sig.as_str().is_none_or(str::is_empty) {
                return Err(protocol());
            }
        }
        if let Some(thought) = obj.get("thought") {
            if !thought.is_boolean() {
                return Err(protocol());
            }
        }
        if let Some(t) = obj.get("text") {
            if obj.contains_key("functionCall") || !id.is_null() {
                return Err(protocol());
            }
            let t = t.as_str().ok_or_else(protocol)?;
            out.push(if obj.get("thought") == Some(&json!(true)) {
                HistoryPart::Reasoning {
                    text: Some(t.into()),
                    state: None,
                }
            } else {
                HistoryPart::Text { text: t.into() }
            });
        } else if let Some(call) = obj.get("functionCall") {
            if obj.get("thought") == Some(&json!(true)) {
                return Err(protocol());
            }
            let name = text(call, "name")?;
            let args = call.get("args").cloned().unwrap_or_else(|| json!({}));
            if !args.is_object()
                || call.as_object().is_none_or(|o| {
                    o.keys()
                        .any(|k| !matches!(k.as_str(), "name" | "args" | "id"))
                })
            {
                return Err(protocol());
            }
            let id = id.as_str().filter(|s| !s.is_empty()).ok_or_else(protocol)?;
            if call
                .get("id")
                .map(|v| v.as_str())
                .unwrap_or(Some(&format!("gemini_{mi}_{i}")))
                != Some(id)
                || !seen.insert(id.to_owned())
            {
                return Err(protocol());
            }
            out.push(HistoryPart::ToolCall {
                id: id.into(),
                name: name.into(),
                arguments_json: serde_json::to_string(&args).map_err(|_| protocol())?,
            });
        } else {
            return Err(ModelError::unsupported("unsupported Gemini native part"));
        }
    }
    Ok(out)
}
async fn emit(
    tx: &Sender<StreamEvent>,
    event: StreamEvent,
    cancel: &CancellationToken,
) -> Result<(), ModelError> {
    tokio::select! { biased; _ = cancel.cancelled() => Err(ModelError::cancelled()), result = tx.send(event) => { let _ = result; Ok(()) } }
}
#[async_trait::async_trait]
impl ModelProvider for GeminiGenerateContentProvider {
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
    fn image_limits(&self) -> Option<ImageLimits> {
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
        let mut raw = Vec::new();
        let mut ids = Vec::new();
        let mut usage = Usage::default();
        let mut stop = None;
        let mut suffix = String::new();
        while let Some(chunk) = response.next_chunk().await? {
            for event in parser.push(&chunk) {
                process_event(
                    &event,
                    &response,
                    &request,
                    &tx,
                    &cancel,
                    &mut raw,
                    &mut ids,
                    &mut usage,
                    &mut stop,
                    &mut suffix,
                )
                .await?;
            }
        }
        if let Some(event) = parser.finish() {
            process_event(
                &event,
                &response,
                &request,
                &tx,
                &cancel,
                &mut raw,
                &mut ids,
                &mut usage,
                &mut stop,
                &mut suffix,
            )
            .await?;
        }
        if cancel.is_cancelled() {
            return Err(ModelError::cancelled());
        }
        let stop = stop.ok_or_else(protocol)?;
        require_signature(&raw)?;
        let mut parts = project(&raw, &ids, request.messages.len())?;
        let has_calls = parts
            .iter()
            .any(|p| matches!(p, HistoryPart::ToolCall { .. }));
        let stop = if has_calls && stop == StopReason::Stop {
            StopReason::ToolUse
        } else {
            stop
        };
        if has_calls && stop != StopReason::ToolUse {
            return Err(protocol());
        }
        response.check_value(&json!({"parts":raw,"ids":ids}))?;
        let signatures: Vec<Value> = raw
            .iter()
            .map(|p| p.get("thoughtSignature").cloned().unwrap_or(Value::Null))
            .collect();
        parts.push(HistoryPart::ProviderState {
            state: ProviderState {
                provider: self.identity.clone(),
                model: Some(request.model),
                format: FORMAT.into(),
                payload: json!({"parts":raw,"ids":ids,"signatures":signatures}),
                required_for_replay: true,
            },
        });
        emit(&tx, StreamEvent::Usage(usage), &cancel).await?;
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
#[cfg(test)]
#[path = "gemini_stream_tests.rs"]
mod stream_tests;

#[allow(clippy::too_many_arguments)]
async fn process_event(
    event: &str,
    response: &crate::native_transport::NativeResponse,
    request: &HistoryRequest,
    tx: &Sender<StreamEvent>,
    cancel: &CancellationToken,
    raw: &mut Vec<Value>,
    ids: &mut Vec<Value>,
    usage: &mut Usage,
    stop: &mut Option<StopReason>,
    suffix: &mut String,
) -> Result<(), ModelError> {
    let v: Value = serde_json::from_str(event).map_err(|_| protocol())?;
    response.check_value(&v)?;
    if let Some(u) = v.get("usageMetadata") {
        let o = u.as_object().ok_or_else(protocol)?;
        for (key, target) in [
            ("promptTokenCount", &mut usage.input_tokens),
            ("candidatesTokenCount", &mut usage.output_tokens),
        ] {
            if let Some(n) = o.get(key) {
                *target = n.as_u64().ok_or_else(protocol)?;
            }
        }
        if let Some(n) = o.get("cachedContentTokenCount") {
            usage.cache_read_tokens = Some(n.as_u64().ok_or_else(protocol)?);
        }
    }
    if let Some(candidates) = v.get("candidates") {
        let a = candidates.as_array().ok_or_else(protocol)?;
        if a.len() != 1 {
            return Err(protocol());
        }
        let c = &a[0];
        if c.get("index").is_some_and(|v| v.as_u64() != Some(0)) {
            return Err(protocol());
        }
        if let Some(content) = c.get("content") {
            if stop.is_some()
                || content
                    .get("role")
                    .is_some_and(|v| v.as_str() != Some("model"))
            {
                return Err(protocol());
            }
            let parts = content
                .get("parts")
                .and_then(Value::as_array)
                .ok_or_else(protocol)?;
            for p in parts {
                let id = if let Some(call) = p.get("functionCall") {
                    let name = text(call, "name")?;
                    if !request.tools.iter().any(|t| t.name == name) {
                        return Err(protocol());
                    }
                    json!(match call.get("id") {
                        Some(v) => v
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .ok_or_else(protocol)?
                            .to_owned(),
                        None => format!("gemini_{}_{}", request.messages.len(), raw.len()),
                    })
                } else {
                    Value::Null
                };
                raw.push(p.clone());
                ids.push(id);
                let projected = project(raw, ids, request.messages.len())?;
                let index = (raw.len() - 1) as u32;
                match projected.last().unwrap() {
                    HistoryPart::Text { text } => {
                        response.check_delta(suffix, text)?;
                        emit(tx, StreamEvent::TextDelta(text.clone()), cancel).await?;
                    }
                    HistoryPart::Reasoning {
                        text: Some(text), ..
                    } => {
                        response.check_delta(suffix, text)?;
                        emit(tx, StreamEvent::ThinkingDelta(text.clone()), cancel).await?;
                    }
                    HistoryPart::ToolCall {
                        id,
                        name,
                        arguments_json,
                    } => {
                        response.check_delta(suffix, arguments_json)?;
                        emit(
                            tx,
                            StreamEvent::ToolCallStart {
                                index,
                                id: id.clone(),
                                name: name.clone(),
                            },
                            cancel,
                        )
                        .await?;
                        emit(
                            tx,
                            StreamEvent::ToolCallArgumentsDelta {
                                index,
                                delta: arguments_json.clone(),
                            },
                            cancel,
                        )
                        .await?;
                    }
                    _ => return Err(protocol()),
                }
            }
        }
        if let Some(reason) = c.get("finishReason") {
            if stop.is_some() {
                return Err(protocol());
            }
            *stop = Some(match reason.as_str() {
                Some("STOP") => StopReason::Stop,
                Some("MAX_TOKENS") => StopReason::Length,
                _ => return Err(ModelError::unsupported("unsupported Gemini finish reason")),
            });
        }
    } else if v.get("promptFeedback").is_some() {
        return Err(ModelError::protocol("Gemini prompt blocked"));
    }
    Ok(())
}
