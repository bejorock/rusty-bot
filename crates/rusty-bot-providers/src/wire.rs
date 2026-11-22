use crate::ModelError;
use crate::{protocol, Api, ChatStreamTermination};
use rusty_bot_messages::{
    BlockStart, ContentBlock as B, ConversationMessage, FinishReason, GenerationMetadata,
    HistoryEntry, MessageStreamPayload as E, ReplayState, TokenUsage, ToolInvocation,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn s<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    v.get(key).and_then(Value::as_str).ok_or_else(protocol)
}
fn nonempty<'a>(v: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    let text = s(v, key)?;
    if text.trim().is_empty() {
        Err(protocol())
    } else {
        Ok(text)
    }
}
fn array<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>, ModelError> {
    v.get(key).and_then(Value::as_array).ok_or_else(protocol)
}
fn fields(v: &Value, allowed: &[&str]) -> Result<(), ModelError> {
    if v.as_object()
        .is_none_or(|o| o.keys().any(|key| !allowed.contains(&key.as_str())))
    {
        return Err(protocol());
    }
    Ok(())
}
fn response_phase(v: &Value) -> Result<(), ModelError> {
    if v.get("phase")
        .is_some_and(|phase| !matches!(phase.as_str(), Some("final_answer" | "commentary")))
    {
        return Err(protocol());
    }
    Ok(())
}
fn empty_annotations(v: &Value) -> Result<(), ModelError> {
    if v.get("annotations")
        .is_some_and(|a| a.as_array().is_none_or(|a| !a.is_empty()))
    {
        return Err(protocol());
    }
    Ok(())
}
fn empty_logprobs(v: &Value) -> Result<(), ModelError> {
    if v.get("logprobs")
        .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
    {
        return Err(protocol());
    }
    Ok(())
}
fn tool(id: &str, name: &str, arguments: &str) -> Result<B, ModelError> {
    if id.trim().is_empty() || name.trim().is_empty() {
        return Err(protocol());
    }
    Ok(match serde_json::from_str::<Value>(arguments) {
        Ok(arguments) if arguments.is_object() => B::ToolCall {
            call: ToolInvocation {
                id: id.into(),
                name: name.into(),
                arguments,
            },
        },
        _ => B::InvalidToolCall {
            id: Some(id.into()),
            name: Some(name.into()),
            arguments_text: arguments.into(),
            error: "tool arguments are not a complete JSON object".into(),
        },
    })
}
/// Project only known wire block types. Raw payload is retained separately for exact replay.
pub(crate) fn project(api: Api, payload: &Value) -> Result<Vec<B>, ModelError> {
    let mut out = Vec::new();
    match api {
        Api::Responses => {
            for item in payload.as_array().ok_or_else(protocol)? {
                match s(item, "type")? {
                    "message" => {
                        fields(item, &["id", "type", "status", "role", "content", "phase"])?;
                        response_phase(item)?;
                        if s(item, "role")? != "assistant" {
                            return Err(protocol());
                        }
                        for b in array(item, "content")? {
                            fields(b, &["type", "text", "annotations", "logprobs", "refusal"])?;
                            if b.get("logprobs")
                                .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
                            {
                                return Err(protocol());
                            }
                            empty_annotations(b)?;
                            out.push(match s(b, "type")? {
                                "output_text" => B::text(s(b, "text")?),
                                "refusal" => B::Refusal {
                                    text: s(b, "refusal")?.into(),
                                },
                                _ => return Err(protocol()),
                            });
                        }
                    }
                    "reasoning" => {
                        fields(
                            item,
                            &[
                                "id",
                                "type",
                                "status",
                                "summary",
                                "encrypted_content",
                                "content",
                            ],
                        )?;
                        if item
                            .get("content")
                            .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
                        {
                            return Err(protocol());
                        }
                        // Required even if summaries are empty: encrypted state is the replay authority.
                        nonempty(item, "encrypted_content")?;
                        for summary in array(item, "summary")? {
                            fields(summary, &["type", "text"])?;
                            if s(summary, "type")? != "summary_text" {
                                return Err(protocol());
                            }
                            out.push(B::Reasoning {
                                text: s(summary, "text")?.into(),
                            });
                        }
                    }
                    "function_call" => {
                        fields(
                            item,
                            &["id", "type", "status", "call_id", "name", "arguments"],
                        )?;
                        if item
                            .get("status")
                            .is_some_and(|v| v.as_str() != Some("completed"))
                        {
                            return Err(protocol());
                        }
                        out.push(tool(
                            nonempty(item, "call_id")?,
                            nonempty(item, "name")?,
                            s(item, "arguments")?,
                        )?);
                    }
                    _ => return Err(protocol()),
                }
            }
        }
        Api::Anthropic => {
            for b in payload.as_array().ok_or_else(protocol)? {
                empty_annotations(b)?;
                match s(b, "type")? {
                    "text" => {
                        fields(b, &["type", "text", "citations"])?;
                        if b.get("citations").is_some_and(|v| {
                            !v.is_null() && v.as_array().is_none_or(|v| !v.is_empty())
                        }) {
                            return Err(protocol());
                        }
                        out.push(B::text(s(b, "text")?));
                    }
                    "thinking" => {
                        fields(b, &["type", "thinking", "signature"])?;
                        nonempty(b, "signature")?;
                        out.push(B::Reasoning {
                            text: s(b, "thinking")?.into(),
                        });
                    }
                    "redacted_thinking" => {
                        fields(b, &["type", "data"])?;
                        nonempty(b, "data")?;
                        out.push(B::Extension {
                            namespace: "anthropic".into(),
                            kind: "redacted_thinking".into(),
                            value: Value::Null,
                        });
                    }
                    "tool_use" => {
                        fields(b, &["type", "id", "name", "input"])?;
                        let input = b.get("input").ok_or_else(protocol)?;
                        out.push(tool(
                            nonempty(b, "id")?,
                            nonempty(b, "name")?,
                            &input.to_string(),
                        )?);
                    }
                    _ => return Err(protocol()),
                }
            }
        }
        Api::Chat | Api::Deepseek => {
            fields(
                payload,
                &[
                    "role",
                    "content",
                    "reasoning_content",
                    "refusal",
                    "tool_calls",
                ],
            )?;
            if s(payload, "role")? != "assistant" {
                return Err(protocol());
            }
            if let Some(reasoning) = payload.get("reasoning_content").filter(|v| !v.is_null()) {
                if api != Api::Deepseek {
                    return Err(protocol());
                }
                out.push(B::Reasoning {
                    text: reasoning.as_str().ok_or_else(protocol)?.into(),
                });
            }
            if let Some(content) = payload.get("content").filter(|v| !v.is_null()) {
                out.push(B::text(content.as_str().ok_or_else(protocol)?));
            }
            if let Some(refusal) = payload.get("refusal").filter(|v| !v.is_null()) {
                out.push(B::Refusal {
                    text: refusal.as_str().ok_or_else(protocol)?.into(),
                });
            }
            if let Some(calls) = payload.get("tool_calls").filter(|v| !v.is_null()) {
                for call in calls.as_array().ok_or_else(protocol)? {
                    fields(call, &["id", "type", "function"])?;
                    if s(call, "type")? != "function" {
                        return Err(protocol());
                    }
                    let function = &call["function"];
                    fields(function, &["name", "arguments"])?;
                    out.push(tool(
                        nonempty(call, "id")?,
                        nonempty(function, "name")?,
                        s(function, "arguments")?,
                    )?);
                }
            }
        }
    }
    Ok(out)
}

pub(crate) struct Decoder {
    api: Api,
    model: String,
    id: String,
    response_id: Option<String>,
    actual_model: Option<String>,
    usage: Option<TokenUsage>,
    raw_finish: Option<String>,
    extras: BTreeMap<String, Value>,
    terminal: bool,
    payload: Value,
    preview_index: usize,
    preview_kind: Option<&'static str>,
    // Chat calls arrive interleaved and are not executable until terminal validation.
    calls: BTreeMap<usize, Value>,
    chat_phase: u8,
    // Anthropic permits one active indexed block and a separate signature phase.
    active: Option<(usize, Value, String)>,
    signature: bool,
    saw_input_delta: bool,
    // Raw malformed Anthropic input fragments indexed by content block.
    invalid_anthropic_args: BTreeMap<usize, String>,
    started: bool,
    stopped: bool,
    // Responses item snapshots must be closed before the final response.
    response_items: BTreeMap<usize, Value>,
    response_done: BTreeMap<usize, Value>,
}
impl Decoder {
    pub(crate) fn new(api: Api, model: &str, id: &str) -> Self {
        Self {
            api,
            model: model.into(),
            id: id.into(),
            response_id: None,
            actual_model: None,
            usage: None,
            raw_finish: None,
            extras: BTreeMap::new(),
            terminal: false,
            payload: match api {
                Api::Chat | Api::Deepseek => json!({"role":"assistant", "content":null}),
                _ => json!([]),
            },
            preview_index: 0,
            preview_kind: None,
            calls: BTreeMap::new(),
            chat_phase: 0,
            active: None,
            signature: false,
            saw_input_delta: false,
            invalid_anthropic_args: BTreeMap::new(),
            started: false,
            stopped: false,
            response_items: BTreeMap::new(),
            response_done: BTreeMap::new(),
        }
    }
    /// Decode a complete native response directly, retaining the authoritative replay payload.
    pub(crate) fn from_nonstream(
        api: Api,
        model: &str,
        id: &str,
        value: &Value,
    ) -> Result<HistoryEntry, ModelError> {
        let mut decoder = Self::new(api, model, id);
        decoder.identity(value)?;
        match api {
            Api::Responses => {
                if value.get("error").is_some_and(|e| !e.is_null()) {
                    return Err(protocol());
                }
                decoder.payload = value.get("output").ok_or_else(protocol)?.clone();
                project(api, &decoder.payload)?;
                decoder.raw_finish = Some(match s(value, "status")? {
                    "completed" => "completed".into(),
                    "incomplete" => nonempty(&value["incomplete_details"], "reason")?.into(),
                    _ => return Err(protocol()),
                });
                decoder.usage = Some(usage(&value["usage"], false)?);
                decoder
                    .extras
                    .insert("native_usage".into(), value["usage"].clone());
            }
            Api::Anthropic => {
                if s(value, "type")? != "message" || s(value, "role")? != "assistant" {
                    return Err(protocol());
                }
                for key in ["diagnostics", "container", "stop_details"] {
                    if value.get(key).is_some_and(|v| !v.is_null()) {
                        return Err(protocol());
                    }
                }
                decoder.payload = value.get("content").ok_or_else(protocol)?.clone();
                project(api, &decoder.payload)?;
                decoder.raw_finish = Some(nonempty(value, "stop_reason")?.into());
                if let Some(sequence) = value.get("stop_sequence").filter(|v| !v.is_null()) {
                    if !sequence.is_string() {
                        return Err(protocol());
                    }
                    decoder
                        .extras
                        .insert("stop_sequence".into(), sequence.clone());
                }
                decoder.usage = Some(usage(&value["usage"], false)?);
                decoder
                    .extras
                    .insert("native_usage_start".into(), value["usage"].clone());
            }
            Api::Chat => {
                let choices = array(value, "choices")?;
                if choices.len() != 1 || choices[0]["index"].as_u64() != Some(0) {
                    return Err(protocol());
                }
                let choice = &choices[0];
                fields(choice, &["index", "message", "finish_reason", "logprobs"])?;
                if choice.get("logprobs").is_some_and(|v| !v.is_null()) {
                    return Err(protocol());
                }
                decoder.payload = choice.get("message").ok_or_else(protocol)?.clone();
                let message = decoder.payload.as_object_mut().ok_or_else(protocol)?;
                for key in ["audio", "function_call"] {
                    if let Some(v) = message.get(key) {
                        if !v.is_null() {
                            return Err(protocol());
                        }
                        message.remove(key);
                    }
                }
                if let Some(v) = message.get("annotations") {
                    if !v.is_null() && v.as_array().is_none_or(|a| !a.is_empty()) {
                        return Err(protocol());
                    }
                    message.remove("annotations");
                }
                project(api, &decoder.payload)?;
                decoder.raw_finish = Some(nonempty(choice, "finish_reason")?.into());
                if let Some(u) = value.get("usage").filter(|u| !u.is_null()) {
                    decoder.usage = Some(usage(u, true)?);
                    decoder.extras.insert("native_usage".into(), u.clone());
                }
            }
            Api::Deepseek => return Err(crate::unsupported()),
        }
        decoder.terminal = true;
        decoder.finish()
    }
    #[cfg(test)]
    pub(crate) fn push(&mut self, data: &str) -> Result<Vec<E>, ModelError> {
        self.push_checked(data, |_| Ok(()))
    }
    pub(crate) fn push_checked(
        &mut self,
        data: &str,
        check: impl FnOnce(&Value) -> Result<(), ModelError>,
    ) -> Result<Vec<E>, ModelError> {
        if self.terminal {
            return Err(protocol());
        }
        if data.len() > 4 * 1024 * 1024 {
            return Err(protocol());
        }
        if matches!(self.api, Api::Chat | Api::Deepseek) && data == "[DONE]" {
            if self.raw_finish.is_none() {
                return Err(protocol());
            }
            self.terminal = true;
            return Ok(self.close());
        }
        let v: Value = serde_json::from_str(data).map_err(|_| protocol())?;
        check(&v)?;
        if v.get("error").is_some_and(|e| !e.is_null())
            || v.get("type").and_then(Value::as_str) == Some("error")
        {
            return Err(crate::error::classify(None, Some(&v), &[]));
        }
        if v.get("type").and_then(Value::as_str) == Some("response.failed") {
            return Err(crate::error::classify(None, v.get("response"), &[]));
        }
        match self.api {
            Api::Chat | Api::Deepseek => self.chat(v),
            Api::Anthropic => self.anthropic(v),
            Api::Responses => self.responses(v),
        }
    }
    /// Called only after the transport and SSE parser have both reached a clean EOF.
    pub(crate) fn eof(&mut self, policy: ChatStreamTermination) -> Vec<E> {
        if self.api == Api::Chat
            && policy == ChatStreamTermination::FinishReasonAndEof
            && !self.terminal
            && self.raw_finish.is_some()
        {
            self.terminal = true;
            self.close()
        } else {
            vec![]
        }
    }
    fn close(&mut self) -> Vec<E> {
        if self.preview_kind.take().is_some() {
            let index = self.preview_index;
            self.preview_index += 1;
            vec![E::BlockEnd { index }]
        } else {
            vec![]
        }
    }
    fn delta(&mut self, kind: &'static str, delta: &str) -> Vec<E> {
        let mut out = Vec::new();
        if self.preview_kind != Some(kind) {
            out.extend(self.close());
            self.preview_kind = Some(kind);
            out.push(E::BlockStart {
                index: self.preview_index,
                block: if kind == "text" {
                    BlockStart::Text
                } else {
                    BlockStart::Reasoning
                },
            });
        }
        out.push(E::BlockDelta {
            index: self.preview_index,
            delta: delta.into(),
        });
        out
    }
    fn complete(&mut self, blocks: &[B]) -> Vec<E> {
        let mut events = self.close();
        for block in blocks {
            if matches!(
                block,
                B::ToolCall { .. }
                    | B::InvalidToolCall { .. }
                    | B::Refusal { .. }
                    | B::Extension { .. }
            ) {
                events.push(E::BlockStart {
                    index: self.preview_index,
                    block: BlockStart::Complete {
                        block: block.clone(),
                    },
                });
                events.push(E::BlockEnd {
                    index: self.preview_index,
                });
                self.preview_index += 1;
            }
        }
        events
    }
    fn identity(&mut self, v: &Value) -> Result<(), ModelError> {
        let id = nonempty(v, "id")?;
        let model = nonempty(v, "model")?;
        if self.response_id.as_deref().is_some_and(|old| old != id)
            || self.actual_model.as_deref().is_some_and(|old| old != model)
        {
            return Err(protocol());
        }
        self.response_id = Some(id.into());
        self.actual_model = Some(model.into());
        Ok(())
    }
    fn chat(&mut self, v: Value) -> Result<Vec<E>, ModelError> {
        self.identity(&v)?;
        let choices = array(&v, "choices")?;
        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            self.usage = Some(usage(u, true)?);
            self.extras.insert("native_usage".into(), u.clone());
        }
        if choices.is_empty() {
            if self.raw_finish.is_none() || v.get("usage").is_none() {
                return Err(protocol());
            }
            return Ok(vec![]);
        }
        if choices.len() != 1
            || choices[0]["index"].as_u64() != Some(0)
            || self.raw_finish.is_some()
        {
            return Err(protocol());
        }
        let c = &choices[0];
        fields(c, &["index", "delta", "finish_reason", "logprobs"])?;
        if c.get("logprobs").is_some_and(|v| !v.is_null()) {
            return Err(protocol());
        }
        let d = c
            .get("delta")
            .and_then(Value::as_object)
            .ok_or_else(protocol)?;
        if d.keys().any(|k| {
            !matches!(
                k.as_str(),
                "role" | "content" | "reasoning_content" | "tool_calls" | "refusal"
            )
        }) {
            return Err(protocol());
        }
        if d.get("role").is_some_and(|r| r != "assistant") {
            return Err(protocol());
        }
        let mut events = Vec::new();
        if let Some(r) = d.get("reasoning_content").filter(|v| !v.is_null()) {
            if self.api != Api::Deepseek || self.chat_phase > 0 {
                return Err(protocol());
            }
            let r = r.as_str().ok_or_else(protocol)?;
            append(&mut self.payload, "reasoning_content", r)?;
            events.extend(self.delta("reasoning", r));
        }
        if let Some(t) = d.get("content").filter(|v| !v.is_null()) {
            let t = t.as_str().ok_or_else(protocol)?;
            if !t.is_empty() {
                if self.chat_phase > 1 {
                    return Err(protocol());
                }
                self.chat_phase = 1;
                events.extend(self.delta("text", t));
            }
            append(&mut self.payload, "content", t)?;
        }
        if let Some(r) = d.get("refusal").filter(|v| !v.is_null()) {
            if self.api != Api::Chat || self.chat_phase == 2 {
                return Err(protocol());
            }
            self.chat_phase = 3;
            append(
                &mut self.payload,
                "refusal",
                r.as_str().ok_or_else(protocol)?,
            )?;
        }
        if let Some(calls) = d.get("tool_calls") {
            if self.chat_phase > 2 {
                return Err(protocol());
            }
            self.chat_phase = 2;
            events.extend(self.close());
            for call in calls.as_array().ok_or_else(protocol)? {
                fields(call, &["index", "id", "type", "function"])?;
                let index = call["index"]
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(protocol)?;
                if index > 127 || index > self.calls.len() {
                    return Err(protocol());
                }
                let target = self.calls.entry(index).or_insert_with(
                    || json!({"id":"", "type":"function", "function":{"name":"", "arguments":""}}),
                );
                if call
                    .get("type")
                    .is_some_and(|v| !v.is_null() && v != "function")
                {
                    return Err(protocol());
                }
                if let Some(id) = call.get("id").filter(|v| !v.is_null()) {
                    append(target, "id", id.as_str().ok_or_else(protocol)?)?;
                }
                if let Some(f) = call.get("function") {
                    fields(f, &["name", "arguments"])?;
                    if let Some(n) = f.get("name").filter(|v| !v.is_null()) {
                        append(
                            &mut target["function"],
                            "name",
                            n.as_str().ok_or_else(protocol)?,
                        )?;
                    }
                    if let Some(a) = f.get("arguments") {
                        append(
                            &mut target["function"],
                            "arguments",
                            a.as_str().ok_or_else(protocol)?,
                        )?;
                    }
                }
            }
        }
        if let Some(reason) = c.get("finish_reason").filter(|r| !r.is_null()) {
            self.raw_finish = Some(reason.as_str().ok_or_else(protocol)?.into());
            if !self.calls.is_empty() {
                self.payload["tool_calls"] = json!(self.calls.values().collect::<Vec<_>>());
            }
            let blocks = project(self.api, &self.payload)?;
            events.extend(self.complete(&blocks));
        }
        Ok(events)
    }
    fn anthropic(&mut self, v: Value) -> Result<Vec<E>, ModelError> {
        let mut events = Vec::new();
        match s(&v, "type")? {
            "ping" => return Ok(events),
            "message_start" if !self.started => {
                let m = &v["message"];
                self.identity(m)?;
                if s(m, "role")? != "assistant" || !array(m, "content")?.is_empty() {
                    return Err(protocol());
                }
                if m.get("diagnostics").is_some_and(|value| !value.is_null()) {
                    return Err(protocol());
                }
                for key in ["container", "stop_details"] {
                    if m.get(key).is_some_and(|value| !value.is_null()) {
                        return Err(protocol());
                    }
                }
                self.usage = Some(usage(&m["usage"], false)?);
                self.extras
                    .insert("native_usage_start".into(), m["usage"].clone());
                self.started = true;
            }
            "content_block_start" if self.started && !self.stopped && self.active.is_none() => {
                let index = v["index"]
                    .as_u64()
                    .and_then(|i| usize::try_from(i).ok())
                    .ok_or_else(protocol)?;
                if index != self.payload.as_array().ok_or_else(protocol)?.len() || index > 127 {
                    return Err(protocol());
                }
                let mut b = v.get("content_block").ok_or_else(protocol)?.clone();
                match s(&b, "type")? {
                    "text" if s(&b, "text")?.is_empty() => {}
                    "thinking" if s(&b, "thinking")?.is_empty() => {
                        if b.get("signature").is_none() {
                            b["signature"] = json!("");
                        }
                        if !s(&b, "signature")?.is_empty() {
                            return Err(protocol());
                        }
                    }
                    "tool_use" if b["input"].as_object().is_some_and(|v| v.is_empty()) => {
                        nonempty(&b, "id")?;
                        nonempty(&b, "name")?;
                    }
                    "redacted_thinking" => {
                        nonempty(&b, "data")?;
                    }
                    _ => return Err(protocol()),
                }
                self.signature = false;
                self.saw_input_delta = false;
                self.active = Some((index, b, String::new()));
            }
            "content_block_delta" if !self.stopped => {
                let (index, mut block, mut args) = self.active.take().ok_or_else(protocol)?;
                if v["index"].as_u64() != Some(index as u64) {
                    return Err(protocol());
                }
                let d = &v["delta"];
                fields(
                    d,
                    &["type", "text", "thinking", "signature", "partial_json"],
                )?;
                match (s(&block, "type")?, s(d, "type")?) {
                    ("text", "text_delta") => {
                        let t = s(d, "text")?;
                        append(&mut block, "text", t)?;
                        events.extend(self.delta("text", t));
                    }
                    ("thinking", "thinking_delta") if !self.signature => {
                        let t = s(d, "thinking")?;
                        append(&mut block, "thinking", t)?;
                        events.extend(self.delta("reasoning", t));
                    }
                    ("thinking", "signature_delta") => {
                        self.signature = true;
                        append(&mut block, "signature", s(d, "signature")?)?;
                    }
                    ("tool_use", "input_json_delta") => {
                        let fragment = s(d, "partial_json")?;
                        if args.len().saturating_add(fragment.len()) > 4 * 1024 * 1024 {
                            return Err(protocol());
                        }
                        args.push_str(fragment);
                        self.saw_input_delta = true;
                    }
                    _ => return Err(protocol()),
                }
                self.active = Some((index, block, args));
            }
            "content_block_stop" if !self.stopped => {
                let (index, mut block, args) = self.active.take().ok_or_else(protocol)?;
                if v["index"].as_u64() != Some(index as u64) {
                    return Err(protocol());
                }
                let mut malformed = false;
                if s(&block, "type")? == "tool_use" && self.saw_input_delta {
                    match serde_json::from_str::<Value>(&args) {
                        Ok(input) if input.is_object() => block["input"] = input,
                        _ => {
                            // A string is deliberately non-executable as tool input. Replay is
                            // refused for InvalidToolCall; the exact fragment is kept separately.
                            block["input"] = json!(args);
                            malformed = true;
                        }
                    }
                }
                let mut blocks = project(Api::Anthropic, &json!([block.clone()]))?;
                if malformed {
                    blocks[0] = tool(nonempty(&block, "id")?, nonempty(&block, "name")?, &args)?;
                    self.invalid_anthropic_args.insert(index, args);
                }
                self.payload
                    .as_array_mut()
                    .ok_or_else(protocol)?
                    .push(block);
                events.extend(self.complete(&blocks));
            }
            "message_delta" if self.started && !self.stopped && self.active.is_none() => {
                fields(
                    &v["delta"],
                    &["stop_reason", "stop_sequence", "stop_details", "container"],
                )?;
                for key in ["stop_details", "container"] {
                    if v["delta"].get(key).is_some_and(|value| !value.is_null()) {
                        return Err(protocol());
                    }
                }
                self.raw_finish = Some(nonempty(&v["delta"], "stop_reason")?.into());
                if let Some(sequence) = v["delta"].get("stop_sequence").filter(|v| !v.is_null()) {
                    if !sequence.is_string() {
                        return Err(protocol());
                    }
                    self.extras.insert("stop_sequence".into(), sequence.clone());
                }
                let update = usage(&v["usage"], false)?;
                self.extras
                    .insert("native_usage_delta".into(), v["usage"].clone());
                let current = self.usage.get_or_insert_with(TokenUsage::default);
                if update.output_tokens.is_some() {
                    current.output_tokens = update.output_tokens;
                }
                if update.input_tokens.is_some() {
                    current.input_tokens = update.input_tokens;
                }
                current.input_details.extend(update.input_details);
                current.output_details.extend(update.output_details);
                self.stopped = true;
            }
            "message_stop" if self.stopped && self.active.is_none() => {
                self.terminal = true;
            }
            _ => return Err(protocol()),
        }
        Ok(events)
    }
    fn responses(&mut self, v: Value) -> Result<Vec<E>, ModelError> {
        let mut events = Vec::new();
        match s(&v, "type")? {
            "response.created" if !self.started => {
                self.identity(&v["response"])?;
                self.started = true;
            }
            "response.in_progress" if self.started => {
                self.identity(&v["response"])?;
            }
            "response.output_item.added" if self.started => {
                let index = index(&v, "output_index")?;
                if index != self.response_items.len()
                    || index > 127
                    || self.response_items.len() != self.response_done.len()
                {
                    return Err(protocol());
                }
                let item = v.get("item").ok_or_else(protocol)?.clone();
                nonempty(&item, "id")?;
                response_phase(&item)?;
                if !matches!(s(&item, "type")?, "message" | "reasoning" | "function_call") {
                    return Err(protocol());
                }
                self.response_items.insert(index, item);
            }
            "response.output_text.delta" | "response.reasoning_summary_text.delta" => {
                let index = self.response_event_index(&v)?;
                let kind = s(&self.response_items[&index], "type")?;
                let text = s(&v, "delta")?;
                if s(&v, "type")? == "response.output_text.delta" {
                    empty_logprobs(&v)?;
                    if kind != "message" {
                        return Err(protocol());
                    }
                    events.extend(self.delta("text", text));
                } else {
                    if kind != "reasoning" {
                        return Err(protocol());
                    }
                    events.extend(self.delta("reasoning", text));
                }
            }
            "response.output_item.done" => {
                let index = self.response_event_index(&v)?;
                if self.response_done.contains_key(&index)
                    || self.response_items[&index]["id"] != v["item"]["id"]
                    || self.response_items[&index]["type"] != v["item"]["type"]
                    || self.response_items[&index]
                        .get("phase")
                        .is_some_and(|phase| v["item"].get("phase") != Some(phase))
                {
                    return Err(protocol());
                }
                let blocks = project(Api::Responses, &json!([v["item"].clone()]))?;
                self.response_done.insert(index, v["item"].clone());
                events.extend(self.complete(&blocks));
            }
            "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done"
            | "response.refusal.delta"
            | "response.refusal.done" => {
                self.response_event_index(&v)?;
                match s(&v, "type")? {
                    "response.output_text.done" => empty_logprobs(&v)?,
                    "response.content_part.added" | "response.content_part.done" => {
                        let part = v.get("part").ok_or_else(protocol)?;
                        empty_logprobs(part)?;
                        empty_annotations(part)?;
                    }
                    _ => {}
                }
            }
            "response.completed" | "response.incomplete" if self.started => {
                let r = &v["response"];
                self.identity(r)?;
                if r.get("error").is_some_and(|e| !e.is_null()) {
                    return Err(crate::error::classify(None, Some(r), &[]));
                }
                let output = array(r, "output")?;
                if output.len() != self.response_items.len()
                    || output.len() != self.response_done.len()
                    || output
                        .iter()
                        .enumerate()
                        .any(|(i, item)| self.response_done.get(&i) != Some(item))
                {
                    return Err(protocol());
                }
                self.payload = json!(output);
                project(Api::Responses, &self.payload)?;
                self.usage = Some(usage(&r["usage"], false)?);
                self.extras
                    .insert("native_usage".into(), r["usage"].clone());
                let status = s(r, "status")?;
                self.raw_finish = Some(match status {
                    "completed" if s(&v, "type")? == "response.completed" => "completed".into(),
                    "incomplete" if s(&v, "type")? == "response.incomplete" => {
                        nonempty(&r["incomplete_details"], "reason")?.into()
                    }
                    _ => return Err(protocol()),
                });
                self.terminal = true;
                events.extend(self.close());
            }
            _ => return Err(protocol()),
        }
        Ok(events)
    }
    fn response_event_index(&self, v: &Value) -> Result<usize, ModelError> {
        let index = index(v, "output_index")?;
        let item = self.response_items.get(&index).ok_or_else(protocol)?;
        if self.response_done.contains_key(&index)
            || v.get("item_id")
                .is_some_and(|id| id.as_str().is_none_or(|id| item["id"] != id))
        {
            return Err(protocol());
        }
        Ok(index)
    }
    pub(crate) fn finish(self) -> Result<HistoryEntry, ModelError> {
        if !self.terminal
            || self.active.is_some()
            || self.preview_kind.is_some()
            || self.response_id.is_none()
            || self.actual_model.is_none()
        {
            return Err(protocol());
        }
        let raw = self.raw_finish.ok_or_else(protocol)?;
        let known = match self.api {
            Api::Responses => matches!(
                raw.as_str(),
                "completed" | "max_output_tokens" | "content_filter"
            ),
            Api::Anthropic => matches!(
                raw.as_str(),
                "end_turn" | "stop_sequence" | "tool_use" | "max_tokens" | "refusal"
            ),
            Api::Chat | Api::Deepseek => matches!(
                raw.as_str(),
                "stop" | "length" | "tool_calls" | "content_filter"
            ),
        };
        if !known {
            return Err(protocol());
        }
        let mut blocks = project(self.api, &self.payload)?;
        for (index, args) in &self.invalid_anthropic_args {
            let block = &self.payload[*index];
            blocks[*index] = tool(nonempty(block, "id")?, nonempty(block, "name")?, args)?;
        }
        // Replay must never turn malformed calls into executable calls on the next request.
        let has_tools = blocks
            .iter()
            .any(|b| matches!(b, B::ToolCall { .. } | B::InvalidToolCall { .. }));
        let refusal = blocks.iter().any(|b| matches!(b, B::Refusal { .. }));
        let finish = match raw.as_str() {
            "stop" | "end_turn" | "stop_sequence" if has_tools => return Err(protocol()),
            "stop" | "end_turn" | "stop_sequence" | "completed" => {
                if has_tools {
                    FinishReason::ToolCalls
                } else if refusal {
                    FinishReason::Refusal
                } else {
                    FinishReason::Stop
                }
            }
            "tool_calls" | "tool_use" if has_tools => FinishReason::ToolCalls,
            "length" | "max_tokens" | "max_output_tokens" => FinishReason::Length,
            "content_filter" | "refusal" => FinishReason::Refusal,
            _ => return Err(protocol()),
        };
        blocks.push(B::Replay {
            state: ReplayState {
                provider: self.api.provider().into(),
                model: Some(self.model),
                format: self.api.format().into(),
                version: 1,
                payload: self.payload,
                required_for_replay: true,
            },
        });
        let mut entry =
            HistoryEntry::completed(self.id, ConversationMessage::Assistant { content: blocks });
        let mut usage = self.usage;
        if let Some(usage) = &mut usage {
            crate::usage::normalize(self.api, usage);
        }
        entry.generation = Some(GenerationMetadata {
            provider: Some(self.api.provider().into()),
            model: self.actual_model,
            response_id: self.response_id,
            finish_reason: Some(finish),
            raw_finish_reason: Some(raw),
            usage,
            extras: self.extras,
            ..Default::default()
        });
        entry.validate().map_err(|_| protocol())?;
        Ok(entry)
    }
}
fn index(v: &Value, key: &str) -> Result<usize, ModelError> {
    v[key]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(protocol)
}
fn append(v: &mut Value, key: &str, delta: &str) -> Result<(), ModelError> {
    let mut text = match v.get(key) {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(t)) => t.clone(),
        _ => return Err(protocol()),
    };
    text.push_str(delta);
    if text.len() > 4 * 1024 * 1024 {
        return Err(protocol());
    }
    v[key] = json!(text);
    Ok(())
}
fn usage(v: &Value, chat: bool) -> Result<TokenUsage, ModelError> {
    if !v.is_object() {
        return Err(protocol());
    }
    let count = |key: &str| -> Result<Option<u64>, ModelError> {
        v.get(key)
            .filter(|v| !v.is_null())
            .map(|v| v.as_u64().ok_or_else(protocol))
            .transpose()
    };
    let mut u = TokenUsage {
        input_tokens: count(if chat {
            "prompt_tokens"
        } else {
            "input_tokens"
        })?,
        output_tokens: count(if chat {
            "completion_tokens"
        } else {
            "output_tokens"
        })?,
        total_tokens: count("total_tokens")?,
        ..Default::default()
    };
    for (key, target) in [
        (
            if chat {
                "prompt_tokens_details"
            } else {
                "input_tokens_details"
            },
            &mut u.input_details,
        ),
        (
            if chat {
                "completion_tokens_details"
            } else {
                "output_tokens_details"
            },
            &mut u.output_details,
        ),
    ] {
        if let Some(details) = v.get(key).filter(|v| !v.is_null()) {
            for (k, n) in details.as_object().ok_or_else(protocol)? {
                if let Some(count) = n.as_u64() {
                    target.insert(k.clone(), count);
                } else {
                    // Nested usage is preserved verbatim in generation extras; accept only
                    // nested numeric counters, never discard arbitrary semantic metadata.
                    fn numeric_tree(value: &Value) -> bool {
                        value.as_u64().is_some()
                            || value
                                .as_object()
                                .is_some_and(|o| o.values().all(numeric_tree))
                    }
                    if !n.is_object() || !numeric_tree(n) {
                        return Err(protocol());
                    }
                }
            }
        }
    }
    for key in [
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
    ] {
        if let Some(n) = count(key)? {
            u.input_details.insert(key.into(), n);
        }
    }
    Ok(u)
}
