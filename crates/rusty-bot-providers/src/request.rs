use crate::cache::{
    AnthropicCacheTtl, CacheMarker, OpenAiCacheRetention, OpenAiResponsesCacheMode,
    OpenAiResponsesCacheTtl, PromptCache,
};
use crate::ModelError;
use crate::{
    invalid, unsupported, wire, Api, GenerationRequest, OutputFormat, ProviderConfig, Reasoning,
    ReasoningEffort, ToolChoice,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rusty_bot_messages::{ContentBlock as B, ConversationMessage as M, MediaSource, MessageStatus};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub(crate) fn validate_config(api: Api, config: &ProviderConfig) -> Result<(), ModelError> {
    validate_cache_config(api, config.prompt_cache.as_ref())?;
    match &config.output_format {
        OutputFormat::Text => {}
        OutputFormat::JsonObject if api != Api::Anthropic => {}
        OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } => {
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || !schema.is_object()
            {
                return Err(invalid("invalid output schema or name"));
            }
            if api == Api::Deepseek || (api == Api::Anthropic && !strict) {
                return Err(unsupported());
            }
        }
        _ => return Err(unsupported()),
    }
    if config.thinking_budget.is_some() && config.reasoning != Reasoning::Default {
        return Err(invalid("conflicting reasoning settings"));
    }
    if config.temperature.is_some_and(|v| {
        !v.is_finite() || !(0.0..=if api == Api::Anthropic { 1.0 } else { 2.0 }).contains(&v)
    }) || config
        .top_p
        .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
    {
        return Err(invalid("invalid sampling settings"));
    }
    let sampling = config.temperature.is_some() || config.top_p.is_some();
    let active = config.thinking_budget.is_some()
        || matches!(
            config.reasoning,
            Reasoning::Enabled { .. } | Reasoning::Adaptive { .. }
        );
    if api == Api::Anthropic
        && ((config.temperature.is_some() && config.top_p.is_some())
            || (active && sampling)
            || (active
                && matches!(
                    config.tool_choice,
                    ToolChoice::Required | ToolChoice::Named { .. }
                )))
    {
        return Err(invalid("incompatible Anthropic settings"));
    }
    if api == Api::Deepseek && sampling && config.reasoning != Reasoning::Disabled {
        return Err(invalid("DeepSeek sampling requires disabled thinking"));
    }
    if api == Api::Deepseek
        && matches!(
            config.tool_choice,
            ToolChoice::Required | ToolChoice::Named { .. }
        )
        && config.reasoning != Reasoning::Disabled
    {
        return Err(invalid(
            "DeepSeek forced tool choice requires disabled thinking",
        ));
    }
    match (api, config.reasoning) {
        (_, Reasoning::Default | Reasoning::Disabled) => {}
        (
            Api::Responses | Api::Chat,
            Reasoning::Enabled {
                budget_tokens: None,
                ..
            },
        ) => {}
        (
            Api::Anthropic,
            Reasoning::Enabled {
                effort: None,
                budget_tokens: Some(b),
            },
        ) if b >= 1024 && b < config.max_output_tokens => {}
        (Api::Anthropic, Reasoning::Adaptive { effort })
            if effort != Some(ReasoningEffort::Minimal) => {}
        (
            Api::Deepseek,
            Reasoning::Enabled {
                budget_tokens: None,
                effort:
                    None | Some(ReasoningEffort::Low | ReasoningEffort::High | ReasoningEffort::Max),
            },
        ) => {}
        _ => return Err(invalid("unsupported reasoning settings")),
    }
    Ok(())
}

pub(crate) fn build(
    api: Api,
    config: &ProviderConfig,
    req: &GenerationRequest,
) -> Result<Value, ModelError> {
    validate_config(api, config)?;
    if req.message_id.trim().is_empty()
        || req.history.entries.iter().any(|e| e.id == req.message_id)
    {
        return Err(invalid("invalid output message identity"));
    }
    req.history
        .validate_complete()
        .map_err(|_| invalid("invalid or incomplete request history"))?;
    if req
        .history
        .entries
        .iter()
        .any(|e| e.status != MessageStatus::Completed)
    {
        return Err(unsupported());
    }
    let markers = validate_markers(api, config.prompt_cache.as_ref(), req)?;
    let mut names = BTreeSet::new();
    for tool in &req.tools {
        if tool.name.is_empty()
            || tool.name.len() > 64
            || !tool
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || !tool.parameters.is_object()
            || !names.insert(&tool.name)
        {
            return Err(invalid("invalid tool declaration"));
        }
    }
    if config.tool_options.keys().any(|name| !names.contains(name)) {
        return Err(invalid("tool options reference an undeclared tool"));
    }
    if config.parallel_tool_calls.is_some() {
        if req.tools.is_empty() {
            return Err(invalid("parallel tool calls require tools"));
        }
        if api == Api::Deepseek {
            return Err(unsupported());
        }
        if api == Api::Anthropic && matches!(config.tool_choice, ToolChoice::None) {
            return Err(invalid(
                "parallel tool control cannot be combined with Anthropic tool choice none",
            ));
        }
    }
    if api == Api::Deepseek
        && config
            .tool_options
            .values()
            .any(|options| options.strict == Some(true))
    {
        if req.tools.iter().any(|tool| {
            config
                .tool_options
                .get(&tool.name)
                .and_then(|options| options.strict)
                != Some(true)
        }) {
            return Err(invalid(
                "DeepSeek strict mode requires all tools to be strict",
            ));
        }
        let beta_endpoint = url::Url::parse(&config.endpoint)
            .ok()
            .is_some_and(|url| url.path().ends_with("/beta/chat/completions"));
        if !beta_endpoint {
            return Err(invalid(
                "DeepSeek strict mode requires a beta chat completions endpoint",
            ));
        }
    }
    match &config.tool_choice {
        ToolChoice::Required if req.tools.is_empty() => {
            return Err(invalid("tool choice requires tools"));
        }
        ToolChoice::Named { name } if !names.contains(name) => {
            return Err(invalid("named tool is not declared"));
        }
        _ => {}
    }
    let mut messages = Vec::new();
    let mut system = Vec::new();
    let mut seen_conversation = false;
    let mut image_bytes = 0usize;
    for entry in &req.history.entries {
        let message = &entry.message;
        if api == Api::Anthropic && matches!(message, M::System { .. }) {
            if seen_conversation {
                return Err(unsupported());
            }
            for (index, b) in message.content().iter().enumerate() {
                match b {
                    B::Text { text } => {
                        let mut value = json!({"type":"text", "text":text});
                        mark_block(
                            api,
                            &mut value,
                            markers.get(&(entry.id.as_str(), index)).copied(),
                        );
                        system.push(value);
                    }
                    _ => return Err(unsupported()),
                }
            }
            continue;
        }
        seen_conversation = true;
        let role = match message {
            M::System { .. } => "system",
            M::Developer { .. } if matches!(api, Api::Responses | Api::Chat) => "developer",
            M::Developer { .. } => return Err(unsupported()),
            M::User { .. } => "user",
            M::Assistant { .. } => "assistant",
            M::Tool { .. } => "tool",
        };
        let (native_replay, portable) = replay(api, &config.model, message)?;
        if let Some(replay) = native_replay {
            match api {
                Api::Responses => {
                    messages.extend(replay.as_array().ok_or_else(unsupported)?.clone())
                }
                Api::Anthropic => messages.push(json!({"role":"assistant", "content":replay})),
                _ => messages.push(replay),
            }
            continue;
        }
        if let M::Tool {
            call_id,
            content,
            is_error,
            ..
        } = message
        {
            let text = text(content)?;
            match api {
                Api::Responses if !is_error => messages.push(json!({"type":"function_call_output", "call_id":call_id, "output":text})),
                Api::Anthropic => messages.push(json!({"role":"user", "content":[{"type":"tool_result", "tool_use_id":call_id, "content":text, "is_error":is_error}]})),
                Api::Chat | Api::Deepseek if !is_error => messages.push(json!({"role":"tool", "tool_call_id":call_id, "content":text})),
                _ => return Err(unsupported()),
            }
            continue;
        }
        let mut content = Vec::new();
        let mut calls = Vec::new();
        let mut call_seen = false;
        for (index, block) in message.content().iter().enumerate() {
            let previous_len = content.len();
            match block {
                B::Replay { .. } | B::Reasoning { .. } if portable => continue,
                B::Extension { namespace, kind, value }
                    if portable && namespace == "anthropic" && kind == "redacted_thinking" && value.is_null() => continue,
                B::Text { text } | B::Refusal { text } if (!call_seen || api == Api::Anthropic) && (portable || matches!(block, B::Text { .. })) => content.push(match api {
                    Api::Responses => json!({"type":if role == "assistant" {"output_text"} else {"input_text"}, "text":text}),
                    _ => json!({"type":"text", "text":text}),
                }),
                B::Image { source } if role == "user" => {
                    content.push(image_block(api, source, &mut image_bytes)?);
                }
                B::ToolCall { call } if role == "assistant" => {
                    // Portable replay lacks the provider-specific reasoning required
                    // for thinking-mode tool continuations, including Anthropic signatures.
                    if portable
                        && ((api == Api::Deepseek && config.reasoning != Reasoning::Disabled)
                            || (api == Api::Anthropic
                                && (config.thinking_budget.is_some()
                                    || matches!(config.reasoning, Reasoning::Enabled { .. } | Reasoning::Adaptive { .. }))))
                    {
                        return Err(unsupported());
                    }
                    call_seen = true;
                    match api {
                        Api::Responses => calls.push(json!({"type":"function_call", "call_id":call.id, "name":call.name, "arguments":call.arguments.to_string()})),
                        Api::Anthropic => content.push(json!({"type":"tool_use", "id":call.id, "name":call.name, "input":call.arguments})),
                        _ => calls.push(json!({"id":call.id, "type":"function", "function":{"name":call.name, "arguments":call.arguments.to_string()}})),
                    }
                }
                _ => return Err(unsupported()),
            }
            if let Some(marker) = markers.get(&(entry.id.as_str(), index)) {
                if content.len() != previous_len + 1 {
                    return Err(unsupported());
                }
                mark_block(
                    api,
                    content.last_mut().ok_or_else(unsupported)?,
                    Some(marker),
                );
            }
        }
        if portable && content.is_empty() && calls.is_empty() {
            return Err(unsupported());
        }
        match api {
            Api::Responses => {
                if !content.is_empty() {
                    messages.push(json!({"role":role, "content":content}));
                }
                messages.extend(calls);
            }
            Api::Anthropic => messages.push(json!({"role":role, "content":content})),
            _ => {
                // Chat wire has one text string for assistant; refuse multiple independently ordered text blocks.
                let value = if role == "assistant" {
                    if content.len() > 1 {
                        return Err(unsupported());
                    }
                    content
                        .first()
                        .map(|v| v["text"].clone())
                        .unwrap_or(Value::Null)
                } else if api == Api::Deepseek
                    && message
                        .content()
                        .iter()
                        .all(|b| matches!(b, B::Text { .. }))
                {
                    json!(text(message.content())?)
                } else {
                    json!(content)
                };
                let mut m = json!({"role":role, "content":value});
                if !calls.is_empty() {
                    m["tool_calls"] = json!(calls);
                }
                messages.push(m);
            }
        }
    }
    let tools: Vec<_> = req.tools.iter().map(|t| {
        let mut tool = match api {
            Api::Responses => json!({"type":"function", "name":t.name, "description":t.description, "parameters":t.parameters}),
            Api::Anthropic => json!({"name":t.name, "description":t.description, "input_schema":t.parameters}),
            _ => json!({"type":"function", "function":{"name":t.name, "description":t.description, "parameters":t.parameters}}),
        };
        if let Some(strict) = config.tool_options.get(&t.name).and_then(|options| options.strict) {
            if matches!(api, Api::Chat | Api::Deepseek) {
                tool["function"]["strict"] = json!(strict);
            } else {
                tool["strict"] = json!(strict);
            }
        }
        tool
    }).collect();
    let mut body = json!({"model":config.model,"stream":true});
    match api {
        Api::Responses => {
            body["input"] = json!(messages);
            body["store"] = json!(false);
            body["include"] = json!(["reasoning.encrypted_content"]);
            body["max_output_tokens"] = json!(config.max_output_tokens);
        }
        Api::Anthropic => {
            body["messages"] = json!(messages);
            if !system.is_empty() {
                body["system"] = json!(system);
            }
            body["max_tokens"] = json!(config.max_output_tokens);
            if let Some(b) = config.thinking_budget {
                body["thinking"] = json!({"type":"enabled", "budget_tokens":b});
            }
        }
        _ => {
            body["messages"] = json!(messages);
            body["stream_options"] = json!({"include_usage":true});
            body[if api == Api::Chat {
                "max_completion_tokens"
            } else {
                "max_tokens"
            }] = json!(config.max_output_tokens);
        }
    }
    apply_cache(&mut body, config.prompt_cache.as_ref());
    if let Some(v) = config.temperature {
        body["temperature"] = json!(v);
    }
    if let Some(v) = config.top_p {
        body["top_p"] = json!(v);
    }
    match (api, config.reasoning) {
        (_, Reasoning::Default) => {}
        (Api::Responses, Reasoning::Disabled) => body["reasoning"] = json!({"effort":"none"}),
        (Api::Chat, Reasoning::Disabled) => body["reasoning_effort"] = json!("none"),
        (Api::Anthropic | Api::Deepseek, Reasoning::Disabled) => {
            body["thinking"] = json!({"type":"disabled"})
        }
        (Api::Responses, Reasoning::Enabled { effort, .. }) => {
            body["reasoning"] = json!({"effort":effort.unwrap_or(ReasoningEffort::Medium).as_str()})
        }
        (Api::Chat, Reasoning::Enabled { effort, .. }) => {
            body["reasoning_effort"] = json!(effort.unwrap_or(ReasoningEffort::Medium).as_str())
        }
        (
            Api::Anthropic,
            Reasoning::Enabled {
                budget_tokens: Some(b),
                ..
            },
        ) => body["thinking"] = json!({"type":"enabled", "budget_tokens":b}),
        (Api::Anthropic, Reasoning::Adaptive { effort }) => {
            body["thinking"] = json!({"type":"adaptive"});
            if let Some(e) = effort {
                body["output_config"] = json!({"effort":e.as_str()});
            }
        }
        (Api::Deepseek, Reasoning::Enabled { effort, .. }) => {
            body["thinking"] = json!({"type":"enabled"});
            if let Some(e) = effort {
                body["reasoning_effort"] = json!(e.as_str());
            }
        }
        _ => return Err(invalid("unsupported reasoning settings")),
    }
    match &config.output_format {
        OutputFormat::Text => {}
        OutputFormat::JsonObject => match api {
            Api::Responses => body["text"] = json!({"format":{"type":"json_object"}}),
            Api::Chat | Api::Deepseek => body["response_format"] = json!({"type":"json_object"}),
            Api::Anthropic => return Err(unsupported()),
        },
        OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } => match api {
            Api::Responses => {
                body["text"] = json!({"format":{"type":"json_schema", "name":name, "schema":schema, "strict":strict}})
            }
            Api::Chat => {
                body["response_format"] = json!({"type":"json_schema", "json_schema":{"name":name, "schema":schema, "strict":strict}})
            }
            Api::Anthropic if *strict => {
                body["output_config"]["format"] = json!({"type":"json_schema", "schema":schema})
            }
            _ => return Err(unsupported()),
        },
    }
    match &config.tool_choice {
        ToolChoice::Auto => {}
        ToolChoice::None => {
            body["tool_choice"] = if api == Api::Anthropic {
                json!({"type":"none"})
            } else {
                json!("none")
            }
        }
        ToolChoice::Required => {
            body["tool_choice"] = if api == Api::Anthropic {
                json!({"type":"any"})
            } else {
                json!("required")
            }
        }
        ToolChoice::Named { name } => {
            body["tool_choice"] = match api {
                Api::Anthropic => json!({"type":"tool", "name":name}),
                Api::Responses => json!({"type":"function", "name":name}),
                _ => json!({"type":"function", "function":{"name":name}}),
            }
        }
    }
    if let Some(parallel) = config.parallel_tool_calls {
        if api == Api::Anthropic {
            if body.get("tool_choice").is_none() {
                body["tool_choice"] = json!({"type":"auto"});
            }
            body["tool_choice"]["disable_parallel_tool_use"] = json!(!parallel);
        } else {
            body["parallel_tool_calls"] = json!(parallel);
        }
    }
    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }
    Ok(body)
}
fn text(blocks: &[B]) -> Result<String, ModelError> {
    // No implicit separators: multiple blocks are not one wire text block.
    match blocks {
        [] => Ok(String::new()),
        [B::Text { text }] => Ok(text.clone()),
        _ => Err(unsupported()),
    }
}
fn image_block(api: Api, source: &MediaSource, total: &mut usize) -> Result<Value, ModelError> {
    let (anthropic_source, url) = match source {
        MediaSource::Url { url, .. } => {
            // Preserve the caller's exact URL. Parsing is only validation; never fetch it.
            let parsed = url::Url::parse(url).map_err(|_| invalid("invalid image URL"))?;
            if parsed.scheme() != "https"
                || parsed.host().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.fragment().is_some()
                || url
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
                || url.split_once("://").is_none_or(|(_, rest)| {
                    rest.split(['/', '?', '#'])
                        .next()
                        .is_none_or(|authority| authority.is_empty() || authority.contains('@'))
                })
            {
                return Err(invalid("invalid image URL"));
            }
            (json!({"type":"url", "url":url}), url.clone())
        }
        MediaSource::Inline { .. } => {
            let (mime, data) = image(source, total)?;
            (
                json!({"type":"base64", "media_type":mime, "data":data}),
                format!("data:{mime};base64,{data}"),
            )
        }
        MediaSource::Reference { .. } => return Err(unsupported()),
    };
    Ok(match api {
        Api::Anthropic => json!({"type":"image", "source":anthropic_source}),
        Api::Responses => json!({"type":"input_image", "image_url":url}),
        _ => json!({"type":"image_url", "image_url":{"url":url}}),
    })
}
fn image<'a>(source: &'a MediaSource, total: &mut usize) -> Result<(&'a str, &'a str), ModelError> {
    let MediaSource::Inline { mime_type, base64 } = source else {
        return Err(unsupported());
    };
    if !matches!(
        mime_type.as_str(),
        "image/png" | "image/jpeg" | "image/webp" | "image/gif"
    ) || base64.len() > 8 * 1024 * 1024
    {
        return Err(unsupported());
    }
    let decoded = STANDARD
        .decode(base64)
        .map_err(|_| invalid("invalid inline image"))?;
    *total = total.saturating_add(decoded.len());
    let magic = match mime_type.as_str() {
        "image/png" => decoded.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => decoded.starts_with(b"\xff\xd8\xff"),
        "image/gif" => decoded.starts_with(b"GIF87a") || decoded.starts_with(b"GIF89a"),
        _ => decoded.starts_with(b"RIFF") && decoded.get(8..12) == Some(b"WEBP"),
    };
    if decoded.is_empty()
        || *total > 6 * 1024 * 1024
        || !magic
        || STANDARD.encode(&decoded) != *base64
    {
        return Err(invalid("invalid or oversized inline image"));
    }
    let format = match mime_type.as_str() {
        "image/png" => image::ImageFormat::Png,
        "image/jpeg" => image::ImageFormat::Jpeg,
        "image/gif" => image::ImageFormat::Gif,
        _ => image::ImageFormat::WebP,
    };
    let limits = crate::ImageLimits {
        max_bytes: 6 * 1024 * 1024,
        max_width: 8192,
        max_height: 8192,
    };
    let animated = crate::image_validation::validate_image_bytes(&decoded, format, &limits)
        .map_err(|_| invalid("invalid or oversized inline image"))?;
    if animated {
        return Err(unsupported());
    }
    Ok((mime_type, base64))
}
fn replay(api: Api, model: &str, message: &M) -> Result<(Option<Value>, bool), ModelError> {
    let Some(B::Replay { state }) = message.content().last() else {
        return Ok((None, false));
    };
    if !matches!(message, M::Assistant { .. })
        || message
            .content()
            .iter()
            .any(|b| matches!(b, B::InvalidToolCall { .. }))
    {
        return Err(unsupported());
    }
    // An explicitly different provider, model or format is portable, not native replay.
    // Missing model identity and unsupported versions on the same target fail closed.
    if state.provider != api.provider()
        || state.format != api.format()
        || state.model.as_deref().is_some_and(|source| source != model)
    {
        // Multiple replay blocks are ambiguous and may hide same-target state.
        if message.content()[..message.content().len() - 1]
            .iter()
            .any(|b| matches!(b, B::Replay { .. }))
        {
            return Err(unsupported());
        }
        return Ok((None, true));
    }
    if !state.compatible_with(api.provider(), model, api.format(), 1)
        || state.model.as_deref() != Some(model)
    {
        return Err(unsupported());
    }
    let projected = wire::project(api, &state.payload).map_err(|_| unsupported())?;
    if projected.as_slice() != &message.content()[..message.content().len() - 1] {
        return Err(unsupported());
    }
    Ok((Some(state.payload.clone()), false))
}

fn validate_cache_config(api: Api, cache: Option<&PromptCache>) -> Result<(), ModelError> {
    match (api, cache) {
        (_, None) => Ok(()),
        (
            Api::Responses | Api::Chat,
            Some(PromptCache::OpenAi {
                key,
                retention,
                responses,
            }),
        ) => {
            if key
                .as_ref()
                .is_some_and(|k| k.trim().is_empty() || k.len() > 64)
            {
                return Err(invalid("invalid prompt cache key"));
            }
            if responses.is_some() && (api != Api::Responses || retention.is_some()) {
                return Err(unsupported());
            }
            if let Some(options) = responses {
                let max_markers = if options.mode == crate::OpenAiResponsesCacheMode::Implicit {
                    3
                } else {
                    4
                };
                if options.markers.len() > max_markers {
                    return Err(invalid("too many OpenAI cache markers"));
                }
                if options.markers.iter().any(|m| m.ttl.is_some()) {
                    return Err(unsupported());
                }
            }
            Ok(())
        }
        (Api::Anthropic, Some(PromptCache::Anthropic { markers })) => {
            if markers.len() > 4 {
                return Err(invalid("too many Anthropic cache markers"));
            }
            Ok(())
        }
        _ => Err(unsupported()),
    }
}

fn validate_markers<'a>(
    api: Api,
    cache: Option<&'a PromptCache>,
    req: &GenerationRequest,
) -> Result<std::collections::BTreeMap<(&'a str, usize), &'a CacheMarker>, ModelError> {
    let markers = match cache {
        Some(PromptCache::OpenAi {
            responses: Some(options),
            ..
        }) => &options.markers[..],
        Some(PromptCache::Anthropic { markers }) => &markers[..],
        _ => &[],
    };
    let mut result = std::collections::BTreeMap::new();
    let mut order = Vec::new();
    for marker in markers {
        if result
            .insert((marker.message_id.as_str(), marker.block_index), marker)
            .is_some()
        {
            return Err(invalid("duplicate cache marker"));
        }
        let Some((entry_index, entry)) = req
            .history
            .entries
            .iter()
            .enumerate()
            .find(|(_, e)| e.id == marker.message_id)
        else {
            return Err(invalid("cache marker references missing entry"));
        };
        let Some(block) = entry.message.content().get(marker.block_index) else {
            return Err(invalid("cache marker references missing block"));
        };
        let supported = match (&entry.message, block) {
            (M::System { .. }, B::Text { .. }) => true,
            (M::Developer { .. }, B::Text { .. }) => api == Api::Responses,
            (M::User { .. }, B::Text { .. } | B::Image { .. }) => true,
            _ => false,
        };
        if !supported {
            return Err(unsupported());
        }
        order.push((entry_index, marker.block_index, marker));
    }
    if api == Api::Anthropic {
        // Provider requires longer TTL breakpoints before shorter TTL breakpoints.
        order.sort_by_key(|(entry, block, _)| (*entry, *block));
        let mut short_seen = false;
        for (_, _, marker) in order {
            match marker.ttl {
                None | Some(AnthropicCacheTtl::FiveMinutes) => short_seen = true,
                Some(AnthropicCacheTtl::OneHour) if short_seen => {
                    return Err(invalid("Anthropic cache TTLs must be in descending order"));
                }
                _ => {}
            }
        }
    }
    Ok(result)
}

fn mark_block(api: Api, block: &mut Value, marker: Option<&CacheMarker>) {
    let Some(marker) = marker else { return };
    match api {
        Api::Responses => block["prompt_cache_breakpoint"] = json!({"mode":"explicit"}),
        Api::Anthropic => {
            let mut control = json!({"type":"ephemeral"});
            if let Some(ttl) = marker.ttl {
                control["ttl"] = json!(match ttl {
                    AnthropicCacheTtl::FiveMinutes => "5m",
                    AnthropicCacheTtl::OneHour => "1h",
                });
            }
            block["cache_control"] = control;
        }
        _ => unreachable!("cache marker validated for API"),
    }
}

fn apply_cache(body: &mut Value, cache: Option<&PromptCache>) {
    if let Some(PromptCache::OpenAi {
        key,
        retention,
        responses,
    }) = cache
    {
        if let Some(key) = key {
            body["prompt_cache_key"] = json!(key);
        }
        if let Some(retention) = retention {
            body["prompt_cache_retention"] = json!(match retention {
                OpenAiCacheRetention::InMemory => "in_memory",
                OpenAiCacheRetention::TwentyFourHours => "24h",
            });
        }
        if let Some(options) = responses {
            let mut value = json!({"mode": match options.mode {
                OpenAiResponsesCacheMode::Implicit => "implicit", OpenAiResponsesCacheMode::Explicit => "explicit"
            }});
            if let Some(ttl) = options.ttl {
                value["ttl"] = json!(match ttl {
                    OpenAiResponsesCacheTtl::ThirtyMinutes => "30m",
                    OpenAiResponsesCacheTtl::TwelveHours => "12h",
                });
            }
            body["prompt_cache_options"] = value;
        }
    }
}
