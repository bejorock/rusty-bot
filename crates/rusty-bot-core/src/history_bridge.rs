//! Compatibility projection for providers implementing the original stream API.

use crate::{
    AssistantTurn, ChatRequest, ContentPart, HistoryPart, HistoryRequest, HistoryTurn, Message,
    ModelError, ProviderMessage, ProviderToolCall, RichRole,
};

/// Project legacy messages using the historical engine mapping. Rich messages
/// are accepted only when their ordered fields are fully representable.
pub fn project_history(request: HistoryRequest) -> Result<ChatRequest, ModelError> {
    let mut messages = Vec::new();
    for message in request.messages {
        let projected = match message {
            Message::System { content, .. } => {
                if !messages.is_empty() {
                    return Err(ModelError::unsupported("system message must lead history"));
                }
                ProviderMessage::System { content }
            }
            Message::User { content } => ProviderMessage::User {
                content: legacy_text(&content)?,
            },
            Message::Assistant {
                content,
                tool_calls,
                ..
            } => {
                let text = legacy_text(&content)?;
                ProviderMessage::Assistant {
                    content: (!text.is_empty()).then_some(text),
                    tool_calls: tool_calls
                        .into_iter()
                        .map(|call| ProviderToolCall {
                            id: call.id,
                            name: call.name,
                            arguments: call.arguments.to_string(),
                        })
                        .collect(),
                }
            }
            Message::ToolResult {
                tool_call_id,
                content,
                ..
            } => ProviderMessage::Tool {
                tool_call_id,
                content,
            },
            Message::Rich { rich_role, parts } => project_rich(rich_role, parts)?,
        };
        messages.push(projected);
    }
    Ok(ChatRequest {
        model: request.model,
        messages,
        tools: request.tools,
        tool_choice: request.tool_choice,
        temperature: request.temperature,
        max_tokens: request.max_tokens,
        metadata: request.metadata,
    })
}

fn legacy_text(parts: &[ContentPart]) -> Result<String, ModelError> {
    if parts.iter().any(|p| matches!(p, ContentPart::Image { .. })) {
        return Err(ModelError::unsupported(
            "image content is unsupported in user or assistant history",
        ));
    }
    Ok(Message::text_content(parts))
}

fn project_rich(role: RichRole, parts: Vec<HistoryPart>) -> Result<ProviderMessage, ModelError> {
    match role {
        RichRole::User => match parts.as_slice() {
            [HistoryPart::Text { text }] => Ok(ProviderMessage::User {
                content: text.clone(),
            }),
            _ => Err(ModelError::unsupported(
                "rich user history cannot be represented",
            )),
        },
        RichRole::Assistant => {
            let mut content = None;
            let mut tool_calls = Vec::new();
            for part in parts {
                match part {
                    HistoryPart::Text { text }
                        if content.is_none() && tool_calls.is_empty() && !text.is_empty() =>
                    {
                        content = Some(text)
                    }
                    HistoryPart::ToolCall {
                        id,
                        name,
                        arguments_json,
                    } => tool_calls.push(ProviderToolCall {
                        id,
                        name,
                        arguments: arguments_json,
                    }),
                    _ => {
                        return Err(ModelError::unsupported(
                            "rich assistant history cannot be represented",
                        ))
                    }
                }
            }
            Ok(ProviderMessage::Assistant {
                content,
                tool_calls,
            })
        }
        // The old tool message has no name, error flag or details.
        RichRole::Tool => Err(ModelError::unsupported(
            "rich tool history cannot be represented",
        )),
    }
}

/// Check duplicated legacy turn fields before forwarding the message unchanged.
pub fn history_turn_from_legacy(turn: AssistantTurn) -> Result<HistoryTurn, ModelError> {
    match &turn.message {
        Message::Assistant {
            tool_calls,
            usage,
            stop,
            ..
        } if *tool_calls == turn.tool_calls
            && usage.is_none_or(|v| v == turn.usage)
            && stop.is_none_or(|v| v == turn.stop) => {}
        _ => return Err(ModelError::protocol("inconsistent legacy assistant turn")),
    }
    Ok(HistoryTurn {
        message: turn.message,
        usage: turn.usage,
        stop: turn.stop,
        provider_metadata: turn.provider_metadata,
    })
}
