//! Additive, ordered transcript parts for native provider replay.
//!
//! Opaque state is sensitive history, not encrypted data. Applications must
//! protect stored transcripts. Older binaries cannot read rich messages.

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;

use serde::{Deserialize, Serialize};

use crate::{ContentPart, Message, ModelError, ToolCall};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RichRole {
    User,
    Assistant,
    Tool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HistoryPart {
    Text {
        text: String,
    },
    Image {
        mime_type: String,
        data_base64: String,
    },
    ToolCall {
        id: String,
        name: String,
        arguments_json: String,
    },
    ToolResult {
        call_id: String,
        name: String,
        content: Vec<ContentPart>,
        is_error: bool,
        details: Option<serde_json::Value>,
    },
    Reasoning {
        text: Option<String>,
        state: Option<ProviderState>,
    },
    ProviderState {
        state: ProviderState,
    },
}

// Never print caller content, tool arguments or provider state, including in nested Debug.
impl std::fmt::Debug for HistoryPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Text { .. } => "Text",
            Self::Image { .. } => "Image",
            Self::ToolCall { .. } => "ToolCall",
            Self::ToolResult { .. } => "ToolResult",
            Self::Reasoning { .. } => "Reasoning",
            Self::ProviderState { .. } => "ProviderState",
        };
        write!(f, "{kind}([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderState {
    pub provider: String,
    #[serde(deserialize_with = "deserialize_model")]
    pub model: Option<String>,
    pub format: String,
    pub payload: serde_json::Value,
    pub required_for_replay: bool,
}

fn deserialize_model<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

impl std::fmt::Debug for ProviderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderState([REDACTED])")
    }
}

/// Extract scheduler calls from an assistant message, rejecting malformed or
/// ambiguous calls. Tool declaration/schema and provider-state validation are
/// engine/adapter responsibilities and must precede commit or execution.
pub fn canonical_tool_calls(message: &Message) -> Result<Vec<ToolCall>, ModelError> {
    let calls = match message {
        Message::Assistant { tool_calls, .. } => tool_calls.clone(),
        Message::Rich {
            rich_role: RichRole::Assistant,
            parts,
        } => {
            let mut calls = Vec::new();
            for part in parts {
                match part {
                    HistoryPart::ToolCall {
                        id,
                        name,
                        arguments_json,
                    } => {
                        let arguments = serde_json::from_str(arguments_json)
                            .map_err(|_| ModelError::protocol("invalid tool arguments JSON"))?;
                        calls.push(ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            arguments,
                        });
                    }
                    HistoryPart::ToolResult { .. } => {
                        return Err(ModelError::protocol("tool result in assistant message"))
                    }
                    _ => {}
                }
            }
            calls
        }
        _ => return Err(ModelError::protocol("expected assistant message")),
    };
    let mut ids = std::collections::HashSet::new();
    for call in &calls {
        if call.id.trim().is_empty()
            || call.name.trim().is_empty()
            || !ids.insert(&call.id)
            || !call.arguments.is_object()
        {
            return Err(ModelError::protocol("invalid or duplicate tool call"));
        }
    }
    Ok(calls)
}
