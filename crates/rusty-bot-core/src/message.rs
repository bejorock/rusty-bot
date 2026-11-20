use serde::{Deserialize, Serialize};

/// A stored transcript message with ordered content parts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    User {
        content: Vec<ContentPart>,
    },
    Assistant {
        content: Vec<ContentPart>,
        tool_calls: Vec<ToolCall>,
        usage: Option<Usage>,
        stop: Option<StopReason>,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        content: Vec<ContentPart>,
        is_error: bool,
        details: Option<serde_json::Value>,
    },
    /// Additive replay-capable history. This variant is a source break for
    /// exhaustive matches; old binaries cannot read rich entries.
    Rich {
        rich_role: crate::history::RichRole,
        parts: Vec<crate::history::HistoryPart>,
    },
    System {
        /// Empty in v1; the effective system prompt is replayed from `sections`.
        content: String,
        /// Ordered patch replay over the system prompt sections.
        sections: std::collections::BTreeMap<String, String>,
        tools_added: Vec<String>,
        tools_removed: Vec<String>,
    },
}

impl Message {
    pub fn user_text(text: impl Into<String>) -> Self {
        Message::User {
            content: vec![ContentPart::Text { text: text.into() }],
        }
    }

    /// Display-only extraction; never use this to propagate model content.
    pub fn text_content(parts: &[ContentPart]) -> String {
        parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                ContentPart::Image { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
    },
    Image {
        mime_type: String,
        data_base64: String,
    },
}

impl std::fmt::Debug for ContentPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text { text } => f.debug_struct("Text").field("text", text).finish(),
            Self::Image {
                mime_type,
                data_base64,
            } => f
                .debug_struct("Image")
                .field("mime_type", mime_type)
                .field(
                    "data_base64",
                    &format_args!("[REDACTED; {} chars]", data_base64.len()),
                )
                .finish(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Stop,
    Length,
    ToolUse,
    Error,
    Overflow,
}

/// A context edit reference stored on an entry. Compaction/fork semantics are
/// deferred; the shape is persisted so history remains self-describing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextEdit {
    pub id: EntryId,
    pub kind: EditKind,
    pub data: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditKind {
    Compaction,
    Fork,
    Custom,
}

use crate::ids::EntryId;
