//! Provider-neutral conversation data. This is not a provider request format.
use crate::{GenerationMetadata, MessageStatus, ReplayState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MediaSource {
    Inline {
        mime_type: String,
        base64: String,
    },
    Url {
        mime_type: String,
        url: String,
    },
    /// Opaque application-managed key, not a local path implicitly opened by the SDK.
    Reference {
        mime_type: String,
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Image {
        source: MediaSource,
    },
    Audio {
        source: MediaSource,
    },
    Video {
        source: MediaSource,
    },
    File {
        source: MediaSource,
        name: Option<String>,
    },
    Json {
        value: Value,
    },
    /// Displayable reasoning only; not a substitute for provider-specific replay state.
    Reasoning {
        text: String,
    },
    ToolCall {
        call: ToolInvocation,
    },
    /// Complete but malformed provider call; never executable.
    InvalidToolCall {
        id: Option<String>,
        name: Option<String>,
        arguments_text: String,
        error: String,
    },
    Refusal {
        text: String,
    },
    /// Provider-unique content. An adapter must explicitly handle it or refuse projection.
    Extension {
        namespace: String,
        kind: String,
        value: Value,
    },
    Replay {
        state: ReplayState,
    },
    /// Optional per-block identity, citations and replay material without duplicating content.
    Annotated {
        id: Option<String>,
        block: Box<ContentBlock>,
        #[serde(default)]
        annotations: Vec<ContentAnnotation>,
        #[serde(default)]
        replay: Vec<ReplayState>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAnnotation {
    pub namespace: String,
    pub kind: String,
    pub value: Value,
}
impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
    pub fn tool_call(&self) -> Option<&ToolInvocation> {
        match self {
            Self::ToolCall { call } => Some(call),
            Self::Annotated { block, .. } => block.tool_call(),
            _ => None,
        }
    }
    fn validate(&self) -> Result<(), HistoryError> {
        match self {
            Self::Image { source }
            | Self::Audio { source }
            | Self::Video { source }
            | Self::File { source, .. } => {
                let (mime, data) = match source {
                    MediaSource::Inline { mime_type, base64 } => (mime_type, base64),
                    MediaSource::Url { mime_type, url } => (mime_type, url),
                    MediaSource::Reference { mime_type, key } => (mime_type, key),
                };
                if mime.trim().is_empty() || data.trim().is_empty() {
                    return Err(HistoryError(
                        "media requires MIME type and a nonblank source".into(),
                    ));
                }
            }
            Self::ToolCall { call } => call.validate()?,
            Self::InvalidToolCall { error, .. } if error.trim().is_empty() => {
                return Err(HistoryError("invalid tool call requires an error".into()));
            }
            Self::Extension {
                namespace, kind, ..
            } if namespace.trim().is_empty() || kind.trim().is_empty() => {
                return Err(HistoryError("extension requires namespace and kind".into()));
            }
            Self::Replay { state } => state.validate()?,
            Self::Annotated {
                id,
                block,
                annotations,
                replay,
            } => {
                if id.as_ref().is_some_and(|id| id.trim().is_empty())
                    || matches!(**block, Self::Annotated { .. })
                {
                    return Err(HistoryError(
                        "invalid block identity or nested annotation wrapper".into(),
                    ));
                }
                block.validate()?;
                for annotation in annotations {
                    if annotation.namespace.trim().is_empty() || annotation.kind.trim().is_empty() {
                        return Err(HistoryError(
                            "annotation requires namespace and kind".into(),
                        ));
                    }
                }
                for state in replay {
                    state.validate()?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    /// Return replay material in block order, including redacted reasoning-only state.
    pub fn replay_states(&self) -> Vec<&ReplayState> {
        match self {
            Self::Replay { state } => vec![state],
            Self::Annotated { block, replay, .. } => {
                let mut states = block.replay_states();
                states.extend(replay);
                states
            }
            _ => vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

impl ToolInvocation {
    pub fn validate(&self) -> Result<(), HistoryError> {
        if self.id.trim().is_empty() || self.name.trim().is_empty() || !self.arguments.is_object() {
            return Err(HistoryError(
                "tool calls require IDs, names and object arguments".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ConversationMessage {
    System {
        content: Vec<ContentBlock>,
    },
    Developer {
        content: Vec<ContentBlock>,
    },
    User {
        content: Vec<ContentBlock>,
    },
    Assistant {
        content: Vec<ContentBlock>,
    },
    Tool {
        call_id: String,
        name: String,
        content: Vec<ContentBlock>,
        is_error: bool,
    },
}
impl ConversationMessage {
    pub fn system(text: impl Into<String>) -> Self {
        Self::System {
            content: vec![ContentBlock::text(text)],
        }
    }
    pub fn user(text: impl Into<String>) -> Self {
        Self::User {
            content: vec![ContentBlock::text(text)],
        }
    }
    pub fn assistant(text: impl Into<String>) -> Self {
        Self::Assistant {
            content: vec![ContentBlock::text(text)],
        }
    }
    pub fn content(&self) -> &[ContentBlock] {
        match self {
            Self::System { content }
            | Self::Developer { content }
            | Self::User { content }
            | Self::Assistant { content, .. }
            | Self::Tool { content, .. } => content,
        }
    }
    pub fn tool_calls(&self) -> impl Iterator<Item = &ToolInvocation> {
        self.content().iter().filter_map(ContentBlock::tool_call)
    }
    /// Strict legacy text projection; never drop replay state, extensions or reasoning.
    pub fn text_only(&self) -> Result<String, HistoryError> {
        self.content()
            .iter()
            .map(|block| match block {
                ContentBlock::Text { text } => Ok(text.as_str()),
                _ => Err(HistoryError(
                    "message is not losslessly representable as text".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| parts.concat())
    }
    /// Before projection, adapters must explicitly declare supported replay formats.
    /// Optional unsupported state may be omitted; required unsupported state is an error.
    pub fn validate_replay_for(
        &self,
        mut supports: impl FnMut(&ReplayState) -> bool,
    ) -> Result<(), HistoryError> {
        for block in self.content() {
            for state in block.replay_states() {
                state.validate()?;
                if state.required_for_replay && !supports(state) {
                    return Err(HistoryError(
                        "required provider replay state is incompatible".into(),
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<(), HistoryError> {
        if self.content().is_empty() && !matches!(self, Self::Tool { .. }) {
            return Err(HistoryError(
                "message requires content or assistant tool calls".into(),
            ));
        }
        let mut block_ids = BTreeSet::new();
        let mut call_ids = BTreeSet::new();
        for part in self.content() {
            part.validate()?;
            if let ContentBlock::Annotated { id: Some(id), .. } = part {
                if !block_ids.insert(id) {
                    return Err(HistoryError("duplicate content block ID".into()));
                }
            }
            let inner = match part {
                ContentBlock::Annotated { block, .. } => block.as_ref(),
                _ => part,
            };
            if matches!(
                inner,
                ContentBlock::ToolCall { .. }
                    | ContentBlock::InvalidToolCall { .. }
                    | ContentBlock::Reasoning { .. }
                    | ContentBlock::Replay { .. }
                    | ContentBlock::Refusal { .. }
            ) && !matches!(self, Self::Assistant { .. })
            {
                return Err(HistoryError(
                    "assistant-only content in another message role".into(),
                ));
            }
            if let Some(call) = part.tool_call() {
                if !call_ids.insert(&call.id) {
                    return Err(HistoryError("duplicate tool call ID".into()));
                }
            }
        }
        if let Self::Tool { call_id, name, .. } = self {
            if call_id.trim().is_empty() || name.trim().is_empty() {
                return Err(HistoryError("tool result requires call ID and name".into()));
            }
        }
        Ok(())
    }
}

// Read legacy split assistant calls, but always write one ordered representation.
impl<'de> Deserialize<'de> for ConversationMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "role", rename_all = "snake_case")]
        enum Wire {
            System {
                content: Vec<ContentBlock>,
            },
            Developer {
                content: Vec<ContentBlock>,
            },
            User {
                content: Vec<ContentBlock>,
            },
            Assistant {
                content: Vec<ContentBlock>,
                #[serde(default)]
                tool_calls: Vec<ToolInvocation>,
            },
            Tool {
                call_id: String,
                name: String,
                content: Vec<ContentBlock>,
                is_error: bool,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::System { content } => Self::System { content },
            Wire::Developer { content } => Self::Developer { content },
            Wire::User { content } => Self::User { content },
            Wire::Assistant {
                mut content,
                tool_calls,
            } => {
                if !tool_calls.is_empty() && content.iter().any(|p| p.tool_call().is_some()) {
                    return Err(serde::de::Error::custom(
                        "ambiguous mixed tool call representations",
                    ));
                }
                content.extend(
                    tool_calls
                        .into_iter()
                        .map(|call| ContentBlock::ToolCall { call }),
                );
                Self::Assistant { content }
            }
            Wire::Tool {
                call_id,
                name,
                content,
                is_error,
            } => Self::Tool {
                call_id,
                name,
                content,
                is_error,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Stable application identifier; generated SDK turn entries use the `turn:` prefix.
    pub id: String,
    pub turn_id: Option<u64>,
    pub message: ConversationMessage,
    pub status: MessageStatus,
    /// Human-readable failure, not a successful assistant answer.
    pub failure: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    /// Unix milliseconds, supplied by the caller/Activity, never a workflow wall clock read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<GenerationMetadata>,
    /// Application-only output (e.g. full tool results). Not model content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<Value>,
}
impl HistoryEntry {
    pub fn validate(&self) -> Result<(), HistoryError> {
        if self.id.trim().is_empty()
            || (self.failure.is_some() && self.status != MessageStatus::Failed)
        {
            return Err(HistoryError(
                "invalid history identity or failure status".into(),
            ));
        }
        if self.generation.is_some()
            && !matches!(self.message, ConversationMessage::Assistant { .. })
        {
            return Err(HistoryError(
                "generation metadata requires assistant role".into(),
            ));
        }
        self.message.validate()
    }
    pub fn completed(id: impl Into<String>, message: ConversationMessage) -> Self {
        Self {
            id: id.into(),
            turn_id: None,
            message,
            status: MessageStatus::Completed,
            failure: None,
            metadata: BTreeMap::new(),
            created_at_ms: None,
            generation: None,
            artifact: None,
        }
    }
}

/// Ordered transcript, including drafts/failures. Not automatically model context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatHistory {
    pub schema_version: u32,
    pub entries: Vec<HistoryEntry>,
}
impl Default for ChatHistory {
    fn default() -> Self {
        Self {
            schema_version: 1,
            entries: vec![],
        }
    }
}
impl ChatHistory {
    pub fn validate(&self) -> Result<(), HistoryError> {
        self.validate_inner(false)
    }
    /// Requires no unresolved tool calls and only completed entries (e.g. initial context).
    pub fn validate_complete(&self) -> Result<(), HistoryError> {
        self.validate_inner(true)
    }
    fn validate_inner(&self, complete: bool) -> Result<(), HistoryError> {
        if self.schema_version != 1 {
            return Err(HistoryError("unsupported history schema version".into()));
        }
        let mut ids = BTreeSet::new();
        let mut calls = BTreeSet::new();
        let mut pending = BTreeMap::new();
        for entry in &self.entries {
            if entry.id.trim().is_empty() || !ids.insert(&entry.id) {
                return Err(HistoryError(
                    "history entry IDs must be nonblank and unique".into(),
                ));
            }
            if complete && entry.status != MessageStatus::Completed {
                return Err(HistoryError(
                    "initial history requires completed entries".into(),
                ));
            }
            if entry.failure.is_some() && entry.status != MessageStatus::Failed {
                return Err(HistoryError("failure details require Failed status".into()));
            }
            entry.validate()?;
            match &entry.message {
                ConversationMessage::Assistant { .. } => {
                    if !pending.is_empty() {
                        return Err(HistoryError(
                            "unresolved tool calls before next assistant message".into(),
                        ));
                    }
                    for call in entry.message.tool_calls() {
                        if !calls.insert(&call.id) {
                            return Err(HistoryError("duplicate tool call ID in history".into()));
                        }
                        pending.insert(&call.id, &call.name);
                    }
                }
                ConversationMessage::Tool { call_id, name, .. } => {
                    if pending.remove(call_id) != Some(name) {
                        return Err(HistoryError(
                            "tool result has no matching outstanding call and name".into(),
                        ));
                    }
                }
                _ if !pending.is_empty() => {
                    return Err(HistoryError(
                        "tool results must precede the next conversation message".into(),
                    ))
                }
                _ => {}
            }
        }
        if complete && !pending.is_empty() {
            return Err(HistoryError(
                "unresolved tool calls in complete history".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid conversation history: {0}")]
pub struct HistoryError(pub String);
