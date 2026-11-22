//! Transport-independent stream contract. Events are not executable tool calls.
use crate::{
    ContentBlock, ConversationMessage, HistoryEntry, HistoryError, MessageStatus, ToolInvocation,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlockStart {
    Text,
    Reasoning,
    ToolCall {
        id: String,
        name: String,
    },
    /// A whole non-incremental block, such as an image or replay state.
    Complete {
        block: ContentBlock,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageStreamEvent {
    pub message_id: String,
    /// Contiguous sequence starting at one. Transport retries must deduplicate first.
    pub sequence: u64,
    pub event: MessageStreamPayload,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageStreamPayload {
    Start,
    BlockStart {
        index: usize,
        block: BlockStart,
    },
    /// Text/reasoning text or raw tool-arguments JSON, never parsed until BlockEnd.
    BlockDelta {
        index: usize,
        delta: String,
    },
    BlockEnd {
        index: usize,
    },
    /// Final provider response is authoritative, not the accumulated preview.
    Completed {
        response: Box<HistoryEntry>,
    },
    Failed {
        error: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct MessageAccumulator {
    message_id: Option<String>,
    sequence: u64,
    blocks: Vec<ContentBlock>,
    active: Option<(BlockStart, String)>,
    response: Option<HistoryEntry>,
    failure: Option<String>,
}
impl MessageAccumulator {
    pub fn blocks(&self) -> &[ContentBlock] {
        &self.blocks
    }
    /// Current unfinished text or arguments; never a validated executable call.
    pub fn partial(&self) -> Option<(&BlockStart, &str)> {
        self.active
            .as_ref()
            .map(|(kind, text)| (kind, text.as_str()))
    }
    pub fn response(&self) -> Option<&HistoryEntry> {
        self.response.as_ref()
    }
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
    /// Invalid events leave the accumulator unchanged.
    pub fn apply(&mut self, event: MessageStreamEvent) -> Result<(), HistoryError> {
        let fail = |text: &str| HistoryError(text.into());
        if self.response.is_some() || self.failure.is_some() {
            return Err(fail("stream already ended"));
        }
        if self.sequence.checked_add(1) != Some(event.sequence)
            || event.message_id.trim().is_empty()
        {
            return Err(fail("invalid stream sequence or message identity"));
        }
        if self
            .message_id
            .as_ref()
            .is_some_and(|id| id != &event.message_id)
        {
            return Err(fail("stream message identity changed"));
        }
        if self.message_id.is_none() && !matches!(event.event, MessageStreamPayload::Start) {
            return Err(fail("stream must start with Start"));
        }
        match event.event {
            MessageStreamPayload::Start => {
                if self.message_id.is_some() {
                    return Err(fail("duplicate stream start"));
                }
                self.message_id = Some(event.message_id);
            }
            MessageStreamPayload::BlockStart { index, block } => {
                if self.active.is_some() || index != self.blocks.len() {
                    return Err(fail("block start is not the next ordered block"));
                }
                if let BlockStart::ToolCall { id, name } = &block {
                    if id.trim().is_empty() || name.trim().is_empty() {
                        return Err(fail("tool stream requires ID and name"));
                    }
                }
                if let BlockStart::Complete { block } = &block {
                    ConversationMessage::Assistant {
                        content: vec![block.clone()],
                    }
                    .validate()?;
                }
                self.active = Some((block, String::new()));
            }
            MessageStreamPayload::BlockDelta { index, delta } => {
                if index != self.blocks.len() {
                    return Err(fail("wrong delta block index"));
                }
                let Some((kind, text)) = &mut self.active else {
                    return Err(fail("delta without active block"));
                };
                if matches!(kind, BlockStart::Complete { .. }) {
                    return Err(fail("complete block cannot receive deltas"));
                }
                text.push_str(&delta);
            }
            MessageStreamPayload::BlockEnd { index } => {
                if index != self.blocks.len() || self.active.is_none() {
                    return Err(fail("end without matching active block"));
                }
                let (kind, text) = self.active.as_ref().expect("checked active block");
                let block = match kind {
                    BlockStart::Text => ContentBlock::Text { text: text.clone() },
                    BlockStart::Reasoning => ContentBlock::Reasoning { text: text.clone() },
                    BlockStart::Complete { block } => block.clone(),
                    BlockStart::ToolCall { id, name } => {
                        match serde_json::from_str::<serde_json::Value>(text) {
                            Ok(arguments) if arguments.is_object() => ContentBlock::ToolCall {
                                call: ToolInvocation {
                                    id: id.clone(),
                                    name: name.clone(),
                                    arguments,
                                },
                            },
                            _ => ContentBlock::InvalidToolCall {
                                id: Some(id.clone()),
                                name: Some(name.clone()),
                                arguments_text: text.clone(),
                                error: "tool arguments are not a complete JSON object".into(),
                            },
                        }
                    }
                };
                let mut content = self.blocks.clone();
                content.push(block.clone());
                ConversationMessage::Assistant { content }.validate()?;
                self.active = None;
                self.blocks.push(block);
            }
            MessageStreamPayload::Completed { response } => {
                if self.active.is_some()
                    || response.id != event.message_id
                    || response.status != MessageStatus::Completed
                    || response.failure.is_some()
                    || !matches!(response.message, ConversationMessage::Assistant { .. })
                {
                    return Err(fail("invalid final response or unfinished block"));
                }
                response.validate()?;
                self.blocks = response.message.content().to_vec();
                self.response = Some(*response);
            }
            MessageStreamPayload::Failed { error } => {
                if error.trim().is_empty() {
                    return Err(fail("failure requires an error"));
                }
                self.failure = Some(error);
            }
        }
        self.sequence = event.sequence;
        Ok(())
    }
}
