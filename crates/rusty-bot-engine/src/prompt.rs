//! Context loading, system-prompt replay, and provider message mapping.

#[cfg(test)]
#[path = "prompt_tests.rs"]
mod tests;

use std::collections::BTreeMap;

use rusty_bot_core::{
    entry_kind, ChatRequest, ContentPart, ConversationId, EntryQuery, Message, PromptSection,
    ProviderMessage, SessionError, Storage, StoredEntry, ToolChoice, ToolSpec,
};

/// Load committed entries in ascending id order starting at the latest head
/// marker (all entries when no marker exists).
pub async fn load_entries(
    storage: &dyn Storage,
    conversation: ConversationId,
) -> Result<Vec<StoredEntry>, SessionError> {
    let start = storage
        .latest_head_marker(conversation)
        .await?
        .map(|entry| entry.record.id.0);
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = storage
            .scan_entries(
                EntryQuery {
                    conversation_id: conversation,
                    kind: None,
                    by_task_id: None,
                    ascending: true,
                },
                cursor,
                500,
            )
            .await?;
        let next = page.next;
        let count = page.items.len();
        for entry in page.items {
            if start
                .map(|start| entry.record.id.0 >= start)
                .unwrap_or(true)
            {
                entries.push(entry);
            }
        }
        match next {
            Some(next) if count > 0 => cursor = Some(next),
            _ => break,
        }
    }
    Ok(entries)
}

/// Ordered sections stored on a `rusty-bot.system` entry.
pub fn sections_from_entry(entry: &StoredEntry) -> Vec<PromptSection> {
    if let Some(sections) = entry.record.data.get("sections").and_then(|v| v.as_array()) {
        let parsed: Vec<PromptSection> = sections
            .iter()
            .filter_map(|section| serde_json::from_value(section.clone()).ok())
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    entry
        .record
        .model
        .iter()
        .find_map(|message| match message {
            Message::System { sections, .. } => Some(
                sections
                    .iter()
                    .map(|(key, content)| PromptSection {
                        key: key.clone(),
                        content: content.clone(),
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

pub fn compose_system_text(sections: &[PromptSection]) -> String {
    sections
        .iter()
        .map(|section| format!("## {}\n{}", section.key, section.content))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Stable fingerprint of the effective system prompt and model-visible tools.
pub fn fingerprint(sections: &[PromptSection], tools: &[String]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for section in sections {
        feed(section.key.as_bytes());
        feed(b"=");
        feed(section.content.as_bytes());
        feed(b"\n");
    }
    for tool in tools {
        feed(b"tool:");
        feed(tool.as_bytes());
        feed(b"\n");
    }
    format!("{hash:016x}")
}

pub fn last_system_entry(entries: &[StoredEntry]) -> Option<&StoredEntry> {
    entries
        .iter()
        .rev()
        .find(|entry| entry.record.kind == entry_kind::SYSTEM)
}

pub fn map_message(message: &Message) -> Option<ProviderMessage> {
    // User/assistant image history is not representable by the current provider
    // interface. The request builder reports this as an error, never text-extracts it.
    if matches!(message, Message::User { content } | Message::Assistant { content, .. } if content.iter().any(|part| matches!(part, ContentPart::Image { .. })))
    {
        return None;
    }
    match message {
        Message::User { content } => Some(ProviderMessage::User {
            content: Message::text_content(content),
        }),
        Message::Assistant {
            content,
            tool_calls,
            ..
        } => {
            let text = Message::text_content(content);
            Some(ProviderMessage::Assistant {
                content: if text.is_empty() { None } else { Some(text) },
                tool_calls: tool_calls
                    .iter()
                    .map(|call| rusty_bot_core::ProviderToolCall {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        arguments: call.arguments.to_string(),
                    })
                    .collect(),
            })
        }
        Message::ToolResult {
            tool_call_id,
            content,
            ..
        } => Some(ProviderMessage::Tool {
            tool_call_id: tool_call_id.clone(),
            content: content.clone(),
        }),
        // System entries are composed into the single leading system message.
        Message::System { .. } | Message::Rich { .. } => None,
    }
}

/// Build an ordered, unflattened request from committed history.
pub async fn build_history_request(
    storage: &dyn Storage,
    conversation: ConversationId,
    model: &str,
    tools: Vec<ToolSpec>,
    metadata: serde_json::Value,
) -> Result<rusty_bot_core::HistoryRequest, SessionError> {
    let entries = load_entries(storage, conversation).await?;
    let sections = last_system_entry(&entries)
        .map(sections_from_entry)
        .unwrap_or_default();
    let mut system = system_message(&sections, &[]);
    if let Message::System { content, .. } = &mut system {
        *content = compose_system_text(&sections);
    }
    let mut messages = vec![system];
    messages.extend(
        entries
            .into_iter()
            .flat_map(|entry| entry.record.model)
            .filter(|message| !matches!(message, Message::System { .. })),
    );
    Ok(rusty_bot_core::HistoryRequest {
        model: model.to_string(),
        messages,
        tool_choice: if tools.is_empty() {
            ToolChoice::None
        } else {
            ToolChoice::Auto
        },
        tools,
        temperature: None,
        max_tokens: None,
        metadata,
    })
}

/// Build the provider request from committed state.
pub async fn build_chat_request(
    storage: &dyn Storage,
    conversation: ConversationId,
    model: &str,
    tools: Vec<ToolSpec>,
    metadata: serde_json::Value,
) -> Result<ChatRequest, SessionError> {
    let entries = load_entries(storage, conversation).await?;
    let sections = last_system_entry(&entries)
        .map(sections_from_entry)
        .unwrap_or_default();
    let mut messages = Vec::new();
    messages.push(ProviderMessage::System {
        content: compose_system_text(&sections),
    });
    for entry in &entries {
        for message in &entry.record.model {
            if matches!(message, Message::Rich { .. }) {
                return Err(SessionError::ContractViolation(
                    "rich history requires a history-aware engine".into(),
                ));
            }
            if matches!(message, Message::User { content } | Message::Assistant { content, .. } if content.iter().any(|part| matches!(part, ContentPart::Image { .. })))
            {
                return Err(SessionError::ContractViolation(
                    "image content is unsupported in user or assistant history".into(),
                ));
            }
            if let Some(mapped) = map_message(message) {
                messages.push(mapped);
            }
        }
    }
    Ok(ChatRequest {
        model: model.to_string(),
        messages,
        tool_choice: if tools.is_empty() {
            ToolChoice::None
        } else {
            ToolChoice::Auto
        },
        tools,
        temperature: None,
        max_tokens: None,
        metadata,
    })
}

/// Assemble the stored system message plus its ordered sections.
pub fn system_message(sections: &[PromptSection], tools: &[String]) -> Message {
    let map: BTreeMap<String, String> = sections
        .iter()
        .map(|section| (section.key.clone(), section.content.clone()))
        .collect();
    Message::System {
        content: String::new(),
        sections: map,
        tools_added: tools.to_vec(),
        tools_removed: Vec::new(),
    }
}

/// Extract text content for engine prompt construction.
pub fn text_content(text: &str) -> Vec<ContentPart> {
    vec![ContentPart::Text {
        text: text.to_string(),
    }]
}
