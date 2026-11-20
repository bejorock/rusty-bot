use super::*;
use rusty_bot_core::{EntryId, EntryRecord, ToolCall};
use serde_json::json;

fn section(key: &str, content: &str) -> PromptSection {
    PromptSection {
        key: key.into(),
        content: content.into(),
    }
}
fn entry(id: i64, kind: &str, data: serde_json::Value, model: Vec<Message>) -> StoredEntry {
    StoredEntry {
        record: EntryRecord {
            id: EntryId(id),
            conversation_id: ConversationId(1),
            kind: kind.into(),
            model,
            data,
            head: None,
            edits: vec![],
            by_task_id: None,
        },
        commit_seq: id,
    }
}

#[test]
fn stored_sections_keep_valid_order_and_take_precedence_over_message_fallback() {
    let sections = vec![section("z", "last"), section("a", "first")];
    let stored = entry(
        1,
        entry_kind::SYSTEM,
        json!({"sections":[{"key":"z","content":"last"},null,{"key":"a","content":"first"}]}),
        vec![system_message(&[section("fallback", "ignored")], &[])],
    );
    assert_eq!(sections_from_entry(&stored), sections);
    assert_eq!(compose_system_text(&sections), "## z\nlast\n\n## a\nfirst");
}

#[test]
fn invalid_sections_fall_back_to_system_message_or_empty() {
    let stored = entry(
        1,
        entry_kind::SYSTEM,
        json!({"sections":[{"bad":true}]}),
        vec![
            Message::user_text("ignored"),
            system_message(&[section("b", "two"), section("a", "one")], &[]),
        ],
    );
    assert_eq!(
        sections_from_entry(&stored),
        [section("a", "one"), section("b", "two")]
    );
    assert!(sections_from_entry(&entry(2, "other", json!({}), vec![])).is_empty());
    assert_eq!(compose_system_text(&[]), "");
}

#[test]
fn last_system_entry_ignores_later_non_system_entries() {
    let entries = vec![
        entry(1, entry_kind::SYSTEM, json!({}), vec![]),
        entry(2, entry_kind::SYSTEM, json!({}), vec![]),
        entry(3, entry_kind::USER, json!({}), vec![]),
    ];
    assert_eq!(last_system_entry(&entries).unwrap().record.id, EntryId(2));
    assert!(last_system_entry(&entries[2..]).is_none());
}

#[test]
fn fingerprint_is_stable_and_sensitive_to_content_tool_and_section_order() {
    assert_eq!(fingerprint(&[], &[]), "cbf29ce484222325");
    let sections = vec![section("a", "one"), section("b", "two")];
    let baseline = fingerprint(&sections, &["read".into(), "edit".into()]);
    assert_eq!(baseline.len(), 16);
    assert_eq!(
        baseline,
        fingerprint(&sections, &["read".into(), "edit".into()])
    );
    assert_ne!(
        baseline,
        fingerprint(&sections, &["edit".into(), "read".into()])
    );
    assert_ne!(
        baseline,
        fingerprint(
            &[section("b", "two"), section("a", "one")],
            &["read".into(), "edit".into()]
        )
    );
    assert_ne!(
        baseline,
        fingerprint(
            &[section("a", "changed"), section("b", "two")],
            &["read".into(), "edit".into()]
        )
    );
}

#[test]
fn message_mapping_preserves_tool_arguments_and_empty_assistant_content() {
    let call = ToolCall {
        id: "c1".into(),
        name: "read".into(),
        arguments: json!({"path":"猫.rs"}),
    };
    let mapped = map_message(&Message::Assistant {
        content: vec![],
        tool_calls: vec![call.clone()],
        usage: None,
        stop: None,
    })
    .unwrap();
    let ProviderMessage::Assistant {
        content,
        tool_calls,
    } = mapped
    else {
        panic!("wrong role")
    };
    assert_eq!(content, None);
    assert_eq!(tool_calls[0].id, call.id);
    assert_eq!(tool_calls[0].name, call.name);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&tool_calls[0].arguments).unwrap(),
        call.arguments
    );
    assert!(
        matches!(map_message(&Message::user_text("hi")),Some(ProviderMessage::User { content }) if content=="hi")
    );
    assert!(
        matches!(map_message(&Message::Assistant { content:text_content("answer"), tool_calls:vec![],usage:None,stop:None }),Some(ProviderMessage::Assistant { content:Some(text), .. }) if text=="answer")
    );
}

#[test]
fn mapping_refuses_lossy_images_and_system_but_keeps_tool_result_parts() {
    let image = ContentPart::Image {
        mime_type: "image/png".into(),
        data_base64: "AA==".into(),
    };
    assert!(map_message(&Message::User {
        content: vec![image.clone()]
    })
    .is_none());
    assert!(map_message(&Message::Assistant {
        content: vec![image.clone()],
        tool_calls: vec![],
        usage: None,
        stop: None
    })
    .is_none());
    assert!(map_message(&system_message(&[], &[])).is_none());
    let parts = vec![image];
    assert!(
        matches!(map_message(&Message::ToolResult { tool_call_id:"c1".into(),tool_name:"read".into(),content:parts.clone(),is_error:false,details:None }),Some(ProviderMessage::Tool { tool_call_id, content }) if tool_call_id=="c1" && content==parts)
    );
}
