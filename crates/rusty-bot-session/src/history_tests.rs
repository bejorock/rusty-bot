use super::*;
use serde_json::json;

fn rich_history() -> ChatHistory {
    let source = MediaSource::Reference {
        mime_type: "image/png".into(),
        key: "uploads/screenshot".into(),
    };
    ChatHistory {
        schema_version: 1,
        entries: vec![
            HistoryEntry::completed(
                "instructions",
                ConversationMessage::system("You are helpful."),
            ),
            HistoryEntry::completed(
                "policy",
                ConversationMessage::Developer {
                    content: vec![ContentBlock::text("Keep replies short.")],
                },
            ),
            HistoryEntry::completed(
                "question",
                ConversationMessage::User {
                    content: vec![
                        ContentBlock::text("Describe these"),
                        ContentBlock::Image { source },
                        ContentBlock::Audio {
                            source: MediaSource::Inline {
                                mime_type: "audio/wav".into(),
                                base64: "AA==".into(),
                            },
                        },
                        ContentBlock::Video {
                            source: MediaSource::Url {
                                mime_type: "video/mp4".into(),
                                url: "https://example.invalid/video".into(),
                            },
                        },
                        ContentBlock::File {
                            source: MediaSource::Reference {
                                mime_type: "application/pdf".into(),
                                key: "document".into(),
                            },
                            name: Some("report.pdf".into()),
                        },
                        ContentBlock::Json {
                            value: json!({"context": true}),
                        },
                    ],
                },
            ),
            HistoryEntry::completed(
                "call",
                ConversationMessage::Assistant {
                    content: vec![
                        ContentBlock::Reasoning {
                            text: "Check the metadata".into(),
                        },
                        ContentBlock::ToolCall {
                            call: ToolInvocation {
                                id: "call-1".into(),
                                name: "inspect".into(),
                                arguments: json!({"key": "document"}),
                            },
                        },
                    ],
                },
            ),
            HistoryEntry::completed(
                "result",
                ConversationMessage::Tool {
                    call_id: "call-1".into(),
                    name: "inspect".into(),
                    content: vec![ContentBlock::Json {
                        value: json!({"pages": 2}),
                    }],
                    is_error: false,
                },
            ),
            HistoryEntry::completed("answer", ConversationMessage::assistant("Two pages.")),
        ],
    }
}

#[test]
fn multimodal_roles_tool_calls_and_metadata_round_trip() {
    let mut history = rich_history();
    history.entries[2]
        .metadata
        .insert("label".into(), json!("original"));
    history.validate_complete().unwrap();
    let json = serde_json::to_string(&history).unwrap();
    assert!(json.contains("\"role\":\"system\""));
    assert!(json.contains("\"type\":\"image\""));
    assert_eq!(serde_json::from_str::<ChatHistory>(&json).unwrap(), history);
}

#[test]
fn invalid_identity_version_and_media_are_rejected() {
    let mut h = rich_history();
    h.schema_version = 2;
    assert!(h.validate().is_err());
    h.schema_version = 1;
    h.entries[1].id = h.entries[0].id.clone();
    assert!(h.validate().is_err());
    h.entries[1].id = " ".into();
    assert!(h.validate().is_err());
    let bad = ConversationMessage::User {
        content: vec![ContentBlock::Image {
            source: MediaSource::Reference {
                mime_type: "image/png".into(),
                key: " ".into(),
            },
        }],
    };
    assert!(bad.validate().is_err());
    assert!(ConversationMessage::User { content: vec![] }
        .validate()
        .is_err());
    assert!(ConversationMessage::Assistant { content: vec![] }
        .validate()
        .is_err());
}

#[test]
fn tool_results_require_unique_prior_matching_calls() {
    let valid = rich_history();
    for mutate in [0, 1, 2, 3] {
        let mut h = valid.clone();
        match mutate {
            0 => {
                if let ConversationMessage::Tool { name, .. } = &mut h.entries[4].message {
                    *name = "other".into();
                }
            }
            1 => {
                h.entries.remove(3);
            }
            2 => {
                h.entries.swap(4, 5);
            }
            _ => {
                let mut entry = h.entries[4].clone();
                entry.id = "duplicate-result".into();
                h.entries.insert(5, entry);
            }
        }
        assert!(h.validate_complete().is_err());
    }
    let mut h = valid;
    h.entries.truncate(4);
    h.validate().unwrap(); // A live transcript may end at a tool-call boundary.
    assert!(h.validate_complete().is_err());
    if let ConversationMessage::Assistant { content } = &mut h.entries[3].message {
        if let ContentBlock::ToolCall { call } = &mut content[1] {
            call.arguments = json!([]);
        }
    }
    assert!(h.validate().is_err());
}

#[test]
fn failed_and_generating_entries_are_not_complete_initial_context() {
    let mut h = rich_history();
    let last = h.entries.last_mut().unwrap();
    last.status = MessageStatus::Generating;
    h.validate().unwrap();
    assert!(h.validate_complete().is_err());
    h.entries.last_mut().unwrap().failure = Some("unavailable".into());
    assert!(h.validate().is_err());
    h.entries.last_mut().unwrap().status = MessageStatus::Failed;
    h.validate().unwrap();
    assert!(h.validate_complete().is_err());
}

fn configured() -> StoredSession {
    StoredSession::new(
        SessionConfig {
            id: "session".into(),
            model: "model".into(),
            namespace: "default".into(),
            task_queue: "queue".into(),
            idle_timeout_ms: 1000,
            initial_history: rich_history(),
        },
        "workflow".into(),
    )
}

#[tokio::test]
async fn adapter_round_trip_preserves_initial_history_and_immutable_configuration() {
    let storage = MemorySessionStorage::default();
    let original = configured();
    storage.create(original.clone()).await.unwrap();
    let restored: StoredSession = serde_json::from_value(
        serde_json::to_value(storage.read("session").await.unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(restored.history(), rich_history());
    let mut conflict = original;
    conflict.config.initial_history.entries[0].message = ConversationMessage::system("Changed");
    assert!(storage.create(conflict).await.is_err());
    let mut bad = configured();
    bad.config.id = "other".into();
    bad.config.initial_history.entries[0].id = "turn:reserved".into();
    assert!(storage.create(bad).await.is_err());
    assert!(matches!(
        storage.read("other").await,
        Err(StorageError::NotFound)
    ));
}

#[test]
fn legacy_storage_documents_default_to_empty_history() {
    let mut original = configured();
    original.config.initial_history = ChatHistory::default();
    let mut json = serde_json::to_value(&original).unwrap();
    json["config"]
        .as_object_mut()
        .unwrap()
        .remove("initial_history");
    assert_eq!(
        serde_json::from_value::<StoredSession>(json).unwrap(),
        original
    );
}

#[test]
fn normalized_history_has_stable_turn_ids_drafts_and_failure_details() {
    let mut s = configured();
    s.apply(&SessionChange::Claim {
        run_id: "run".into(),
    })
    .unwrap();
    s.apply(&SessionChange::Begin {
        run_id: "run".into(),
        request_id: "request".into(),
        text: "Hi".into(),
    })
    .unwrap();
    let pending = s.history();
    assert_eq!(pending.entries[6].id, "turn:1:user");
    assert_eq!(pending.entries[7].status, MessageStatus::Generating);
    s.apply(&SessionChange::Draft {
        run_id: "run".into(),
        turn_id: 1,
        sequence: 1,
        text: "partial".into(),
    })
    .unwrap();
    s.apply(&SessionChange::Finish {
        run_id: "run".into(),
        turn_id: 1,
        result: Err("backend unavailable".into()),
    })
    .unwrap();
    let failed = s.history();
    assert_eq!(failed.entries[7].id, pending.entries[7].id);
    assert_eq!(
        failed.entries[7].message,
        ConversationMessage::assistant("partial")
    );
    assert_eq!(
        failed.entries[7].failure.as_deref(),
        Some("backend unavailable")
    );
    failed.validate().unwrap();
    assert!(s.completed_context().is_empty());
}
