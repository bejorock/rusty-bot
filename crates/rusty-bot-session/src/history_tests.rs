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
