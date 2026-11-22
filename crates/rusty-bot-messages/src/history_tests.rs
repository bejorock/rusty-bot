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
fn session_import_rebases_every_id_without_changing_payloads() {
    let mut user = HistoryEntry::completed("import:1", ConversationMessage::user("héllo"));
    user.turn_id = Some(7);
    user.metadata.insert("bytes".into(), json!([0, 127, 255]));
    user.created_at_ms = Some(1234);
    user.artifact = Some(json!({"opaque": "\\u0000é"}));
    let mut assistant = HistoryEntry::completed(
        "turn:7:assistant",
        ConversationMessage::Assistant {
            content: vec![
                ContentBlock::text("response"),
                ContentBlock::Replay {
                    state: ReplayState {
                        provider: "anthropic".into(),
                        model: Some("model".into()),
                        format: "messages.content".into(),
                        version: 1,
                        payload: json!({"signature": "AA+/==", "bytes": [0, 255]}),
                        required_for_replay: true,
                    },
                },
            ],
        },
    );
    assistant.turn_id = Some(7);
    assistant.generation = Some(GenerationMetadata {
        provider: Some("anthropic".into()),
        response_id: Some("response-id".into()),
        finish_reason: Some(FinishReason::Stop),
        usage: Some(TokenUsage {
            input_tokens: Some(3),
            output_tokens: Some(4),
            ..Default::default()
        }),
        ..Default::default()
    });
    let history = ChatHistory {
        schema_version: 1,
        entries: vec![
            user,
            assistant,
            HistoryEntry::completed("import:0", ConversationMessage::user("next")),
        ],
    };
    history.validate_complete().unwrap();
    let imported = history.clone().for_session_import();
    assert_eq!(imported.schema_version, history.schema_version);
    assert_eq!(imported.entries.len(), history.entries.len());
    for (index, (source, rebased)) in history.entries.iter().zip(&imported.entries).enumerate() {
        let mut expected = source.clone();
        expected.id = format!("import:{index}");
        assert_eq!(rebased, &expected);
        assert_eq!(
            serde_json::to_vec(&rebased.message).unwrap(),
            serde_json::to_vec(&source.message).unwrap()
        );
        assert_eq!(
            serde_json::to_vec(&rebased.generation).unwrap(),
            serde_json::to_vec(&source.generation).unwrap()
        );
    }
    imported.validate_complete().unwrap();
    assert_eq!(imported.clone().for_session_import(), imported);
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
