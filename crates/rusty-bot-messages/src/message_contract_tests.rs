use super::*;
use serde_json::json;

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall {
        call: ToolInvocation {
            id: id.into(),
            name: "inspect".into(),
            arguments: json!({"path":"file"}),
        },
    }
}
fn replay(required: bool) -> ReplayState {
    ReplayState {
        provider: "anthropic".into(),
        model: Some("model".into()),
        format: "thinking".into(),
        version: 1,
        payload: json!({"secret":"do-not-log"}),
        required_for_replay: required,
    }
}
fn event(sequence: u64, event: MessageStreamPayload) -> MessageStreamEvent {
    MessageStreamEvent {
        message_id: "answer".into(),
        sequence,
        event,
    }
}
fn send(stream: &mut MessageAccumulator, seq: &mut u64, payload: MessageStreamPayload) {
    *seq += 1;
    let original = event(*seq, payload);
    let wire = serde_json::to_string(&original).unwrap();
    stream.apply(serde_json::from_str(&wire).unwrap()).unwrap();
}

#[test]
fn ordered_calls_and_block_metadata_round_trip_without_duplicate_authority() {
    let message = ConversationMessage::Assistant {
        content: vec![
            ContentBlock::Reasoning {
                text: "inspect first".into(),
            },
            call("a"),
            ContentBlock::Annotated {
                id: Some("cited-text".into()),
                block: Box::new(ContentBlock::text("source")),
                annotations: vec![ContentAnnotation {
                    namespace: "citation".into(),
                    kind: "url".into(),
                    value: json!({"url":"https://example.invalid"}),
                }],
                replay: vec![replay(true)],
            },
            call("b"),
            ContentBlock::Extension {
                namespace: "provider".into(),
                kind: "server_tool_result".into(),
                value: json!({"items":[]}),
            },
            ContentBlock::Refusal {
                text: "not allowed".into(),
            },
        ],
    };
    message.validate().unwrap();
    let wire = serde_json::to_value(&message).unwrap();
    assert!(wire.get("tool_calls").is_none());
    assert_eq!(wire["content"][1]["call"]["id"], "a");
    assert_eq!(wire["content"][3]["call"]["id"], "b");
    assert_eq!(
        serde_json::from_value::<ConversationMessage>(wire).unwrap(),
        message
    );
    assert_eq!(
        message
            .tool_calls()
            .map(|c| c.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(message.text_only().is_err());
}

#[test]
fn old_split_calls_are_read_but_ambiguous_mixed_calls_are_rejected() {
    let legacy = json!({"role":"assistant","content":[{"type":"text","text":"look"}],"tool_calls":[{"id":"a","name":"inspect","arguments":{}}]});
    let message: ConversationMessage = serde_json::from_value(legacy).unwrap();
    assert_eq!(message.content().len(), 2);
    message.validate().unwrap();
    let mut canonical = serde_json::to_value(&message).unwrap();
    canonical["tool_calls"] = json!([{"id":"a","name":"inspect","arguments":{}}]);
    assert!(serde_json::from_value::<ConversationMessage>(canonical).is_err());
}

#[test]
fn replay_requires_explicit_provider_model_format_and_version_support() {
    let state = replay(true);
    assert!(state.compatible_with("anthropic", "model", "thinking", 1));
    for (provider, model, format, version) in [
        ("other", "model", "thinking", 1),
        ("anthropic", "other", "thinking", 1),
        ("anthropic", "model", "other", 1),
        ("anthropic", "model", "thinking", 2),
    ] {
        assert!(!state.compatible_with(provider, model, format, version));
    }
    let message = ConversationMessage::Assistant {
        content: vec![ContentBlock::Replay { state }],
    };
    assert!(!format!("{message:?}").contains("do-not-log"));
    assert!(message.validate_replay_for(|_| false).is_err());
    message
        .validate_replay_for(|s| s.compatible_with("anthropic", "model", "thinking", 1))
        .unwrap();
    let optional = ConversationMessage::Assistant {
        content: vec![ContentBlock::Replay {
            state: replay(false),
        }],
    };
    optional.validate_replay_for(|_| false).unwrap();
    assert!(optional.text_only().is_err());
}

#[test]
fn roles_duplicate_blocks_and_invalid_replay_are_rejected() {
    assert!(ConversationMessage::User {
        content: vec![call("a")]
    }
    .validate()
    .is_err());
    assert!(ConversationMessage::Assistant {
        content: vec![call("a"), call("a")]
    }
    .validate()
    .is_err());
    let block = ContentBlock::Annotated {
        id: Some("same".into()),
        block: Box::new(ContentBlock::text("x")),
        annotations: vec![],
        replay: vec![],
    };
    assert!(ConversationMessage::Assistant {
        content: vec![block.clone(), block]
    }
    .validate()
    .is_err());
    let mut bad = replay(true);
    bad.version = 0;
    assert!(ConversationMessage::Assistant {
        content: vec![ContentBlock::Replay { state: bad }]
    }
    .validate()
    .is_err());
    // Empty tool output is valid and still paired by history validation.
    ConversationMessage::Tool {
        call_id: "a".into(),
        name: "inspect".into(),
        content: vec![],
        is_error: false,
    }
    .validate()
    .unwrap();
}

#[test]
fn generation_facts_and_application_artifacts_are_not_model_content() {
    let mut entry = HistoryEntry::completed("answer", ConversationMessage::assistant("hello"));
    entry.generation = Some(GenerationMetadata {
        provider: Some("provider".into()),
        model: Some("model".into()),
        response_id: Some("remote-id".into()),
        finish_reason: Some(FinishReason::Length),
        raw_finish_reason: Some("max_tokens".into()),
        usage: Some(TokenUsage {
            input_tokens: Some(100),
            output_tokens: Some(10),
            total_tokens: Some(110),
            input_details: [("cache_read".into(), 80)].into(),
            output_details: [("reasoning".into(), 4)].into(),
        }),
        duration_ms: Some(300),
        extras: Default::default(),
    });
    entry.artifact = Some(json!({"full_output":"not-context"}));
    entry.created_at_ms = Some(123);
    entry.validate().unwrap();
    let restored: HistoryEntry =
        serde_json::from_value(serde_json::to_value(&entry).unwrap()).unwrap();
    assert_eq!(restored, entry);
    assert_eq!(restored.message.text_only().unwrap(), "hello");
    let old: HistoryEntry = serde_json::from_value(json!({"id":"old","turn_id":null,"message":{"role":"assistant","content":[{"type":"text","text":"ok"}]},"status":"Completed","failure":null})).unwrap();
    assert!(old.generation.is_none() && old.created_at_ms.is_none() && old.artifact.is_none());
    entry.message = ConversationMessage::user("wrong role");
    assert!(entry.validate().is_err());
}

#[test]
fn partial_json_is_not_executable_and_malformed_final_calls_are_preserved() {
    let mut stream = MessageAccumulator::default();
    let mut seq = 0;
    send(&mut stream, &mut seq, MessageStreamPayload::Start);
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockStart {
            index: 0,
            block: BlockStart::ToolCall {
                id: "a".into(),
                name: "inspect".into(),
            },
        },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockDelta {
            index: 0,
            delta: "{\"path\":".into(),
        },
    );
    assert!(stream.blocks().is_empty());
    assert_eq!(stream.partial().unwrap().1, "{\"path\":");
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockEnd { index: 0 },
    );
    assert!(
        matches!(&stream.blocks()[0], ContentBlock::InvalidToolCall { arguments_text, .. } if arguments_text == "{\"path\":")
    );
    assert!(stream.blocks()[0].tool_call().is_none());
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockStart {
            index: 1,
            block: BlockStart::ToolCall {
                id: "b".into(),
                name: "inspect".into(),
            },
        },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockDelta {
            index: 1,
            delta: "{\"path\":\"file\"}".into(),
        },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockEnd { index: 1 },
    );
    assert!(stream.blocks()[1].tool_call().is_some());
}

#[test]
fn stream_boundaries_identity_sequence_and_final_authority() {
    let mut stream = MessageAccumulator::default();
    assert!(stream
        .apply(event(1, MessageStreamPayload::BlockEnd { index: 0 }))
        .is_err());
    let mut seq = 0;
    send(&mut stream, &mut seq, MessageStreamPayload::Start);
    assert!(stream.apply(event(3, MessageStreamPayload::Start)).is_err());
    let mut other = event(2, MessageStreamPayload::Start);
    other.message_id = "other".into();
    assert!(stream.apply(other).is_err());
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockStart {
            index: 0,
            block: BlockStart::Text,
        },
    );
    assert!(stream
        .apply(event(
            3,
            MessageStreamPayload::BlockDelta {
                index: 1,
                delta: "wrong".into()
            }
        ))
        .is_err());
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockDelta {
            index: 0,
            delta: "preview".into(),
        },
    );
    let response =
        HistoryEntry::completed("answer", ConversationMessage::assistant("authoritative"));
    assert!(stream
        .apply(event(
            4,
            MessageStreamPayload::Completed {
                response: Box::new(response.clone())
            }
        ))
        .is_err());
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockEnd { index: 0 },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::Completed {
            response: Box::new(response.clone()),
        },
    );
    assert_eq!(stream.response(), Some(&response));
    assert_eq!(stream.blocks(), response.message.content());
    assert!(stream
        .apply(event(seq + 1, MessageStreamPayload::Start))
        .is_err());
}

#[test]
fn complete_blocks_cannot_receive_deltas_and_failure_keeps_partial_preview() {
    let mut stream = MessageAccumulator::default();
    let mut seq = 0;
    send(&mut stream, &mut seq, MessageStreamPayload::Start);
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockStart {
            index: 0,
            block: BlockStart::Complete {
                block: ContentBlock::Replay {
                    state: replay(true),
                },
            },
        },
    );
    assert!(stream
        .apply(event(
            3,
            MessageStreamPayload::BlockDelta {
                index: 0,
                delta: "bad".into()
            }
        ))
        .is_err());
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockEnd { index: 0 },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockStart {
            index: 1,
            block: BlockStart::Reasoning,
        },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::BlockDelta {
            index: 1,
            delta: "thinking".into(),
        },
    );
    send(
        &mut stream,
        &mut seq,
        MessageStreamPayload::Failed {
            error: "cancelled".into(),
        },
    );
    assert_eq!(stream.failure(), Some("cancelled"));
    assert_eq!(stream.partial().unwrap().1, "thinking");
    assert!(stream.response().is_none());
}
