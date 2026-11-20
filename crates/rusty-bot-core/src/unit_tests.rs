use crate::*;
use serde_json::json;

fn request(messages: Vec<Message>) -> HistoryRequest {
    HistoryRequest {
        model: "unit-model".into(),
        messages,
        tools: vec![],
        tool_choice: ToolChoice::None,
        temperature: None,
        max_tokens: Some(32),
        metadata: json!({"tag":"keep"}),
    }
}
fn assistant(calls: Vec<ToolCall>) -> Message {
    Message::Assistant {
        content: vec![],
        tool_calls: calls,
        usage: None,
        stop: None,
    }
}
fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "read".into(),
        arguments: json!({"path":"x"}),
    }
}
fn running() -> TaskState {
    TaskState::Running {
        checkpoint: None,
        wake: None,
    }
}
fn terminal() -> TaskState {
    TaskState::Terminal {
        outcome: TaskOutcome::Completed {
            result: json!(null),
        },
    }
}

#[test]
fn pending_task_can_start_but_cannot_skip_execution() {
    let pending = TaskState::Pending {
        checkpoint: None,
        wake: None,
    };
    assert!(pending.can_transition_to(&running()));
    assert!(!pending.can_transition_to(&terminal()));
    assert!(!pending.is_terminal());
    assert!(terminal().is_terminal());
}

#[test]
fn waiting_task_can_resume_or_abort_but_cannot_become_pending() {
    let waiting = TaskState::Waiting {
        checkpoint: None,
        on: vec!["signal".into()],
        policy: WaitPolicy::All,
    };
    assert!(waiting.can_transition_to(&running()));
    assert!(waiting.can_transition_to(&terminal()));
    assert!(!waiting.can_transition_to(&TaskState::Pending {
        checkpoint: None,
        wake: None
    }));
    assert!(!terminal().can_transition_to(&running()));
}

#[test]
fn checkpoint_replay_defaults_to_unsafe_and_survives_state_access() {
    assert_eq!(ReplayPolicy::default(), ReplayPolicy::Unsafe);
    assert_eq!(ToolCheckpoint::Call.replay(), None);
    let checkpoint = ToolCheckpoint::Execute {
        arguments: json!({}),
        replay: ReplayPolicy::Safe,
        wake: None,
        checkpoint: None,
    };
    let state = TaskState::Running {
        checkpoint: Some(checkpoint.clone()),
        wake: None,
    };
    assert_eq!(state.checkpoint(), Some(&checkpoint));
    assert_eq!(
        state.checkpoint().unwrap().replay(),
        Some(ReplayPolicy::Safe)
    );
    assert!(terminal().checkpoint().is_none());
}

#[test]
fn canonical_calls_reject_duplicate_ids_and_nonobject_arguments() {
    assert_eq!(
        canonical_tool_calls(&assistant(vec![call("a"), call("a")]))
            .unwrap_err()
            .kind,
        ModelErrorKind::Protocol
    );
    let mut invalid = call("a");
    invalid.arguments = json!([]);
    assert!(canonical_tool_calls(&assistant(vec![invalid])).is_err());
    assert!(canonical_tool_calls(&assistant(vec![call(" ")])).is_err());
}

#[test]
fn rich_calls_preserve_order_and_decode_json_arguments() {
    let message = Message::Rich {
        rich_role: RichRole::Assistant,
        parts: vec![
            HistoryPart::Text {
                text: "working".into(),
            },
            HistoryPart::ToolCall {
                id: "a".into(),
                name: "read".into(),
                arguments_json: r#"{"path":"x"}"#.into(),
            },
            HistoryPart::ToolCall {
                id: "b".into(),
                name: "ls".into(),
                arguments_json: "{}".into(),
            },
        ],
    };
    let calls = canonical_tool_calls(&message).unwrap();
    assert_eq!(
        calls.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(calls[0].arguments, json!({"path":"x"}));
}

#[test]
fn legacy_projection_preserves_options_and_tool_argument_encoding() {
    let original = request(vec![
        Message::user_text("hello"),
        assistant(vec![call("a")]),
    ]);
    let projected = project_history(original.clone()).unwrap();
    assert_eq!(projected.model, original.model);
    assert_eq!(projected.metadata, original.metadata);
    assert_eq!(projected.max_tokens, Some(32));
    let ProviderMessage::Assistant {
        content,
        tool_calls,
    } = &projected.messages[1]
    else {
        panic!("expected assistant")
    };
    assert_eq!(*content, None);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&tool_calls[0].arguments).unwrap(),
        json!({"path":"x"})
    );
}

#[test]
fn legacy_projection_refuses_lossy_images_and_out_of_order_rich_text() {
    let image = Message::User {
        content: vec![ContentPart::Image {
            mime_type: "image/png".into(),
            data_base64: "not-decoded".into(),
        }],
    };
    assert_eq!(
        project_history(request(vec![image])).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
    let rich = Message::Rich {
        rich_role: RichRole::Assistant,
        parts: vec![
            HistoryPart::ToolCall {
                id: "a".into(),
                name: "read".into(),
                arguments_json: "{}".into(),
            },
            HistoryPart::Text {
                text: "after call".into(),
            },
        ],
    };
    assert!(project_history(request(vec![rich])).is_err());
}

#[test]
fn legacy_turn_rejects_mismatched_duplicate_fields() {
    let mut turn = AssistantTurn {
        message: assistant(vec![call("a")]),
        tool_calls: vec![call("a")],
        usage: Usage::default(),
        stop: StopReason::ToolUse,
        provider_metadata: json!({}),
    };
    assert!(history_turn_from_legacy(turn.clone()).is_ok());
    turn.tool_calls.clear();
    assert_eq!(
        history_turn_from_legacy(turn).unwrap_err().kind,
        ModelErrorKind::Protocol
    );
}
