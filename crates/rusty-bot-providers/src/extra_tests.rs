use super::*;
use crate::tests::{chat, config, request};
use rusty_bot_messages::{ContentBlock as B, ConversationMessage as M, FinishReason};
use serde_json::json;

#[test]
fn anthropic_streamed_tool_input_and_error_result() {
    let mut decoder = wire::Decoder::new(Api::Anthropic, "requested-model", "answer");
    for frame in [
        json!({"type":"message_start","message":{"id":"a","model":"actual","role":"assistant","content":[],"usage":{"input_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"1}"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}),
        json!({"type":"message_stop"}),
    ] {
        decoder.push(&frame.to_string()).unwrap();
    }
    let entry = decoder.finish().unwrap();
    assert!(
        matches!(&entry.message.content()[0],B::ToolCall {call} if call.arguments==json!({"x":1}))
    );
    let req = request(vec![
        entry.message,
        M::Tool {
            call_id: "t".into(),
            name: "lookup".into(),
            content: vec![B::text("failed")],
            is_error: true,
        },
    ]);
    let body = request::build(Api::Anthropic, &config(), &req).unwrap();
    assert_eq!(body["messages"][1]["content"][0]["is_error"], true);
}
#[test]
fn refusal_length_and_unknown_finish() {
    for (delta, reason, expected) in [
        (json!({"refusal":"cannot"}), "stop", FinishReason::Refusal),
        (json!({"content":"cut"}), "length", FinishReason::Length),
    ] {
        let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
        d.push(&chat(delta, json!(reason)).to_string()).unwrap();
        d.push("[DONE]").unwrap();
        let entry = d.finish().unwrap();
        assert_eq!(entry.generation.unwrap().finish_reason, Some(expected));
        request::build(Api::Chat, &config(), &request(vec![entry.message])).unwrap();
    }
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    d.push(&chat(json!({"content":"x"}), json!("future_reason")).to_string())
        .unwrap();
    d.push("[DONE]").unwrap();
    assert!(d.finish().is_err());
}
#[test]
fn unknown_stream_fields_and_citations_are_not_dropped() {
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    assert!(d
        .push(&chat(json!({"audio":{"data":"x"}}), Value::Null).to_string())
        .is_err());
    assert!(wire::project(
        Api::Anthropic,
        &json!([{"type":"text","text":"answer","citations":[{"url":"https://invalid.test"}]}])
    )
    .is_err());
    assert!(wire::project(Api::Responses,&json!([{"type":"message","role":"assistant","content":[{"type":"output_text","text":"a","annotations":[{"type":"url_citation"}]}]}])).is_err());
    assert!(wire::project(Api::Responses, &json!([{"type":"reasoning","summary":[]}])).is_err());
    assert!(wire::project(
        Api::Anthropic,
        &json!([{"type":"thinking","thinking":"x","signature":""}])
    )
    .is_err());
    assert!(wire::project(
        Api::Anthropic,
        &json!([{"type":"server_tool_use","id":"t"}])
    )
    .is_err());
    let mut d = wire::Decoder::new(Api::Chat, "requested-model", "answer");
    assert!(d.push(&chat(json!({"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"lookup","arguments":"{}","rich_unknown":"must_not_discard"}}]}),json!("tool_calls")).to_string()).is_err());
}
#[test]
fn unsupported_tool_errors_and_unresolved_history() {
    let call = rusty_bot_messages::ToolInvocation {
        id: "t".into(),
        name: "lookup".into(),
        arguments: json!({}),
    };
    let unresolved = request(vec![M::Assistant {
        content: vec![B::ToolCall { call: call.clone() }],
    }]);
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        assert!(request::build(api, &config(), &unresolved).is_err());
    }
    let req = request(vec![
        M::Assistant {
            content: vec![B::ToolCall { call }],
        },
        M::Tool {
            call_id: "t".into(),
            name: "lookup".into(),
            content: vec![B::text("failed")],
            is_error: true,
        },
    ]);
    for api in [Api::Responses, Api::Chat, Api::Deepseek] {
        assert!(request::build(api, &config(), &req).is_err());
    }
}

#[test]
fn chat_nullable_tool_delta_metadata_and_invalid_nonnull_values() {
    for api in [Api::Chat, Api::Deepseek] {
        let mut d = wire::Decoder::new(api, "requested-model", "answer");
        for delta in [
            json!({"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"lookup","arguments":"{\"x\":"}}]}),
            json!({"tool_calls":[{"index":0,"id":null,"type":null,"function":{"name":null,"arguments":"1}"}}]}),
        ] {
            d.push(&chat(delta, Value::Null).to_string()).unwrap();
        }
        d.push(&chat(json!({}), json!("tool_calls")).to_string())
            .unwrap();
        d.push("[DONE]").unwrap();
        let entry = d.finish().unwrap();
        assert!(
            matches!(&entry.message.content()[0], B::ToolCall { call } if call.id == "t" && call.name == "lookup" && call.arguments == json!({"x":1}))
        );
        for bad in [
            json!({"id":3}),
            json!({"type":"other"}),
            json!({"function":{"name":false}}),
        ] {
            let mut d = wire::Decoder::new(api, "requested-model", "answer");
            let mut call = bad;
            call["index"] = json!(0);
            assert!(d
                .push(&chat(json!({"tool_calls":[call]}), Value::Null).to_string())
                .is_err());
        }
    }
}

#[test]
fn anthropic_thinking_signature_can_start_absent_but_must_finish_present() {
    for signature in [None, Some(""), Some("signed")] {
        let mut d = wire::Decoder::new(Api::Anthropic, "requested-model", "answer");
        for frame in [
            json!({"type":"message_start","message":{"id":"a","model":"actual","role":"assistant","content":[],"usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"thought"}}),
        ] {
            d.push(&frame.to_string()).unwrap();
        }
        if let Some(signature) = signature {
            d.push(&json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":signature}}).to_string()).unwrap();
        }
        let stop = d.push(&json!({"type":"content_block_stop","index":0}).to_string());
        if signature != Some("signed") {
            assert!(stop.is_err());
            continue;
        }
        stop.unwrap();
        d.push(&json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}).to_string()).unwrap();
        d.push(&json!({"type":"message_stop"}).to_string()).unwrap();
        let entry = d.finish().unwrap();
        assert!(matches!(&entry.message.content()[0], B::Reasoning { text } if text == "thought"));
        request::build(Api::Anthropic, &config(), &request(vec![entry.message])).unwrap();
    }
}

#[test]
fn anthropic_text_nullable_citations_are_empty_only() {
    for citations in [None, Some(Value::Null), Some(json!([]))] {
        let mut block = json!({"type":"text","text":"answer"});
        if let Some(citations) = citations {
            block["citations"] = citations;
        }
        assert_eq!(
            wire::project(Api::Anthropic, &json!([block])).unwrap(),
            vec![B::text("answer")]
        );
    }
    for citations in [json!([{}]), json!({}), json!("citation")] {
        assert!(wire::project(
            Api::Anthropic,
            &json!([{"type":"text","text":"answer","citations":citations}])
        )
        .is_err());
    }
}

#[test]
fn anthropic_empty_or_nonobject_tool_fragment_retains_raw_text_and_requires_terminal() {
    for (terminal, raw) in [(false, ""), (true, ""), (true, "  42  "), (true, " [1] ")] {
        let mut d = wire::Decoder::new(Api::Anthropic, "requested-model", "answer");
        for frame in [
            json!({"type":"message_start","message":{"id":"a","model":"actual","role":"assistant","content":[],"usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"lookup","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":raw}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":1}}),
        ] {
            d.push(&frame.to_string()).unwrap();
        }
        if terminal {
            d.push(&json!({"type":"message_stop"}).to_string()).unwrap();
        }
        let entry = d.finish();
        if terminal {
            assert!(
                matches!(&entry.unwrap().message.content()[0], B::InvalidToolCall { arguments_text, .. } if arguments_text == raw)
            );
        } else {
            assert!(entry.is_err());
        }
    }
}

#[test]
fn anthropic_truncated_tool_input_is_retained_and_never_replayed() {
    for reason in ["max_tokens", "tool_use", "end_turn"] {
        let mut d = wire::Decoder::new(Api::Anthropic, "requested-model", "answer");
        for frame in [
            json!({"type":"message_start","message":{"id":"a","model":"actual","role":"assistant","content":[],"usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"lookup","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}),
        ] {
            d.push(&frame.to_string()).unwrap();
        }
        let events = d
            .push(&json!({"type":"content_block_stop","index":0}).to_string())
            .unwrap();
        assert!(events.iter().any(|e| matches!(e, rusty_bot_messages::MessageStreamPayload::BlockStart { block: rusty_bot_messages::BlockStart::Complete { block: B::InvalidToolCall { arguments_text, .. } }, .. } if arguments_text == "{\"x\":")));
        d.push(&json!({"type":"message_delta","delta":{"stop_reason":reason},"usage":{"output_tokens":1}}).to_string()).unwrap();
        d.push(&json!({"type":"message_stop"}).to_string()).unwrap();
        let entry = d.finish();
        if reason == "end_turn" {
            assert!(entry.is_err());
            continue;
        }
        let entry = entry.unwrap();
        assert!(
            matches!(&entry.message.content()[0], B::InvalidToolCall { id: Some(id), name: Some(name), arguments_text, .. } if id == "t" && name == "lookup" && arguments_text == "{\"x\":")
        );
        assert!(
            matches!(&entry.message.content()[1], B::Replay { state } if state.payload[0]["input"] == "{\"x\":")
        );
        assert!(request::build(Api::Anthropic, &config(), &request(vec![entry.message])).is_err());
    }
}

#[test]
fn chat_empty_content_and_null_have_distinct_native_replay_without_empty_preview() {
    for api in [Api::Chat, Api::Deepseek] {
        for content in [json!(""), Value::Null] {
            let mut decoder = wire::Decoder::new(api, "requested-model", "answer");
            let mut accumulator = rusty_bot_messages::MessageAccumulator::default();
            let mut sequence = 0;
            let mut apply = |event| {
                sequence += 1;
                accumulator
                    .apply(MessageStreamEvent {
                        message_id: "answer".into(),
                        sequence,
                        event,
                    })
                    .unwrap();
            };
            apply(MessageStreamPayload::Start);
            let preview = decoder
                .push(&chat(json!({"content":content}), Value::Null).to_string())
                .unwrap();
            assert!(preview.is_empty());
            for event in decoder
                .push(&chat(json!({}), json!("stop")).to_string())
                .unwrap()
            {
                apply(event);
            }
            for event in decoder.push("[DONE]").unwrap() {
                apply(event);
            }
            let entry = decoder.finish().unwrap();
            let B::Replay { state } = entry.message.content().last().unwrap() else {
                panic!("missing replay")
            };
            assert_eq!(state.payload["content"], content);
            let replay = state.payload.clone();
            let body =
                request::build(api, &config(), &request(vec![entry.message.clone()])).unwrap();
            assert_eq!(body["messages"][0], replay);
            apply(MessageStreamPayload::Completed {
                response: Box::new(entry.clone()),
            });
            assert_eq!(accumulator.response(), Some(&entry));
        }
    }
}

#[test]
fn empty_chat_content_does_not_change_phase_or_emit_preview() {
    for api in [Api::Chat, Api::Deepseek] {
        let mut decoder = wire::Decoder::new(api, "requested-model", "answer");
        let frames = [
            json!({"content":""}),
            json!({"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"lookup","arguments":"{}"}}]}),
            json!({"content":""}),
        ];
        for delta in frames {
            assert!(decoder
                .push(&chat(delta, Value::Null).to_string())
                .unwrap()
                .is_empty());
        }
        decoder
            .push(&chat(json!({}), json!("tool_calls")).to_string())
            .unwrap();
        decoder.push("[DONE]").unwrap();
        let entry = decoder.finish().unwrap();
        let B::Replay { state } = entry.message.content().last().unwrap() else {
            panic!("missing replay")
        };
        assert_eq!(state.payload["content"], "");
    }
    let mut decoder = wire::Decoder::new(Api::Deepseek, "requested-model", "answer");
    assert!(decoder
        .push(&chat(json!({"content":""}), Value::Null).to_string())
        .unwrap()
        .is_empty());
    assert!(!decoder
        .push(&chat(json!({"reasoning_content":"thought"}), Value::Null).to_string())
        .unwrap()
        .is_empty());
    decoder
        .push(&chat(json!({}), json!("stop")).to_string())
        .unwrap();
    decoder.push("[DONE]").unwrap();
    decoder.finish().unwrap();
}
