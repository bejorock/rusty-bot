use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::{
    ContentBlock as B, ConversationMessage as M, ReplayState, ToolInvocation,
};
use serde_json::json;

fn foreign(blocks: Vec<B>, source: Api) -> M {
    let mut content = blocks;
    content.push(B::Replay {
        state: ReplayState {
            provider: source.provider().into(),
            model: Some("requested-model".into()),
            format: source.format().into(),
            version: 1,
            payload: json!({"secret":"opaque"}),
            required_for_replay: true,
        },
    });
    M::Assistant { content }
}

#[test]
fn native_output_formats_and_validation() {
    let req = request(vec![M::user("return json")]);
    let schema = json!({"type":"object", "properties":{"a":{"type":"string"}}, "additionalProperties":false});
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let mut c = config();
        let body = request::build(api, &c, &req).unwrap();
        assert!(
            body.get("response_format").is_none()
                && body.get("text").is_none()
                && body.get("output_config").is_none()
        );
        c.output_format = OutputFormat::JsonObject;
        let result = request::build(api, &c, &req);
        if api == Api::Anthropic {
            assert!(result.is_err());
        } else {
            let body = result.unwrap();
            assert_eq!(
                if api == Api::Responses {
                    &body["text"]["format"]
                } else {
                    &body["response_format"]
                },
                &json!({"type":"json_object"})
            );
        }
        for strict in [false, true] {
            c.output_format = OutputFormat::JsonSchema {
                name: "my_schema-1".into(),
                schema: schema.clone(),
                strict,
            };
            if api == Api::Anthropic {
                c.reasoning = Reasoning::Adaptive {
                    effort: Some(ReasoningEffort::High),
                };
            }
            let result = request::build(api, &c, &req);
            if api == Api::Deepseek || (api == Api::Anthropic && !strict) {
                assert!(result.is_err());
                continue;
            }
            let body = result.unwrap();
            match api {
                Api::Responses => assert_eq!(
                    body["text"]["format"],
                    json!({"type":"json_schema","name":"my_schema-1","schema":schema,"strict":strict})
                ),
                Api::Chat => assert_eq!(
                    body["response_format"],
                    json!({"type":"json_schema","json_schema":{"name":"my_schema-1","schema":schema,"strict":strict}})
                ),
                Api::Anthropic => assert_eq!(
                    body["output_config"],
                    json!({"effort":"high","format":{"type":"json_schema","schema":schema}})
                ),
                _ => unreachable!(),
            }
        }
        for (name, value) in [
            ("", json!({})),
            ("bad name", json!({})),
            ("é", json!({})),
            (&"a".repeat(65), json!({})),
            ("valid", json!([])),
        ] {
            c.output_format = OutputFormat::JsonSchema {
                name: name.into(),
                schema: value,
                strict: true,
            };
            assert!(request::validate_config(api, &c).is_err());
        }
    }
}

#[test]
fn foreign_reasoning_and_replay_are_stripped_but_text_retained() {
    for source in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        for target in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
            if source == target {
                continue;
            }
            let msg = foreign(
                vec![
                    B::Reasoning {
                        text: "private reasoning".into(),
                    },
                    B::text("visible"),
                ],
                source,
            );
            let body = request::build(target, &config(), &request(vec![msg])).unwrap();
            let serialized = body.to_string();
            assert!(serialized.contains("visible"));
            assert!(!serialized.contains("private reasoning") && !serialized.contains("opaque"));
        }
    }
}

#[test]
fn foreign_tool_calls_refusals_and_invalid_content() {
    let call = ToolInvocation {
        id: "c1".into(),
        name: "lookup".into(),
        arguments: json!({"x":1}),
    };
    let assistant = foreign(
        vec![
            B::Refusal {
                text: "cannot".into(),
            },
            B::ToolCall { call },
        ],
        Api::Responses,
    );
    let result = M::Tool {
        call_id: "c1".into(),
        content: vec![B::text("result")],
        is_error: false,
        name: "lookup".into(),
    };
    let body =
        request::build(Api::Anthropic, &config(), &request(vec![assistant, result])).unwrap();
    assert_eq!(
        body["messages"][0]["content"],
        json!([{"type":"text","text":"cannot"},{"type":"tool_use","id":"c1","name":"lookup","input":{"x":1}}])
    );
    assert_eq!(body["messages"][1]["content"][0]["tool_use_id"], "c1");
    for block in [
        B::Reasoning {
            text: "only reasoning".into(),
        },
        B::Extension {
            namespace: "x".into(),
            kind: "x".into(),
            value: json!(1),
        },
        B::InvalidToolCall {
            id: None,
            name: None,
            arguments_text: "{".into(),
            error: "bad".into(),
        },
    ] {
        assert!(request::build(
            Api::Anthropic,
            &config(),
            &request(vec![foreign(vec![block], Api::Responses)])
        )
        .is_err());
    }
}

#[test]
fn same_target_bad_replay_never_falls_back_and_model_change_is_portable() {
    let msg = foreign(vec![B::text("edited")], Api::Chat);
    assert!(request::build(Api::Chat, &config(), &request(vec![msg.clone()])).is_err());
    let mut changed = config();
    changed.model = "different-model".into();
    let body = request::build(Api::Chat, &changed, &request(vec![msg])).unwrap();
    assert_eq!(body["messages"][0]["content"], "edited");
}

#[test]
fn projected_anthropic_redacted_thinking_is_portable_but_unknown_extensions_are_not() {
    let payload = json!([
        {"type":"redacted_thinking","data":"opaque-data"},
        {"type":"text","text":"visible"}
    ]);
    let blocks = wire::project(Api::Anthropic, &payload).unwrap();
    assert!(matches!(&blocks[0], B::Extension { namespace, kind, value }
        if namespace == "anthropic" && kind == "redacted_thinking" && value.is_null()));
    let body = request::build(
        Api::Chat,
        &config(),
        &request(vec![foreign(blocks, Api::Anthropic)]),
    )
    .unwrap();
    assert_eq!(body["messages"][0]["content"], "visible");
    assert!(!body.to_string().contains("opaque-data"));

    for (namespace, kind, value) in [
        ("other", "redacted_thinking", json!(null)),
        ("anthropic", "other", json!(null)),
        ("anthropic", "redacted_thinking", json!({"extra":true})),
    ] {
        let blocks = vec![
            B::Extension {
                namespace: namespace.into(),
                kind: kind.into(),
                value,
            },
            B::text("visible"),
        ];
        assert!(request::build(
            Api::Chat,
            &config(),
            &request(vec![foreign(blocks, Api::Anthropic)])
        )
        .is_err());
    }
}

#[test]
fn foreign_tool_call_to_deepseek_requires_disabled_thinking() {
    let assistant = foreign(
        vec![B::ToolCall {
            call: ToolInvocation {
                id: "c1".into(),
                name: "lookup".into(),
                arguments: json!({"x":1}),
            },
        }],
        Api::Anthropic,
    );
    let result = M::Tool {
        call_id: "c1".into(),
        name: "lookup".into(),
        content: vec![B::text("result")],
        is_error: false,
    };
    let req = request(vec![assistant, result]);
    let mut cfg = config();
    assert!(request::build(Api::Deepseek, &cfg, &req).is_err());
    cfg.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: None,
    };
    assert!(request::build(Api::Deepseek, &cfg, &req).is_err());
    cfg.reasoning = Reasoning::Disabled;
    let body = request::build(Api::Deepseek, &cfg, &req).unwrap();
    assert_eq!(body["messages"][0]["tool_calls"][0]["id"], "c1");
    assert_eq!(body["messages"][1]["tool_call_id"], "c1");
    assert!(body["messages"][0].get("reasoning_content").is_none());
}

#[test]
fn foreign_tool_call_to_anthropic_requires_inactive_thinking() {
    let call = ToolInvocation {
        id: "c1".into(),
        name: "lookup".into(),
        arguments: json!({"x":1}),
    };
    let result = M::Tool {
        call_id: "c1".into(),
        name: "lookup".into(),
        content: vec![B::text("result")],
        is_error: false,
    };
    let req = request(vec![
        foreign(vec![B::ToolCall { call }], Api::Responses),
        result.clone(),
    ]);
    let mut enabled = config();
    enabled.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: Some(1024),
    };
    let mut adaptive = config();
    adaptive.reasoning = Reasoning::Adaptive { effort: None };
    let mut legacy = config();
    legacy.thinking_budget = Some(1024);
    for cfg in [&enabled, &adaptive, &legacy] {
        assert!(request::build(Api::Anthropic, cfg, &req).is_err());
        // Portable text without a tool call remains supported.
        let text = request(vec![foreign(vec![B::text("visible")], Api::Responses)]);
        assert!(request::build(Api::Anthropic, cfg, &text).is_ok());
    }
    let default = config();
    let mut disabled = config();
    disabled.reasoning = Reasoning::Disabled;
    for cfg in [&default, &disabled] {
        let body = request::build(Api::Anthropic, cfg, &req).unwrap();
        assert_eq!(body["messages"][0]["content"][0]["id"], "c1");
        assert_eq!(body["messages"][1]["content"][0]["tool_use_id"], "c1");
        if cfg.reasoning == Reasoning::Default {
            assert!(body.get("thinking").is_none());
        } else {
            assert_eq!(body["thinking"]["type"], "disabled");
        }
    }

    // Compatible native replay keeps its original authenticated thinking block.
    let payload = json!([
        {"type":"thinking", "thinking":"plan", "signature":"signature"},
        {"type":"tool_use", "id":"c1", "name":"lookup", "input":{"x":1}}
    ]);
    let mut blocks = wire::project(Api::Anthropic, &payload).unwrap();
    blocks.push(B::Replay {
        state: ReplayState {
            provider: Api::Anthropic.provider().into(),
            model: Some("requested-model".into()),
            format: Api::Anthropic.format().into(),
            version: 1,
            payload: payload.clone(),
            required_for_replay: true,
        },
    });
    let native = request(vec![M::Assistant { content: blocks }, result]);
    for cfg in [&enabled, &adaptive, &legacy] {
        let body = request::build(Api::Anthropic, cfg, &native).unwrap();
        assert_eq!(body["messages"][0]["content"], payload);
    }
    let mut changed_model = enabled.clone();
    changed_model.model = "different-model".into();
    assert!(request::build(Api::Anthropic, &changed_model, &native).is_err());
}
