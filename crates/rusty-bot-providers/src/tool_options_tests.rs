use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::ConversationMessage;
use serde_json::json;

fn fixture() -> GenerationRequest {
    let mut r = request(vec![ConversationMessage::user("hello")]);
    for name in ["lookup", "other"] {
        r.tools.push(ToolSpec {
            name: name.into(),
            description: format!("{name} description"),
            parameters: json!({"type":"object","properties":{"x":{"type":"string"}}}),
        });
    }
    r
}

#[test]
fn strict_and_parallel_mapping_and_unchanged_inputs() {
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        for strict in [None, Some(false), Some(true)] {
            let mut c = config();
            c.endpoint = "https://proxy.invalid/prefix/beta/chat/completions".into();
            let r = fixture();
            let original_tools = r
                .tools
                .iter()
                .map(|t| (&t.name, &t.description, t.parameters.clone()))
                .collect::<Vec<_>>();
            for tool in &r.tools {
                c.tool_options
                    .insert(tool.name.clone(), ToolOptions { strict });
            }
            let b = request::build(api, &c, &r).unwrap();
            for (i, t) in r.tools.iter().enumerate() {
                let native = if matches!(api, Api::Chat | Api::Deepseek) {
                    &b["tools"][i]["function"]
                } else {
                    &b["tools"][i]
                };
                assert_eq!(native.get("strict"), strict.map(Value::Bool).as_ref());
                assert_eq!(
                    native[if api == Api::Anthropic {
                        "input_schema"
                    } else {
                        "parameters"
                    }],
                    t.parameters
                );
            }
            assert_eq!(
                original_tools,
                r.tools
                    .iter()
                    .map(|t| (&t.name, &t.description, t.parameters.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(c.tool_options["lookup"].strict, strict);
            assert!(b.get("parallel_tool_calls").is_none());
            assert!(b.get("tool_choice").is_none());
            if api != Api::Deepseek {
                for parallel in [true, false] {
                    c.parallel_tool_calls = Some(parallel);
                    let b = request::build(api, &c, &r).unwrap();
                    if api == Api::Anthropic {
                        assert_eq!(
                            b["tool_choice"],
                            json!({"type":"auto", "disable_parallel_tool_use":!parallel})
                        );
                    } else {
                        assert_eq!(b["parallel_tool_calls"], parallel);
                    }
                }
            }
        }
    }
}

#[test]
fn anthropic_parallel_preserves_each_choice() {
    let r = fixture();
    for (choice, expected) in [
        (ToolChoice::Auto, json!({"type":"auto"})),
        (ToolChoice::Required, json!({"type":"any"})),
        (
            ToolChoice::Named {
                name: "lookup".into(),
            },
            json!({"type":"tool", "name":"lookup"}),
        ),
    ] {
        for parallel in [true, false] {
            let mut c = config();
            c.tool_choice = choice.clone();
            c.parallel_tool_calls = Some(parallel);
            let mut expected = expected.clone();
            expected["disable_parallel_tool_use"] = json!(!parallel);
            assert_eq!(
                request::build(Api::Anthropic, &c, &r).unwrap()["tool_choice"],
                expected
            );
        }
    }
}

#[test]
fn anthropic_none_refuses_parallel_control() {
    let r = fixture();
    let mut c = config();
    c.tool_choice = ToolChoice::None;
    assert_eq!(
        request::build(Api::Anthropic, &c, &r).unwrap()["tool_choice"],
        json!({"type":"none"})
    );
    for parallel in [true, false] {
        c.parallel_tool_calls = Some(parallel);
        assert_eq!(
            request::build(Api::Anthropic, &c, &r).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
    }
}

#[test]
fn reject_unknown_options_and_parallel_without_tools() {
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let mut c = config();
        c.tool_options
            .insert("missing".into(), ToolOptions::default());
        assert_eq!(
            request::build(api, &c, &fixture()).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
        let empty = request(vec![ConversationMessage::user("hello")]);
        assert_eq!(
            request::build(api, &c, &empty).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
        c.tool_options.clear();
        for parallel in [true, false] {
            c.parallel_tool_calls = Some(parallel);
            assert_eq!(
                request::build(api, &c, &empty).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
            if api == Api::Deepseek {
                assert_eq!(
                    request::build(api, &c, &fixture()).unwrap_err().kind,
                    ModelErrorKind::Unsupported
                );
            }
        }
    }
}

#[test]
fn deepseek_strict_beta_and_forced_choice() {
    let r = fixture();
    let mut c = config();
    c.tool_options
        .insert("lookup".into(), ToolOptions { strict: Some(true) });
    c.endpoint = "https://example.invalid/beta/chat/completions".into();
    assert_eq!(
        request::build(Api::Deepseek, &c, &r).unwrap_err().kind,
        ModelErrorKind::InvalidRequest
    );
    c.tool_options.insert(
        "other".into(),
        ToolOptions {
            strict: Some(false),
        },
    );
    assert_eq!(
        request::build(Api::Deepseek, &c, &r).unwrap_err().kind,
        ModelErrorKind::InvalidRequest
    );
    c.tool_options.get_mut("other").unwrap().strict = Some(true);
    for endpoint in [
        "https://api.deepseek.com/chat/completions",
        "https://example.invalid/beta/chat/completions/",
        "https://example.invalid/notbeta/chat/completions",
    ] {
        c.endpoint = endpoint.into();
        assert_eq!(
            request::build(Api::Deepseek, &c, &r).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
    }
    for endpoint in [
        "https://api.deepseek.com/beta/chat/completions",
        "https://proxy.invalid/custom/beta/chat/completions",
    ] {
        c.endpoint = endpoint.into();
        assert!(request::build(Api::Deepseek, &c, &r).is_ok());
        assert_eq!(c.endpoint, endpoint);
    }
    c.endpoint = "https://example.invalid/chat/completions".into();
    for options in c.tool_options.values_mut() {
        options.strict = Some(false);
    }
    assert_eq!(
        request::build(Api::Deepseek, &c, &r).unwrap()["tools"][0]["function"]["strict"],
        false
    );
    for choice in [
        ToolChoice::Required,
        ToolChoice::Named {
            name: "lookup".into(),
        },
    ] {
        c.tool_choice = choice;
        for reasoning in [
            Reasoning::Default,
            Reasoning::Enabled {
                effort: None,
                budget_tokens: None,
            },
        ] {
            c.reasoning = reasoning;
            assert_eq!(
                request::build(Api::Deepseek, &c, &r).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
        c.reasoning = Reasoning::Disabled;
        assert!(request::build(Api::Deepseek, &c, &r).is_ok());
    }
}

#[test]
fn controls_preserve_reasoning_cache_and_output() {
    let r = fixture();
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let mut c = config();
        c.reasoning = if api == Api::Anthropic {
            Reasoning::Adaptive {
                effort: Some(ReasoningEffort::High),
            }
        } else {
            Reasoning::Disabled
        };
        c.output_format = if api == Api::Anthropic {
            OutputFormat::JsonSchema {
                name: "out".into(),
                schema: json!({"type":"object"}),
                strict: true,
            }
        } else {
            OutputFormat::JsonObject
        };
        c.prompt_cache = match api {
            Api::Responses | Api::Chat => Some(PromptCache::OpenAi {
                key: Some("cache-key".into()),
                retention: None,
                responses: None,
            }),
            Api::Anthropic => Some(PromptCache::Anthropic { markers: vec![] }),
            Api::Deepseek => None,
        };
        let baseline = request::build(api, &c, &r).unwrap();
        c.tool_options.insert(
            "lookup".into(),
            ToolOptions {
                strict: Some(false),
            },
        );
        if api != Api::Deepseek {
            c.parallel_tool_calls = Some(false);
        }
        let actual = request::build(api, &c, &r).unwrap();
        for field in [
            "reasoning",
            "reasoning_effort",
            "thinking",
            "text",
            "response_format",
            "output_config",
            "prompt_cache_key",
            "prompt_cache_options",
            "prompt_cache_retention",
        ] {
            assert_eq!(baseline.get(field), actual.get(field), "{api:?}: {field}");
        }
    }
}
