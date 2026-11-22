use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::ConversationMessage;
use serde_json::json;

fn body(api: Api, config: &ProviderConfig) -> Result<Value, ModelError> {
    request::build(
        api,
        config,
        &request(vec![ConversationMessage::user("hello")]),
    )
}
#[test]
fn reasoning_and_sampling_validation() {
    let mut c = config();
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        assert!(body(api, &c).unwrap().get("thinking").is_none());
        c.reasoning = Reasoning::Disabled;
        let b = body(api, &c).unwrap();
        match api {
            Api::Responses => assert_eq!(b["reasoning"]["effort"], "none"),
            Api::Chat => assert_eq!(b["reasoning_effort"], "none"),
            _ => assert_eq!(b["thinking"]["type"], "disabled"),
        }
        c.reasoning = Reasoning::Default;
    }
    c.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: None,
    };
    assert_eq!(
        body(Api::Responses, &c).unwrap()["reasoning"]["effort"],
        "medium"
    );
    assert!(body(Api::Anthropic, &c).is_err());
    assert_eq!(
        body(Api::Deepseek, &c).unwrap()["thinking"]["type"],
        "enabled"
    );
    c.reasoning = Reasoning::Enabled {
        effort: Some(ReasoningEffort::Medium),
        budget_tokens: None,
    };
    assert!(body(Api::Deepseek, &c).is_err());
    c.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: Some(1024),
    };
    assert_eq!(
        body(Api::Anthropic, &c).unwrap()["thinking"]["budget_tokens"],
        1024
    );
    assert!(body(Api::Responses, &c).is_err());
    c.temperature = Some(0.5);
    assert!(body(Api::Anthropic, &c).is_err());
    c.temperature = None;
    c.reasoning = Reasoning::Adaptive {
        effort: Some(ReasoningEffort::XHigh),
    };
    assert_eq!(
        body(Api::Anthropic, &c).unwrap()["output_config"]["effort"],
        "xhigh"
    );
    assert!(body(Api::Responses, &c).is_err());
    c.reasoning = Reasoning::Adaptive {
        effort: Some(ReasoningEffort::Minimal),
    };
    assert!(body(Api::Anthropic, &c).is_err());
    c.reasoning = Reasoning::Default;
    c.thinking_budget = Some(1024);
    c.reasoning = Reasoning::Disabled;
    assert!(body(Api::Anthropic, &c).is_err());
    c.thinking_budget = None;
    c.temperature = Some(f64::NAN);
    assert!(body(Api::Chat, &c).is_err());
    c.temperature = Some(1.5);
    assert!(body(Api::Anthropic, &c).is_err());
    assert!(body(Api::Chat, &c).is_ok());
    c.temperature = Some(0.5);
    c.top_p = Some(0.5);
    assert!(body(Api::Anthropic, &c).is_err());
    c.reasoning = Reasoning::Default;
    assert!(body(Api::Deepseek, &c).is_err());
    c.reasoning = Reasoning::Disabled;
    assert!(body(Api::Deepseek, &c).is_ok());
}
#[test]
fn tool_choices_and_base_urls() {
    let mut c = config();
    let mut r = request(vec![ConversationMessage::user("hi")]);
    c.tool_choice = ToolChoice::Required;
    assert!(request::build(Api::Chat, &c, &r).is_err());
    r.tools.push(ToolSpec {
        name: "lookup".into(),
        description: "desc".into(),
        parameters: json!({"type":"object"}),
    });
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        c.reasoning = if api == Api::Deepseek {
            Reasoning::Disabled
        } else {
            Reasoning::Default
        };
        c.tool_choice = ToolChoice::Required;
        assert!(request::build(api, &c, &r).is_ok());
        c.tool_choice = ToolChoice::Named {
            name: "lookup".into(),
        };
        let b = request::build(api, &c, &r).unwrap();
        assert!(b["tool_choice"].to_string().contains("lookup"));
        c.tool_choice = ToolChoice::Named {
            name: "missing".into(),
        };
        assert!(request::build(api, &c, &r).is_err());
        c.tool_choice = ToolChoice::None;
        assert!(request::build(api, &c, &r)
            .unwrap()
            .get("tool_choice")
            .is_some());
    }
    c.tool_choice = ToolChoice::Required;
    c.reasoning = Reasoning::Adaptive { effort: None };
    assert!(request::build(Api::Anthropic, &c, &r).is_err());
    let resolver = static_resolver(ProviderCredential::new("secret"));
    let p = OpenAIProvider::from_base_url_with_protocol(
        "https://example.com/proxy/v1/",
        "m",
        OpenAIProtocol::ChatCompletions,
        resolver.clone(),
    )
    .unwrap();
    assert_eq!(
        p.0.config.endpoint,
        "https://example.com/proxy/v1/chat/completions"
    );
    assert_eq!(
        p.0.config.list_models_endpoint.as_deref(),
        Some("https://example.com/proxy/v1/models")
    );
    assert!(!format!("{p:?}").contains("secret"));
    assert!(AnthropicProvider::from_base_url("http://example.com/v1", "m", resolver).is_err());
}

#[test]
fn explicit_reasoning_efforts_and_anthropic_tool_conflicts() {
    let mut c = config();
    for (effort, name) in [
        (ReasoningEffort::High, "high"),
        (ReasoningEffort::Max, "max"),
    ] {
        c.reasoning = Reasoning::Enabled {
            effort: Some(effort),
            budget_tokens: None,
        };
        assert_eq!(
            body(Api::Responses, &c).unwrap()["reasoning"]["effort"],
            name
        );
        assert_eq!(body(Api::Chat, &c).unwrap()["reasoning_effort"], name);
    }
    for (effort, name) in [
        (ReasoningEffort::Low, "low"),
        (ReasoningEffort::High, "high"),
        (ReasoningEffort::Max, "max"),
    ] {
        c.reasoning = Reasoning::Enabled {
            effort: Some(effort),
            budget_tokens: None,
        };
        let b = body(Api::Deepseek, &c).unwrap();
        assert_eq!(b["thinking"]["type"], "enabled");
        assert_eq!(b["reasoning_effort"], name);
    }
    c.reasoning = Reasoning::Disabled;
    let b = body(Api::Deepseek, &c).unwrap();
    assert_eq!(b["thinking"]["type"], "disabled");
    assert!(b.get("reasoning_effort").is_none());
    c.reasoning = Reasoning::Adaptive {
        effort: Some(ReasoningEffort::High),
    };
    let b = body(Api::Anthropic, &c).unwrap();
    assert_eq!(b["thinking"]["type"], "adaptive");
    assert_eq!(b["output_config"]["effort"], "high");
    c.reasoning = Reasoning::Enabled {
        effort: None,
        budget_tokens: Some(1024),
    };
    assert_eq!(
        body(Api::Anthropic, &c).unwrap()["thinking"],
        json!({"type":"enabled", "budget_tokens":1024})
    );
    c.reasoning = Reasoning::Disabled;
    assert_eq!(
        body(Api::Anthropic, &c).unwrap()["thinking"]["type"],
        "disabled"
    );
    let mut r = request(vec![ConversationMessage::user("hi")]);
    r.tools.push(ToolSpec {
        name: "lookup".into(),
        description: "desc".into(),
        parameters: json!({"type":"object"}),
    });
    for choice in [
        ToolChoice::Required,
        ToolChoice::Named {
            name: "lookup".into(),
        },
    ] {
        c.tool_choice = choice;
        for reasoning in [
            Reasoning::Adaptive {
                effort: Some(ReasoningEffort::High),
            },
            Reasoning::Enabled {
                effort: None,
                budget_tokens: Some(1024),
            },
        ] {
            c.reasoning = reasoning;
            assert_eq!(
                request::build(Api::Anthropic, &c, &r).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
        c.reasoning = Reasoning::Disabled;
        assert!(request::build(Api::Anthropic, &c, &r).is_ok());
    }
}

#[test]
fn sampling_boundaries_and_nonfinite() {
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let mut c = config();
        if api == Api::Deepseek {
            c.reasoning = Reasoning::Disabled;
        }
        let max = if api == Api::Anthropic { 1.0 } else { 2.0 };
        for value in [0.0, max] {
            c.temperature = Some(value);
            assert_eq!(body(api, &c).unwrap()["temperature"], value);
        }
        for value in [
            -0.01,
            max + 0.01,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            c.temperature = Some(value);
            assert_eq!(
                body(api, &c).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
        c.temperature = None;
        for value in [0.0, 1.0] {
            c.top_p = Some(value);
            assert_eq!(body(api, &c).unwrap()["top_p"], value);
        }
        for value in [-0.01, 1.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            c.top_p = Some(value);
            assert_eq!(
                body(api, &c).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
    }
}

#[test]
fn hostile_base_urls_and_debug_redaction() {
    let resolver = static_resolver(ProviderCredential::new("private-token"));
    for url in [
        "https://user:pass@example.com/v1",
        "https://@example.com/v1",
        "https://example.com/v1?q=secret",
        "https://example.com/v1#secret",
        "http://example.com/v1",
        "http://192.0.2.1/v1",
    ] {
        assert!(
            OpenAIProvider::from_base_url(url, "m", resolver.clone()).is_err(),
            "{url}"
        );
    }
    let c = ProviderConfig::new("https://example.com/private-path", "private-model");
    assert!(!format!("{c:?}").contains("private"));
    assert!(!format!("{:?}", ProviderCredential::new("private-token")).contains("private-token"));
    let openai =
        OpenAIProvider::with_api_key(c.clone(), ProviderCredential::new("private-token")).unwrap();
    let anthropic =
        AnthropicProvider::with_api_key(c.clone(), ProviderCredential::new("private-token"))
            .unwrap();
    let deepseek =
        DeepseekProvider::with_api_key(c, ProviderCredential::new("private-token")).unwrap();
    for debug in [
        format!("{openai:?}"),
        format!("{anthropic:?}"),
        format!("{deepseek:?}"),
    ] {
        assert!(!debug.contains("private"));
    }
}
