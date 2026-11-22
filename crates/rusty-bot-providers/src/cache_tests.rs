use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::{ContentBlock as B, ConversationMessage as M, MediaSource};
use serde_json::json;

fn marker(id: &str, block: usize, ttl: Option<AnthropicCacheTtl>) -> CacheMarker {
    CacheMarker {
        message_id: id.into(),
        block_index: block,
        ttl,
    }
}
fn openai(mode: OpenAiResponsesCacheMode, markers: Vec<CacheMarker>) -> PromptCache {
    PromptCache::OpenAi {
        key: Some("tenant-key".into()),
        retention: None,
        responses: Some(OpenAiResponsesCache {
            mode,
            ttl: Some(OpenAiResponsesCacheTtl::TwelveHours),
            markers,
        }),
    }
}

#[test]
fn openai_options_and_exact_boundaries() {
    let mut c = config();
    let req = request(vec![
        M::system("rules"),
        M::User {
            content: vec![B::text("first"), B::text("second")],
        },
    ]);
    let original = req.history.clone();
    c.prompt_cache = Some(openai(
        OpenAiResponsesCacheMode::Explicit,
        vec![marker("h0", 0, None), marker("h1", 1, None)],
    ));
    let b = request::build(Api::Responses, &c, &req).unwrap();
    assert_eq!(b["prompt_cache_key"], "tenant-key");
    assert_eq!(
        b["prompt_cache_options"],
        json!({"mode":"explicit", "ttl":"12h"})
    );
    assert_eq!(
        b["input"][0]["content"][0]["prompt_cache_breakpoint"],
        json!({"mode":"explicit"})
    );
    assert!(b["input"][1]["content"][0]
        .get("prompt_cache_breakpoint")
        .is_none());
    assert_eq!(
        b["input"][1]["content"][1]["prompt_cache_breakpoint"],
        json!({"mode":"explicit"})
    );
    assert_eq!(req.history, original);
    assert_eq!(
        request::build(Api::Chat, &c, &req).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
    c.prompt_cache = Some(openai(OpenAiResponsesCacheMode::Implicit, vec![]));
    if let Some(PromptCache::OpenAi {
        responses: Some(options),
        ..
    }) = &mut c.prompt_cache
    {
        options.ttl = Some(OpenAiResponsesCacheTtl::ThirtyMinutes);
    }
    assert_eq!(
        request::build(Api::Responses, &c, &req).unwrap()["prompt_cache_options"],
        json!({"mode":"implicit", "ttl":"30m"})
    );
}

#[test]
fn legacy_cache_and_provider_mismatch() {
    let req = request(vec![M::user("hi")]);
    let mut c = config();
    for (retention, expected) in [
        (OpenAiCacheRetention::InMemory, "in_memory"),
        (OpenAiCacheRetention::TwentyFourHours, "24h"),
    ] {
        c.prompt_cache = Some(PromptCache::OpenAi {
            key: None,
            retention: Some(retention),
            responses: None,
        });
        for api in [Api::Responses, Api::Chat] {
            assert_eq!(
                request::build(api, &c, &req).unwrap()["prompt_cache_retention"],
                expected
            );
        }
        for api in [Api::Anthropic, Api::Deepseek] {
            assert_eq!(
                request::build(api, &c, &req).unwrap_err().kind,
                ModelErrorKind::Unsupported
            );
        }
    }
    c.prompt_cache = Some(openai(OpenAiResponsesCacheMode::Explicit, vec![]));
    if let Some(PromptCache::OpenAi { retention, .. }) = &mut c.prompt_cache {
        *retention = Some(OpenAiCacheRetention::InMemory);
    }
    assert!(request::build(Api::Responses, &c, &req).is_err());
    c.prompt_cache = Some(PromptCache::OpenAi {
        key: Some(" ".into()),
        retention: None,
        responses: None,
    });
    assert!(request::build(Api::Chat, &c, &req).is_err());
}

#[test]
fn anthropic_markers_follow_history_not_array_position() {
    let req = request(vec![
        M::system("rule"),
        M::User {
            content: vec![
                B::text("question"),
                B::Image {
                    source: MediaSource::Url {
                        mime_type: "image/png".into(),
                        url: "https://example.test/image.png".into(),
                    },
                },
            ],
        },
    ]);
    let mut c = config();
    c.prompt_cache = Some(PromptCache::Anthropic {
        markers: vec![
            marker("h1", 1, Some(AnthropicCacheTtl::FiveMinutes)),
            marker("h0", 0, Some(AnthropicCacheTtl::OneHour)),
        ],
    });
    let b = request::build(Api::Anthropic, &c, &req).unwrap();
    assert_eq!(
        b["system"][0]["cache_control"],
        json!({"type":"ephemeral", "ttl":"1h"})
    );
    assert!(b["messages"][0]["content"][0]
        .get("cache_control")
        .is_none());
    assert_eq!(
        b["messages"][0]["content"][1]["cache_control"],
        json!({"type":"ephemeral", "ttl":"5m"})
    );
    c.prompt_cache = Some(PromptCache::Anthropic {
        markers: vec![marker("h0", 0, None)],
    });
    assert_eq!(
        request::build(Api::Anthropic, &c, &req).unwrap()["system"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
    c.prompt_cache = Some(PromptCache::Anthropic {
        markers: vec![
            marker("h0", 0, None),
            marker("h1", 0, Some(AnthropicCacheTtl::OneHour)),
        ],
    });
    assert!(request::build(Api::Anthropic, &c, &req).is_err());
}

#[test]
fn markers_reject_ambiguous_or_missing_boundaries() {
    let req = request(vec![M::user("hi"), M::assistant("done")]);
    let mut c = config();
    for markers in [
        vec![marker("missing", 0, None)],
        vec![marker("h0", 9, None)],
        vec![marker("h0", 0, None), marker("h0", 0, None)],
        vec![marker("h1", 0, None)],
    ] {
        c.prompt_cache = Some(openai(OpenAiResponsesCacheMode::Explicit, markers.clone()));
        assert!(request::build(Api::Responses, &c, &req).is_err());
        c.prompt_cache = Some(PromptCache::Anthropic { markers });
        assert!(request::build(Api::Anthropic, &c, &req).is_err());
    }
    c.prompt_cache = Some(PromptCache::Anthropic {
        markers: vec![marker("h0", 0, None); 5],
    });
    assert!(request::build(Api::Anthropic, &c, &req).is_err());
    c.prompt_cache = Some(openai(
        OpenAiResponsesCacheMode::Explicit,
        vec![marker("h0", 0, Some(AnthropicCacheTtl::OneHour))],
    ));
    assert_eq!(
        request::build(Api::Responses, &c, &req).unwrap_err().kind,
        ModelErrorKind::Unsupported
    );
}

#[test]
fn markers_on_tool_or_replay_are_never_moved() {
    use rusty_bot_messages::{ReplayState, ToolInvocation};
    let req = request(vec![
        M::Assistant {
            content: vec![B::ToolCall {
                call: ToolInvocation {
                    id: "call".into(),
                    name: "lookup".into(),
                    arguments: json!({}),
                },
            }],
        },
        M::Tool {
            call_id: "call".into(),
            name: "lookup".into(),
            content: vec![B::text("result")],
            is_error: false,
        },
        M::Assistant {
            content: vec![
                B::text("answer"),
                B::Replay {
                    state: ReplayState {
                        provider: "other".into(),
                        model: None,
                        format: "other".into(),
                        version: 1,
                        payload: json!([]),
                        required_for_replay: false,
                    },
                },
            ],
        },
    ]);
    for (id, index) in [("h0", 0), ("h1", 0), ("h2", 0), ("h2", 1)] {
        let mut c = config();
        c.prompt_cache = Some(openai(
            OpenAiResponsesCacheMode::Explicit,
            vec![marker(id, index, None)],
        ));
        assert_eq!(
            request::build(Api::Responses, &c, &req).unwrap_err().kind,
            ModelErrorKind::Unsupported
        );
        c.prompt_cache = Some(PromptCache::Anthropic {
            markers: vec![marker(id, index, None)],
        });
        assert_eq!(
            request::build(Api::Anthropic, &c, &req).unwrap_err().kind,
            ModelErrorKind::Unsupported
        );
    }
}

#[test]
fn cache_is_opt_in_and_four_anthropic_boundaries_are_supported() {
    let req = request(vec![M::User {
        content: vec![B::text("a"), B::text("b"), B::text("c"), B::text("d")],
    }]);
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let body = request::build(api, &config(), &request(vec![M::user("hello")])).unwrap();
        for field in [
            "prompt_cache_options",
            "prompt_cache_key",
            "prompt_cache_retention",
        ] {
            assert!(body.get(field).is_none());
        }
        assert!(!body.to_string().contains("cache_control"));
    }
    let mut c = config();
    c.prompt_cache = Some(PromptCache::Anthropic {
        markers: (0..4).map(|i| marker("h0", i, None)).collect(),
    });
    let body = request::build(Api::Anthropic, &c, &req).unwrap();
    for index in 0..4 {
        assert_eq!(
            body["messages"][0]["content"][index]["cache_control"],
            json!({"type":"ephemeral"})
        );
    }
}
