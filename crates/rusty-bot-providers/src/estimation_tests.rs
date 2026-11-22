use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::{ContentBlock, ConversationMessage as M, MediaSource};
use serde_json::json;
fn provider(model: &str) -> OpenAIProvider {
    let mut c = config();
    c.model = model.into();
    OpenAIProvider::with_api_key(c, ProviderCredential::new("never-used")).unwrap()
}
#[test]
fn known_mapping_and_explicit_proxy_are_deterministic() {
    let p = provider("gpt-4o");
    let req = request(vec![M::user("Hello 世界 <|endoftext|>")]);
    let estimate = estimate_input_tokens(&p, &req, EstimationEncoding::Auto).unwrap();
    assert!(estimate.model_encoding_known);
    assert!(!estimate.used_fallback);
    assert!(estimate.includes_all_input);
    assert_eq!(estimate.encoding, "O200kBase");
    let body = p.build_request(&req).unwrap().to_string();
    assert_eq!(
        estimate.estimated_input_tokens as usize,
        tiktoken_rs::o200k_base_singleton()
            .encode_ordinary(&body)
            .len()
    );
    assert_eq!(
        estimate,
        estimate_input_tokens(&p, &req, EstimationEncoding::Auto).unwrap()
    );
    let explicit = estimate_input_tokens(&p, &req, EstimationEncoding::Cl100kBase).unwrap();
    assert!(!explicit.model_encoding_known);
    assert!(!explicit.used_fallback);
    assert_eq!(explicit.encoding, "Cl100kBase");
}
#[test]
fn unknown_and_non_openai_models_use_labelled_proxy() {
    for model in ["cx/gpt-6-luna", "claude-haiku-4-5", "deepseek-flash"] {
        let p = provider(model);
        let req = request(vec![M::user("hello")]);
        let estimate = estimate_input_tokens(&p, &req, EstimationEncoding::Auto).unwrap();
        assert!(estimate.used_fallback);
        assert!(!estimate.model_encoding_known);
        assert!(estimate.estimated_input_tokens > 0);
    }
}
#[test]
fn images_mark_incomplete_without_counting_urls_or_base64() {
    let p = provider("gpt-4o");
    let req = request(vec![M::User {
        content: vec![
            ContentBlock::text("what is this?"),
            ContentBlock::Image {
                source: MediaSource::Url {
                    mime_type: "image/png".into(),
                    url: format!("https://example.com/{}", "a".repeat(10000)),
                },
            },
        ],
    }]);
    let estimate = estimate_input_tokens(&p, &req, EstimationEncoding::Auto).unwrap();
    assert_eq!(estimate.omitted_media, 1);
    assert!(!estimate.includes_all_input);
    assert!(estimate.estimated_input_tokens < 1000);
}
#[test]
fn tool_schemas_and_output_constraints_count_without_mutating_history() {
    let mut c = config();
    c.model = "gpt-4o".into();
    c.output_format = OutputFormat::JsonSchema {
        name: "answer".into(),
        schema: json!({"type":"object", "properties":{"data":{"type":"string","description":"useful words ".repeat(100)}}}),
        strict: true,
    };
    let p = OpenAIProvider::with_api_key(c, ProviderCredential::new("never-used")).unwrap();
    let mut req = request(vec![M::user("hi")]);
    req.tools.push(ToolSpec {
        name: "lookup".into(),
        description: "tool documentation ".repeat(100),
        parameters: json!({"type":"object","properties":{"data":{"type":"string"}}}),
    });
    let before = req.history.clone();
    let rich = estimate_input_tokens(&p, &req, EstimationEncoding::Auto).unwrap();
    let plain = estimate_input_tokens(&provider("gpt-4o"), &req, EstimationEncoding::Auto).unwrap();
    assert!(rich.estimated_input_tokens > plain.estimated_input_tokens + 100);
    assert_eq!(rich.omitted_opaque_fields, 0);
    assert_eq!(req.history, before);
}
#[test]
fn invalid_requests_still_fail_before_estimation() {
    let mut req = request(vec![M::user("hi")]);
    req.message_id.clear();
    assert!(estimate_input_tokens(&provider("gpt-4o"), &req, EstimationEncoding::Auto).is_err());
}
