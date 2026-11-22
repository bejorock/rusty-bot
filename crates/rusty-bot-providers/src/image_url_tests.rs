use super::*;
use crate::tests::{config, request};
use rusty_bot_messages::{ContentBlock as B, ConversationMessage as M, MediaSource};
use serde_json::json;

fn image(url: &str) -> B {
    B::Image {
        source: MediaSource::Url {
            mime_type: "image/png".into(),
            url: url.into(),
        },
    }
}
#[test]
fn user_https_urls_are_passed_through_without_fetching() {
    let url = "https://example.test/photo.png?size=2&token=abc%2Fdef";
    let req = request(vec![M::User {
        content: vec![B::text("describe"), image(url)],
    }]);
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let b = request::build(api, &config(), &req).unwrap();
        let block = if api == Api::Responses {
            &b["input"][0]["content"][1]
        } else {
            &b["messages"][0]["content"][1]
        };
        match api {
            Api::Responses => assert_eq!(block, &json!({"type":"input_image", "image_url":url})),
            Api::Anthropic => assert_eq!(
                block,
                &json!({"type":"image", "source":{"type":"url", "url":url}})
            ),
            _ => assert_eq!(block, &json!({"type":"image_url", "image_url":{"url":url}})),
        }
    }
}
#[test]
fn malformed_or_non_https_urls_and_non_user_images_fail() {
    for url in [
        "http://127.0.0.1/a",
        "data:image/png;base64,AA==",
        "https://name@example.test/a",
        "https://@example.test/a",
        "https://example.test/a#frag",
        "https://example.test/a\n",
        "https://example.test\\@evil.test/a",
        "relative/path",
        "https:///no-host",
    ] {
        for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
            let req = request(vec![M::User {
                content: vec![image(url)],
            }]);
            assert!(
                request::build(api, &config(), &req).is_err(),
                "accepted {url:?} for {api:?}"
            );
        }
    }
    for api in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
        let req = request(vec![M::Assistant {
            content: vec![image("https://example.test/image.png")],
        }]);
        assert!(request::build(api, &config(), &req).is_err());
    }
}
