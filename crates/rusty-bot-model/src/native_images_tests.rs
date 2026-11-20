use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rusty_bot_core::{ProviderState, RichRole};
use serde_json::json;

fn png() -> Vec<u8> {
    let mut out = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(image::RgbaImage::new(1, 1))
        .write_to(&mut out, image::ImageFormat::Png)
        .unwrap();
    out.into_inner()
}
fn limits() -> ImageLimits {
    ImageLimits {
        max_bytes: 4096,
        max_width: 4,
        max_height: 4,
    }
}
fn state(payload: Value) -> ProviderState {
    ProviderState {
        provider: "test".into(),
        model: None,
        format: "test".into(),
        payload,
        required_for_replay: true,
    }
}
fn rich(parts: Vec<HistoryPart>) -> Message {
    Message::Rich {
        rich_role: RichRole::Assistant,
        parts,
    }
}
fn image() -> ContentPart {
    ContentPart::Image {
        mime_type: "image/png".into(),
        data_base64: STANDARD.encode(png()),
    }
}

#[test]
fn text_only_history_needs_no_image_capability() {
    assert!(validate_history_images(
        &[
            Message::user_text("hi"),
            rich(vec![HistoryPart::Text {
                text: "answer".into()
            }])
        ],
        None
    )
    .is_ok());
}

#[test]
fn images_in_each_message_shape_require_explicit_capability() {
    let data_base64 = STANDARD.encode(png());
    for message in [
        Message::User {
            content: vec![image()],
        },
        Message::Assistant {
            content: vec![image()],
            tool_calls: vec![],
            usage: None,
            stop: None,
        },
        Message::ToolResult {
            tool_call_id: "c".into(),
            tool_name: "read".into(),
            content: vec![image()],
            is_error: false,
            details: None,
        },
        rich(vec![HistoryPart::Image {
            mime_type: "image/png".into(),
            data_base64,
        }]),
        rich(vec![HistoryPart::ToolResult {
            call_id: "c".into(),
            name: "read".into(),
            content: vec![image()],
            is_error: false,
            details: None,
        }]),
    ] {
        assert_eq!(
            validate_history_images(std::slice::from_ref(&message), None)
                .unwrap_err()
                .kind,
            ModelErrorKind::Unsupported
        );
        assert!(validate_history_images(&[message], Some(&limits())).is_ok());
    }
}

#[test]
fn provider_state_extracts_nested_openai_gemini_and_anthropic_image_forms() {
    let data = STANDARD.encode(png());
    let payload = json!({"nested":[
        {"image_url":format!("data:image/png;base64,{data}")},
        {"inlineData":{"mimeType":"image/png","data":data}},
        {"inline_data":{"mime_type":"image/png","data":data}},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":data}}
    ]});
    let mut images = vec![];
    state_images(&payload, &mut images).unwrap();
    assert_eq!(images, vec![image(); 4]);
    for part in [
        HistoryPart::ProviderState {
            state: state(payload.clone()),
        },
        HistoryPart::Reasoning {
            text: None,
            state: Some(state(payload.clone())),
        },
    ] {
        assert!(validate_history_images(&[rich(vec![part])], Some(&limits())).is_ok());
    }
}

#[test]
fn malformed_embedded_images_are_refused_without_echoing_payloads() {
    for payload in [
        json!({"image_url":3}),
        json!({"image_url":"https://private.invalid/image"}),
        json!({"inlineData":{"mimeType":"image/png"}}),
        json!({"inline_data":{"data":"private"}}),
        json!({"type":"image"}),
        json!({"type":"image","source":{"type":"url","url":"private"}}),
        json!({"type":"image","source":{"type":"base64","media_type":"image/png"}}),
    ] {
        let error = validate_history_images(
            &[rich(vec![HistoryPart::ProviderState {
                state: state(payload),
            }])],
            Some(&limits()),
        )
        .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
        assert_eq!(error.message, "invalid history image");
        assert!(!error.retryable);
    }
}

#[test]
fn invalid_capability_limits_and_invalid_image_bytes_are_rejected() {
    for limits in [
        ImageLimits {
            max_bytes: 0,
            ..limits()
        },
        ImageLimits {
            max_width: 0,
            ..limits()
        },
        ImageLimits {
            max_height: 0,
            ..limits()
        },
        ImageLimits {
            max_bytes: rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES + 1,
            ..limits()
        },
    ] {
        assert_eq!(
            validate_history_images(
                &[Message::User {
                    content: vec![image()]
                }],
                Some(&limits)
            )
            .unwrap_err()
            .kind,
            ModelErrorKind::InvalidRequest
        );
    }
    let message = Message::User {
        content: vec![ContentPart::Image {
            mime_type: "image/png".into(),
            data_base64: "private-invalid".into(),
        }],
    };
    assert_eq!(
        validate_history_images(&[message], Some(&limits()))
            .unwrap_err()
            .message,
        "invalid history image"
    );
}

#[test]
fn image_budget_is_shared_across_ordinary_rich_and_opaque_history() {
    let bytes = png();
    let limits = ImageLimits {
        max_bytes: bytes.len(),
        ..limits()
    };
    let messages = [
        Message::User {
            content: vec![image()],
        },
        rich(vec![HistoryPart::ProviderState {
            state: state(
                json!({"image_url":format!("data:image/png;base64,{}",STANDARD.encode(&bytes))}),
            ),
        }]),
    ];
    assert!(validate_history_images(&messages[..1], Some(&limits)).is_ok());
    assert_eq!(
        validate_history_images(&messages, Some(&limits))
            .unwrap_err()
            .kind,
        ModelErrorKind::InvalidRequest
    );
}
