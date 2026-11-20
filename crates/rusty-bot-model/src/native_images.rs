//! Image admission at the native provider boundary, over the entire history.
use rusty_bot_core::{ContentPart, HistoryPart, ImageLimits, Message, ModelError, ModelErrorKind};
use serde_json::Value;

fn invalid() -> ModelError {
    ModelError::new(
        ModelErrorKind::InvalidRequest,
        "invalid history image",
        false,
    )
}

fn state_images(value: &Value, images: &mut Vec<ContentPart>) -> Result<(), ModelError> {
    match value {
        Value::Object(object) => {
            if let Some(url) = object.get("image_url") {
                let url = url.as_str().ok_or_else(invalid)?;
                let (mime, data) = url
                    .strip_prefix("data:")
                    .and_then(|v| v.split_once(";base64,"))
                    .ok_or_else(invalid)?;
                images.push(ContentPart::Image {
                    mime_type: mime.into(),
                    data_base64: data.into(),
                });
            }
            if let Some(inline) = object
                .get("inlineData")
                .or_else(|| object.get("inline_data"))
            {
                let mime = inline
                    .get("mimeType")
                    .or_else(|| inline.get("mime_type"))
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                let data = inline
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                images.push(ContentPart::Image {
                    mime_type: mime.into(),
                    data_base64: data.into(),
                });
            }
            if object.get("type").and_then(Value::as_str) == Some("image") {
                let source = object.get("source").ok_or_else(invalid)?;
                if source.get("type").and_then(Value::as_str) != Some("base64") {
                    return Err(invalid());
                }
                let mime = source
                    .get("media_type")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                let data = source
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                images.push(ContentPart::Image {
                    mime_type: mime.into(),
                    data_base64: data.into(),
                });
            }
            for child in object.values() {
                state_images(child, images)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                state_images(child, images)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn validate_history_images(
    messages: &[Message],
    limits: Option<&ImageLimits>,
) -> Result<(), ModelError> {
    let mut images = Vec::new();
    for message in messages {
        match message {
            Message::User { content }
            | Message::Assistant { content, .. }
            | Message::ToolResult { content, .. } => images.extend(
                content
                    .iter()
                    .filter(|p| matches!(p, ContentPart::Image { .. }))
                    .cloned(),
            ),
            Message::Rich { parts, .. } => {
                for part in parts {
                    match part {
                        HistoryPart::Image {
                            mime_type,
                            data_base64,
                        } => images.push(ContentPart::Image {
                            mime_type: mime_type.clone(),
                            data_base64: data_base64.clone(),
                        }),
                        HistoryPart::ToolResult { content, .. } => images.extend(
                            content
                                .iter()
                                .filter(|p| matches!(p, ContentPart::Image { .. }))
                                .cloned(),
                        ),
                        HistoryPart::ProviderState { state }
                        | HistoryPart::Reasoning {
                            state: Some(state), ..
                        } => state_images(&state.payload, &mut images)?,
                        _ => {}
                    }
                }
            }
            Message::System { .. } => {}
        }
    }
    if !images.is_empty() {
        let limits =
            limits.ok_or_else(|| ModelError::unsupported("image capability not configured"))?;
        if limits.max_bytes == 0
            || limits.max_width == 0
            || limits.max_height == 0
            || limits.max_bytes > rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES
        {
            return Err(invalid());
        }
        rusty_bot_core::image_validation::validate_images(&images, limits)
            .map_err(|_| invalid())?;
    }
    Ok(())
}
