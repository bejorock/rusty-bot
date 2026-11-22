//! Resource-bounded validation for image parts at persistence and provider boundaries.

use base64::Engine as _;
use image::ImageFormat;
pub use rusty_bot_providers::image_validation::validate_image_bytes;
use rusty_bot_providers::image_validation::{validate_image_bytes_with_budget, WorkBudget};

use crate::{ContentPart, ImageLimits};

#[cfg(test)]
#[path = "image_validation_tests.rs"]
mod tests;

pub fn validate_images(parts: &[ContentPart], limits: &ImageLimits) -> Result<(), &'static str> {
    let max_bytes = limits.max_bytes.min(crate::model::MAX_TOOL_IMAGE_BYTES);
    let mut total = 0usize;
    let mut budget = WorkBudget::default();
    for part in parts {
        let ContentPart::Image {
            mime_type,
            data_base64,
        } = part
        else {
            continue;
        };
        if data_base64.len() > max_bytes.saturating_add(2) / 3 * 4 {
            return Err("image exceeds byte limit");
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data_base64)
            .map_err(|_| "invalid image base64")?;
        total = total
            .checked_add(bytes.len())
            .ok_or("image exceeds byte limit")?;
        if total > max_bytes {
            return Err("image exceeds byte limit");
        }
        let format = image::guess_format(&bytes).map_err(|_| "invalid image data")?;
        let expected = match format {
            ImageFormat::Png => "image/png",
            ImageFormat::Jpeg => "image/jpeg",
            ImageFormat::Gif => "image/gif",
            ImageFormat::WebP => "image/webp",
            _ => return Err("unsupported image data"),
        };
        if mime_type != expected {
            return Err("image MIME does not match data");
        }
        validate_image_bytes_with_budget(&bytes, format, limits, &mut budget)?;
    }
    Ok(())
}
