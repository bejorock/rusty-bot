use super::*;
use image::{DynamicImage, RgbaImage};
use std::io::Cursor;

fn limits() -> ImageLimits {
    ImageLimits {
        max_bytes: 4096,
        max_width: 8,
        max_height: 8,
    }
}

fn png() -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(RgbaImage::new(2, 2))
        .write_to(&mut bytes, ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
}

fn part(bytes: &[u8], mime: &str) -> ContentPart {
    ContentPart::Image {
        mime_type: mime.into(),
        data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

#[test]
fn png_accepts_exact_byte_and_dimension_limits() {
    let bytes = png();
    let limits = ImageLimits {
        max_bytes: bytes.len(),
        max_width: 2,
        max_height: 2,
    };
    assert_eq!(
        validate_images(&[part(&bytes, "image/png")], &limits),
        Ok(())
    );
}

#[test]
fn text_is_ignored_but_invalid_base64_is_refused() {
    assert_eq!(
        validate_images(
            &[ContentPart::Text {
                text: "not an image".into()
            }],
            &limits()
        ),
        Ok(())
    );
    let image = ContentPart::Image {
        mime_type: "image/png".into(),
        data_base64: "!!!!".into(),
    };
    assert_eq!(
        validate_images(&[image], &limits()),
        Err("invalid image base64")
    );
}

#[test]
fn mime_mismatch_and_unknown_data_are_refused() {
    assert_eq!(
        validate_images(&[part(&png(), "image/jpeg")], &limits()),
        Err("image MIME does not match data")
    );
    assert_eq!(
        validate_images(&[part(b"not an image", "image/png")], &limits()),
        Err("invalid image data")
    );
}

#[test]
fn image_bytes_are_limited_across_parts_not_just_per_image() {
    let bytes = png();
    let limit = ImageLimits {
        max_bytes: bytes.len(),
        ..limits()
    };
    assert_eq!(
        validate_images(
            &[part(&bytes, "image/png"), part(&bytes, "image/png")],
            &limit
        ),
        Err("image exceeds byte limit")
    );
    let too_small = ImageLimits {
        max_bytes: 1,
        ..limits()
    };
    assert_eq!(
        validate_images(&[part(&bytes, "image/png")], &too_small),
        Err("image exceeds byte limit")
    );
}

#[test]
fn decoder_refuses_images_exceeding_dimension_cap() {
    let limit = ImageLimits {
        max_width: 1,
        ..limits()
    };
    assert!(validate_images(&[part(&png(), "image/png")], &limit).is_err());
}
