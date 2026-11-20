use super::*;
use image::{DynamicImage, RgbaImage};

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
    assert_eq!(png_animation(&bytes), Ok(None));
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

#[test]
fn png_checks_chunk_crc_and_rejects_trailing_data() {
    let mut bytes = png();
    bytes[29] ^= 1; // IHDR checksum, not pixel data.
    assert_eq!(png_animation(&bytes), Err("invalid PNG checksum"));
    let mut trailing = png();
    trailing.push(0);
    assert_eq!(png_animation(&trailing), Err("invalid PNG end"));
    let mut truncated = png();
    truncated.truncate(truncated.len() - 1);
    assert_eq!(png_animation(&truncated), Err("truncated PNG chunk"));
}

#[test]
fn work_budget_enforces_aggregate_frame_and_pixel_caps() {
    let mut frames = WorkBudget::default();
    assert_eq!(frames.reserve(1, 1, MAX_FRAMES), Ok(()));
    assert_eq!(frames.reserve(1, 1, 1), Err("image frame limit exceeded"));
    let mut pixels = WorkBudget::default();
    assert_eq!(pixels.reserve(8000, 8000, 1), Ok(()));
    assert_eq!(pixels.reserve(1, 1, 1), Err("image frame limit exceeded"));
}

#[test]
fn gif_parser_requires_frames_trailer_and_complete_blocks() {
    let mut gif = b"GIF89a\x01\x00\x01\x00\x00\x00\x00".to_vec();
    gif.push(0x3b);
    assert_eq!(gif_frame_count(&gif), Err("GIF has no frames"));
    gif.pop();
    gif.extend_from_slice(&[0x2c, 0, 0, 0, 0, 1, 0, 1, 0, 0, 2, 1, 0, 0, 0x3b]);
    assert_eq!(gif_frame_count(&gif), Ok(1));
    gif.pop();
    assert_eq!(gif_frame_count(&gif), Err("truncated GIF"));
    assert_eq!(gif_frame_count(b"GIF"), Err("truncated GIF"));
}
