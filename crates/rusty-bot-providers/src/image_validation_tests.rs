use super::*;
use image::{DynamicImage, RgbaImage};

fn png() -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(RgbaImage::new(2, 2))
        .write_to(&mut bytes, ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
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

#[test]
fn static_png_parser_reports_no_animation() {
    assert_eq!(png_animation(&png()), Ok(None));
}
