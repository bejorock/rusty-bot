use super::*;
use image::{DynamicImage, RgbaImage};
use std::io::{Seek, SeekFrom, Write};

fn encoded(format: ImageFormat, width: u32, height: u32) -> Vec<u8> {
    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(RgbaImage::new(width, height))
        .write_to(&mut output, format)
        .unwrap();
    output.into_inner()
}
fn limits() -> ImageLimits {
    ImageLimits {
        max_bytes: 4096,
        max_width: 8,
        max_height: 8,
    }
}
fn output_image(output: ToolOutput) -> (String, Vec<u8>) {
    assert!(!output.is_error);
    assert!(output.details.is_none());
    let [ContentPart::Text { text }, ContentPart::Image {
        mime_type,
        data_base64,
    }] = output.content.as_slice()
    else {
        panic!("expected text and image");
    };
    assert_eq!(text, &format!("Read image file [{mime_type}]"));
    (
        mime_type.clone(),
        base64::engine::general_purpose::STANDARD
            .decode(data_base64)
            .unwrap(),
    )
}

#[test]
fn sniff_recognizes_only_supported_signatures_without_decoding() {
    for (bytes, expected) in [
        (b"\x89PNG\r\n\x1a\n".as_slice(), ImageFormat::Png),
        (b"\xff\xd8\xff".as_slice(), ImageFormat::Jpeg),
        (b"GIF87a".as_slice(), ImageFormat::Gif),
        (b"GIF89a".as_slice(), ImageFormat::Gif),
        (b"RIFFxxxxWEBP".as_slice(), ImageFormat::WebP),
        (b"BM".as_slice(), ImageFormat::Bmp),
    ] {
        assert_eq!(sniff(bytes), Some(expected));
    }
    for bytes in [
        b"".as_slice(),
        b"RIFF",
        b"RIFFxxxxWAVE",
        b"gif89a",
        b"not an image",
    ] {
        assert_eq!(sniff(bytes), None);
    }
}

#[test]
fn webp_chunk_parser_handles_padding_exif_and_truncated_boundaries() {
    let mut bytes = b"RIFFxxxxWEBP".to_vec();
    assert_eq!(webp_orientation(&bytes), Ok(None));
    bytes.extend_from_slice(b"JUNK\x01\0\0\0x");
    assert_eq!(webp_orientation(&bytes), Err("truncated WebP padding"));
    bytes.push(0);
    assert_eq!(webp_orientation(&bytes), Ok(None));
    bytes.extend_from_slice(b"EXIF\x06\0\0\0Exif\0\0");
    assert_eq!(webp_orientation(&bytes), Ok(None));
    assert_eq!(
        webp_orientation(&bytes[..bytes.len() - 1]),
        Err("truncated WebP chunk")
    );
    assert_eq!(
        webp_orientation(b"RIFFxxxxWEBPx"),
        Err("truncated WebP chunk")
    );
}

#[test]
fn capped_cursor_rejects_oversized_writes_without_allocating_them() {
    let mut writer = CappedCursor(Cursor::new(Vec::new()));
    writer.seek(SeekFrom::Start(MAX_ENCODED as u64)).unwrap();
    let error = writer.write(b"x").unwrap_err();
    assert_eq!(error.to_string(), "encoded image exceeds hard limit");
    assert!(writer.0.get_ref().is_empty());
    writer.seek(SeekFrom::Start(0)).unwrap();
    writer.write_all(b"abc").unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.0.into_inner(), b"abc");
}

#[test]
fn cancellation_and_zero_limits_are_refused_before_decoding() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        process(vec![], ImageFormat::Png, limits(), cancel),
        Err("read aborted")
    );
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
    ] {
        assert_eq!(
            process(vec![], ImageFormat::Png, limits, CancellationToken::new()),
            Err("invalid image limits")
        );
    }
}

#[test]
fn png_at_exact_limits_is_returned_byte_for_byte() {
    let bytes = encoded(ImageFormat::Png, 2, 2);
    let limits = ImageLimits {
        max_bytes: bytes.len(),
        max_width: 2,
        max_height: 2,
    };
    let (mime, actual) = output_image(
        process(
            bytes.clone(),
            ImageFormat::Png,
            limits,
            CancellationToken::new(),
        )
        .unwrap(),
    );
    assert_eq!(mime, "image/png");
    assert_eq!(actual, bytes);
}

#[test]
fn resizing_preserves_aspect_ratio_and_produces_decodable_bounded_png() {
    let bytes = encoded(ImageFormat::Png, 8, 4);
    let limits = ImageLimits {
        max_width: 4,
        max_height: 4,
        ..limits()
    };
    let (mime, data) = output_image(
        process(
            bytes,
            ImageFormat::Png,
            limits.clone(),
            CancellationToken::new(),
        )
        .unwrap(),
    );
    assert_eq!(mime, "image/png");
    assert!(data.len() <= limits.max_bytes);
    let decoded = image::load_from_memory(&data).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (4, 2));
}

#[test]
fn bmp_is_converted_to_png_even_without_resizing() {
    let bytes = encoded(ImageFormat::Bmp, 2, 2);
    let (mime, data) =
        output_image(process(bytes, ImageFormat::Bmp, limits(), CancellationToken::new()).unwrap());
    assert_eq!(mime, "image/png");
    assert_eq!(sniff(&data), Some(ImageFormat::Png));
    let decoded = image::load_from_memory(&data).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (2, 2));
}

#[test]
fn byte_budget_reencoding_terminates_when_one_pixel_still_cannot_fit() {
    let bytes = encoded(ImageFormat::Png, 4, 4);
    assert_eq!(
        process(
            bytes,
            ImageFormat::Png,
            ImageLimits {
                max_bytes: 1,
                ..limits()
            },
            CancellationToken::new()
        ),
        Err("image cannot fit configured byte limit")
    );
    assert!(process(
        b"not an image".to_vec(),
        ImageFormat::Bmp,
        limits(),
        CancellationToken::new()
    )
    .is_err());
}
