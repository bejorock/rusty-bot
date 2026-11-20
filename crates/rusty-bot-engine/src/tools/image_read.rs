//! Bounded image decoding for `read`. Behavioral reference: Pi Durable
//! c815ddaa3736bd0b009ce6a997ee5003f8a3a532 tools/image*.ts and tools-images.test.ts.
use base64::Engine as _;
use image::{metadata::Orientation, ImageDecoder, ImageFormat, ImageReader};
use rusty_bot_core::{ContentPart, ImageLimits, ToolOutput};
use std::io::Cursor;
use tokio_util::sync::CancellationToken;

pub const MAX_INPUT: usize = 20 * 1024 * 1024;
const MAX_PIXELS: u64 = 16_000_000;
const MAX_ENCODED: usize = 128 * 1024 * 1024;

struct CappedCursor(Cursor<Vec<u8>>);
impl std::io::Write for CappedCursor {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.position().saturating_add(bytes.len() as u64) > MAX_ENCODED as u64 {
            return Err(std::io::Error::other("encoded image exceeds hard limit"));
        }
        std::io::Write::write(&mut self.0, bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl std::io::Seek for CappedCursor {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.0, pos)
    }
}
const MAX_ALLOC: u64 = 128 * 1024 * 1024;

pub fn sniff(bytes: &[u8]) -> Option<ImageFormat> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(ImageFormat::Png)
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some(ImageFormat::Jpeg)
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some(ImageFormat::Gif)
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some(ImageFormat::WebP)
    } else if bytes.starts_with(b"BM") {
        Some(ImageFormat::Bmp)
    } else {
        None
    }
}

fn webp_orientation(bytes: &[u8]) -> Result<Option<Orientation>, &'static str> {
    let mut pos = 12usize;
    while pos < bytes.len() {
        let header = bytes.get(pos..pos + 8).ok_or("truncated WebP chunk")?;
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let start = pos.checked_add(8).ok_or("invalid WebP chunk")?;
        let end = start.checked_add(len).ok_or("invalid WebP chunk")?;
        let payload = bytes.get(start..end).ok_or("truncated WebP chunk")?;
        if &header[..4] == b"EXIF" {
            let tiff = payload.strip_prefix(b"Exif\0\0").unwrap_or(payload);
            return Ok(Orientation::from_exif_chunk(tiff));
        }
        pos = end.checked_add(len % 2).ok_or("invalid WebP chunk")?;
        if pos > bytes.len() {
            return Err("truncated WebP padding");
        }
    }
    Ok(None)
}

pub fn process(
    bytes: Vec<u8>,
    format: ImageFormat,
    limits: ImageLimits,
    cancel: CancellationToken,
) -> Result<ToolOutput, &'static str> {
    if cancel.is_cancelled() {
        return Err("read aborted");
    }
    let limits = ImageLimits {
        max_bytes: limits
            .max_bytes
            .min(rusty_bot_core::model::MAX_TOOL_IMAGE_BYTES),
        ..limits
    };
    if bytes.len() > MAX_INPUT {
        return Err("image exceeds input byte limit");
    }
    if limits.max_bytes == 0 || limits.max_width == 0 || limits.max_height == 0 {
        return Err("invalid image limits");
    }
    // Validate all frames before either returning the original bytes or transforming.
    // Input limits here are hard decode limits, not the model's output limits.
    let animated = if format == ImageFormat::Bmp {
        false
    } else {
        rusty_bot_core::image_validation::validate_image_bytes(
            &bytes,
            format,
            &ImageLimits {
                max_bytes: MAX_INPUT,
                max_width: 16_000,
                max_height: 16_000,
            },
        )?
    };
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    let mut decode_limits = image::Limits::default();
    decode_limits.max_image_width = Some(16_000);
    decode_limits.max_image_height = Some(16_000);
    decode_limits.max_alloc = Some(MAX_ALLOC);
    reader.limits(decode_limits);
    let mut decoder = reader
        .into_decoder()
        .map_err(|_| "invalid or oversized image")?;
    let (width, height) = decoder.dimensions();
    if width == 0
        || height == 0
        || u64::from(width) * u64::from(height) > MAX_PIXELS
        || decoder.total_bytes() > MAX_ALLOC
    {
        return Err("image dimensions exceed decode limits");
    }
    let orientation = decoder
        .orientation()
        .map_err(|_| "invalid image orientation")?;
    let orientation = if format == ImageFormat::WebP {
        webp_orientation(&bytes)?.unwrap_or(orientation)
    } else {
        orientation
    };
    if cancel.is_cancelled() {
        return Err("read aborted");
    }
    let mut decoded =
        image::DynamicImage::from_decoder(decoder).map_err(|_| "invalid image data")?;
    if cancel.is_cancelled() {
        return Err("read aborted");
    }
    let unchanged = format != ImageFormat::Bmp
        && orientation == Orientation::NoTransforms
        && width <= limits.max_width
        && height <= limits.max_height
        && bytes.len() <= limits.max_bytes;
    let (mime, data) = if unchanged {
        (
            match format {
                ImageFormat::Png => "image/png",
                ImageFormat::Jpeg => "image/jpeg",
                ImageFormat::Gif => "image/gif",
                ImageFormat::WebP => "image/webp",
                _ => return Err("unsupported image format"),
            },
            bytes,
        )
    } else {
        if animated {
            return Err("cannot resize or re-encode animated image");
        }
        decoded.apply_orientation(orientation);
        if decoded.width() > limits.max_width || decoded.height() > limits.max_height {
            decoded = decoded.resize(
                limits.max_width,
                limits.max_height,
                image::imageops::FilterType::Lanczos3,
            );
        }
        let mut encoded = CappedCursor(Cursor::new(Vec::new()));
        loop {
            if cancel.is_cancelled() {
                return Err("read aborted");
            }
            encoded.0.get_mut().clear();
            encoded.0.set_position(0);
            decoded
                .write_to(&mut encoded, ImageFormat::Png)
                .map_err(|_| "image encoding failed or exceeded hard limit")?;
            if encoded.0.get_ref().len() <= limits.max_bytes {
                break;
            }
            let w = decoded.width();
            let h = decoded.height();
            if w == 1 && h == 1 {
                return Err("image cannot fit configured byte limit");
            }
            decoded = decoded.resize_exact(
                (w / 2).max(1),
                (h / 2).max(1),
                image::imageops::FilterType::Lanczos3,
            );
        }
        ("image/png", encoded.0.into_inner())
    };
    Ok(ToolOutput {
        content: vec![
            ContentPart::Text {
                text: format!("Read image file [{mime}]"),
            },
            ContentPart::Image {
                mime_type: mime.into(),
                data_base64: base64::engine::general_purpose::STANDARD.encode(data),
            },
        ],
        details: None,
        is_error: false,
    })
}
