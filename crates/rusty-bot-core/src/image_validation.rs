//! Resource-bounded validation for image parts at persistence and provider boundaries.
use std::io::Cursor;

use base64::Engine as _;
use image::{AnimationDecoder, ImageDecoder, ImageFormat, ImageReader};

use crate::{ContentPart, ImageLimits};

const MAX_DIMENSION: u32 = 16_000;
const MAX_PIXELS: u64 = 16_000_000;
const MAX_ALLOC: u64 = 128 * 1024 * 1024;
const MAX_FRAMES: usize = 128;
const MAX_FRAME_PIXELS: u64 = 64_000_000;

// Walk GIF blocks rather than searching for image markers inside compressed data.
// This also requires a trailer, which some decoders tolerate missing.
fn gif_frame_count(bytes: &[u8]) -> Result<usize, &'static str> {
    fn skip_blocks(bytes: &[u8], pos: &mut usize) -> Result<(), &'static str> {
        loop {
            let len = *bytes.get(*pos).ok_or("truncated GIF")? as usize;
            *pos = pos.checked_add(1 + len).ok_or("invalid GIF")?;
            if *pos > bytes.len() {
                return Err("truncated GIF");
            }
            if len == 0 {
                return Ok(());
            }
        }
    }
    let header = bytes.get(..13).ok_or("truncated GIF")?;
    let mut pos = 13usize;
    if header[10] & 0x80 != 0 {
        pos += 3 * (1 << ((header[10] & 7) + 1));
    }
    let mut frames = 0;
    loop {
        let marker = *bytes.get(pos).ok_or("truncated GIF")?;
        pos += 1;
        match marker {
            0x3b => {
                return if frames > 0 {
                    Ok(frames)
                } else {
                    Err("GIF has no frames")
                }
            }
            0x21 => {
                pos = pos.checked_add(1).ok_or("invalid GIF")?;
                skip_blocks(bytes, &mut pos)?;
            }
            0x2c => {
                let descriptor = bytes.get(pos..pos + 9).ok_or("truncated GIF")?;
                pos += 9;
                let value = |i| u32::from(u16::from_le_bytes([descriptor[i], descriptor[i + 1]]));
                let (left, top, width, height) = (value(0), value(2), value(4), value(6));
                let canvas_width = u32::from(u16::from_le_bytes([header[6], header[7]]));
                let canvas_height = u32::from(u16::from_le_bytes([header[8], header[9]]));
                if width == 0
                    || height == 0
                    || left.checked_add(width).is_none_or(|x| x > canvas_width)
                    || top.checked_add(height).is_none_or(|y| y > canvas_height)
                {
                    return Err("GIF frame outside canvas");
                }
                if descriptor[8] & 0x80 != 0 {
                    pos += 3 * (1 << ((descriptor[8] & 7) + 1));
                }
                pos = pos.checked_add(1).ok_or("invalid GIF")?; // LZW code size
                skip_blocks(bytes, &mut pos)?;
                frames += 1;
                if frames > MAX_FRAMES {
                    return Err("image frame limit exceeded");
                }
            }
            _ => return Err("invalid GIF block"),
        }
    }
}

// Enforce chunk completeness and find acTL even after IDAT; APNG requires it
// before IDAT, so a late acTL is invalid rather than silently treated as PNG.
fn png_animation(bytes: &[u8]) -> Result<Option<(usize, bool)>, &'static str> {
    let mut pos = 8usize;
    let mut declared = None;
    let mut frames = 0usize;
    let mut idat = false;
    let mut thumbnail = false;
    loop {
        let header = bytes.get(pos..pos + 8).ok_or("truncated PNG chunk")?;
        let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let end = pos
            .checked_add(12)
            .and_then(|v| v.checked_add(len))
            .ok_or("invalid PNG chunk")?;
        let chunk = bytes.get(pos + 4..end).ok_or("truncated PNG chunk")?;
        let expected = u32::from_be_bytes(chunk[chunk.len() - 4..].try_into().unwrap());
        if crc32fast::hash(&chunk[..chunk.len() - 4]) != expected {
            return Err("invalid PNG checksum");
        }
        match &header[4..8] {
            b"acTL" => {
                if declared.is_some() || idat || len != 8 {
                    return Err("invalid PNG animation control");
                }
                let count = u32::from_be_bytes(chunk[4..8].try_into().unwrap()) as usize;
                if count == 0 || count > MAX_FRAMES {
                    return Err("image frame limit exceeded");
                }
                declared = Some(count);
            }
            b"fcTL" => {
                if declared.is_none() || len != 26 {
                    return Err("invalid PNG frame control");
                }
                frames += 1;
                if frames > MAX_FRAMES {
                    return Err("image frame limit exceeded");
                }
            }
            b"fdAT" if declared.is_none() || frames == 0 || len < 4 => {
                return Err("invalid PNG frame data")
            }
            b"IDAT" => {
                if !idat && declared.is_some() && frames == 0 {
                    thumbnail = true;
                }
                idat = true;
            }
            b"IEND" => {
                return if len == 0
                    && idat
                    && end == bytes.len()
                    && declared.is_none_or(|n| n == frames)
                {
                    Ok(declared.map(|count| (count, thumbnail)))
                } else {
                    Err("invalid PNG end")
                }
            }
            _ => {}
        }
        pos = end;
    }
}

#[derive(Default)]
struct WorkBudget {
    frames: usize,
    pixels: u64,
}
impl WorkBudget {
    fn reserve(&mut self, width: u32, height: u32, count: usize) -> Result<(), &'static str> {
        self.frames = self
            .frames
            .checked_add(count)
            .ok_or("image frame limit exceeded")?;
        let pixels = u64::from(width)
            .checked_mul(u64::from(height))
            .and_then(|value| value.checked_mul(u64::try_from(count).ok()?))
            .ok_or("image frame limit exceeded")?;
        self.pixels = self
            .pixels
            .checked_add(pixels)
            .ok_or("image frame limit exceeded")?;
        if self.frames > MAX_FRAMES || self.pixels > MAX_FRAME_PIXELS {
            return Err("image frame limit exceeded");
        }
        Ok(())
    }
}
fn consume_frames(frames: image::Frames<'_>, expected: usize) -> Result<(), &'static str> {
    let mut count = 0;
    for frame in frames {
        count += 1;
        if count > expected {
            return Err("unexpected image frame");
        }
        frame.map_err(|_| "invalid image frame")?;
    }
    if count != expected || count == 0 {
        return Err("invalid image frame count");
    }
    Ok(())
}

// image-webp 0.2.4 decodes VP8 before comparing its embedded dimensions to
// the canvas/frame descriptor. Check them before that potentially large allocation.
fn validate_webp_vp8(data: &[u8], width: u32, height: u32) -> Result<(), &'static str> {
    let header = data.get(..10).ok_or("truncated WebP VP8")?;
    let w = u32::from(u16::from_le_bytes([header[6], header[7]]) & 0x3fff);
    let h = u32::from(u16::from_le_bytes([header[8], header[9]]) & 0x3fff);
    if header[0] & 1 != 0 || header[3..6] != [0x9d, 0x01, 0x2a] || w != width || h != height {
        return Err("invalid WebP VP8 dimensions");
    }
    Ok(())
}

fn webp_frame_count(bytes: &[u8], width: u32, height: u32) -> Result<usize, &'static str> {
    let header = bytes.get(..12).ok_or("truncated WebP")?;
    let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if size.checked_add(8) != Some(bytes.len()) {
        return Err("invalid WebP length");
    }
    let mut pos = 12usize;
    let mut count = 0;
    while pos < bytes.len() {
        let chunk = bytes.get(pos..pos + 8).ok_or("truncated WebP chunk")?;
        let len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
        let start = pos.checked_add(8).ok_or("invalid WebP chunk")?;
        let end = start.checked_add(len).ok_or("invalid WebP chunk")?;
        let data = bytes.get(start..end).ok_or("truncated WebP chunk")?;
        pos = end.checked_add(len % 2).ok_or("invalid WebP chunk")?;
        if pos > bytes.len() {
            return Err("truncated WebP chunk");
        }
        match &chunk[..4] {
            b"VP8 " => validate_webp_vp8(data, width, height)?,
            b"ANMF" => {
                count += 1;
                if count > MAX_FRAMES {
                    return Err("image frame limit exceeded");
                }
                let frame = data.get(..16).ok_or("truncated WebP frame")?;
                let u24 = |i| {
                    u32::from(frame[i])
                        | (u32::from(frame[i + 1]) << 8)
                        | (u32::from(frame[i + 2]) << 16)
                };
                let (x, y, w, h) = (u24(0) * 2, u24(3) * 2, u24(6) + 1, u24(9) + 1);
                if x.checked_add(w).is_none_or(|n| n > width)
                    || y.checked_add(h).is_none_or(|n| n > height)
                {
                    return Err("WebP frame outside canvas");
                }
                let mut subpos = 16usize;
                while subpos < data.len() {
                    let sub = data
                        .get(subpos..subpos + 8)
                        .ok_or("truncated WebP frame chunk")?;
                    let sublen = u32::from_le_bytes(sub[4..8].try_into().unwrap()) as usize;
                    let substart = subpos + 8;
                    let subend = substart
                        .checked_add(sublen)
                        .ok_or("invalid WebP frame chunk")?;
                    let payload = data
                        .get(substart..subend)
                        .ok_or("truncated WebP frame chunk")?;
                    if &sub[..4] == b"VP8 " {
                        validate_webp_vp8(payload, w, h)?;
                    }
                    subpos = subend
                        .checked_add(sublen % 2)
                        .ok_or("invalid WebP frame chunk")?;
                    if subpos > data.len() {
                        return Err("truncated WebP frame chunk");
                    }
                }
            }
            _ => {}
        }
    }
    Ok(count)
}

/// Fully decode every image and animation frame, one at a time. The byte cap is
/// aggregate per call. Invoke on a blocking pool from asynchronous entry points.
/// Returns whether the image contains more than one frame (or is APNG/WebP animation).
pub fn validate_image_bytes(
    bytes: &[u8],
    format: ImageFormat,
    limits: &ImageLimits,
) -> Result<bool, &'static str> {
    validate_image_bytes_with_budget(bytes, format, limits, &mut WorkBudget::default())
}

fn validate_image_bytes_with_budget(
    bytes: &[u8],
    format: ImageFormat,
    limits: &ImageLimits,
    budget: &mut WorkBudget,
) -> Result<bool, &'static str> {
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    let mut decoder_limits = image::Limits::default();
    decoder_limits.max_image_width = Some(MAX_DIMENSION.min(limits.max_width));
    decoder_limits.max_image_height = Some(MAX_DIMENSION.min(limits.max_height));
    decoder_limits.max_alloc = Some(MAX_ALLOC);
    reader.limits(decoder_limits.clone());
    let decoder = reader
        .into_decoder()
        .map_err(|_| "invalid or oversized image")?;
    let (width, height) = decoder.dimensions();
    if width == 0
        || height == 0
        || width > limits.max_width
        || height > limits.max_height
        || u64::from(width) * u64::from(height) > MAX_PIXELS
        || decoder.total_bytes() > MAX_ALLOC
    {
        return Err("image dimensions exceed decode limits");
    }
    match format {
        ImageFormat::Gif => {
            let expected = gif_frame_count(bytes)?;
            budget.reserve(width, height, expected)?;
            let mut decoder = image::codecs::gif::GifDecoder::new(Cursor::new(bytes))
                .map_err(|_| "invalid image data")?;
            decoder
                .set_limits(decoder_limits)
                .map_err(|_| "invalid or oversized image")?;
            consume_frames(decoder.into_frames(), expected)?;
            Ok(expected > 1)
        }
        ImageFormat::Png if png_animation(bytes)?.is_some() => {
            let (expected, thumbnail) = png_animation(bytes)?.ok_or("invalid PNG animation")?;
            // The decoder also decompresses an optional default-image thumbnail.
            budget.reserve(width, height, expected + usize::from(thumbnail))?;
            let mut decoder = image::codecs::png::PngDecoder::new(Cursor::new(bytes))
                .map_err(|_| "invalid image data")?;
            decoder
                .set_limits(decoder_limits)
                .map_err(|_| "invalid or oversized image")?;
            if !decoder.is_apng().map_err(|_| "invalid image data")? {
                return Err("invalid PNG animation");
            }
            consume_frames(
                decoder
                    .apng()
                    .map_err(|_| "invalid image data")?
                    .into_frames(),
                expected,
            )?;
            Ok(true)
        }
        ImageFormat::WebP => {
            let decoder = image::codecs::webp::WebPDecoder::new(Cursor::new(bytes))
                .map_err(|_| "invalid image data")?;
            let count = webp_frame_count(bytes, width, height)?;
            if decoder.has_animation() {
                if count == 0 {
                    return Err("image has no frames");
                }
                budget.reserve(width, height, count)?;
                consume_frames(decoder.into_frames(), count)?;
                Ok(true)
            } else {
                if count != 0 {
                    return Err("invalid WebP animation");
                }
                budget.reserve(width, height, 1)?;
                image::DynamicImage::from_decoder(decoder).map_err(|_| "invalid image data")?;
                Ok(false)
            }
        }
        _ => {
            budget.reserve(width, height, 1)?;
            image::DynamicImage::from_decoder(decoder).map_err(|_| "invalid image data")?;
            Ok(false)
        }
    }
}

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
