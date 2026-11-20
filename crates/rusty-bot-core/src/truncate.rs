//! Output truncation with head/tail retention.
//!
//! Ported behaviorally from Pi Durable `src/truncate.ts` at
//! earendil-works/pi `c815ddaa3736bd0b009ce6a997ee5003f8a3a532` (MIT,
//! Copyright (c) 2025 Mario Zechner). This Rust implementation is original;
//! the limits and head/tail preservation semantics are shared.

pub const DEFAULT_MAX_LINES: usize = 2_000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncatedText {
    pub text: String,
    pub truncated: bool,
    pub total_lines: usize,
    pub total_bytes: usize,
}

/// Keep the first and last halves of the allowed budget and mark the elision.
/// The marker never hides the fact that content was dropped.
pub fn truncate_text(text: &str, max_lines: usize, max_bytes: usize) -> TruncatedText {
    let max_lines = max_lines.max(2);
    let max_bytes = max_bytes.max(2);
    let total_bytes = text.len();
    let total_lines = text.lines().count();

    if total_bytes <= max_bytes && total_lines <= max_lines {
        return TruncatedText {
            text: text.to_string(),
            truncated: false,
            total_lines,
            total_bytes,
        };
    }

    let lines: Vec<&str> = text.lines().collect();
    let line_limited = lines.len() > max_lines;
    let bytes_limited = total_bytes > max_bytes;

    let (head, tail, omitted_lines, omitted_bytes) = if line_limited {
        let keep_head = max_lines / 2;
        let keep_tail = max_lines - keep_head;
        let head_lines = &lines[..keep_head.min(lines.len())];
        let tail_lines = &lines[lines.len().saturating_sub(keep_tail)..];
        let head = head_lines.join("\n");
        let tail = tail_lines.join("\n");
        let omitted_lines = lines
            .len()
            .saturating_sub(head_lines.len() + tail_lines.len());
        let head_bytes = head.len();
        let tail_bytes = tail.len();
        let omitted_bytes = total_bytes.saturating_sub(head_bytes + tail_bytes);
        (head, tail, omitted_lines, omitted_bytes)
    } else {
        let half = max_bytes / 2;
        let head = prefix_at_char_boundary(text, half);
        let tail = suffix_at_char_boundary(text, half);
        let omitted_bytes = total_bytes.saturating_sub(head.len() + tail.len());
        let omitted_lines = 0;
        (
            head.to_string(),
            tail.to_string(),
            omitted_lines,
            omitted_bytes,
        )
    };

    // Re-check the byte budget after line selection; a line window can still
    // exceed the byte budget for very long lines.
    let (head, tail) = if head.len() + tail.len() > max_bytes {
        let half = max_bytes / 2;
        (
            prefix_at_char_boundary(&head, half).to_string(),
            suffix_at_char_boundary(&tail, half).to_string(),
        )
    } else {
        (head, tail)
    };

    let marker = format!(
        "\n... [truncated: {omitted_lines} lines / {omitted_bytes} bytes omitted{}] ...\n",
        if bytes_limited {
            ", output exceeded limits"
        } else {
            ""
        }
    );
    let mut result = String::with_capacity(head.len() + marker.len() + tail.len());
    result.push_str(&head);
    result.push_str(&marker);
    result.push_str(&tail);

    TruncatedText {
        text: result,
        truncated: true,
        total_lines,
        total_bytes,
    }
}

fn prefix_at_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn suffix_at_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut start = text.len() - max_bytes;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}
