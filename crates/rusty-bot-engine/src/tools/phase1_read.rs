use super::*;
use futures::StreamExt;

pub struct ReadTool;

// Bounded selected-line storage with complete byte/line accounting. Coding-agent
// 4afe78de read.ts uses Buffer-style lossy decoding and preserves a UTF-8 BOM.
struct Scan {
    offset: usize,
    limit: usize,
    line: usize,
    selected_lines: usize,
    selected_bytes: usize,
    last_empty: bool,
    first_bytes: usize,
    head: String,
    line_bytes: usize,
    lines: Vec<String>,
    stored_bytes: usize,
}
impl Scan {
    fn selected(&self) -> bool {
        self.line >= self.offset && self.line.saturating_sub(self.offset) < self.limit
    }
    fn push(&mut self, ch: char) {
        if ch == '\n' {
            if self.selected() {
                self.finish_line();
                if self.line.saturating_sub(self.offset).saturating_add(1) < self.limit {
                    self.selected_bytes = self.selected_bytes.saturating_add(1);
                }
            }
            self.line = self.line.saturating_add(1);
            self.head.clear();
            self.line_bytes = 0;
        } else if self.selected() {
            let n = ch.len_utf8();
            self.selected_bytes = self.selected_bytes.saturating_add(n);
            self.line_bytes = self.line_bytes.saturating_add(n);
            if self.lines.len() <= DEFAULT_MAX_LINES
                && self.stored_bytes <= DEFAULT_MAX_BYTES
                && self.head.len() + n <= DEFAULT_MAX_BYTES + 4
            {
                self.head.push(ch);
            }
        }
    }
    fn finish_line(&mut self) {
        if self.selected_lines == 0 {
            self.first_bytes = self.line_bytes;
        }
        self.selected_lines = self.selected_lines.saturating_add(1);
        self.last_empty = self.line_bytes == 0;
        if self.lines.len() <= DEFAULT_MAX_LINES && self.stored_bytes <= DEFAULT_MAX_BYTES {
            self.stored_bytes = self
                .stored_bytes
                .saturating_add(self.line_bytes)
                .saturating_add(usize::from(!self.lines.is_empty()));
            self.lines.push(std::mem::take(&mut self.head));
        }
    }
}
fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "read",
            "Read a file with one-based line pagination.",
            json!({
                "type":"object", "properties": {"path":{"type":"string"}, "offset":{"type":"number"}, "limit":{"type":"number"}}, "required":["path"]
            }),
        )
    }
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }
    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let Some(path) = arguments.get("path").and_then(Value::as_str) else {
            return Ok(invalid("invalid read arguments: missing or invalid path"));
        };
        let number = |name: &str| -> Result<Option<f64>, ToolOutput> {
            match arguments.get(name) {
                None => Ok(None),
                Some(Value::Null) => Ok(Some(0.0)),
                Some(value) => value.as_f64().map(Some).ok_or_else(|| {
                    invalid(format!("invalid read arguments: {name} must be a number"))
                }),
            }
        };
        let raw_offset = match number("offset") {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };
        let raw_limit = match number("limit") {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };
        let start = raw_offset.map_or(0.0, |v| (v - 1.0).max(0.0));
        let offset = (start.trunc() as usize).saturating_add(1);
        let mut stream = match ctx
            .files
            .open_read(Path::new(path), ctx.cancel.clone())
            .await
        {
            Ok(s) => s,
            Err(e) => {
                // Only adapt an actual host ENOENT, not arbitrary provider NotFound errors.
                if cfg!(unix)
                    && e.code == FileErrorCode::NotFound
                    && e.message == std::io::Error::from_raw_os_error(2).to_string()
                {
                    if let Some(path) = &e.path {
                        return Ok(ToolOutput {
                            content: crate::prompt::text_content(&format!(
                                "ENOENT: no such file or directory, access '{}'",
                                path.display()
                            )),
                            details: None,
                            is_error: true,
                        });
                    }
                }
                return Ok(file_error(e.code, e));
            }
        };
        let mut prefix = Vec::new();
        let mut remainder = None;
        while prefix.len() < 12 {
            let next = tokio::select! {
                _ = ctx.cancel.cancelled() => return Ok(file_error(FileErrorCode::Aborted, "read aborted")),
                next = stream.next() => next,
            };
            match next {
                Some(Ok(chunk)) => {
                    let take = (12 - prefix.len()).min(chunk.len());
                    prefix.extend_from_slice(&chunk[..take]);
                    if take < chunk.len() {
                        remainder = Some(chunk.slice(take..));
                    }
                }
                Some(Err(e)) => return Ok(file_error(e.code, e)),
                None => break,
            }
        }
        if let Some(format) = super::image_read::sniff(&prefix) {
            let Some(limits) = ctx.image_limits.clone() else {
                return Ok(ToolOutput::error(
                    "model does not support image input",
                    "unsupported_image",
                ));
            };
            if let Some(chunk) = remainder.take() {
                if chunk.len() > super::image_read::MAX_INPUT.saturating_sub(prefix.len()) {
                    return Ok(ToolOutput::error(
                        "image exceeds input byte limit",
                        "too_large",
                    ));
                }
                prefix.extend_from_slice(&chunk);
            }
            while let Some(chunk) = tokio::select! {
                _ = ctx.cancel.cancelled() => return Ok(file_error(FileErrorCode::Aborted, "read aborted")),
                next = stream.next() => next,
            } {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(e) => return Ok(file_error(e.code, e)),
                };
                if chunk.len() > super::image_read::MAX_INPUT.saturating_sub(prefix.len()) {
                    return Ok(ToolOutput::error(
                        "image exceeds input byte limit",
                        "too_large",
                    ));
                }
                prefix.extend_from_slice(&chunk);
            }
            let cancel = ctx.cancel.clone();
            let work = tokio::task::spawn_blocking(move || {
                super::image_read::process(prefix, format, limits, cancel)
            });
            let completed = tokio::select! {
                _ = ctx.cancel.cancelled() => return Ok(file_error(FileErrorCode::Aborted, "read aborted")),
                result = work => result,
            };
            return match completed {
                Ok(Ok(output)) => Ok(output),
                Ok(Err("read aborted")) => Ok(file_error(FileErrorCode::Aborted, "read aborted")),
                Ok(Err(error)) => Ok(ToolOutput::error(error, "invalid_image")),
                Err(_) => Ok(ToolOutput::error(
                    "image processing failed",
                    "invalid_image",
                )),
            };
        }
        let mut head = vec![Ok(bytes::Bytes::from(prefix))];
        if let Some(chunk) = remainder {
            head.push(Ok(chunk));
        }
        let mut stream: rusty_bot_core::FileByteStream =
            Box::pin(futures::stream::iter(head).chain(stream));
        let end = raw_limit.map(|v| start + v);
        // Negative Array.slice end indexes are relative to EOF. Count newlines
        // without retaining the file, then reopen through the same provider.
        let end_index = if end.is_some_and(|v| v.trunc() < 0.0) {
            let mut total = 1usize;
            while let Some(chunk) = tokio::select! {
                _ = ctx.cancel.cancelled() => return Ok(file_error(FileErrorCode::Aborted, "read aborted")),
                chunk = stream.next() => chunk,
            } {
                let chunk = match chunk {
                    Ok(v) => v,
                    Err(e) => return Ok(file_error(e.code, e)),
                };
                total = total.saturating_add(chunk.iter().filter(|&&b| b == b'\n').count());
            }
            stream = match ctx
                .files
                .open_read(Path::new(path), ctx.cancel.clone())
                .await
            {
                Ok(v) => v,
                Err(e) => return Ok(file_error(e.code, e)),
            };
            Some(((total as f64) + end.unwrap().trunc()).max(0.0) as usize)
        } else {
            end.map(|v| v.trunc() as usize)
        };
        let mut scan = Scan {
            offset,
            limit: end_index.map_or(usize::MAX, |end| end.saturating_sub(offset - 1)),
            line: 1,
            selected_lines: 0,
            selected_bytes: 0,
            last_empty: true,
            first_bytes: 0,
            head: String::new(),
            line_bytes: 0,
            lines: Vec::new(),
            stored_bytes: 0,
        };
        let mut pending = Vec::new();
        while let Some(chunk) = stream.next().await {
            if ctx.cancel.is_cancelled() {
                return Ok(file_error(FileErrorCode::Aborted, "read aborted"));
            }
            let chunk = match chunk {
                Ok(v) => v,
                Err(e) => return Ok(file_error(e.code, e)),
            };
            for window in chunk.chunks(8192) {
                if ctx.cancel.is_cancelled() {
                    return Ok(file_error(FileErrorCode::Aborted, "read aborted"));
                }
                pending.extend_from_slice(window);
                let mut start = 0;
                while start < pending.len() {
                    match std::str::from_utf8(&pending[start..]) {
                        Ok(valid) => {
                            for ch in valid.chars() {
                                scan.push(ch);
                            }
                            start = pending.len();
                        }
                        Err(e) => {
                            let end = start + e.valid_up_to();
                            let valid = std::str::from_utf8(&pending[start..end])
                                .expect("validated prefix");
                            for ch in valid.chars() {
                                scan.push(ch);
                            }
                            start = end;
                            if let Some(len) = e.error_len() {
                                scan.push('\u{fffd}');
                                start += len;
                            } else {
                                break;
                            }
                        }
                    }
                }
                pending.drain(..start);
            }
        }
        if ctx.cancel.is_cancelled() {
            return Ok(file_error(FileErrorCode::Aborted, "read aborted"));
        }
        if !pending.is_empty() {
            scan.push('\u{fffd}');
        }
        if scan.selected() {
            scan.finish_line();
        }
        let total = scan.line;
        if start >= total as f64 {
            return Ok(ToolOutput {
                content: crate::prompt::text_content(&format!(
                    "Offset {} is beyond end of file ({total} lines total)",
                    raw_offset.unwrap_or(1.0)
                )),
                details: None,
                is_error: true,
            });
        }
        let selected_count = if scan.selected_bytes == 0 {
            0
        } else {
            scan.selected_lines - usize::from(scan.last_empty)
        };
        let truncated =
            selected_count > DEFAULT_MAX_LINES || scan.selected_bytes > DEFAULT_MAX_BYTES;
        let first_exceeds = truncated && scan.first_bytes > DEFAULT_MAX_BYTES;
        let mut content = String::new();
        let mut output_lines = 0;
        let mut truncated_by = "lines";
        if !truncated {
            content = scan.lines.join("\n");
            output_lines = selected_count;
        } else if first_exceeds {
            truncated_by = "bytes";
        } else {
            for line in scan
                .lines
                .iter()
                .take(selected_count)
                .take(DEFAULT_MAX_LINES)
            {
                let extra = usize::from(output_lines > 0);
                if content.len() + extra + line.len() > DEFAULT_MAX_BYTES {
                    truncated_by = "bytes";
                    break;
                }
                if extra != 0 {
                    content.push('\n');
                }
                content.push_str(line);
                output_lines += 1;
            }
            if output_lines >= DEFAULT_MAX_LINES {
                truncated_by = "lines";
            }
        }
        let details = if truncated {
            Some(json!({"truncation": {
                "content":content, "truncated":true, "truncatedBy":truncated_by,
                "totalLines":selected_count, "totalBytes":scan.selected_bytes,
                "outputLines":output_lines, "outputBytes":content.len(), "lastLinePartial":false,
                "firstLineExceedsLimit":first_exceeds, "maxLines":DEFAULT_MAX_LINES, "maxBytes":DEFAULT_MAX_BYTES
            }}))
        } else {
            None
        };
        if first_exceeds {
            let display = start + 1.0;
            content = format!("[Line {display} is {}, exceeds {} limit. Use bash: sed -n '{display}p' {path} | head -c {DEFAULT_MAX_BYTES}]", format_size(scan.first_bytes), format_size(DEFAULT_MAX_BYTES));
        } else if truncated {
            let display = start + 1.0;
            let end = display + output_lines as f64 - 1.0;
            let limit_note = if truncated_by == "bytes" {
                format!(" ({} limit)", format_size(DEFAULT_MAX_BYTES))
            } else {
                String::new()
            };
            content.push_str(&format!("\n\n[Showing lines {display}-{end} of {total}{limit_note}. Use offset={} to continue.]", end + 1.0));
        } else if let Some(limit) = raw_limit {
            let selected = (start + limit).min(total as f64) - start;
            if start + selected < total as f64 {
                content.push_str(&format!(
                    "\n\n[{} more lines in file. Use offset={} to continue.]",
                    total as f64 - (start + selected),
                    start + selected + 1.0
                ));
            }
        }
        Ok(ToolOutput {
            content: crate::prompt::text_content(&content),
            details,
            is_error: false,
        })
    }
}
