/// Incremental Server-Sent Events parser for OpenAI-compatible streams.
///
/// Accepts `data:` lines, joins multi-line data with newlines, ignores
/// comments/`event:`/`id:` lines, and tolerates CRLF. One call to `push`
/// returns every data payload completed by that chunk.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    data: String,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buffer.extend_from_slice(chunk);
        let mut completed = Vec::new();
        while let Some(position) = self.buffer.iter().position(|&byte| byte == b'\n') {
            let line_bytes: Vec<u8> = self.buffer.drain(..=position).collect();
            let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1])
                .trim_end_matches('\r')
                .to_string();
            if self.push_line(&line) {
                completed.push(std::mem::take(&mut self.data));
            }
        }
        completed
    }

    fn push_line(&mut self, line: &str) -> bool {
        if line.is_empty() {
            return !self.data.is_empty();
        }
        if line.starts_with(':') {
            return false;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            if !self.data.is_empty() {
                self.data.push('\n');
            }
            self.data.push_str(rest);
        }
        // `event:`, `id:`, and `retry:` are intentionally ignored.
        false
    }

    /// Flush a final event that was not terminated by a blank line. A trailing
    /// line without a newline is still a complete SSE field, so parse it first.
    pub fn finish(&mut self) -> Option<String> {
        if !self.buffer.is_empty() {
            let bytes = std::mem::take(&mut self.buffer);
            let line = String::from_utf8_lossy(&bytes)
                .trim_end_matches('\r')
                .to_string();
            self.push_line(&line);
        }
        if self.data.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.data))
        }
    }
}
