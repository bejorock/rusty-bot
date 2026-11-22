//! Strict UTF-8, bounded SSE framing. Event names are carried in provider JSON.
use crate::protocol;
use crate::ModelError;
#[derive(Default)]
pub(crate) struct Sse {
    line: Vec<u8>,
    data: String,
    cr: bool,
}
impl Sse {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, ModelError> {
        let mut events = Vec::new();
        for &byte in bytes {
            if self.cr {
                self.cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                if let Some(event) = self.end_line()? {
                    events.push(event);
                }
                self.cr = byte == b'\r';
            } else {
                self.line.push(byte);
                if self.line.len() > 4 * 1024 * 1024 {
                    return Err(protocol());
                }
            }
        }
        Ok(events)
    }
    fn end_line(&mut self) -> Result<Option<String>, ModelError> {
        let line = std::str::from_utf8(&self.line).map_err(|_| protocol())?;
        if line.is_empty() {
            return if self.data.is_empty() {
                Ok(None)
            } else {
                self.line.clear();
                Ok(Some(std::mem::take(&mut self.data)))
            };
        }
        if let Some(rest) = line.strip_prefix("data:") {
            if !self.data.is_empty() {
                self.data.push('\n');
            }
            self.data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
            if self.data.len() > 4 * 1024 * 1024 {
                return Err(protocol());
            }
        }
        self.line.clear();
        Ok(None)
    }
    pub(crate) fn finish(&mut self) -> Result<Option<String>, ModelError> {
        if !self.line.is_empty() {
            self.end_line()?;
        }
        Ok(if self.data.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.data))
        })
    }
}
