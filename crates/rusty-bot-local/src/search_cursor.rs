//! Stateless query-bound continuation tokens and bounded rendered-row paging.

#[cfg(test)]
#[path = "search_cursor_tests.rs"]
mod tests;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rusty_bot_core::{FileError, FileErrorCode, SearchPage};
use sha2::{Digest, Sha256};

pub(crate) fn binding(query: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(query.as_bytes()))
}

pub(crate) struct Pager {
    pub page: SearchPage,
    binding: String,
    start: u64,
    offset: usize,
    index: u64,
    limit: usize,
    cap: usize,
    used: usize,
    done: bool,
}
impl Pager {
    pub fn new(
        binding: String,
        cursor: Option<&str>,
        limit: usize,
        cap: usize,
    ) -> Result<Self, FileError> {
        let mut start = 0;
        let mut offset = 0;
        if let Some(cursor) = cursor {
            if cursor.len() > 256 {
                return Err(FileError::new(
                    FileErrorCode::Invalid,
                    None,
                    "invalid search cursor",
                ));
            }
            let invalid = || {
                FileError::new(
                    FileErrorCode::Invalid,
                    None,
                    "invalid or cross-query search cursor",
                )
            };
            let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
            let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
            let mut fields = text.split(':');
            if fields.next() != Some(binding.as_str()) {
                return Err(invalid());
            }
            start = fields
                .next()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?;
            offset = fields
                .next()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?;
            if fields.next().is_some() {
                return Err(invalid());
            }
        }
        if limit == 0 || cap == 0 {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                None,
                "search limit and byte cap must be positive",
            ));
        }
        Ok(Self {
            page: SearchPage {
                next_cursor: None,
                lines: vec![],
                matched: 0,
                has_more: false,
                truncated_bytes: false,
                truncated_lines: false,
                output_totals: None,
            },
            binding,
            start,
            offset,
            index: 0,
            limit: limit.min(50_000),
            cap: cap.min(50 * 1024),
            used: 0,
            done: false,
        })
    }
    fn token(&self, index: u64, offset: usize) -> String {
        URL_SAFE_NO_PAD.encode(format!("{}:{index}:{offset}", self.binding))
    }
    /// Return true when the page is full and a continuation is available.
    pub fn push(&mut self, row: String, matched: bool, clipped: bool) -> Result<bool, FileError> {
        let index = self.index;
        self.index = self.index.saturating_add(1);
        if index < self.start {
            return Ok(false);
        }
        if self.done {
            return Ok(true);
        }
        let offset = if index == self.start { self.offset } else { 0 };
        if offset > row.len() || !row.is_char_boundary(offset) {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                None,
                "invalid search cursor row offset",
            ));
        }
        if self.page.lines.len() >= self.limit {
            self.page.has_more = true;
            self.page.next_cursor = Some(self.token(index, offset));
            self.done = true;
            return Ok(true);
        }
        let sep = usize::from(!self.page.lines.is_empty());
        let available = self.cap.saturating_sub(self.used + sep);
        let remain = &row[offset..];
        if remain.len() > available {
            if !self.page.lines.is_empty() {
                self.page.has_more = true;
                self.page.truncated_bytes = true;
                self.page.next_cursor = Some(self.token(index, offset));
                self.done = true;
                return Ok(true);
            }
            let mut end = available.min(remain.len());
            while end > 0 && !remain.is_char_boundary(end) {
                end -= 1;
            }
            if end == 0 {
                return Err(FileError::new(
                    FileErrorCode::Invalid,
                    None,
                    "search byte cap too small for next UTF-8 character",
                ));
            }
            self.page.lines.push(remain[..end].to_owned());
            self.page.matched += u64::from(matched);
            self.page.truncated_lines |= clipped;
            self.page.truncated_bytes = true;
            self.page.has_more = true;
            self.page.next_cursor = Some(self.token(index, offset + end));
            self.done = true;
            return Ok(true);
        }
        self.used += sep + remain.len();
        self.page.lines.push(remain.to_owned());
        self.page.matched += u64::from(matched);
        self.page.truncated_lines |= clipped;
        Ok(false)
    }
}
