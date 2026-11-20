//! Bounded top-k selection for unordered fd and directory streams.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rusty_bot_core::{FileError, FileErrorCode, SearchPage};
use std::collections::BTreeSet;

#[cfg(test)]
#[path = "search_key_page_tests.rs"]
mod tests;

pub(crate) struct KeyPage {
    binding: String,
    start: String,
    offset: usize,
    rows: BTreeSet<String>,
    limit: usize,
    cap: usize,
    retained_bytes: usize,
}
impl KeyPage {
    pub fn new(
        binding: String,
        cursor: Option<&str>,
        limit: usize,
        cap: usize,
    ) -> Result<Self, FileError> {
        let invalid = || {
            FileError::new(
                FileErrorCode::Invalid,
                None,
                "invalid or cross-query search cursor",
            )
        };
        let (start, offset) = if let Some(token) = cursor {
            if token.len() > 4 * 1024 * 1024 {
                return Err(invalid());
            }
            let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
            let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
            let mut parts = text.split(':');
            if parts.next() != Some(binding.as_str()) {
                return Err(invalid());
            }
            let offset = parts
                .next()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?;
            let key = URL_SAFE_NO_PAD
                .decode(parts.next().ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if parts.next().is_some() {
                return Err(invalid());
            }
            (String::from_utf8(key).map_err(|_| invalid())?, offset)
        } else {
            (String::new(), 0)
        };
        if limit == 0 || cap == 0 {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                None,
                "search limit and byte cap must be positive",
            ));
        }
        Ok(Self {
            binding,
            start,
            offset,
            rows: BTreeSet::new(),
            limit: limit.min(50_000),
            cap: cap.min(50 * 1024),
            retained_bytes: 0,
        })
    }
    pub fn push(&mut self, row: String) {
        if row < self.start {
            return;
        }
        let size = row
            .len()
            .saturating_sub(if row == self.start { self.offset } else { 0 })
            .saturating_add(1);
        if self.rows.insert(row) {
            self.retained_bytes = self.retained_bytes.saturating_add(size);
        }
        let max = self.limit.min(self.cap.saturating_add(1)).saturating_add(1);
        // Retain a lookahead even if the first key itself exceeds the byte cap
        // or is being resumed partway through.
        while self.rows.len() > 2 {
            let last = self.rows.last().unwrap();
            let last_size = last
                .len()
                .saturating_sub(if *last == self.start { self.offset } else { 0 })
                .saturating_add(1);
            if self.rows.len() <= max
                && self.retained_bytes.saturating_sub(last_size) <= self.cap.saturating_add(1)
            {
                break;
            }
            self.rows.pop_last();
            self.retained_bytes -= last_size;
        }
    }
    fn token(&self, row: &str, offset: usize) -> String {
        URL_SAFE_NO_PAD.encode(format!(
            "{}:{offset}:{}",
            self.binding,
            URL_SAFE_NO_PAD.encode(row)
        ))
    }
    pub fn finish(self) -> Result<SearchPage, FileError> {
        let mut page = SearchPage {
            next_cursor: None,
            lines: vec![],
            matched: 0,
            has_more: false,
            truncated_bytes: false,
            truncated_lines: false,
            output_totals: None,
        };
        let mut used = 0usize;
        for row in &self.rows {
            let offset = if *row == self.start { self.offset } else { 0 };
            if offset > row.len() || !row.is_char_boundary(offset) {
                return Err(FileError::new(
                    FileErrorCode::Invalid,
                    None,
                    "invalid search cursor row offset",
                ));
            }
            let sep = usize::from(!page.lines.is_empty());
            let available = self.cap.saturating_sub(used + sep);
            let remain = &row[offset..];
            if page.lines.len() >= self.limit || remain.len() > available {
                page.has_more = true;
                page.truncated_bytes = page.lines.len() < self.limit;
                if page.lines.is_empty() {
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
                    page.lines.push(remain[..end].to_owned());
                    page.matched += 1;
                    page.next_cursor = Some(self.token(row, offset + end));
                } else {
                    page.next_cursor = Some(self.token(row, offset));
                }
                break;
            }
            used += sep + remain.len();
            page.lines.push(remain.to_owned());
            page.matched += 1;
        }
        Ok(page)
    }
}
