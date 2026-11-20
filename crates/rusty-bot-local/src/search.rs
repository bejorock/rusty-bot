//! Bounded in-process workspace search. Traversal never follows directory symlinks.
use crate::stream_match::Matcher;
use globset::{GlobBuilder, GlobMatcher};
use ignore::WalkBuilder;
use rusty_bot_core::{FileError, FileErrorCode, GrepQuery, ListQuery, SearchPage, SearchRequest};
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

const MAX_RESULTS: usize = 50_000;
const MAX_BYTES: usize = 50 * 1024;

fn invalid(message: impl Into<String>) -> FileError {
    FileError::new(FileErrorCode::Invalid, None, message)
}
fn aborted() -> FileError {
    FileError::new(FileErrorCode::Aborted, None, "search aborted")
}
fn check(cancel: &CancellationToken) -> Result<(), FileError> {
    if cancel.is_cancelled() {
        Err(aborted())
    } else {
        Ok(())
    }
}
fn text_path(path: &Path) -> Result<&str, FileError> {
    path.to_str()
        .ok_or_else(|| invalid("non-UTF-8 path in search"))
}
fn glob(pattern: &str, case_insensitive: bool) -> Result<GlobMatcher, FileError> {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .case_insensitive(case_insensitive)
        .build()
        .map(|g| g.compile_matcher())
        .map_err(|e| invalid(format!("invalid glob: {e}")))
}
fn strip_dot_prefix(mut pattern: &str) -> &str {
    while let Some(rest) = pattern.strip_prefix("./") {
        pattern = rest;
    }
    pattern
}
fn matches_glob(matcher: &GlobMatcher, pattern: &str, path: &Path) -> bool {
    if pattern.contains('/') {
        matcher.is_match(path)
    } else {
        path.file_name().is_some_and(|n| matcher.is_match(n))
    }
}
struct Collector {
    page: SearchPage,
    cap: usize,
    used: usize,
}
impl Collector {
    fn new(cap: usize) -> Self {
        Self {
            page: SearchPage {
                next_cursor: None,
                lines: vec![],
                matched: 0,
                has_more: false,
                truncated_bytes: false,
                truncated_lines: false,
                output_totals: Some((0, 0)),
            },
            cap: cap.min(MAX_BYTES),
            used: 0,
        }
    }
    fn push(&mut self, line: String) {
        if let Some((count, bytes)) = &mut self.page.output_totals {
            *bytes = bytes.saturating_add(line.len() as u64 + u64::from(*count > 0));
            *count = count.saturating_add(1);
        }
        let extra = line.len() + usize::from(!self.page.lines.is_empty());
        if self.page.truncated_bytes || self.used.saturating_add(extra) > self.cap {
            self.page.truncated_bytes = true;
            return;
        }
        self.used += extra;
        self.page.lines.push(line);
    }
}
fn walk(root: &Path, positive_glob: Option<&str>) -> Result<ignore::Walk, FileError> {
    let mut b = WalkBuilder::new(root);
    if let Some(pattern) = positive_glob {
        let mut overrides = ignore::overrides::OverrideBuilder::new(root);
        overrides.add(pattern).map_err(walk_error)?;
        b.overrides(overrides.build().map_err(walk_error)?);
    }
    // Like fd in the pinned Pi source, enable repository boundaries when the
    // search starts inside a repository, but honor .gitignore outside one too.
    let inside_git = root.ancestors().any(|dir| dir.join(".git").exists());
    b.hidden(false)
        .follow_links(false)
        .require_git(inside_git)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(false)
        .filter_entry(|entry| entry.depth() == 0 || entry.file_name() != ".git");
    Ok(b.build())
}
fn walk_error(e: ignore::Error) -> FileError {
    FileError::new(FileErrorCode::Unknown, None, e.to_string())
}

pub fn search(
    root: PathBuf,
    request: SearchRequest,
    cancel: CancellationToken,
) -> Result<SearchPage, FileError> {
    check(&cancel)?;
    if let SearchRequest::Find(q) = request {
        #[cfg(unix)]
        return crate::find_fd::find(&root, q, &cancel);
        #[cfg(not(unix))]
        let _ = q;
        #[cfg(not(unix))]
        return Err(FileError::new(
            FileErrorCode::NotSupported,
            Some(root),
            "fd find adapter is only supported on Unix",
        ));
    }
    let meta = std::fs::metadata(&root).map_err(|e| FileError::from_io(&root, &e))?;
    match request {
        SearchRequest::List(q) => list(&root, meta.is_dir(), q, &cancel),
        SearchRequest::Grep(q) => {
            #[cfg(unix)]
            {
                crate::grep_rg::grep(&root, meta.is_dir(), q, &cancel)
            }
            #[cfg(not(unix))]
            {
                let _ = q;
                Err(FileError::new(
                    FileErrorCode::NotSupported,
                    Some(root),
                    "rg grep adapter is only supported on Unix",
                ))
            }
        }
        SearchRequest::Find(_) => unreachable!(),
    }
}
fn list(
    root: &Path,
    dir: bool,
    q: ListQuery,
    cancel: &CancellationToken,
) -> Result<SearchPage, FileError> {
    if !dir {
        return Err(FileError::new(
            FileErrorCode::NotDirectory,
            Some(root.into()),
            "not a directory",
        ));
    }
    let binding = crate::search_cursor::binding(&format!("list:{root:?}:{}", q.include_hidden));
    let mut entries =
        crate::search_key_page::KeyPage::new(binding, q.cursor.as_deref(), q.limit, q.byte_cap)?;
    for e in std::fs::read_dir(root).map_err(|e| FileError::from_io(root, &e))? {
        check(cancel)?;
        let e = e.map_err(|e| FileError::from_io(root, &e))?;
        let name = e
            .file_name()
            .into_string()
            .map_err(|_| invalid("non-UTF-8 directory entry"))?;
        if !q.include_hidden && name.starts_with('.') {
            continue;
        }
        let ty = e
            .file_type()
            .map_err(|err| FileError::from_io(&e.path(), &err))?;
        let name = if ty.is_dir() {
            format!("{name}/")
        } else {
            name
        };
        entries.push(name);
    }
    entries.finish()
}
fn grep(
    root: &Path,
    dir: bool,
    q: GrepQuery,
    cancel: &CancellationToken,
) -> Result<SearchPage, FileError> {
    if q.limit == 0 {
        return Err(invalid("limit must be positive"));
    }
    let pattern = if q.literal {
        regex::escape(&q.pattern)
    } else {
        q.pattern.clone()
    };
    let mut regex = Matcher::new(&pattern, q.ignore_case)
        .map_err(|e| invalid(format!("invalid regex: {e}")))?;
    let glob_pattern = q
        .glob
        .as_deref()
        .map(|p| p.strip_prefix('!').unwrap_or(p))
        .map(strip_dot_prefix);
    let negate_glob = q.glob.as_deref().is_some_and(|p| p.starts_with('!'));
    let matcher = glob_pattern.map(|p| glob(p, false)).transpose()?;
    let mut out = Collector::new(q.byte_cap);
    if q.limit > MAX_RESULTS {
        return Err(invalid("limit exceeds safety maximum of 50000"));
    }
    let limit = q.limit;
    if !dir {
        scan(
            root,
            text_path(Path::new(
                root.file_name()
                    .ok_or_else(|| invalid("missing filename"))?,
            ))?,
            &mut regex,
            &q,
            limit,
            &mut out,
            cancel,
        )?;
        return Ok(out.page);
    }
    for entry in walk(root, if negate_glob { None } else { glob_pattern })? {
        check(cancel)?;
        let entry = entry.map_err(walk_error)?;
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|e| invalid(e.to_string()))?;
        let display = text_path(relative)?;
        if let (Some(m), Some(p)) = (&matcher, glob_pattern) {
            if matches_glob(m, p, relative) == negate_glob {
                continue;
            }
        }
        scan(
            entry.path(),
            display,
            &mut regex,
            &q,
            limit,
            &mut out,
            cancel,
        )?;
    }
    Ok(out.page)
}
fn scan(
    path: &Path,
    display: &str,
    regex: &mut Matcher,
    q: &GrepQuery,
    limit: usize,
    out: &mut Collector,
    cancel: &CancellationToken,
) -> Result<(), FileError> {
    let file = File::open(path).map_err(|e| FileError::from_io(path, &e))?;
    let mut reader = BufReader::new(file);
    let mut first = [0u8; 8192];
    let n = reader
        .read(&mut first)
        .map_err(|e| FileError::from_io(path, &e))?;
    let (encoding, skip) = if first[..n].starts_with(&[0xff, 0xfe]) {
        (encoding_rs::UTF_16LE, 2)
    } else if first[..n].starts_with(&[0xfe, 0xff]) {
        (encoding_rs::UTF_16BE, 2)
    } else if first[..n].starts_with(&[0xef, 0xbb, 0xbf]) {
        (encoding_rs::UTF_8, 3)
    } else {
        (encoding_rs::UTF_8, 0)
    };
    // Classify the entire UTF-8 file before publishing any hits. This avoids
    // both late-NUL false positives and unbounded per-file result buffering.
    // The second pass is over the same descriptor, not a workspace snapshot.
    if encoding == encoding_rs::UTF_8 {
        if first[skip..n].contains(&0) {
            return Ok(());
        }
        let mut probe = [0u8; 8192];
        loop {
            check(cancel)?;
            let count = reader
                .read(&mut probe)
                .map_err(|e| FileError::from_io(path, &e))?;
            if count == 0 {
                break;
            }
            if probe[..count].contains(&0) {
                return Ok(());
            }
        }
        reader
            .seek(SeekFrom::Start(n as u64))
            .map_err(|e| FileError::from_io(path, &e))?;
    }
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut line = String::new();
    let mut units = 0usize;
    let mut clipped = false;
    let mut pending_cr = false;
    let mut has_content = false;
    regex.reset();
    let mut number = 0usize;
    let mut before: VecDeque<(usize, String, bool)> = VecDeque::new();
    let mut before_bytes = 0usize;
    let mut last_emitted = 0usize;
    let mut trailing = 0usize;
    let mut process = |line: &str, clipped: bool, matched: bool| {
        number += 1;
        let line = if clipped {
            format!("{line}… [line truncated]")
        } else {
            line.to_owned()
        };
        if matched {
            out.page.matched += 1;
            if out.page.matched > limit as u64 {
                out.page.has_more = true;
            }
        }
        let allowed = matched && out.page.matched <= limit as u64;
        if allowed {
            // Context can be arbitrarily large. Retain only a bounded suffix;
            // mark a byte truncation if earlier requested context was evicted.
            let wanted_start = number
                .saturating_sub(q.context)
                .max(last_emitted.saturating_add(1));
            let lost_context = before.front().map_or(number, |(i, _, _)| *i) > wanted_start;
            for (i, text, was_clipped) in &before {
                if *i > last_emitted {
                    out.page.truncated_lines |= *was_clipped;
                    out.push(format!("{display}-{i}- {text}"));
                    last_emitted = *i;
                }
            }
            out.page.truncated_lines |= clipped;
            out.push(format!("{display}:{number}: {line}"));
            if lost_context {
                out.page.truncated_bytes = true;
                out.page.output_totals = None;
            }
            last_emitted = number;
            trailing = q.context;
        } else if trailing > 0 {
            out.page.truncated_lines |= clipped;
            out.push(format!("{display}-{number}- {line}"));
            last_emitted = number;
            trailing -= 1;
        }
        if q.context > 0 {
            before_bytes += line.len() + 1;
            before.push_back((number, line, clipped));
            while before.len() > q.context || before_bytes > MAX_BYTES {
                if let Some((_, removed, _)) = before.pop_front() {
                    before_bytes -= removed.len() + 1;
                }
            }
        }
    };
    let mut pending = &first[skip..n];
    let mut buffer = [0u8; 8192];
    let mut eof = false;
    loop {
        check(cancel)?;
        let mut decoded = String::with_capacity(pending.len().saturating_mul(3).saturating_add(16));
        let (res, consumed, _) = decoder.decode_to_string(pending, &mut decoded, eof);
        for ch in decoded.chars() {
            // A file changed between the classification and matching passes.
            if encoding == encoding_rs::UTF_8 && ch == '\0' {
                return Err(FileError::new(
                    FileErrorCode::Unknown,
                    Some(path.into()),
                    "file changed to binary during search",
                ));
            }
            if ch == '\n' {
                if pending_cr {
                    regex.push('\r');
                }
                process(&line, clipped, regex.finish());
                regex.reset();
                line.clear();
                units = 0;
                clipped = false;
                pending_cr = false;
                has_content = false;
                continue;
            }
            has_content = true;
            if pending_cr {
                regex.push('\r');
                if units < 500 && !clipped {
                    line.push('\r');
                    units += 1;
                } else {
                    clipped = true;
                }
            }
            pending_cr = ch == '\r';
            if !pending_cr {
                regex.push(ch);
                if !clipped && units + ch.len_utf16() <= 500 {
                    line.push(ch);
                    units += ch.len_utf16();
                } else {
                    clipped = true;
                }
            }
        }
        pending = &pending[consumed..];
        if matches!(res, encoding_rs::CoderResult::OutputFull) {
            continue;
        }
        if eof {
            break;
        }
        let count = reader
            .read(&mut buffer)
            .map_err(|e| FileError::from_io(path, &e))?;
        eof = count == 0;
        pending = &buffer[..count];
    }
    if has_content {
        if pending_cr {
            regex.push('\r');
            if units < 500 && !clipped {
                line.push('\r');
            } else {
                clipped = true;
            }
        }
        process(&line, clipped, regex.finish());
    }
    Ok(())
}
