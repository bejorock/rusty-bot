//! Local Unix grep adapter for pinned coding-agent grep.ts.
use crate::find_fd::{nonblocking, Process};
use base64::Engine;
use rusty_bot_core::{FileError, FileErrorCode, GrepQuery, SearchPage};
use serde_json::Value;
use std::{
    io::{self, Read},
    os::unix::{io::AsRawFd, process::CommandExt},
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const RECORD_CAP: usize = 64 * 1024;
const TEXT_PREFIX: usize = 8192;
fn error(root: &Path, code: FileErrorCode, message: impl Into<String>) -> FileError {
    FileError::new(code, Some(root.to_path_buf()), message)
}

// rg emits newline-delimited JSON. Retain a bounded JSON string prefix for the
// line and discard submatches (which grep.ts never uses), including arbitrarily
// many of them. This retains more than 500 UTF-16 units even for JSON escapes
// and base64; truncation is recorded separately. Never retain an 8 MiB line.
struct Record {
    bytes: Vec<u8>,
    in_line: bool,
    escape: bool,
    clipped: bool,
    discard: bool,
    prefix_start: usize,
}
impl Record {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            in_line: false,
            escape: false,
            clipped: false,
            discard: false,
            prefix_start: 0,
        }
    }
    fn byte(&mut self, b: u8) -> Result<(), &'static str> {
        if self.discard {
            return Ok(());
        }
        if self.in_line {
            if !self.escape && b == b'"' {
                if self.clipped {
                    // End at a valid JSON/UTF-8 boundary (also handles a split escape).
                    while serde_json::from_slice::<String>(
                        &[b"\"".as_slice(), self.line_prefix(), b"\""].concat(),
                    )
                    .is_err()
                    {
                        self.bytes.pop();
                    }
                }
                self.bytes.push(b);
                self.in_line = false;
                return Ok(());
            }
            if self.line_prefix().len() < TEXT_PREFIX {
                self.bytes.push(b);
            } else {
                self.clipped = true;
            }
            if self.escape {
                self.escape = false;
            } else if b == b'\\' {
                self.escape = true;
            }
            return Ok(());
        }
        self.bytes.push(b);
        if self.bytes.ends_with(b"\"lines\":{\"text\":\"")
            || self.bytes.ends_with(b"\"lines\":{\"bytes\":\"")
        {
            self.in_line = true;
            self.prefix_start = self.bytes.len();
        }
        if self.bytes.ends_with(b",\"submatches\":") {
            self.bytes
                .truncate(self.bytes.len() - b",\"submatches\":".len());
            self.bytes.extend_from_slice(b"}}");
            self.discard = true;
        }
        if self.bytes.len() > RECORD_CAP {
            return Err("ripgrep JSON record exceeds safety maximum");
        }
        Ok(())
    }
    fn line_prefix(&self) -> &[u8] {
        &self.bytes[self.prefix_start..]
    }
}
fn decode(v: &Value) -> String {
    if let Some(s) = v.get("text").and_then(Value::as_str) {
        return s.into();
    }
    if let Some(s) = v.get("bytes").and_then(Value::as_str) {
        let s = &s[..s.len() / 4 * 4];
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
            return String::from_utf8_lossy(&bytes).into_owned();
        }
    }
    String::new()
}
struct Results {
    dir: bool,
    pager: crate::search_cursor::Pager,
}
impl Results {
    fn event(&mut self, record: &Record) -> Result<bool, FileError> {
        let Ok(v) = serde_json::from_slice::<Value>(&record.bytes) else {
            return Ok(false);
        };
        let ty = v["type"].as_str().unwrap_or("");
        if ty != "match" && ty != "context" {
            return Ok(false);
        }
        let data = &v["data"];
        let path = decode(&data["path"]);
        let Some(n) = data["line_number"].as_u64() else {
            return Ok(false);
        };
        if path.is_empty() {
            return Ok(false);
        }
        let mut text = decode(&data["lines"]);
        if text.ends_with('\n') {
            text.pop();
            if text.ends_with('\r') {
                text.pop();
            }
        }
        let mut units = 0;
        let mut end = text.len();
        for (i, c) in text.char_indices() {
            if units + c.len_utf16() > 500 {
                end = i;
                break;
            }
            units += c.len_utf16();
        }
        let clipped = record.clipped || end < text.len();
        text.truncate(end);
        if clipped {
            text.push_str("... [truncated]");
        }
        let display = if self.dir {
            path.strip_prefix("./").unwrap_or(&path).to_owned()
        } else {
            Path::new(&path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        };
        let matched = ty == "match";
        let line = if matched {
            format!("{display}:{n}: {text}")
        } else {
            format!("{display}-{n}- {text}")
        };
        self.pager.push(line, matched, clipped)
    }
}

pub fn grep(
    root: &Path,
    dir: bool,
    q: GrepQuery,
    cancel: &CancellationToken,
) -> Result<SearchPage, FileError> {
    let binding = crate::search_cursor::binding(&format!(
        "grep:{root:?}:{:?}:{:?}:{}:{}:{}",
        q.pattern, q.glob, q.literal, q.ignore_case, q.context
    ));
    let pager =
        crate::search_cursor::Pager::new(binding, q.cursor.as_deref(), q.limit, q.byte_cap)?;
    if cancel.is_cancelled() {
        return Err(error(root, FileErrorCode::Aborted, "search aborted"));
    }
    let mut cmd = Command::new("rg");
    cmd.args([
        "--no-config",
        "--json",
        "--line-number",
        "--color=never",
        "--hidden",
        "--sort",
        "path",
    ]);
    if q.ignore_case {
        cmd.arg("--ignore-case");
    }
    if q.literal {
        cmd.arg("--fixed-strings");
    }
    if let Some(glob) = &q.glob {
        let (neg, mut g) = glob
            .strip_prefix('!')
            .map_or(("", glob.as_str()), |g| ("!", g));
        while let Some(rest) = g.strip_prefix("./") {
            g = rest;
        }
        cmd.args(["--glob", &format!("{neg}{g}")]);
    }
    if q.context > 0 {
        cmd.args([
            "--context",
            &q.context.min(9_007_199_254_740_991).to_string(),
        ]);
    }
    cmd.arg("--")
        .arg(&q.pattern)
        .arg(if dir { Path::new(".") } else { root });
    if dir {
        cmd.current_dir(root);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = cmd.spawn().map_err(|e| {
        error(
            root,
            if e.kind() == io::ErrorKind::NotFound {
                FileErrorCode::NotSupported
            } else {
                FileErrorCode::Unknown
            },
            format!("Failed to run ripgrep: {e}"),
        )
    })?;
    let mut child = Process {
        child,
        reaped: false,
    };
    let mut stdout = child.child.stdout.take().unwrap();
    let mut stderr = child.child.stderr.take().unwrap();
    nonblocking(stdout.as_raw_fd())
        .and_then(|_| nonblocking(stderr.as_raw_fd()))
        .map_err(|e| error(root, FileErrorCode::Unknown, e.to_string()))?;
    let mut out = Results { dir, pager };
    let mut record = Record::new();
    let mut err = Vec::new();
    let mut buf = [0; 8192];
    let (mut out_open, mut err_open, mut stopped) = (true, true, false);
    while out_open || err_open {
        if cancel.is_cancelled() {
            return Err(error(root, FileErrorCode::Aborted, "search aborted"));
        }
        let mut fds = [
            libc::pollfd {
                fd: if out_open { stdout.as_raw_fd() } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if err_open { stderr.as_raw_fd() } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), 2, 30) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error(root, FileErrorCode::Unknown, e.to_string()));
        }
        for (i, fd) in fds.iter().enumerate() {
            if fd.revents == 0 {
                continue;
            }
            let reader: &mut dyn Read = if i == 0 { &mut stdout } else { &mut stderr };
            match reader.read(&mut buf) {
                Ok(0) => {
                    if i == 0 {
                        out_open = false;
                    } else {
                        err_open = false;
                    }
                }
                Ok(n) if i == 1 => {
                    let count = n.min(16384usize.saturating_sub(err.len()));
                    err.extend_from_slice(&buf[..count]);
                }
                Ok(n) => {
                    for &b in &buf[..n] {
                        if b == b'\n' {
                            if !stopped && out.event(&record)? {
                                stopped = true;
                                unsafe {
                                    libc::kill(-(child.child.id() as i32), libc::SIGKILL);
                                }
                            }
                            record = Record::new();
                        } else if !stopped {
                            record
                                .byte(b)
                                .map_err(|e| error(root, FileErrorCode::Invalid, e))?;
                        }
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(error(root, FileErrorCode::Unknown, e.to_string())),
            }
        }
    }
    loop {
        if cancel.is_cancelled() {
            return Err(error(root, FileErrorCode::Aborted, "search aborted"));
        }
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                child.child.id() as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error(root, FileErrorCode::Unknown, e.to_string()));
        }
        if unsafe { info.si_pid() } != 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    unsafe {
        libc::kill(-(child.child.id() as i32), libc::SIGKILL);
    }
    let status = child
        .child
        .wait()
        .map_err(|e| error(root, FileErrorCode::Unknown, e.to_string()))?;
    child.reaped = true;
    if !stopped && !matches!(status.code(), Some(0 | 1)) {
        let message = String::from_utf8_lossy(&err).trim().to_owned();
        return Err(error(
            root,
            FileErrorCode::Unknown,
            if message.is_empty() {
                format!(
                    "ripgrep exited with code {}",
                    status.code().map_or("null".into(), |c| c.to_string())
                )
            } else {
                message
            },
        ));
    }
    Ok(out.pager.page)
}
