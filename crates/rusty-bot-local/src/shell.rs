use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusty_bot_core::{
    CommandSpec, ExecOptions, ExecutionError, ExecutionErrorCode, OutputChunk, OutputMode,
    OutputStream, Shell, ShellExecResult, SpillPolicy,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;

static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Local subprocess execution. Trusted-host only: commands run with the
/// embedding process's privileges and see the same filesystem as `LocalFileSystem`.
#[derive(Debug)]
pub struct LocalShell {
    id: String,
    default_cwd: PathBuf,
    active: Arc<Mutex<HashSet<i32>>>,
}

impl LocalShell {
    pub fn new(id: impl Into<String>, default_cwd: PathBuf) -> Self {
        Self {
            id: id.into(),
            default_cwd,
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn default_cwd(&self) -> &Path {
        &self.default_cwd
    }

    fn track(&self, pgid: i32) {
        self.active
            .lock()
            .expect("shell tracking poisoned")
            .insert(pgid);
    }

    fn untrack(&self, pgid: i32) {
        self.active
            .lock()
            .expect("shell tracking poisoned")
            .remove(&pgid);
    }

    /// Kill every process group still tracked, without waiting. Used by
    /// `cleanup` and by provider shutdown.
    pub fn kill_all_blocking(&self) {
        let active = self.active.lock().expect("shell tracking poisoned");
        for &pgid in active.iter() {
            kill_process_group(pgid);
        }
    }
}

fn kill_process_group(pgid: i32) {
    unsafe {
        if pgid > 0 {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

// A shell may exit after launching a fully redirected background process.
// Pipe EOF and leader exit therefore do not establish group completion.
fn process_group_exists(pgid: i32) -> bool {
    if pgid <= 0 {
        return false;
    }
    let result = unsafe { libc::kill(-pgid, 0) };
    if result != 0 {
        return std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
    }
    // Linux container PID 1 may leave orphaned zombies unreaped indefinitely.
    // Zombies cannot perform effects and must not hold a command open.
    #[cfg(target_os = "linux")]
    {
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return true;
        };
        for entry in entries.flatten() {
            if !entry
                .file_name()
                .to_string_lossy()
                .bytes()
                .all(|b| b.is_ascii_digit())
            {
                continue;
            }
            let stat = match std::fs::read_to_string(entry.path().join("stat")) {
                Ok(stat) => stat,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return true,
            };
            let Some((_, tail)) = stat.rsplit_once(')') else {
                return true;
            };
            let mut fields = tail.split_whitespace();
            let (Some(state), Some(_parent), Some(group)) =
                (fields.next(), fields.next(), fields.next())
            else {
                return true;
            };
            if group.parse::<i32>().ok() == Some(pgid) && state != "Z" && state != "X" {
                return true;
            }
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        true
    }
}

fn spill_path(policy: &SpillPolicy) -> PathBuf {
    let counter = SPILL_COUNTER.fetch_add(1, Ordering::SeqCst);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    policy.dir.join(format!(
        "rusty-bot-shell-{}-{millis}-{counter}.log",
        std::process::id()
    ))
}

#[cfg(test)]
#[path = "shell_capture_tests.rs"]
mod tests;

struct StreamCapture {
    tail: VecDeque<u8>,
    full: Option<Vec<u8>>,
    total_bytes: usize,
    decoded_bytes: usize,
    current_line_bytes: usize,
    pending_utf8: Vec<u8>,
    newlines: usize,
    total_newlines: usize,
    last_byte: Option<u8>,
    truncated: bool,
    max_bytes: usize,
    max_lines: usize,
}

impl StreamCapture {
    fn new(max_bytes: usize, max_lines: usize) -> Self {
        Self {
            tail: VecDeque::new(),
            full: Some(Vec::new()),
            total_bytes: 0,
            decoded_bytes: 0,
            current_line_bytes: 0,
            pending_utf8: Vec::new(),
            newlines: 0,
            total_newlines: 0,
            last_byte: None,
            truncated: false,
            // One extra byte distinguishes a complete boundary-sized last line
            // followed by LF from a genuinely partial line.
            max_bytes: max_bytes.max(1),
            max_lines: max_lines.max(1),
        }
    }

    fn account_text(&mut self, text: &str) {
        self.decoded_bytes = self.decoded_bytes.saturating_add(text.len());
        if let Some((_, tail)) = text.rsplit_once('\n') {
            self.current_line_bytes = tail.len();
        } else {
            self.current_line_bytes = self.current_line_bytes.saturating_add(text.len());
        }
        if self.decoded_bytes > self.max_bytes {
            self.truncated = true;
        }
    }

    fn decode(&mut self, bytes: &[u8], finish: bool) {
        self.pending_utf8.extend_from_slice(bytes);
        let mut used = 0;
        while used < self.pending_utf8.len() {
            match std::str::from_utf8(&self.pending_utf8[used..]) {
                Ok(text) => {
                    let text = text.to_owned();
                    used = self.pending_utf8.len();
                    self.account_text(&text);
                }
                Err(e) => {
                    let end = used + e.valid_up_to();
                    let text = std::str::from_utf8(&self.pending_utf8[used..end])
                        .unwrap()
                        .to_owned();
                    self.account_text(&text);
                    used = end;
                    if let Some(n) = e.error_len() {
                        self.account_text("�");
                        used += n;
                    } else if finish {
                        self.account_text("�");
                        used = self.pending_utf8.len();
                    } else {
                        break;
                    }
                }
            }
        }
        self.pending_utf8.drain(..used);
    }

    fn push(&mut self, bytes: &[u8]) {
        self.decode(bytes, false);
        self.total_bytes = self.total_bytes.saturating_add(bytes.len());
        self.total_newlines = self
            .total_newlines
            .saturating_add(bytes.iter().filter(|&&b| b == b'\n').count());
        if let Some(&last) = bytes.last() {
            self.last_byte = Some(last);
        }
        if self.total_bytes > self.max_bytes
            || self.total_newlines + usize::from(self.last_byte.is_some_and(|b| b != b'\n'))
                > self.max_lines
        {
            self.truncated = true;
        }
        if let Some(full) = &mut self.full {
            if full.len().saturating_add(bytes.len()) <= 1024 * 1024 {
                full.extend_from_slice(bytes);
            } else {
                self.full = None;
            }
        }
        for &byte in bytes {
            self.tail.push_back(byte);
            self.newlines += usize::from(byte == b'\n');
            while self.tail.len() > self.max_bytes.saturating_add(1)
                || self.newlines + usize::from(self.tail.back() != Some(&b'\n')) > self.max_lines
            {
                if self.tail.pop_front() == Some(b'\n') {
                    self.newlines -= 1;
                }
                self.truncated = true;
            }
        }
    }

    fn pi_truncation(&self) -> Option<serde_json::Value> {
        if !self.truncated {
            return None;
        }
        let total_lines =
            self.total_newlines + usize::from(self.last_byte.is_some_and(|b| b != b'\n'));
        let bytes: Vec<_> = self.tail.iter().copied().collect();
        let mut start = 0;
        while start < bytes.len() && (bytes[start] & 0xc0) == 0x80 {
            start += 1;
        }
        let mut text = String::from_utf8_lossy(&bytes[start..]).into_owned();
        if text.ends_with('\n') {
            text.pop();
        }
        let tail_lines = if text.is_empty() {
            0
        } else {
            text.bytes().filter(|&b| b == b'\n').count() + 1
        };
        let partial = tail_lines == 1 && self.current_line_bytes > self.max_bytes;
        let by =
            if tail_lines >= self.max_lines || (total_lines == 1 && text.len() <= self.max_bytes) {
                "lines"
            } else {
                "bytes"
            };
        if partial {
            let mut start = text.len().saturating_sub(self.max_bytes);
            while !text.is_char_boundary(start) {
                start += 1;
            }
            text = text[start..].to_string();
        } else if by == "bytes" {
            if let Some(pos) = text.find('\n') {
                text = text[pos + 1..].to_string();
            } else {
                text.clear();
            }
        }
        let lines = if text.is_empty() {
            0
        } else {
            text.bytes().filter(|&b| b == b'\n').count() + 1
        };
        Some(
            serde_json::json!({"content":text,"truncated":true,"truncatedBy":by,"totalLines":total_lines,"totalBytes":self.decoded_bytes,"outputLines":lines,"outputBytes":text.len(),"lastLinePartial":partial,"firstLineExceedsLimit":false,"maxLines":self.max_lines,"maxBytes":self.max_bytes}),
        )
    }

    fn display(&self, marker: &str) -> String {
        let bytes: Vec<u8> = self.tail.iter().copied().collect();
        let mut start = 0;
        if self.truncated {
            while start < bytes.len() && (bytes[start] & 0xc0) == 0x80 {
                start += 1;
            }
        }
        let text = String::from_utf8_lossy(&bytes[start..]);
        if self.truncated {
            format!("{marker}{text}")
        } else {
            text.into_owned()
        }
    }
}

impl LocalShell {
    async fn spawn(
        &self,
        spec: CommandSpec,
        opts: &ExecOptions,
    ) -> Result<(tokio::process::Child, i32), ExecutionError> {
        let mut command = match spec {
            CommandSpec::Shell { command: script } => {
                let mut cmd = Command::new("/bin/sh");
                cmd.arg("-c").arg(script);
                cmd
            }
            CommandSpec::Argv { program, args } => {
                let mut command = Command::new(program);
                command.args(args);
                command
            }
        };

        let cwd = opts.cwd.clone().unwrap_or_else(|| self.default_cwd.clone());
        if !cwd.is_dir() {
            return Err(ExecutionError::new(
                ExecutionErrorCode::SpawnError,
                format!(
                    "cwd does not exist or is not a directory: {}",
                    cwd.display()
                ),
            ));
        }
        command.current_dir(cwd);
        if !opts.inherit_env {
            command.env_clear();
        }
        for (key, value) in &opts.env {
            command.env(key, value);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.process_group(0);

        let child = command.spawn().map_err(|error| {
            ExecutionError::new(
                ExecutionErrorCode::SpawnError,
                format!("failed to spawn command: {error}"),
            )
        })?;
        let pgid = child.id().unwrap_or(0) as i32;
        if pgid > 0 {
            self.track(pgid);
        }
        Ok((child, pgid))
    }
}

#[async_trait::async_trait]
impl Shell for LocalShell {
    fn id(&self) -> &str {
        &self.id
    }

    async fn exec(
        &self,
        spec: CommandSpec,
        opts: ExecOptions,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<ShellExecResult, ExecutionError> {
        if cancel.is_cancelled() {
            return Err(ExecutionError::new(
                ExecutionErrorCode::Aborted,
                "command aborted before spawn",
            ));
        }
        let started = Instant::now();
        let deadline = match opts.timeout {
            Some(timeout) => Some(started.checked_add(timeout).ok_or_else(|| {
                ExecutionError::new(ExecutionErrorCode::Unknown, "timeout is too large")
            })?),
            None => None,
        };
        let (mut child, pgid) = self.spawn(spec, &opts).await?;

        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExecutionError::new(ExecutionErrorCode::SpawnError, "missing stdout"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecutionError::new(ExecutionErrorCode::SpawnError, "missing stderr"))?;

        let (tx, mut rx) = mpsc::channel::<(OutputStream, Vec<u8>)>(64);
        let stdout_tx = tx.clone();
        let stdout_task = tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            loop {
                match stdout.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => {
                        if stdout_tx
                            .send((OutputStream::Stdout, buffer[..read].to_vec()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let stderr_task = tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            loop {
                match stderr.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => {
                        if tx
                            .send((OutputStream::Stderr, buffer[..read].to_vec()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let mut capture = StreamCapture::new(opts.spill.max_bytes, opts.spill.max_lines);
        let mut spill_file: Option<(PathBuf, tokio::fs::File)> = None;
        let mut callback_error: Option<ExecutionError> = None;
        let mut interrupted: Option<ExecutionErrorCode> = None;
        let mut status = None;
        let mut pipes_open = true;
        let mut group_check = tokio::time::interval(Duration::from_millis(25));

        let timeout_future = async move {
            match deadline {
                Some(deadline) => {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_future);

        loop {
            if status.is_some() && !pipes_open && !process_group_exists(pgid) {
                break;
            }
            tokio::select! {
                // There is no portable async notification for the final member
                // of a process group; poll only after the leader has exited.
                _ = group_check.tick(), if status.is_some() && !pipes_open => {}
                result = child.wait(), if status.is_none() => {
                    match result {
                        Ok(exit) => status = Some(exit),
                        Err(error) => {
                            callback_error = Some(ExecutionError::new(ExecutionErrorCode::Unknown, format!("failed to wait for child: {error}")));
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    interrupted = Some(ExecutionErrorCode::Aborted);
                    break;
                }
                _ = &mut timeout_future => {
                    interrupted = Some(ExecutionErrorCode::Timeout);
                    break;
                }
                chunk = rx.recv(), if pipes_open => {
                    let Some((stream, bytes)) = chunk else { pipes_open = false; continue };
                    if let Err(error) = capture_chunk(&mut capture, &mut spill_file, &opts, stream, &bytes).await {
                        callback_error = Some(error); break;
                    }
                }
            }
        }

        if callback_error.is_some() || interrupted.is_some() {
            kill_process_group(pgid);
            // Allow an awaiting shell a brief chance to reap its killed children
            // before killing the leader too. This also bounds reaping when it
            // does not exit (or has closed its pipes).
            if status.is_none() {
                if let Ok(Ok(exit)) =
                    tokio::time::timeout(Duration::from_millis(75), child.wait()).await
                {
                    status = Some(exit);
                } else {
                    let _ = child.start_kill();
                }
            }
        }
        let wait_result = match status {
            Some(status) => Ok(status),
            None => child.wait().await.map_err(|error| {
                ExecutionError::new(
                    ExecutionErrorCode::Unknown,
                    format!("failed to wait for child: {error}"),
                )
            }),
        };
        if interrupted.is_some() && callback_error.is_none() {
            let drain = async {
                while let Some((stream, bytes)) = rx.recv().await {
                    capture_chunk(&mut capture, &mut spill_file, &opts, stream, &bytes).await?;
                }
                Ok::<(), ExecutionError>(())
            };
            if let Ok(Err(error)) = tokio::time::timeout(Duration::from_millis(200), drain).await {
                callback_error = Some(error);
            }
        }
        self.untrack(pgid);
        stdout_task.abort();
        stderr_task.abort();
        let status = wait_result?;

        let spill_path = if let Some((path, mut file)) = spill_file {
            file.flush().await.map_err(spill_error)?;
            Some(path)
        } else {
            None
        };

        capture.decode(&[], true);
        let duration_ms = started.elapsed().as_millis() as u64;

        if let Some(error) = callback_error {
            return Err(ExecutionError {
                spill_path,
                ..error
            });
        }
        if let Some(code) = interrupted {
            let mut error = ExecutionError::new(
                code,
                match code {
                    ExecutionErrorCode::Timeout => "command timed out".to_string(),
                    _ => "command aborted".to_string(),
                },
            );
            let marker = spill_path
                .as_ref()
                .map(|path| format!("[truncated: full output at {}]\n", path.display()))
                .unwrap_or_else(|| "[truncated]\n".into());
            let partial = capture.display(&marker);
            if !partial.is_empty() {
                error.message = format!("{partial}\n{}", error.message);
            }
            error.partial_output = Some(partial);
            error.truncated = capture.truncated;
            error.truncation = capture.pi_truncation();
            error.last_line_bytes = capture.current_line_bytes;
            error.spill_path = spill_path;
            return Err(error);
        }

        let marker = match &spill_path {
            Some(path) => format!("\n... [truncated: full output at {}] ...\n", path.display()),
            None => "\n... [truncated] ...\n".to_string(),
        };
        let stdout_text = capture.display(&marker);
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        Ok(ShellExecResult {
            exit_code: status.code(),
            signal,
            total_bytes: capture.total_bytes,
            full_output: capture.full.as_ref().and_then(|full| {
                let decoded = String::from_utf8_lossy(full);
                (decoded.len() <= 1024 * 1024).then(|| decoded.into_owned())
            }),
            stdout: stdout_text,
            stderr: String::new(),
            truncated: capture.truncated,
            spill_path,
            duration_ms,
            truncation: capture.pi_truncation(),
            last_line_bytes: capture.current_line_bytes,
        })
    }

    async fn cleanup(&self) -> Result<(), ExecutionError> {
        self.kill_all_blocking();
        Ok(())
    }
}

async fn capture_chunk(
    capture: &mut StreamCapture,
    spill: &mut Option<(PathBuf, tokio::fs::File)>,
    opts: &ExecOptions,
    stream: OutputStream,
    bytes: &[u8],
) -> Result<(), ExecutionError> {
    if spill.is_none() {
        let last = bytes.last().or_else(|| capture.tail.back());
        let lines = capture
            .newlines
            .saturating_add(bytes.iter().filter(|&&b| b == b'\n').count())
            .saturating_add(usize::from(last.is_some_and(|&b| b != b'\n')));
        let exceeds = capture.tail.len().saturating_add(bytes.len()) > capture.max_bytes
            || lines > capture.max_lines;
        if exceeds {
            let (path, mut file) = create_spill(&opts.spill)
                .await?
                .expect("spill creation returns a file");
            let (first, second) = capture.tail.as_slices();
            file.write_all(first).await.map_err(spill_error)?;
            file.write_all(second).await.map_err(spill_error)?;
            *spill = Some((path, file));
        }
    }
    if let Some((_, file)) = spill.as_mut() {
        file.write_all(bytes).await.map_err(spill_error)?;
    }
    capture.push(bytes);
    if capture.truncated && spill.is_none() {
        let (path, mut file) = create_spill(&opts.spill)
            .await?
            .expect("spill creation returns a file");
        let full = capture.full.as_ref().ok_or_else(|| {
            ExecutionError::new(
                ExecutionErrorCode::Unknown,
                "cannot reconstruct spill output",
            )
        })?;
        file.write_all(full).await.map_err(spill_error)?;
        *spill = Some((path, file));
    }
    if let OutputMode::Stream(callback) = &opts.output {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            callback(OutputChunk {
                stream,
                bytes: bytes.to_vec(),
            })
        }))
        .map_err(|_| {
            ExecutionError::new(
                ExecutionErrorCode::CallbackError,
                "output callback panicked",
            )
        })?;
    }
    Ok(())
}

async fn create_spill(
    policy: &SpillPolicy,
) -> Result<Option<(PathBuf, tokio::fs::File)>, ExecutionError> {
    tokio::fs::create_dir_all(&policy.dir)
        .await
        .map_err(spill_error)?;
    for _ in 0..16 {
        let path = spill_path(policy);
        let mut options = tokio::fs::OpenOptions::new();
        options.create_new(true).write(true);
        options.mode(0o600);
        match options.open(&path).await {
            Ok(file) => return Ok(Some((path, file))),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(spill_error(error)),
        }
    }
    Err(ExecutionError::new(
        ExecutionErrorCode::Unknown,
        "failed to create spill file: too many name collisions",
    ))
}

fn spill_error(error: std::io::Error) -> ExecutionError {
    ExecutionError::new(
        ExecutionErrorCode::Unknown,
        format!("failed to write spill file: {error}"),
    )
}

/// Helper used by callers that need a spill directory under a workspace.
pub fn default_spill_policy(dir: impl Into<PathBuf>) -> SpillPolicy {
    SpillPolicy {
        max_bytes: rusty_bot_core::DEFAULT_MAX_BYTES,
        max_lines: rusty_bot_core::DEFAULT_MAX_LINES,
        dir: dir.into(),
    }
}
