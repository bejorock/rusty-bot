//! Unix fd adapter for coding-agent find.ts (4afe78de1e51162c01c946dcf080e6a266685a01).
use rusty_bot_core::{FileError, FileErrorCode, FindQuery, SearchPage};
use std::{
    io::{self, Read},
    os::unix::{io::AsRawFd, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

// A hostile or broken executable must not make a single unterminated line unbounded.
const MAX_FD_LINE: usize = 1024 * 1024;
const MAX_STDERR: usize = 16 * 1024;

fn error(code: FileErrorCode, root: &Path, message: impl Into<String>) -> FileError {
    FileError::new(code, Some(root.to_path_buf()), message)
}
pub(crate) struct Process {
    pub(crate) child: Child,
    pub(crate) reaped: bool,
}
impl Drop for Process {
    fn drop(&mut self) {
        if !self.reaped {
            // The unreaped leader reserves the process-group ID until this wait.
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
    }
}
pub(crate) fn nonblocking(fd: i32) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

struct Results<'a> {
    root: &'a Path,
    page: crate::search_key_page::KeyPage,
}
impl Results<'_> {
    fn line(&mut self, raw: &[u8]) {
        let decoded = String::from_utf8_lossy(raw);
        let line = decoded.trim();
        if line.is_empty() {
            return;
        }
        let path = Path::new(line);
        let relative = if path.is_absolute() {
            path.strip_prefix(self.root)
                .unwrap_or(path)
                .to_string_lossy()
                .into_owned()
        } else {
            line.to_owned()
        };
        let relative = if line.ends_with('/') && !relative.ends_with('/') {
            format!("{relative}/")
        } else {
            relative
        };
        self.page.push(relative);
    }
}

pub fn find(
    root: &Path,
    q: FindQuery,
    cancel: &CancellationToken,
) -> Result<SearchPage, FileError> {
    let binding = crate::search_cursor::binding(&format!("find:{root:?}:{:?}", q.pattern));
    let page =
        crate::search_key_page::KeyPage::new(binding, q.cursor.as_deref(), q.limit, q.byte_cap)?;
    if cancel.is_cancelled() {
        return Err(error(FileErrorCode::Aborted, root, "search aborted"));
    }
    let mut pattern = q.pattern.as_str();
    while let Some(rest) = pattern.strip_prefix("./") {
        pattern = rest;
    }
    let path_pattern = pattern.contains('/');
    let relative_pattern = path_pattern && !Path::new(pattern).is_absolute();
    let mut effective = pattern.to_owned();
    let mut command = Command::new("fd");
    command.args(["--color=never", "--hidden"]);
    if !root.ancestors().any(|dir| dir.join(".git").exists()) {
        command.arg("--no-require-git");
    }
    command.args(["--glob", "--threads", "1"]);
    if path_pattern {
        command.arg("--full-path");
    }
    if relative_pattern {
        let root_str = root
            .to_str()
            .ok_or_else(|| error(FileErrorCode::Invalid, root, "non-UTF-8 search root"))?;
        let mut escaped = String::new();
        for c in root_str.trim_end_matches('/').chars() {
            if matches!(c, '\\' | '*' | '?' | '[' | ']' | '{' | '}') {
                escaped.push('\\');
            }
            escaped.push(c);
        }
        effective = format!("{escaped}/{pattern}");
        if pattern.to_lowercase() == pattern {
            command.arg("--ignore-case");
        }
    }
    if cancel.is_cancelled() {
        return Err(error(FileErrorCode::Aborted, root, "search aborted"));
    }
    command
        .arg("--")
        .arg(effective)
        .arg(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = command.spawn().map_err(|e| {
        let code = if e.kind() == io::ErrorKind::NotFound {
            FileErrorCode::NotSupported
        } else {
            FileErrorCode::Unknown
        };
        error(code, root, format!("Failed to run fd: {e}"))
    })?;
    let mut child = Process {
        child,
        reaped: false,
    };
    let mut stdout = child.child.stdout.take().expect("piped stdout");
    let mut stderr = child.child.stderr.take().expect("piped stderr");
    nonblocking(stdout.as_raw_fd())
        .and_then(|_| nonblocking(stderr.as_raw_fd()))
        .map_err(|e| error(FileErrorCode::Unknown, root, e.to_string()))?;
    let mut results = Results { root, page };
    let mut line = Vec::new();
    let mut err = Vec::new();
    let (mut out_open, mut err_open) = (true, true);
    let mut buf = [0; 8192];
    while out_open || err_open {
        if cancel.is_cancelled() {
            return Err(error(FileErrorCode::Aborted, root, "search aborted"));
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
        let polled = unsafe {
            libc::poll(
                fds.as_mut_ptr(),
                fds.len() as _,
                Duration::from_millis(30).as_millis() as i32,
            )
        };
        if polled < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error(FileErrorCode::Unknown, root, e.to_string()));
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
                        if !line.is_empty() {
                            results.line(&line);
                            line.clear();
                        }
                    } else {
                        err_open = false;
                    }
                }
                Ok(n) if i == 1 => {
                    let retain = n.min(MAX_STDERR.saturating_sub(err.len()));
                    err.extend_from_slice(&buf[..retain]);
                }
                Ok(n) => {
                    for &b in &buf[..n] {
                        if b == b'\n' {
                            results.line(&line);
                            line.clear();
                        } else {
                            if line.len() == MAX_FD_LINE {
                                return Err(error(
                                    FileErrorCode::Invalid,
                                    root,
                                    "fd output line exceeds safety maximum",
                                ));
                            }
                            line.push(b);
                        }
                    }
                }
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(error(FileErrorCode::Unknown, root, e.to_string())),
            }
        }
    }
    // Observe exit without reaping: the leader reserves its process-group ID
    // until any redirected helpers have been killed. Pipes alone cannot prove
    // the group has no remaining members.
    loop {
        if cancel.is_cancelled() {
            return Err(error(FileErrorCode::Aborted, root, "search aborted"));
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
            return Err(error(FileErrorCode::Unknown, root, e.to_string()));
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
        .map_err(|e| error(FileErrorCode::Unknown, root, e.to_string()))?;
    child.reaped = true;
    if !status.success() {
        let msg = String::from_utf8_lossy(&err).trim().to_owned();
        return Err(error(
            FileErrorCode::Unknown,
            root,
            if msg.is_empty() {
                format!(
                    "fd exited with code {}",
                    status
                        .code()
                        .map_or_else(|| "null".into(), |c| c.to_string())
                )
            } else {
                msg
            },
        ));
    }
    results.page.finish()
}
