//! Built-in workspace tools.
//!
//! File tools access the workspace `FileSystem`; `bash` additionally requires a
//! selected `Shell`. Local execution is trusted-host only, not a sandbox.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rusty_bot_core::{
    truncate_text, CommandSpec, ContentPart, ExecOptions, ExecutionErrorCode, FileErrorCode,
    LiveFrame, LiveKind, OutputMode, ReplayPolicy, ShellExecResult, SpillPolicy, Tool, ToolContext,
    ToolError, ToolOutput, ToolSpec, DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value};

mod image_read;
mod phase1_edit;
mod phase1_read;
mod search;
pub use phase1_edit::EditTool;
pub use phase1_read::ReadTool;
pub use search::{FindTool, GrepTool, LsTool};

static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn spec(name: &str, description: &str, parameters: Value) -> ToolSpec {
    ToolSpec {
        name: name.to_string(),
        description: description.to_string(),
        parameters,
    }
}

fn invalid(message: impl Into<String>) -> ToolOutput {
    ToolOutput::error(message, "invalid_arguments")
}

fn file_error(code: FileErrorCode, message: impl std::fmt::Display) -> ToolOutput {
    let code = match code {
        FileErrorCode::Aborted => "aborted",
        FileErrorCode::NotFound => "not_found",
        FileErrorCode::PermissionDenied => "permission_denied",
        FileErrorCode::NotDirectory => "not_directory",
        FileErrorCode::IsDirectory => "is_directory",
        FileErrorCode::Invalid => "invalid",
        FileErrorCode::NotSupported => "not_supported",
        FileErrorCode::Unknown => "unknown",
    };
    ToolOutput::error(message.to_string(), code)
}

fn spill_path(ctx: &ToolContext, label: &str) -> PathBuf {
    let unique = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rusty-bot-{label}-{}-{}-{unique}.txt",
        ctx.session_id.0, ctx.task_id.0
    ))
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ReadArgs {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

pub struct LegacyReadTool;

#[async_trait]
impl Tool for LegacyReadTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "read",
            "Read a UTF-8 text file with one-based line numbers.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "integer", "minimum": 1 },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"]
            }),
        )
    }

    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: ReadArgs = match serde_json::from_value(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(invalid(format!("invalid read arguments: {error}"))),
        };
        let content = match ctx.files.read_text_file(Path::new(&args.path)).await {
            Ok(content) => content,
            Err(error) => return Ok(file_error(error.code, error)),
        };
        if args.offset == Some(0) || args.limit == Some(0) {
            return Ok(invalid("offset and limit must be at least 1"));
        }
        let offset = args.offset.unwrap_or(1);
        let lines: Vec<&str> = content.lines().collect();
        let selected = &lines[offset.saturating_sub(1).min(lines.len())..];
        let selected = match args.limit {
            Some(limit) => &selected[..limit.min(selected.len())],
            None => selected,
        };
        let numbered = selected
            .iter()
            .enumerate()
            .map(|(index, line)| format!("{:>6}\t{}", offset + index, line))
            .collect::<Vec<_>>()
            .join("\n");
        let truncated = truncate_text(&numbered, DEFAULT_MAX_LINES, DEFAULT_MAX_BYTES);
        if !truncated.truncated {
            return Ok(ToolOutput {
                content: crate::prompt::text_content(&numbered),
                details: Some(json!({ "lines": selected.len(), "truncated": false })),
                is_error: false,
            });
        }
        let path = spill_path(ctx, "read");
        ctx.files
            .write_file(&path, numbered.as_bytes())
            .await
            .map_err(|error| ToolError::new("spill_failed", error.to_string()))?;
        let content = format!("{}\n[spill: {}]", truncated.text, path.display());
        Ok(ToolOutput {
            content: crate::prompt::text_content(&content),
            details: Some(json!({
                "truncated": true,
                "spill_path": path,
                "total_lines": truncated.total_lines,
                "total_bytes": truncated.total_bytes,
            })),
            is_error: false,
        })
    }
}

pub struct WriteTool;

#[async_trait]
impl Tool for WriteTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "write",
            "Create or overwrite a text file, creating parent directories.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
        )
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let Some(path_text) = arguments.get("path").and_then(Value::as_str) else {
            return Ok(invalid("invalid write arguments: missing or invalid path"));
        };
        let path = Path::new(path_text);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(error) = ctx.files.create_dir(parent, true).await {
                    return Ok(file_error(error.code, error));
                }
            }
        }
        let content = match arguments.get("content") {
            Some(Value::String(content)) => content,
            value => {
                let received = match value {
                    None => "undefined".to_owned(),
                    Some(Value::Null) => "null".to_owned(),
                    Some(Value::Bool(v)) => format!("type boolean ({v})"),
                    Some(Value::Number(v)) => format!("type number ({v})"),
                    Some(Value::Object(_)) => "an instance of Object".to_owned(),
                    _ => return Ok(invalid("invalid write arguments: content must be a string")),
                };
                return Ok(ToolOutput { content: crate::prompt::text_content(&format!("The \"data\" argument must be of type string or an instance of Buffer, TypedArray, or DataView. Received {received}")), details: None, is_error: true });
            }
        };
        match ctx.files.write_file(path, content.as_bytes()).await {
            Ok(()) => Ok(ToolOutput::success(format!(
                "Successfully wrote to {}",
                path_text
            ))),
            Err(error) => Ok(file_error(error.code, error)),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EditArgs {
    path: String,
    old_text: String,
    new_text: String,
}

pub struct LegacyEditTool;

#[async_trait]
impl Tool for LegacyEditTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "edit",
            "Replace exactly one occurrence of old_text in a file.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_text": { "type": "string" },
                    "new_text": { "type": "string" }
                },
                "required": ["path", "old_text", "new_text"]
            }),
        )
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: EditArgs = match serde_json::from_value(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(invalid(format!("invalid edit arguments: {error}"))),
        };
        if args.old_text.is_empty() {
            return Ok(invalid("old_text must not be empty"));
        }
        let content = match ctx.files.read_text_file(Path::new(&args.path)).await {
            Ok(content) => content,
            Err(error) => return Ok(file_error(error.code, error)),
        };
        let matches = content.match_indices(&args.old_text).count();
        match matches {
            1 => {}
            0 => {
                return Ok(ToolOutput::error(
                    format!("old_text was not found in {}", args.path),
                    "not_found",
                ))
            }
            count => {
                return Ok(ToolOutput::error(
                    format!(
                        "old_text matched {count} times in {}; refusing to edit",
                        args.path
                    ),
                    "ambiguous_match",
                ))
            }
        }
        let updated = content.replacen(&args.old_text, &args.new_text, 1);
        match ctx
            .files
            .write_file(Path::new(&args.path), updated.as_bytes())
            .await
        {
            Ok(()) => Ok(ToolOutput {
                content: crate::prompt::text_content(&format!("edited {}", args.path)),
                details: Some(json!({ "bytes": updated.len() })),
                is_error: false,
            }),
            Err(error) => Ok(file_error(error.code, error)),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LsArgs {
    path: Option<String>,
    all: Option<bool>,
}

pub struct LegacyLsTool;

#[async_trait]
impl Tool for LegacyLsTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "ls",
            "List directory entries; directories are suffixed with '/'.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "all": { "type": "boolean" }
                }
            }),
        )
    }

    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: LsArgs = match serde_json::from_value(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(invalid(format!("invalid ls arguments: {error}"))),
        };
        let path = args.path.unwrap_or_else(|| ".".to_string());
        let mut entries = match ctx.files.list_dir(Path::new(&path)).await {
            Ok(entries) => entries,
            Err(error) => return Ok(file_error(error.code, error)),
        };
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let show_all = args.all.unwrap_or(false);
        let lines = entries
            .iter()
            .filter(|entry| show_all || !entry.name.starts_with('.'))
            .map(|entry| {
                if matches!(entry.kind, rusty_bot_core::FileKind::Directory) {
                    format!("{}/", entry.name)
                } else {
                    entry.name.clone()
                }
            })
            .collect::<Vec<_>>();
        if lines.is_empty() {
            return Ok(ToolOutput::success("(empty)"));
        }
        Ok(ToolOutput {
            content: crate::prompt::text_content(&lines.join("\n")),
            details: Some(json!({ "count": lines.len() })),
            is_error: false,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GrepArgs {
    pattern: String,
    path: Option<String>,
    glob: Option<String>,
    ignore_case: Option<bool>,
    max_results: Option<usize>,
}

pub struct LegacyGrepTool;

fn glob_matches(pattern: &str, name: &str) -> bool {
    // Minimal glob support: `*` and `?`. Anything else is literal.
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    let mut p = 0;
    let mut n = 0;
    let mut star: Option<usize> = None;
    let mut star_name = 0;
    while n < name.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == name[n]) {
            p += 1;
            n += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some(p);
            star_name = n;
            p += 1;
        } else if let Some(star_p) = star {
            p = star_p + 1;
            star_name += 1;
            n = star_name;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(8 * 1024).any(|byte| *byte == 0)
}

#[async_trait]
impl Tool for LegacyGrepTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "grep",
            "Recursively search text files with a regular expression.",
            json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "glob": { "type": "string" },
                    "ignore_case": { "type": "boolean" },
                    "max_results": { "type": "integer", "minimum": 1 }
                },
                "required": ["pattern"]
            }),
        )
    }

    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: GrepArgs = match serde_json::from_value(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(invalid(format!("invalid grep arguments: {error}"))),
        };
        let regex = match regex::RegexBuilder::new(&args.pattern)
            .case_insensitive(args.ignore_case.unwrap_or(false))
            .build()
        {
            Ok(regex) => regex,
            Err(error) => return Ok(invalid(format!("invalid grep pattern: {error}"))),
        };
        let root = PathBuf::from(args.path.clone().unwrap_or_else(|| ".".to_string()));
        let glob = args.glob.clone().unwrap_or_else(|| "*".to_string());
        let requested = args.max_results.unwrap_or(100);
        if requested == 0 {
            return Ok(invalid("max_results must be at least 1"));
        }
        let max_results = requested.min(1000);
        let cwd = ctx.files.cwd().to_path_buf();
        let mut stack = vec![root];
        let mut results = Vec::new();
        while let Some(directory) = stack.pop() {
            let entries = match ctx.files.list_dir(&directory).await {
                Ok(entries) => entries,
                Err(error) => return Ok(file_error(error.code, error)),
            };
            for entry in entries {
                if results.len() >= max_results {
                    break;
                }
                if entry.name == ".git" {
                    continue;
                }
                match entry.kind {
                    rusty_bot_core::FileKind::Directory => stack.push(entry.path),
                    rusty_bot_core::FileKind::File => {
                        if !glob_matches(&glob, &entry.name) {
                            continue;
                        }
                        let bytes = match ctx.files.read_bytes(&entry.path).await {
                            Ok(bytes) => bytes,
                            Err(_) => continue,
                        };
                        if is_binary(&bytes) {
                            continue;
                        }
                        let text = match String::from_utf8(bytes) {
                            Ok(text) => text,
                            Err(_) => continue,
                        };
                        let display = entry
                            .path
                            .strip_prefix(&cwd)
                            .unwrap_or(&entry.path)
                            .display()
                            .to_string();
                        for (index, line) in text.lines().enumerate() {
                            if regex.is_match(line) {
                                results.push(format!("{display}:{}: {line}", index + 1));
                                if results.len() >= max_results {
                                    break;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if results.len() >= max_results {
                break;
            }
        }
        if results.is_empty() {
            return Ok(ToolOutput {
                content: crate::prompt::text_content(""),
                details: Some(json!({ "matches": 0 })),
                is_error: false,
            });
        }
        Ok(ToolOutput {
            content: crate::prompt::text_content(&results.join("\n")),
            details: Some(json!({ "matches": results.len(), "max_results": max_results })),
            is_error: false,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BashArgs {
    command: String,
    timeout_ms: Option<u64>,
    timeout: Option<f64>,
}

pub struct BashTool;
#[async_trait]
impl Tool for BashTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "bash",
            "Run a shell command in the workspace. Requires a selected execution environment.",
            json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout_ms": { "type": "integer", "minimum": 1 },
                    "timeout": { "type": "number", "exclusiveMinimum": 0 }
                },
                "not": { "required": ["timeout", "timeout_ms"] },
                "required": ["command"]
            }),
        )
    }

    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        execute_shell(arguments, ctx).await
    }
}

async fn execute_shell(arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
    let args: BashArgs = match serde_json::from_value(arguments) {
        Ok(args) => args,
        Err(error) => return Ok(invalid(format!("invalid bash arguments: {error}"))),
    };
    let Some(shell) = &ctx.shell else {
        return Ok(ToolOutput::error(
            "no execution environment is selected for this workspace",
            "shell_unavailable",
        ));
    };
    if args.timeout_ms == Some(0) || (args.timeout_ms.is_some() && args.timeout.is_some()) {
        return Ok(invalid("invalid or conflicting timeout"));
    }
    let timeout = if let Some(seconds) = args.timeout {
        match Duration::try_from_secs_f64(seconds) {
            Ok(value) if !value.is_zero() => Some(value),
            _ => {
                return Ok(ToolOutput {
                    content: crate::prompt::text_content(
                        "Invalid timeout: must be a finite number of seconds",
                    ),
                    details: None,
                    is_error: true,
                })
            }
        }
    } else {
        args.timeout_ms.map(Duration::from_millis)
    };
    if timeout.is_some_and(|value| std::time::Instant::now().checked_add(value).is_none()) {
        return Ok(invalid("timeout is too large"));
    }
    let live = ctx.live.clone();
    let session_id = ctx.session_id;
    let task_id = ctx.task_id;
    let pending = std::sync::Arc::new(std::sync::Mutex::new((
        Vec::<u8>::new(),
        None::<std::time::Instant>,
    )));
    let live_pending = pending.clone();
    let live_sink = live.clone();
    let output = OutputMode::Stream(std::sync::Arc::new(move |chunk| {
        let mut pending = live_pending.lock().expect("live decoder poisoned");
        pending.0.extend_from_slice(&chunk.bytes);
        if pending.0.len() > DEFAULT_MAX_BYTES {
            let excess = pending.0.len() - DEFAULT_MAX_BYTES;
            pending.0.drain(..excess);
        }
        if pending
            .1
            .is_none_or(|last| last.elapsed() >= Duration::from_millis(100))
        {
            flush_live(&mut pending, &*live_sink, session_id, task_id, false);
        }
    }));
    let options = ExecOptions {
        cwd: Some(ctx.files.cwd().to_path_buf()),
        timeout,
        output,
        spill: SpillPolicy::default(),
        ..ExecOptions::default()
    };
    let command = CommandSpec::Shell {
        command: args.command,
    };
    let run = shell.exec(command, options, ctx.cancel.clone());
    tokio::pin!(run);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let result = loop {
        tokio::select! {
            result = &mut run => break result,
            _ = tick.tick() => {
                let mut pending = pending.lock().expect("live decoder poisoned");
                if pending.1.is_some_and(|last| last.elapsed() >= Duration::from_millis(100)) {
                    flush_live(&mut pending, &*live, session_id, task_id, false);
                }
            }
        }
    };
    flush_live(
        &mut pending.lock().expect("live decoder poisoned"),
        &*live,
        session_id,
        task_id,
        true,
    );
    match result {
        Ok(result) => Ok(bash_output(result)),
        Err(error) => {
            let code = match error.code {
                ExecutionErrorCode::Aborted => "aborted",
                ExecutionErrorCode::Timeout => "timeout",
                ExecutionErrorCode::ShellUnavailable => "shell_unavailable",
                ExecutionErrorCode::SpawnError => "spawn_error",
                ExecutionErrorCode::CallbackError => "callback_error",
                ExecutionErrorCode::Unknown => "unknown",
            };
            if matches!(
                error.code,
                ExecutionErrorCode::Timeout | ExecutionErrorCode::Aborted
            ) {
                let seconds = args
                    .timeout
                    .map(|v| v.to_string())
                    .or_else(|| args.timeout_ms.map(|v| (v as f64 / 1000.0).to_string()))
                    .unwrap_or_default();
                let partial = match (&error.truncation, &error.spill_path) {
                    (Some(truncation), Some(path)) => {
                        format_truncated_shell(truncation, path, error.last_line_bytes)
                    }
                    _ => error.partial_output.clone().unwrap_or_default(),
                };
                let status = if error.code == ExecutionErrorCode::Timeout {
                    format!("Command timed out after {seconds} seconds")
                } else {
                    "Command aborted".to_owned()
                };
                return Ok(ToolOutput {
                    content: crate::prompt::text_content(&if partial.is_empty() {
                        status
                    } else {
                        format!("{partial}\n\n{status}")
                    }),
                    details: None,
                    is_error: true,
                });
            }
            Ok(ToolOutput {
                content: crate::prompt::text_content(&error.message),
                details: Some(
                    json!({ "code": code, "spill_path": error.spill_path, "partial_output": error.partial_output, "truncated": error.truncated }),
                ),
                is_error: true,
            })
        }
    }
}

fn flush_live(
    pending: &mut (Vec<u8>, Option<std::time::Instant>),
    live: &dyn rusty_bot_core::LiveSink,
    session_id: rusty_bot_core::SessionId,
    task_id: rusty_bot_core::TaskId,
    final_flush: bool,
) {
    let end = if final_flush {
        pending.0.len()
    } else {
        let mut offset = 0;
        loop {
            match std::str::from_utf8(&pending.0[offset..]) {
                Ok(_) => break pending.0.len(),
                Err(error) => {
                    offset += error.valid_up_to();
                    match error.error_len() {
                        Some(length) => offset += length,
                        None => break offset,
                    }
                }
            }
        }
    };
    if end > 0 {
        let text = String::from_utf8_lossy(&pending.0[..end]).into_owned();
        pending.0.drain(..end);
        live.publish(
            session_id,
            LiveFrame {
                task_id: Some(task_id),
                kind: LiveKind::ToolDelta,
                data: json!({"text": text}),
            },
        );
        pending.1 = Some(std::time::Instant::now());
    }
}

#[cfg(test)]
#[path = "shell_output_tests.rs"]
mod shell_output_tests;

fn bash_output(result: ShellExecResult) -> ToolOutput {
    let mut content = String::new();
    if !result.stdout.is_empty() {
        content.push_str(&result.stdout);
    }
    if !result.stderr.is_empty() {
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(&result.stderr);
    }
    let details = if let (Some(truncation), Some(path)) = (&result.truncation, &result.spill_path) {
        content = format_truncated_shell(truncation, path, result.last_line_bytes);
        Some(json!({"truncation":truncation,"fullOutputPath":path}))
    } else {
        None
    };
    if content.is_empty() {
        content = "(no output)".into();
    }
    if result.exit_code != Some(0) {
        let status = match (result.exit_code, result.signal) {
            (Some(code), _) => format!("Command exited with code {code}"),
            (None, Some(signal)) => format!("Command terminated by signal {signal}"),
            (None, None) => "Command exit status unknown".to_string(),
        };
        if !content.is_empty() {
            content.push_str("\n\n");
        }
        content.push_str(&status);
    }
    ToolOutput {
        content: crate::prompt::text_content(&content),
        details,
        is_error: result.exit_code != Some(0),
    }
}

fn format_truncated_shell(truncation: &Value, path: &Path, last_line_bytes: usize) -> String {
    let displayed = truncation["content"].as_str().unwrap_or("");
    let total = truncation["totalLines"].as_u64().unwrap_or(0);
    let shown = truncation["outputLines"].as_u64().unwrap_or(0);
    let note = if truncation["lastLinePartial"] == true {
        format!(
            "Showing last {} of line {total} (line is {}). Full output: {}",
            shell_size(truncation["outputBytes"].as_u64().unwrap_or(0)),
            shell_size(last_line_bytes as u64),
            path.display()
        )
    } else if truncation["truncatedBy"] == "lines" {
        format!(
            "Showing lines {}-{total} of {total}. Full output: {}",
            total.saturating_sub(shown).saturating_add(1),
            path.display()
        )
    } else {
        format!(
            "Showing lines {}-{total} of {total} (50.0KB limit). Full output: {}",
            total.saturating_sub(shown).saturating_add(1),
            path.display()
        )
    };
    format!("{displayed}\n\n[{note}]")
}

fn shell_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / 1_048_576.0)
    }
}

/// Text content helper for tool results.
pub fn text_parts(text: &str) -> Vec<ContentPart> {
    vec![ContentPart::Text {
        text: text.to_string(),
    }]
}
