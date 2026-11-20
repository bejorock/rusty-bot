use super::{file_error, invalid, spec};
use async_trait::async_trait;
use rusty_bot_core::{
    FileSystem, FindQuery, GrepQuery, ListQuery, ReplayPolicy, SearchPage, SearchRequest, Tool,
    ToolContext, ToolError, ToolOutput, ToolSpec,
};
use serde::Deserialize;
use serde_json::{json, Value};

const CAP: usize = 50 * 1024;
#[derive(Deserialize)]
struct LsArgs {
    cursor: Option<String>,
    path: Option<String>,
    all: Option<bool>,
    limit: Option<usize>,
}
#[derive(Deserialize)]
struct FindArgs {
    cursor: Option<String>,
    pattern: String,
    path: Option<String>,
    limit: Option<f64>,
}
#[derive(Deserialize)]
struct GrepArgs {
    cursor: Option<String>,
    pattern: String,
    path: Option<String>,
    glob: Option<String>,
    literal: Option<bool>,
    #[serde(rename = "ignoreCase")]
    ignore_case_pi: Option<bool>,
    ignore_case: Option<bool>,
    context: Option<f64>,
    limit: Option<f64>,
    max_results: Option<usize>,
}

fn schema_path() -> Value {
    json!({"type":"string"})
}
fn limit_schema() -> Value {
    json!({"type":"integer","minimum":1})
}
fn output(page: SearchPage, empty: &str, kind: &str, limit: usize) -> ToolOutput {
    if let Some(cursor) = &page.next_cursor {
        let mut content = page.lines.join("\n");
        content.push_str(&format!(
            "\n\n[More results available. Repeat this search with cursor: {cursor}]"
        ));
        return ToolOutput {
            content: crate::prompt::text_content(&content),
            details: Some(
                json!({"next_cursor": cursor, "has_more": true, "truncated_bytes": page.truncated_bytes, "truncated_lines": page.truncated_lines}),
            ),
            is_error: false,
        };
    }
    if !page.truncated_bytes {
        let mut content = if page.lines.is_empty() && page.matched == 0 {
            empty.to_owned()
        } else {
            page.lines.join("\n")
        };
        let mut details = serde_json::Map::new();
        let mut notices = Vec::new();
        if page.has_more {
            let (noun, key) = match kind {
                "grep" => ("matches", "matchLimitReached"),
                "find" => ("results", "resultLimitReached"),
                _ => ("entries", "entryLimitReached"),
            };
            let extra = if kind == "ls" {
                ""
            } else {
                ", or refine pattern"
            };
            notices.push(format!(
                "{limit} {noun} limit reached. Use limit={} for more{extra}",
                limit.saturating_mul(2)
            ));
            details.insert(key.into(), json!(limit));
        }
        if page.truncated_lines {
            notices
                .push("Some lines truncated to 500 chars. Use read tool to see full lines".into());
            details.insert("linesTruncated".into(), json!(true));
        }
        if !notices.is_empty() {
            let suffix = format!("\n\n[{}]", notices.join(". "));
            if kind == "ls" && content.len().saturating_add(suffix.len()) > CAP {
                let mut end = CAP.saturating_sub(suffix.len());
                while !content.is_char_boundary(end) {
                    end -= 1;
                }
                content.truncate(end);
            }
            content.push_str(&suffix);
        }
        return ToolOutput {
            content: crate::prompt::text_content(&content),
            details: if details.is_empty() {
                None
            } else {
                Some(Value::Object(details))
            },
            is_error: false,
        };
    }
    if let Some((total_lines, total_bytes)) = page.output_totals {
        let mut content = page.lines.join("\n");
        let mut details = serde_json::Map::new();
        let mut notices = Vec::new();
        if page.has_more {
            let (noun, key) = match kind {
                "grep" => ("matches", "matchLimitReached"),
                "find" => ("results", "resultLimitReached"),
                _ => ("entries", "entryLimitReached"),
            };
            let extra = if kind == "ls" {
                ""
            } else {
                ", or refine pattern"
            };
            notices.push(format!(
                "{limit} {noun} limit reached. Use limit={} for more{extra}",
                limit.saturating_mul(2)
            ));
            details.insert(key.into(), json!(limit));
        }
        let truncation = json!({
            "content":content, "truncated":true, "truncatedBy":"bytes",
            "totalLines":total_lines, "totalBytes":total_bytes,
            "outputLines":page.lines.len(), "outputBytes":content.len(),
            "lastLinePartial":false, "firstLineExceedsLimit":page.lines.is_empty(),
            "maxLines":9007199254740991u64, "maxBytes":CAP
        });
        details.insert("truncation".into(), truncation);
        notices.push("50.0KB limit reached".into());
        if page.truncated_lines {
            notices
                .push("Some lines truncated to 500 chars. Use read tool to see full lines".into());
            details.insert("linesTruncated".into(), json!(true));
        }
        content.push_str(&format!("\n\n[{}]", notices.join(". ")));
        return ToolOutput {
            content: crate::prompt::text_content(&content),
            details: Some(Value::Object(details)),
            is_error: false,
        };
    }
    let mut content = if page.lines.is_empty() && page.matched == 0 && !page.truncated_bytes {
        empty.to_owned()
    } else {
        page.lines.join("\n")
    };
    let notice_start = content.len();
    let mut truncated_bytes = page.truncated_bytes;
    if page.has_more {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str("[result limit reached]");
    }
    if page.truncated_bytes {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str("[output byte limit reached]");
    }
    if page.truncated_lines {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str("[some lines truncated to 500 characters]");
    }
    if content.len() > CAP {
        // Preserve all diagnostics, and report any additional shortening here.
        if !truncated_bytes {
            content.push_str("\n[output byte limit reached]");
            truncated_bytes = true;
        }
        let suffix = content[notice_start..].to_owned();
        let mut prefix_end = CAP.saturating_sub(suffix.len());
        while !content.is_char_boundary(prefix_end) {
            prefix_end -= 1;
        }
        content = format!("{}{}", &content[..prefix_end], suffix);
    }
    ToolOutput {
        content: crate::prompt::text_content(&content),
        details: Some(
            json!({"matches":page.matched,"has_more":page.has_more,"truncated_bytes":truncated_bytes,"truncated_lines":page.truncated_lines}),
        ),
        is_error: false,
    }
}
async fn run(files: &dyn FileSystem, request: SearchRequest, ctx: &ToolContext) -> ToolOutput {
    let (empty, kind, limit) = match &request {
        SearchRequest::Grep(q) => ("No matches found", "grep", q.limit),
        SearchRequest::Find(q) => ("No files found matching pattern", "find", q.limit),
        SearchRequest::List(q) => ("(empty directory)", "ls", q.limit),
    };
    match files.search(request, ctx.cancel.clone()).await {
        Ok(page) => output(page, empty, kind, limit),
        Err(e)
            if matches!(
                e.code,
                rusty_bot_core::FileErrorCode::NotFound
                    | rusty_bot_core::FileErrorCode::NotDirectory
            ) && e.path.is_some() =>
        {
            let path = e.path.as_ref().unwrap().display();
            let message = if kind == "find" {
                format!("[fd error]: Search path '{path}' is not a directory.\n[fd error]: No valid search paths given.")
            } else if e.code == rusty_bot_core::FileErrorCode::NotFound {
                format!("Path not found: {path}")
            } else {
                format!("Not a directory: {path}")
            };
            ToolOutput {
                content: crate::prompt::text_content(&message),
                details: None,
                is_error: true,
            }
        }
        Err(e)
            if matches!(kind, "find" | "grep")
                && e.code == rusty_bot_core::FileErrorCode::Unknown =>
        {
            ToolOutput {
                content: crate::prompt::text_content(&e.message),
                details: None,
                is_error: true,
            }
        }
        Err(e) => file_error(e.code, e),
    }
}
pub struct LsTool;
#[async_trait]
impl Tool for LsTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "ls",
            "List directory entries.",
            json!({"type":"object","properties":{"path":schema_path(),"all":{"type":"boolean"},"limit":limit_schema(),"cursor":{"type":"string","description":"Continuation cursor returned by the same search query"}}}),
        )
    }
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }
    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: LsArgs = match serde_json::from_value(arguments) {
            Ok(v) => v,
            Err(e) => return Ok(invalid(e.to_string())),
        };
        if args.limit == Some(0) {
            return Ok(invalid("limit must be positive"));
        }
        Ok(run(
            ctx.files.as_ref(),
            SearchRequest::List(ListQuery {
                root: args.path.unwrap_or_else(|| ".".into()).into(),
                include_hidden: args.all.unwrap_or(true),
                limit: args.limit.unwrap_or(500),
                byte_cap: CAP,
                cursor: args.cursor,
            }),
            ctx,
        )
        .await)
    }
}
pub struct FindTool;
#[async_trait]
impl Tool for FindTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "find",
            "Find paths matching a glob, respecting ignore files.",
            json!({"type":"object","properties":{"pattern":{"type":"string"},"path":schema_path(),"limit":limit_schema(),"cursor":{"type":"string","description":"Continuation cursor returned by the same search query"}},"required":["pattern"]}),
        )
    }
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }
    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: FindArgs = match serde_json::from_value(arguments) {
            Ok(v) => v,
            Err(e) => return Ok(invalid(e.to_string())),
        };
        let limit = args.limit.unwrap_or(1000.0).floor().max(1.0);
        if !limit.is_finite() {
            return Ok(invalid("limit must be finite"));
        }
        Ok(run(
            ctx.files.as_ref(),
            SearchRequest::Find(FindQuery {
                root: args.path.unwrap_or_else(|| ".".into()).into(),
                pattern: args.pattern,
                limit: limit as usize,
                byte_cap: CAP,
                cursor: args.cursor,
            }),
            ctx,
        )
        .await)
    }
}
pub struct GrepTool;
#[async_trait]
impl Tool for GrepTool {
    fn spec(&self) -> ToolSpec {
        spec(
            "grep",
            "Search text files, respecting ignore files.",
            json!({"type":"object","properties":{"pattern":{"type":"string"},"path":schema_path(),"glob":{"type":"string"},"literal":{"type":"boolean"},"ignoreCase":{"type":"boolean"},"ignore_case":{"type":"boolean"},"context":{"type":"number"},"limit":{"type":"number"},"max_results":limit_schema(),"cursor":{"type":"string","description":"Continuation cursor returned by the same search query"}},"required":["pattern"]}),
        )
    }
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Safe
    }
    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: GrepArgs = match serde_json::from_value(arguments) {
            Ok(v) => v,
            Err(e) => return Ok(invalid(e.to_string())),
        };
        if args.limit.is_some() && args.max_results.is_some() {
            return Ok(invalid("conflicting aliases"));
        }
        if args.max_results == Some(0) {
            return Ok(invalid("invalid limit"));
        }
        let limit = args
            .limit
            .map(|v| v.floor().max(1.0) as usize)
            .or(args.max_results)
            .unwrap_or(100);
        Ok(run(
            ctx.files.as_ref(),
            SearchRequest::Grep(GrepQuery {
                root: args.path.unwrap_or_else(|| ".".into()).into(),
                pattern: args.pattern,
                glob: args.glob,
                literal: args.literal.unwrap_or(false),
                ignore_case: args.ignore_case_pi.or(args.ignore_case).unwrap_or(false),
                // Rust's float-to-integer cast saturates; floor matches Pi.
                context: args.context.unwrap_or(0.0).floor() as usize,
                limit,
                byte_cap: CAP,
                cursor: args.cursor,
            }),
            ctx,
        )
        .await)
    }
}
