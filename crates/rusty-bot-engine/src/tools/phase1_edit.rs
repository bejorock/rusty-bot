use super::*;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

#[cfg(test)]
#[path = "phase1_edit_tests.rs"]
mod tests;

pub struct EditTool;

fn edits(args: &Value) -> Result<(String, Vec<(String, String)>), String> {
    let obj = args.as_object().ok_or("expected object")?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or("missing path")?
        .to_owned();
    let pairs = |v: &Value| -> Result<(String, String), String> {
        let o = v.as_object().ok_or("edit must be an object")?;
        let canonical = o.contains_key("oldText") || o.contains_key("newText");
        let legacy = o.contains_key("old_text") || o.contains_key("new_text");
        if canonical == legacy {
            return Err("conflicting or missing edit aliases".into());
        }
        let (a, b) = if canonical {
            ("oldText", "newText")
        } else {
            ("old_text", "new_text")
        };
        Ok((
            o.get(a)
                .and_then(Value::as_str)
                .ok_or("missing old text")?
                .to_owned(),
            o.get(b)
                .and_then(Value::as_str)
                .ok_or("missing new text")?
                .to_owned(),
        ))
    };
    let has_top = ["oldText", "newText", "old_text", "new_text"]
        .iter()
        .any(|k| obj.contains_key(*k));
    let all = if let Some(v) = obj.get("edits") {
        if has_top {
            return Err("mixed edit representations".into());
        }
        let v = if let Some(s) = v.as_str() {
            serde_json::from_str(s).map_err(|_| "invalid JSON edits")?
        } else {
            v.clone()
        };
        match v {
            Value::Array(a) => a.iter().map(pairs).collect::<Result<Vec<_>, _>>()?,
            Value::Object(_) => vec![pairs(&v)?],
            _ => return Err("invalid edits".into()),
        }
    } else {
        vec![pairs(args)?]
    };
    if all.is_empty() || all.iter().any(|(old, _)| old.is_empty()) {
        return Err("edits and old text must not be empty".into());
    }
    Ok((path, all))
}

fn norm_line_end(s: &str) -> String {
    s.replace("\r\n", "\n").replace('\r', "\n")
}
fn append_segment(output: &mut String, segment: &str) {
    if output.ends_with('\r') && segment.starts_with('\n') {
        output.push('\n');
    }
    output.push_str(segment);
}

// Adapted from Pi coding-agent edit-diff.ts at 4afe78de1e51162c01c946dcf080e6a266685a01,
// MIT, Copyright (c) 2025 Mario Zechner; see THIRD_PARTY_NOTICES.md.
// Replacements are byte offsets in the actual old and new texts, including seam repairs.
fn edit_patch(
    path: &str,
    old: &str,
    new: &str,
    replacements: &[(usize, usize, usize, usize)],
) -> String {
    const MARGIN: usize = 32;
    let line_start = |offset: usize| old[..offset].rfind('\n').map_or(0, |i| i + 1);
    let mut windows: Vec<(usize, usize, usize, usize)> = Vec::new();
    for &(a, b, c, d) in replacements {
        let mut start = line_start(a);
        for _ in 0..MARGIN {
            if start == 0 {
                break;
            }
            start = line_start(start - 1);
        }
        let mut end = b;
        for i in 0..=MARGIN {
            if end == old.len() {
                break;
            }
            let search = if i == 0 {
                b.saturating_sub(1).max(a)
            } else {
                end
            };
            end = old.as_bytes()[search..]
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(old.len(), |n| search + n + 1);
        }
        let new_end = d + (end - b);
        if let Some(last) = windows.last_mut().filter(|w| start <= w.1) {
            last.1 = last.1.max(end);
            last.3 = d + (last.1 - b);
        } else {
            let new_start = c - (a - start);
            windows.push((start, end, new_start, new_end));
        }
    }
    let mut patch = String::new();
    let (mut old_cursor, mut new_cursor, mut old_line, mut new_line) = (0, 0, 0, 0);
    for (a, b, c, d) in windows {
        old_line += old.as_bytes()[old_cursor..a]
            .iter()
            .filter(|&&v| v == b'\n')
            .count();
        new_line += new.as_bytes()[new_cursor..c]
            .iter()
            .filter(|&&v| v == b'\n')
            .count();
        old_cursor = a;
        new_cursor = c;
        let old_lines: Vec<_> = old[a..b].split_inclusive('\n').collect();
        let new_lines: Vec<_> = new[c..d].split_inclusive('\n').collect();
        let diff = similar::TextDiff::configure()
            .newline_terminated(true)
            .diff_slices(&old_lines, &new_lines);
        let mut formatter = diff.unified_diff();
        formatter.context_radius(4);
        for hunk in formatter.iter_hunks() {
            use std::fmt::Write;
            if patch.is_empty() {
                // A tab terminates the filename for traditional patch implementations;
                // prefix relative paths so leading whitespace is not stripped.
                if path
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
                    && !path.starts_with('-')
                {
                    writeln!(patch, "--- {path}\n+++ {path}").unwrap();
                } else {
                    let header_path = if Path::new(path).is_absolute() {
                        path.to_owned()
                    } else {
                        format!("./{path}")
                    };
                    writeln!(patch, "--- {header_path}\t\n+++ {header_path}\t").unwrap();
                }
            }
            let changes: Vec<_> = hunk.iter_changes().collect();
            let old_start = changes
                .iter()
                .filter_map(|c| c.old_index())
                .next()
                .unwrap_or_else(|| hunk.ops()[0].old_range().start);
            let new_start = changes
                .iter()
                .filter_map(|c| c.new_index())
                .next()
                .unwrap_or_else(|| hunk.ops()[0].new_range().start);
            let old_count = changes.iter().filter(|c| c.old_index().is_some()).count();
            let new_count = changes.iter().filter(|c| c.new_index().is_some()).count();
            let range = |start: usize, end: usize, offset: usize| {
                let len = end - start;
                let line = start + offset + usize::from(len != 0);
                format!("{line},{len}")
            };
            let old_range = range(old_start, old_start + old_count, old_line);
            let new_range = range(new_start, new_start + new_count, new_line);
            writeln!(patch, "@@ -{old_range} +{new_range} @@").unwrap();
            for change in changes {
                write!(patch, "{}{}", change.tag(), change.value()).unwrap();
                if !change.value().ends_with('\n') {
                    patch.push_str("\n\\ No newline at end of file\n");
                }
            }
        }
    }
    patch
}

// Display format adapted from coding-agent edit-diff.ts at the pinned MIT commit.
fn display_diff(old: &str, new: &str) -> (String, usize) {
    let width = (old
        .bytes()
        .filter(|b| *b == b'\n')
        .count()
        .max(new.bytes().filter(|b| *b == b'\n').count())
        + 1)
    .to_string()
    .len();
    // Myers can take quadratic work for adversarial replacements. Choose the
    // fallback by input size, never by a wall-clock deadline: identical inputs
    // therefore always have identical model-visible output.
    let old_lines = old.lines().count();
    let new_lines = new.lines().count();
    if old_lines.saturating_add(new_lines) > 4_000
        || old_lines.saturating_mul(new_lines) > 1_000_000
    {
        let first = old
            .lines()
            .zip(new.lines())
            .take_while(|(a, b)| a == b)
            .count()
            + 1;
        return (
            "[display diff omitted: deterministic computation limit; see patch]".into(),
            first,
        );
    }
    let diff = similar::TextDiff::configure().diff_lines(old, new);
    let mut lines = Vec::new();
    let mut first = None;
    let mut old_num = 1;
    let mut new_num = 1;
    let ops = diff.ops();
    for (i, op) in ops.iter().enumerate() {
        let changes: Vec<_> = diff.iter_changes(op).collect();
        if op.tag() == similar::DiffTag::Equal {
            let before = i > 0;
            let after = i + 1 < ops.len();
            let n = changes.len();
            let mut skipped = false;
            for (j, change) in changes.iter().enumerate() {
                let show = (before && j < 4) || (after && j >= n.saturating_sub(4));
                if show {
                    lines.push(format!(
                        " {:>width$} {}",
                        old_num,
                        change.value().strip_suffix('\n').unwrap_or(change.value()),
                        width = width
                    ));
                } else if !skipped && (before || after) {
                    lines.push(format!(" {:width$} ...", "", width = width));
                    skipped = true;
                }
                old_num += 1;
                new_num += 1;
            }
        } else {
            first.get_or_insert(new_num);
            for change in changes {
                let (prefix, num) = match change.tag() {
                    similar::ChangeTag::Delete => {
                        let n = old_num;
                        old_num += 1;
                        ('-', n)
                    }
                    similar::ChangeTag::Insert => {
                        let n = new_num;
                        new_num += 1;
                        ('+', n)
                    }
                    similar::ChangeTag::Equal => unreachable!(),
                };
                lines.push(format!(
                    "{prefix}{num:>width$} {}",
                    change.value().strip_suffix('\n').unwrap_or(change.value()),
                    width = width
                ));
            }
        }
    }
    (lines.join("\n"), first.unwrap_or(1))
}
fn pi_edit_error(message: String) -> ToolOutput {
    ToolOutput {
        content: crate::prompt::text_content(&message),
        details: None,
        is_error: true,
    }
}

fn normalize(s: &str) -> String {
    let normalized: String = s.nfkc().collect();
    normalized
        .split('\n')
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .map(|c| match c {
            '\u{2018}'..='\u{201b}' => '\'',
            '\u{201c}'..='\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            _ => c,
        })
        .collect()
}

#[async_trait]
impl Tool for EditTool {
    fn spec(&self) -> ToolSpec {
        let pair = json!({"type":"object", "properties":{"oldText":{"type":"string"},"newText":{"type":"string"},"old_text":{"type":"string"},"new_text":{"type":"string"}},"oneOf":[{"required":["oldText","newText"],"not":{"anyOf":[{"required":["old_text"]},{"required":["new_text"]}]}},{"required":["old_text","new_text"],"not":{"anyOf":[{"required":["oldText"]},{"required":["newText"]}]}}]});
        spec(
            "edit",
            "Replace unique, non-overlapping regions of the original file.",
            json!({"type":"object", "properties":{"path":{"type":"string"},"edits":{"anyOf":[{"type":"array","minItems":1,"items":pair},pair,{"type":"string"}]},"oldText":{"type":"string"},"newText":{"type":"string"},"old_text":{"type":"string"},"new_text":{"type":"string"}}, "required":["path"], "oneOf":[{"required":["edits"],"not":{"anyOf":[{"required":["oldText"]},{"required":["newText"]},{"required":["old_text"]},{"required":["new_text"]}]}},{"required":["oldText","newText"],"not":{"anyOf":[{"required":["edits"]},{"required":["old_text"]},{"required":["new_text"]}]}},{"required":["old_text","new_text"],"not":{"anyOf":[{"required":["edits"]},{"required":["oldText"]},{"required":["newText"]}]}}]}),
        )
    }
    async fn execute(&self, arguments: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        // Pinned Pi prepareArguments leaves unrecognized aliases untouched; its
        // execute validation then reports the absent edits array without details.
        if let Some(object) = arguments.as_object() {
            let has_alias = ["oldText", "newText", "old_text", "new_text"]
                .iter()
                .any(|key| object.contains_key(*key));
            let missing_replacements = match object.get("edits") {
                None => true,
                Some(Value::Array(items)) => items.is_empty(),
                Some(Value::String(raw)) => match serde_json::from_str::<Value>(raw) {
                    Ok(Value::Array(items)) => items.is_empty(),
                    Ok(Value::Object(pair)) => {
                        !pair.contains_key("oldText") && !pair.contains_key("old_text")
                    }
                    _ => true,
                },
                _ => false,
            };
            if !has_alias && missing_replacements {
                return Ok(ToolOutput {
                    content: crate::prompt::text_content(
                        "Edit tool input is invalid. edits must contain at least one replacement.",
                    ),
                    details: None,
                    is_error: true,
                });
            }
        }
        let (path, changes) = match edits(&arguments) {
            Ok(v) => v,
            Err(e) => return Ok(invalid(e)),
        };
        // Traditional patch does not portably decode quoted control characters in
        // filenames. Reject them before writing rather than return an unusable patch.
        if path.chars().any(char::is_control) {
            return Ok(invalid(
                "edit path contains a control character unsupported in patch headers",
            ));
        }
        if ctx.cancel.is_cancelled() {
            return Ok(file_error(FileErrorCode::Aborted, "edit aborted"));
        }
        let original = match ctx.files.read_text_file(Path::new(&path)).await {
            Ok(v) => v,
            Err(e) => return Ok(file_error(e.code, e)),
        };
        let bom = original.starts_with('\u{feff}');
        let content = if bom {
            &original['\u{feff}'.len_utf8()..]
        } else {
            &original
        };
        // Map normalized character boundaries back to original UTF-8 byte offsets.
        let mut base = String::new();
        let mut map = vec![0usize];
        let mut it = content.char_indices().peekable();
        while let Some((i, c)) = it.next() {
            let (c, end) = if c == '\r' {
                if matches!(it.peek(), Some((_, '\n'))) {
                    let (p, _) = it.next().unwrap();
                    ('\n', p + 1)
                } else {
                    ('\n', i + 1)
                }
            } else {
                (c, i + c.len_utf8())
            };
            base.push(c);
            for _ in 0..c.len_utf8() - 1 {
                map.push(i);
            }
            map.push(end);
        }
        let mut replacements = Vec::new();
        let count = changes.len();
        for (edit_index, (old, new)) in changes.into_iter().enumerate() {
            let old = norm_line_end(&old);
            let new = norm_line_end(&new);
            let exact: Vec<_> = base
                .match_indices(&old)
                .map(|(i, _)| (i, i + old.len()))
                .collect();
            let matches = if exact.is_empty() {
                // Build a byte-boundary map for normalized characters; trailing-space and NFKC transformations may widen spans.
                let mut fuzzy = String::new();
                let mut fm = vec![0usize];
                let mut boundaries = vec![true];
                for (i, grapheme) in base.grapheme_indices(true) {
                    if grapheme != "\n"
                        && grapheme.chars().all(char::is_whitespace)
                        && base[i + grapheme.len()..]
                            .chars()
                            .take_while(|c| *c != '\n')
                            .all(char::is_whitespace)
                    {
                        continue;
                    }
                    let n: String = grapheme
                        .nfkc()
                        .map(|c| match c {
                            '\u{2018}'..='\u{201b}' => '\'',
                            '\u{201c}'..='\u{201f}' => '"',
                            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
                            _ => c,
                        })
                        .collect();
                    let n_len = n.len();
                    for (j, ch) in n.char_indices() {
                        fuzzy.push(ch);
                        for _ in 0..ch.len_utf8() - 1 {
                            fm.push(i);
                            boundaries.push(false);
                        }
                        fm.push(i + grapheme.len());
                        boundaries.push(j + ch.len_utf8() == n_len);
                    }
                }
                let needle = normalize(&old);
                if needle.is_empty() {
                    return Ok(invalid("empty normalized old text"));
                }
                // A rejected match may start inside an NFKC expansion and overlap
                // a later valid match. Search each boundary before skipping a span.
                let mut matches = Vec::new();
                let mut next = 0;
                for (i, _) in fuzzy.char_indices() {
                    let end = i + needle.len();
                    if i >= next
                        && end <= fuzzy.len()
                        && boundaries[i]
                        && boundaries[end]
                        && fuzzy[i..].starts_with(&needle)
                    {
                        matches.push((fm[i], fm[end]));
                        next = end;
                    }
                }
                matches
            } else {
                exact
            };
            if matches.is_empty() {
                return Ok(pi_edit_error(if count == 1 {
                    format!("Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines.")
                } else {
                    format!("Could not find edits[{edit_index}] in {path}. The oldText must match exactly including all whitespace and newlines.")
                }));
            }
            if matches.len() > 1 {
                let n = matches.len();
                return Ok(pi_edit_error(if count == 1 {
                    format!("Found {n} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique.")
                } else {
                    format!("Found {n} occurrences of edits[{edit_index}] in {path}. Each oldText must be unique. Please provide more context to make it unique.")
                }));
            }
            let (a, b) = matches[0];
            replacements.push((map[a], map[b], new, edit_index));
        }
        replacements.sort_by_key(|r| r.0);
        if let Some(w) = replacements.windows(2).find(|w| w[0].1 > w[1].0) {
            return Ok(pi_edit_error(format!("edits[{}] and edits[{}] overlap in {path}. Merge them into one edit or target disjoint regions.", w[0].3, w[1].3)));
        }
        let replacements: Vec<_> = replacements
            .into_iter()
            .map(|(a, b, c, _)| (a, b, c))
            .collect();
        drop(map);
        drop(base);
        let (updated, diff_replacements) = apply_original(content, bom, &replacements);
        if original == updated {
            return Ok(pi_edit_error(if count == 1 {
                format!("No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected.")
            } else {
                format!("No changes made to {path}. The replacements produced identical content.")
            }));
        }
        // Patch the actual bytes so untouched mixed endings remain intact.
        let patch = edit_patch(&path, &original, &updated, &diff_replacements);
        let normalized_old = norm_line_end(content);
        let normalized_new = norm_line_end(updated.strip_prefix('\u{feff}').unwrap_or(&updated));
        let (diff, first_changed_line) = display_diff(&normalized_old, &normalized_new);
        if ctx.cancel.is_cancelled() {
            return Ok(file_error(FileErrorCode::Aborted, "edit aborted"));
        }
        match ctx
            .files
            .write_file(Path::new(&path), updated.as_bytes())
            .await
        {
            Ok(()) => Ok(ToolOutput {
                content: crate::prompt::text_content(&format!(
                    "Successfully replaced {count} block(s) in {path}."
                )),
                details: Some(
                    json!({"diff":diff,"firstChangedLine":first_changed_line,"patch":patch}),
                ),
                is_error: false,
            }),
            Err(e) => Ok(file_error(e.code, e)),
        }
    }
}

fn apply_original(
    content: &str,
    bom: bool,
    replacements: &[(usize, usize, String)],
) -> (String, Vec<(usize, usize, usize, usize)>) {
    let mut updated = String::new();
    let mut diff_replacements = Vec::new();
    if bom {
        updated.push('\u{feff}');
    }
    let mut pos = 0;
    for &(start, end, ref new) in replacements {
        let kept = &content[pos..start];
        if updated.ends_with('\r') && kept.starts_with('\n') {
            let at = pos + if bom { 3 } else { 0 };
            let new_at = updated.len();
            diff_replacements.push((at, at, new_at, new_at + 1));
        }
        append_segment(&mut updated, kept);
        let new_start = updated.len();
        let lf = content[start..]
            .find('\n')
            .map(|i| start + i)
            .or_else(|| content[..start].rfind('\n'));
        let ending = match lf {
            Some(i) if i > 0 && content.as_bytes()[i - 1] == b'\r' => "\r\n",
            Some(_) => "\n",
            None if content.contains('\r') => "\r",
            None => "\n",
        };
        append_segment(&mut updated, &new.replace('\n', ending));
        let shift = if bom { 3 } else { 0 };
        diff_replacements.push((start + shift, end + shift, new_start, updated.len()));
        pos = end;
    }
    if updated.ends_with('\r') && content[pos..].starts_with('\n') {
        let at = pos + if bom { 3 } else { 0 };
        let new_at = updated.len();
        diff_replacements.push((at, at, new_at, new_at + 1));
    }
    append_segment(&mut updated, &content[pos..]);
    (updated, diff_replacements)
}
