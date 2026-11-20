use super::*;
use rusty_bot_core::Message;

fn page() -> SearchPage {
    SearchPage {
        next_cursor: None,
        lines: vec![],
        matched: 0,
        has_more: false,
        truncated_bytes: false,
        truncated_lines: false,
        output_totals: None,
    }
}
fn text(output: &ToolOutput) -> String {
    Message::text_content(&output.content)
}

#[test]
fn empty_results_use_operation_specific_text_and_no_details() {
    for (kind, empty) in [
        ("ls", "(empty directory)"),
        ("grep", "No matches found"),
        ("find", "No files found matching pattern"),
    ] {
        let output = output(page(), empty, kind, 10);
        assert_eq!(text(&output), empty);
        assert_eq!(output.details, None);
        assert!(!output.is_error);
    }
    let output = output(
        SearchPage {
            lines: vec!["a".into(), "b".into()],
            matched: 2,
            ..page()
        },
        "empty",
        "find",
        10,
    );
    assert_eq!(text(&output), "a\nb");
    assert_eq!(output.details, None);
}

#[test]
fn continuation_cursor_takes_precedence_over_legacy_diagnostics() {
    let output = output(
        SearchPage {
            next_cursor: Some("opaque".into()),
            lines: vec!["a".into()],
            matched: 1,
            has_more: true,
            truncated_bytes: true,
            truncated_lines: true,
            output_totals: Some((100, 200)),
        },
        "empty",
        "grep",
        1,
    );
    assert_eq!(
        text(&output),
        "a\n\n[More results available. Repeat this search with cursor: opaque]"
    );
    assert_eq!(
        output.details,
        Some(
            json!({"next_cursor":"opaque","has_more":true,"truncated_bytes":true,"truncated_lines":true})
        )
    );
    assert!(!output.is_error);
}

#[test]
fn legacy_limit_notices_use_correct_kind_and_saturating_suggestion() {
    for (kind, noun, key, extra) in [
        ("ls", "entries", "entryLimitReached", ""),
        (
            "grep",
            "matches",
            "matchLimitReached",
            ", or refine pattern",
        ),
        (
            "find",
            "results",
            "resultLimitReached",
            ", or refine pattern",
        ),
    ] {
        let output = output(
            SearchPage {
                lines: vec!["a".into()],
                matched: 1,
                has_more: true,
                ..page()
            },
            "empty",
            kind,
            usize::MAX,
        );
        assert_eq!(
            text(&output),
            format!(
                "a\n\n[{} {noun} limit reached. Use limit={} for more{extra}]",
                usize::MAX,
                usize::MAX
            )
        );
        assert_eq!(output.details.unwrap()[key], json!(usize::MAX));
    }
}

#[test]
fn line_truncation_notice_and_listing_byte_cap_preserve_utf8() {
    let output = output(
        SearchPage {
            lines: vec!["猫".repeat(CAP / 3)],
            matched: 1,
            has_more: true,
            truncated_lines: true,
            ..page()
        },
        "empty",
        "ls",
        1,
    );
    let text = text(&output);
    assert!(text.len() <= CAP);
    assert!(text.contains("1 entries limit reached"));
    assert!(text.ends_with("Some lines truncated to 500 chars. Use read tool to see full lines]"));
    assert_eq!(output.details.unwrap()["linesTruncated"], true);
}

#[test]
fn exact_output_totals_generate_structured_byte_truncation_metadata() {
    let output = output(
        SearchPage {
            lines: vec!["猫".into(), "b".into()],
            matched: 2,
            has_more: true,
            truncated_bytes: true,
            truncated_lines: true,
            output_totals: Some((100, 100_000)),
            ..page()
        },
        "empty",
        "grep",
        2,
    );
    let details = output.details.as_ref().unwrap();
    assert_eq!(details["matchLimitReached"], 2);
    assert_eq!(details["linesTruncated"], true);
    assert_eq!(
        details["truncation"],
        json!({"content":"猫\nb","truncated":true,"truncatedBy":"bytes","totalLines":100,"totalBytes":100_000,"outputLines":2,"outputBytes":5,"lastLinePartial":false,"firstLineExceedsLimit":false,"maxLines":9007199254740991u64,"maxBytes":CAP})
    );
    assert!(text(&output).contains("50.0KB limit reached"));
    let empty = super::output(
        SearchPage {
            truncated_bytes: true,
            output_totals: Some((1, 100_000)),
            ..page()
        },
        "empty",
        "find",
        1,
    );
    assert_eq!(
        empty.details.unwrap()["truncation"]["firstLineExceedsLimit"],
        true
    );
}

#[test]
fn unknown_totals_preserve_all_diagnostics_within_byte_budget() {
    let output = output(
        SearchPage {
            lines: vec!["猫".repeat(CAP)],
            matched: 7,
            has_more: true,
            truncated_bytes: true,
            truncated_lines: true,
            ..page()
        },
        "empty",
        "grep",
        7,
    );
    let text = text(&output);
    assert!(text.len() <= CAP);
    assert!(text.ends_with("\n[result limit reached]\n[output byte limit reached]\n[some lines truncated to 500 characters]"));
    assert_eq!(
        output.details,
        Some(json!({"matches":7,"has_more":true,"truncated_bytes":true,"truncated_lines":true}))
    );
}
