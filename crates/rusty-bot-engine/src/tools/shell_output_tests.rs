use super::*;

fn result() -> ShellExecResult {
    ShellExecResult {
        exit_code: Some(0),
        signal: None,
        total_bytes: 0,
        full_output: None,
        stdout: String::new(),
        stderr: String::new(),
        truncated: false,
        spill_path: None,
        duration_ms: 0,
        truncation: None,
        last_line_bytes: 0,
    }
}
fn text(output: &ToolOutput) -> String {
    rusty_bot_core::Message::text_content(&output.content)
}

#[test]
fn successful_empty_output_and_stream_separators_are_explicit() {
    assert_eq!(text(&bash_output(result())), "(no output)");
    for (stdout, expected) in [("out", "out\nerr"), ("out\n", "out\nerr"), ("", "err")] {
        let output = bash_output(ShellExecResult {
            stdout: stdout.into(),
            stderr: "err".into(),
            ..result()
        });
        assert_eq!(text(&output), expected);
        assert!(!output.is_error);
        assert!(output.details.is_none());
    }
}

#[test]
fn nonzero_signal_and_unknown_status_are_errors_with_retained_output() {
    for (exit_code, signal, status) in [
        (Some(2), None, "Command exited with code 2"),
        (None, Some(15), "Command terminated by signal 15"),
        (None, None, "Command exit status unknown"),
    ] {
        let output = bash_output(ShellExecResult {
            exit_code,
            signal,
            stdout: "partial".into(),
            ..result()
        });
        assert!(output.is_error);
        assert_eq!(text(&output), format!("partial\n\n{status}"));
    }
}

#[test]
fn spilled_output_uses_bounded_tail_and_structured_spill_path() {
    let truncation =
        json!({"content":"tail", "totalLines":10, "outputLines":2, "truncatedBy":"lines"});
    let output = bash_output(ShellExecResult {
        stdout: "ignored".into(),
        stderr: "ignored".into(),
        truncated: true,
        spill_path: Some("/virtual/output".into()),
        truncation: Some(truncation.clone()),
        ..result()
    });
    assert_eq!(
        text(&output),
        "tail\n\n[Showing lines 9-10 of 10. Full output: /virtual/output]"
    );
    assert_eq!(
        output.details,
        Some(json!({"truncation":truncation,"fullOutputPath":"/virtual/output"}))
    );
    assert!(!output.is_error);
}

#[test]
fn incomplete_spill_metadata_does_not_replace_actual_output() {
    for (truncation, spill_path) in [
        (Some(json!({"content":"ignored"})), None),
        (None, Some(PathBuf::from("/virtual/output"))),
    ] {
        let output = bash_output(ShellExecResult {
            stdout: "actual".into(),
            truncation,
            spill_path,
            ..result()
        });
        assert_eq!(text(&output), "actual");
        assert!(output.details.is_none());
    }
}

#[test]
fn partial_line_notice_takes_precedence_over_line_limit_notice() {
    let value = json!({"content":"猫", "totalLines":9, "outputLines":1, "lastLinePartial":true, "outputBytes":1024, "truncatedBy":"lines"});
    assert_eq!(
        format_truncated_shell(&value, Path::new("/virtual/tail"), 2048),
        "猫\n\n[Showing last 1.0KB of line 9 (line is 2.0KB). Full output: /virtual/tail]"
    );
    let value = json!({"content":"tail", "totalLines":2, "outputLines":9, "truncatedBy":"bytes"});
    assert_eq!(
        format_truncated_shell(&value, Path::new("/virtual/tail"), 0),
        "tail\n\n[Showing lines 1-2 of 2 (50.0KB limit). Full output: /virtual/tail]"
    );
}

#[test]
fn size_formatting_and_text_parts_preserve_exact_content() {
    for (bytes, expected) in [
        (0, "0B"),
        (1023, "1023B"),
        (1024, "1.0KB"),
        (1048576, "1.0MB"),
    ] {
        assert_eq!(shell_size(bytes), expected);
    }
    assert_eq!(
        text_parts("猫\n"),
        vec![ContentPart::Text {
            text: "猫\n".into()
        }]
    );
}
