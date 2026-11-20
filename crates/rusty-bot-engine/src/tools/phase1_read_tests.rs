use super::*;

fn scan(input: &str, offset: usize, limit: usize) -> Scan {
    let mut scan = Scan {
        offset,
        limit,
        line: 1,
        selected_lines: 0,
        selected_bytes: 0,
        last_empty: true,
        first_bytes: 0,
        head: String::new(),
        line_bytes: 0,
        lines: vec![],
        stored_bytes: 0,
    };
    for ch in input.chars() {
        scan.push(ch);
    }
    if scan.selected() {
        scan.finish_line();
    }
    scan
}

#[test]
fn selection_counts_only_requested_lines_and_interline_separators() {
    let result = scan("skip\n猫\nx\nlast", 2, 2);
    assert_eq!(result.lines, ["猫", "x"]);
    assert_eq!((result.selected_lines, result.selected_bytes), (2, 5));
    assert_eq!((result.first_bytes, result.stored_bytes), (3, 5));
    assert_eq!(result.line, 4);
}

#[test]
fn trailing_newline_and_empty_input_keep_empty_last_line_accounting() {
    let result = scan("a\n", 1, usize::MAX);
    assert_eq!(result.lines, ["a", ""]);
    assert_eq!((result.selected_lines, result.selected_bytes), (2, 2));
    assert!(result.last_empty);
    let result = scan("", 1, 1);
    assert_eq!(result.lines, [""]);
    assert_eq!(result.selected_bytes, 0);
}

#[test]
fn zero_limit_and_past_eof_select_nothing() {
    for (offset, limit) in [(1, 0), (5, 2)] {
        let result = scan("a\nb", offset, limit);
        assert!(result.lines.is_empty());
        assert_eq!(
            (
                result.selected_lines,
                result.selected_bytes,
                result.first_bytes
            ),
            (0, 0, 0)
        );
    }
}

#[test]
fn long_lines_keep_bounded_utf8_prefix_but_count_complete_bytes() {
    let input = "猫".repeat(DEFAULT_MAX_BYTES);
    let result = scan(&input, 1, 1);
    assert_eq!(result.first_bytes, input.len());
    assert_eq!(result.selected_bytes, input.len());
    assert!(result.lines[0].len() <= DEFAULT_MAX_BYTES + 4);
    assert!(input.starts_with(&result.lines[0]));
    assert_eq!(result.stored_bytes, input.len());
}

#[test]
fn storage_stops_after_line_cap_but_totals_continue() {
    let count = DEFAULT_MAX_LINES + 10;
    let result = scan(&vec!["x"; count].join("\n"), 1, usize::MAX);
    assert_eq!(result.selected_lines, count);
    assert_eq!(result.selected_bytes, count * 2 - 1);
    assert_eq!(result.lines.len(), DEFAULT_MAX_LINES + 1);
    assert_eq!(result.lines.last().unwrap(), "x");
}

#[test]
fn carriage_returns_bom_and_size_thresholds_are_preserved() {
    let result = scan("\u{feff}猫\r\n犬", 1, 2);
    assert_eq!(result.lines, ["\u{feff}猫\r", "犬"]);
    assert_eq!(result.selected_bytes, 11);
    for (bytes, expected) in [
        (0, "0B"),
        (1023, "1023B"),
        (1024, "1.0KB"),
        (1048575, "1024.0KB"),
        (1048576, "1.0MB"),
    ] {
        assert_eq!(format_size(bytes), expected);
    }
}
