use super::*;

#[test]
fn exact_limits_preserve_empty_and_multibyte_input_and_totals() {
    for input in ["", "a\nb\n", "猫\n犬"] {
        let result = truncate_text(input, 2, input.len());
        assert_eq!(result.text, input);
        assert!(!result.truncated);
        assert_eq!(result.total_bytes, input.len());
        assert_eq!(result.total_lines, input.lines().count());
    }
}

#[test]
fn line_limit_retains_head_and_tail_with_odd_budget() {
    let result = truncate_text("a\nb\nc\nd\ne", 3, 100);
    assert!(result.truncated);
    assert_eq!(
        result.text,
        "a\n... [truncated: 2 lines / 5 bytes omitted] ...\nd\ne"
    );
    assert_eq!((result.total_lines, result.total_bytes), (5, 9));
}

#[test]
fn byte_limit_retains_character_aligned_edges_and_reports_elision() {
    let result = truncate_text("猫ab犬cd鳥", 100, 8);
    assert_eq!(
        result.text,
        "猫a\n... [truncated: 0 lines / 5 bytes omitted, output exceeded limits] ...\nd鳥"
    );
    assert_eq!((result.total_lines, result.total_bytes), (1, 13));
}

#[test]
fn combined_limits_recheck_long_selected_lines_against_byte_budget() {
    let result = truncate_text("abcdefgh\ninside\ntail\n12345678", 2, 6);
    assert!(result.text.starts_with("abc\n... [truncated: 2 lines /"));
    assert!(result.text.ends_with("output exceeded limits] ...\n678"));
    assert_eq!((result.total_lines, result.total_bytes), (4, 29));
}

#[test]
fn tiny_limits_are_clamped_and_marker_is_not_part_of_retained_budget() {
    let result = truncate_text("abcdef", 0, 0);
    assert_eq!(
        result.text,
        "a\n... [truncated: 0 lines / 4 bytes omitted, output exceeded limits] ...\nf"
    );
    assert!(result.text.len() > 2);
    let result = truncate_text("猫犬", 1, 1);
    assert!(result.truncated);
    assert!(result.text.starts_with("\n..."));
    assert!(result.text.ends_with("...\n"));
}

#[test]
fn boundary_helpers_never_split_multibyte_characters() {
    for (budget, prefix, suffix) in [
        (0, "", ""),
        (1, "a", "z"),
        (2, "a", "z"),
        (3, "a", "z"),
        (4, "a猫", "犬z"),
        (8, "a猫犬z", "a猫犬z"),
    ] {
        assert_eq!(prefix_at_char_boundary("a猫犬z", budget), prefix);
        assert_eq!(suffix_at_char_boundary("a猫犬z", budget), suffix);
    }
}
