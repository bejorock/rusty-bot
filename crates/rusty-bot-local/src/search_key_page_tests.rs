use super::*;

fn page(
    cursor: Option<&str>,
    limit: usize,
    cap: usize,
    rows: &[&str],
) -> Result<SearchPage, FileError> {
    let mut page = KeyPage::new("query".into(), cursor, limit, cap)?;
    for row in rows {
        page.push((*row).into());
    }
    page.finish()
}

#[test]
fn unordered_rows_are_sorted_deduplicated_and_paginated_with_lookahead() {
    let rows = ["d", "b", "a", "c", "a"];
    let first = page(None, 2, 100, &rows).unwrap();
    assert_eq!(first.lines, ["a", "b"]);
    assert_eq!(first.matched, 2);
    assert!(first.has_more);
    assert!(!first.truncated_bytes);
    let second = page(first.next_cursor.as_deref(), 2, 100, &rows).unwrap();
    assert_eq!(second.lines, ["c", "d"]);
    assert!(!second.has_more);
    assert!(second.next_cursor.is_none());
}

#[test]
fn byte_budget_includes_separators_and_resumes_at_unreturned_row() {
    let first = page(None, 10, 3, &["bb", "a"]).unwrap();
    assert_eq!(first.lines, ["a"]);
    assert!(first.truncated_bytes);
    let second = page(first.next_cursor.as_deref(), 10, 3, &["bb", "a"]).unwrap();
    assert_eq!(second.lines, ["bb"]);
    assert!(!second.has_more);
    assert_eq!(page(None, 10, 4, &["bb", "a"]).unwrap().lines, ["a", "bb"]);
}

#[test]
fn long_utf8_row_resumes_at_character_boundaries_without_losing_suffix() {
    let first = page(None, 1, 4, &["猫猫z"]).unwrap();
    assert_eq!(first.lines, ["猫"]);
    let second = page(first.next_cursor.as_deref(), 1, 4, &["猫猫z"]).unwrap();
    assert_eq!(second.lines, ["猫z"]);
    assert!(!second.has_more);
    assert_eq!(
        page(None, 1, 2, &["猫"]).unwrap_err().code,
        FileErrorCode::Invalid
    );
}

#[test]
fn invalid_cross_query_and_non_boundary_cursors_are_refused() {
    let first = page(None, 1, 100, &["a", "b"]).unwrap();
    assert!(KeyPage::new("different".into(), first.next_cursor.as_deref(), 1, 100).is_err());
    for token in [
        "!",
        &URL_SAFE_NO_PAD.encode("query:not-a-number:YQ"),
        &URL_SAFE_NO_PAD.encode("query:0:YQ:extra"),
    ] {
        assert!(KeyPage::new("query".into(), Some(token), 1, 100).is_err());
    }
    for offset in [1, 9] {
        let token =
            URL_SAFE_NO_PAD.encode(format!("query:{offset}:{}", URL_SAFE_NO_PAD.encode("猫")));
        assert_eq!(
            page(Some(&token), 1, 100, &["猫"]).unwrap_err().code,
            FileErrorCode::Invalid
        );
    }
}

#[test]
fn refuses_zero_budgets_and_clamps_extreme_limits() {
    assert!(KeyPage::new("q".into(), None, 0, 1).is_err());
    assert!(KeyPage::new("q".into(), None, 1, 0).is_err());
    let page = KeyPage::new("q".into(), None, usize::MAX, usize::MAX).unwrap();
    assert_eq!(page.limit, 50_000);
    assert_eq!(page.cap, 50 * 1024);
    assert!(page.finish().unwrap().lines.is_empty());
}

#[test]
fn retained_top_keys_stay_bounded_independent_of_input_order() {
    let mut page = KeyPage::new("q".into(), None, 2, 100).unwrap();
    for n in (0..1000).rev() {
        page.push(format!("{n:04}"));
    }
    assert!(page.rows.len() <= 3);
    let result = page.finish().unwrap();
    assert_eq!(result.lines, ["0000", "0001"]);
    assert!(result.has_more);
    assert!(result.next_cursor.is_some());
}
