use super::*;

#[test]
fn cursor_rejects_other_queries_and_malformed_tokens() {
    let mut pager = Pager::new(binding("first"), None, 1, 100).unwrap();
    pager.push("one".into(), true, false).unwrap();
    assert!(pager.push("two".into(), true, false).unwrap());
    let token = pager.page.next_cursor.unwrap();
    assert!(Pager::new(binding("second"), Some(&token), 1, 100).is_err());
    assert!(Pager::new(binding("first"), Some("bad!"), 1, 100).is_err());
    assert!(Pager::new(binding("first"), None, 0, 100).is_err());
    assert!(Pager::new(binding("first"), None, 1, 0).is_err());
}

#[test]
fn line_limit_cursor_resumes_at_first_unreturned_row() {
    let rows = ["one", "two", "three"];
    let mut first = Pager::new(binding("query"), None, 2, 100).unwrap();
    for row in rows {
        if first.push(row.into(), true, false).unwrap() {
            break;
        }
    }
    assert_eq!(first.page.lines, ["one", "two"]);
    let mut second =
        Pager::new(binding("query"), first.page.next_cursor.as_deref(), 2, 100).unwrap();
    for row in rows {
        second.push(row.into(), true, false).unwrap();
    }
    assert_eq!(second.page.lines, ["three"]);
    assert!(!second.page.has_more);
}

#[test]
fn byte_limit_resumes_within_row_at_utf8_boundary() {
    let mut first = Pager::new(binding("utf8"), None, 10, 3).unwrap();
    assert!(first.push("αβ".into(), true, false).unwrap());
    assert_eq!(first.page.lines, ["α"]);
    assert!(first.page.truncated_bytes);
    let mut second = Pager::new(binding("utf8"), first.page.next_cursor.as_deref(), 10, 3).unwrap();
    assert!(!second.push("αβ".into(), true, false).unwrap());
    assert_eq!(second.page.lines, ["β"]);
    assert!(!second.page.has_more);
}

#[test]
fn page_budget_counts_separators_and_rejects_nonprogressing_utf8_cap() {
    let mut pager = Pager::new(binding("q"), None, 10, 3).unwrap();
    assert!(!pager.push("abc".into(), false, true).unwrap());
    assert!(pager.push("d".into(), true, false).unwrap());
    assert_eq!(pager.page.lines, ["abc"]);
    assert_eq!(pager.page.matched, 0);
    assert!(pager.page.truncated_lines);
    assert!(pager.page.truncated_bytes);
    let mut tiny = Pager::new(binding("q"), None, 1, 1).unwrap();
    assert_eq!(
        tiny.push("α".into(), true, false).unwrap_err().code,
        FileErrorCode::Invalid
    );
}
