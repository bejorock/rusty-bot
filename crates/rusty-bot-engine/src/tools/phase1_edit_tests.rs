use super::*;
use serde_json::json;

#[test]
fn accepts_canonical_legacy_array_and_json_string_edits() {
    let expected = ("a.rs".to_owned(), vec![("old".into(), "new".into())]);
    for args in [
        json!({"path":"a.rs", "oldText":"old", "newText":"new"}),
        json!({"path":"a.rs", "old_text":"old", "new_text":"new"}),
        json!({"path":"a.rs", "edits":[{"oldText":"old", "newText":"new"}]}),
        json!({"path":"a.rs", "edits":{"old_text":"old", "new_text":"new"}}),
        json!({"path":"a.rs", "edits":"[{\"oldText\":\"old\",\"newText\":\"new\"}]"}),
    ] {
        assert_eq!(edits(&args).unwrap(), expected);
    }
}

#[test]
fn refuses_ambiguous_aliases_and_mixed_representations() {
    assert_eq!(
        edits(&json!({"path":"a", "oldText":"x", "newText":"y", "old_text":"x"})).unwrap_err(),
        "conflicting or missing edit aliases"
    );
    assert_eq!(
        edits(&json!({"path":"a", "oldText":"x", "newText":"y", "edits":[]})).unwrap_err(),
        "mixed edit representations"
    );
    assert_eq!(
        edits(&json!({"path":"a", "edits":{}})).unwrap_err(),
        "conflicting or missing edit aliases"
    );
}

#[test]
fn refuses_invalid_shapes_and_empty_matches_but_allows_deletion() {
    for (args, error) in [
        (json!(null), "expected object"),
        (json!({}), "missing path"),
        (json!({"path":"a", "edits":"["}), "invalid JSON edits"),
        (json!({"path":"a", "edits":1}), "invalid edits"),
        (json!({"path":"a", "edits":[1]}), "edit must be an object"),
        (json!({"path":"a", "oldText":"x"}), "missing new text"),
        (json!({"path":"a", "newText":"x"}), "missing old text"),
        (
            json!({"path":"a", "edits":[]}),
            "edits and old text must not be empty",
        ),
        (
            json!({"path":"a", "oldText":"", "newText":"x"}),
            "edits and old text must not be empty",
        ),
    ] {
        assert_eq!(edits(&args).unwrap_err(), error);
    }
    assert_eq!(
        edits(&json!({"path":"a", "oldText":"x", "newText":""}))
            .unwrap()
            .1,
        [("x".into(), "".into())]
    );
}

#[test]
fn normalization_handles_unicode_punctuation_and_line_seams() {
    assert_eq!(
        normalize("Ａ ‘quote’\u{00a0}—\u{2009}“yes”  \nnext\t"),
        "A 'quote' - \"yes\"\nnext"
    );
    assert_eq!(norm_line_end("a\r\nb\rc\n"), "a\nb\nc\n");
    let mut out = "a\r".to_owned();
    append_segment(&mut out, "\nb");
    assert_eq!(out, "a\r\n\nb"); // Prevent two formerly separate terminators merging.
}

#[test]
fn patch_encodes_unusual_paths_and_missing_final_newlines() {
    let patch = edit_patch("-odd name", "old", "new", &[(0, 3, 0, 3)]);
    assert_eq!(patch, "--- ./-odd name\t\n+++ ./-odd name\t\n@@ -1,1 +1,1 @@\n-old\n\\ No newline at end of file\n+new\n\\ No newline at end of file\n");
    let patch = edit_patch("a.rs", "a\nb\n", "a\nc\n", &[(2, 4, 2, 4)]);
    assert!(patch.starts_with("--- a.rs\n+++ a.rs\n"));
    assert!(patch.contains("-b\n+c\n"));
}

#[test]
fn display_diff_reports_first_changed_line_and_deterministic_size_fallback() {
    let (diff, first) = display_diff("a\nb\nc\n", "a\nB\nc\n");
    assert_eq!(first, 2);
    assert!(diff.contains("-2 b"));
    assert!(diff.contains("+2 B"));
    assert_eq!(display_diff("same\n", "same\n"), (String::new(), 1));
    let old = format!("prefix\n{}", "a\n".repeat(2100));
    let new = format!("prefix\n{}", "b\n".repeat(2100));
    assert_eq!(
        display_diff(&old, &new),
        (
            "[display diff omitted: deterministic computation limit; see patch]".into(),
            2
        )
    );
}
