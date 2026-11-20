use super::*;

fn matches(pattern: &str, text: &str, ignore_case: bool) -> bool {
    let mut matcher = Matcher::new(pattern, ignore_case).unwrap();
    for ch in text.chars() {
        matcher.push(ch);
    }
    matcher.finish()
}

#[test]
fn anchors_require_complete_line_and_finish_boundary() {
    let mut matcher = Matcher::new("^猫+$", false).unwrap();
    matcher.push('猫');
    matcher.push('猫');
    assert!(!matcher.matched);
    assert!(matcher.finish());
    assert!(!matches("^cat$", "cats", false));
    assert!(!matches("^cat$", "a cat", false));
    assert!(matches("^$", "", false));
}

#[test]
fn unicode_word_boundaries_use_adjacent_complete_characters() {
    assert!(matches(r"\b猫\b", " 猫!", false));
    assert!(!matches(r"\b猫\b", "a猫b", false));
    assert!(matches(r"\bcat\b", "a cat!", false));
    assert!(!matches(r"\bcat\b", "scatter", false));
}

#[test]
fn case_folding_alternation_and_repetition_match_without_retaining_line() {
    assert!(matches("(rust|猫)+", "--RuSt猫--", true));
    assert!(!matches("rust", "RUST", false));
    assert!(matches("a[0-9]{2}z", "prefix a42z suffix", false));
    assert!(!matches("a[0-9]{2}z", "a4z", false));
}

#[test]
fn reset_discards_previous_match_and_partial_prefix() {
    let mut matcher = Matcher::new("^ab$", false).unwrap();
    matcher.push('a');
    matcher.reset();
    matcher.push('b');
    assert!(!matcher.finish());
    matcher.reset();
    matcher.push('a');
    matcher.push('b');
    assert!(matcher.finish());
    matcher.push('x'); // A settled boolean match remains true until reset.
    assert!(matcher.finish());
    matcher.reset();
    assert!(!matcher.finish());
}

#[test]
fn invalid_regex_is_rejected_and_generation_wrap_preserves_matching() {
    assert!(Matcher::new("[", false).is_err());
    let mut matcher = Matcher::new("cat", false).unwrap();
    matcher.generation = u32::MAX;
    matcher.seen.fill(1);
    for ch in "cat".chars() {
        matcher.push(ch);
    }
    assert!(matcher.finish());
    assert_ne!(matcher.generation, 0);
}
