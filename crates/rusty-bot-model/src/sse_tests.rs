use super::*;

#[test]
fn every_split_point_preserves_crlf_multiline_and_multibyte_events() {
    let input = "data: 猫\r\ndata: 犬\r\n\r\ndata: done\n\n".as_bytes();
    for split in 0..=input.len() {
        let mut parser = SseParser::new();
        let mut events = parser.push(&input[..split]);
        events.extend(parser.push(&input[split..]));
        assert_eq!(events, ["猫\n犬", "done"], "split {split}");
        assert_eq!(parser.finish(), None);
    }
}

#[test]
fn comments_metadata_unknown_fields_and_empty_events_are_ignored() {
    let mut parser = SseParser::new();
    assert!(parser
        .push(b": ping\nevent: message\nid: 1\nretry: 2\nunknown: field\n\ndata:\n\n")
        .is_empty());
    assert_eq!(parser.push(b"data: actual\n\n"), ["actual"]);
    assert_eq!(parser.finish(), None);
}

#[test]
fn only_one_optional_space_is_stripped_from_data_fields() {
    let mut parser = SseParser::new();
    assert_eq!(
        parser.push(b"data:no-space\ndata:  two\ndata:\ndata:\ttab\n\n"),
        ["no-space\n two\n\n\ttab"]
    );
}

#[test]
fn finish_flushes_unterminated_field_once_and_parser_can_be_reused() {
    let mut parser = SseParser::new();
    assert!(parser.push(b"data: first\ndata: second\r").is_empty());
    assert_eq!(parser.finish().as_deref(), Some("first\nsecond"));
    assert_eq!(parser.finish(), None);
    assert_eq!(parser.push(b"data: next\n\n"), ["next"]);
}

#[test]
fn invalid_utf8_is_lossy_but_incomplete_valid_codepoints_wait_for_more_bytes() {
    let mut parser = SseParser::new();
    assert!(parser.push(b"data: \xe7").is_empty());
    assert_eq!(
        parser.push(b"\x8c\xab\n\ndata: \xff\n\n"),
        ["猫", "\u{fffd}"]
    );
    assert!(parser.push(b"data: \xe7").is_empty());
    assert_eq!(parser.finish().as_deref(), Some("\u{fffd}"));
}

#[test]
fn final_comments_do_not_create_events_or_discard_pending_data() {
    let mut parser = SseParser::new();
    assert!(parser.push(b"data: keep\n: final comment").is_empty());
    assert_eq!(parser.finish().as_deref(), Some("keep"));
    assert!(parser.push(b": comment only").is_empty());
    assert_eq!(parser.finish(), None);
}
