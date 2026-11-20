use super::*;

#[test]
fn empty_capture_is_untruncated_and_limits_are_at_least_one() {
    let capture = StreamCapture::new(0, 0);
    assert_eq!((capture.max_bytes, capture.max_lines), (1, 1));
    assert_eq!(capture.display("marker"), "");
    assert!(capture.pi_truncation().is_none());
    assert_eq!(capture.full, Some(vec![]));
}

#[test]
fn split_utf8_decoding_is_invariant_at_every_boundary() {
    let input = "猫\nx犬".as_bytes();
    for split in 0..=input.len() {
        let mut capture = StreamCapture::new(100, 10);
        capture.push(&input[..split]);
        capture.push(&input[split..]);
        capture.decode(&[], true);
        assert_eq!(capture.display("marker"), "猫\nx犬");
        assert_eq!(capture.total_bytes, input.len());
        assert_eq!(capture.decoded_bytes, input.len());
        assert_eq!(capture.current_line_bytes, 4);
        assert!(capture.pending_utf8.is_empty());
        assert!(capture.pi_truncation().is_none());
    }
}

#[test]
fn invalid_utf8_and_unfinished_codepoints_count_replacement_bytes() {
    let mut capture = StreamCapture::new(100, 10);
    capture.push(b"\xff\xe7");
    assert_eq!(capture.decoded_bytes, 3);
    capture.decode(&[], true);
    assert_eq!(capture.display("marker"), "��");
    assert_eq!(capture.total_bytes, 2);
    assert_eq!(capture.decoded_bytes, 6);
    assert_eq!(capture.current_line_bytes, 6);
    assert!(capture.pending_utf8.is_empty());
}

#[test]
fn exact_line_boundary_and_trailing_newline_do_not_truncate() {
    for input in [b"a\nb".as_slice(), b"a\nb\n"] {
        let mut capture = StreamCapture::new(100, 2);
        capture.push(input);
        assert!(!capture.truncated);
        assert_eq!(
            capture.total_newlines,
            input.iter().filter(|&&b| b == b'\n').count()
        );
        assert_eq!(capture.display("marker").as_bytes(), input);
    }
}

#[test]
fn exceeding_line_limit_retains_only_latest_complete_lines() {
    let mut capture = StreamCapture::new(100, 2);
    capture.push(b"a\nb\nc\n");
    assert_eq!(capture.display("marker:"), "marker:b\nc\n");
    let metadata = capture.pi_truncation().unwrap();
    assert_eq!(metadata["content"], "b\nc");
    assert_eq!(metadata["totalLines"], 3);
    assert_eq!(metadata["outputLines"], 2);
    assert_eq!(metadata["truncatedBy"], "lines");
    assert_eq!(metadata["lastLinePartial"], false);
}

#[test]
fn partial_multibyte_line_tail_starts_on_a_utf8_boundary() {
    let mut capture = StreamCapture::new(5, 10);
    capture.push("猫犬鳥".as_bytes());
    let metadata = capture.pi_truncation().unwrap();
    assert_eq!(metadata["content"], "鳥");
    assert_eq!(metadata["outputBytes"], 3);
    assert_eq!(metadata["totalBytes"], 9);
    assert_eq!(metadata["lastLinePartial"], true);
    assert!(!capture.display("").contains('�'));
}

#[test]
fn byte_limited_tail_discards_partial_first_line() {
    let mut capture = StreamCapture::new(8, 10);
    capture.push(b"123456\nabc\ndef");
    let metadata = capture.pi_truncation().unwrap();
    assert_eq!(metadata["truncatedBy"], "bytes");
    assert_eq!(metadata["content"], "abc\ndef");
    assert_eq!(metadata["outputLines"], 2);
    assert_eq!(metadata["outputBytes"], 7);
    assert_eq!(metadata["totalLines"], 3);
}

#[test]
fn full_capture_is_dropped_only_after_one_megabyte_and_tail_stays_bounded() {
    let mut capture = StreamCapture::new(16, 2);
    let chunk = vec![b'x'; 1024 * 1024];
    capture.push(&chunk);
    assert_eq!(capture.full.as_ref().unwrap().len(), chunk.len());
    capture.push(b"y");
    assert!(capture.full.is_none());
    assert_eq!(capture.total_bytes, chunk.len() + 1);
    assert!(capture.tail.len() <= 17);
    assert!(capture.display("").ends_with('y'));
}
