use super::*;
use serde_json::json;

fn record(input: &[u8]) -> Record {
    let mut record = Record::new();
    for &byte in input {
        record.byte(byte).unwrap();
    }
    record
}

#[test]
fn text_records_preserve_escaped_quotes_and_backslashes() {
    let input = br#"{"type":"match","data":{"lines":{"text":"a\"b\\c\n"}}}"#;
    let record = record(input);
    assert_eq!(record.bytes, input);
    assert!(!record.clipped);
    assert!(!record.in_line);
    let value: Value = serde_json::from_slice(&record.bytes).unwrap();
    assert_eq!(decode(&value["data"]["lines"]), "a\"b\\c\n");
}

#[test]
fn submatches_are_discarded_without_retaining_large_payloads() {
    let mut input = br#"{"type":"match","data":{"lines":{"text":"a"},"submatches":"#.to_vec();
    input.extend(std::iter::repeat_n(b'x', RECORD_CAP * 2));
    let result = record(&input);
    assert!(result.discard);
    assert!(result.bytes.len() < 100);
    let value: Value = serde_json::from_slice(&result.bytes).unwrap();
    assert_eq!(value["data"]["lines"]["text"], "a");
    assert!(value["data"].get("submatches").is_none());
}

#[test]
fn long_multibyte_lines_clip_at_a_valid_json_and_utf8_boundary() {
    let input = format!(
        "{{\"data\":{{\"lines\":{{\"text\":\"{}\"}}}}}}",
        "猫".repeat(TEXT_PREFIX)
    );
    let result = record(input.as_bytes());
    assert!(result.clipped);
    let value: Value = serde_json::from_slice(&result.bytes).unwrap();
    let prefix = value["data"]["lines"]["text"].as_str().unwrap();
    assert!(prefix.len() <= TEXT_PREFIX);
    assert!(prefix.len() >= TEXT_PREFIX - 3);
    assert!("猫".repeat(TEXT_PREFIX).starts_with(prefix));
}

#[test]
fn clipping_repairs_incomplete_unicode_escape_sequences() {
    let input = format!(
        "{{\"lines\":{{\"text\":\"{}\\u732brest\"}}}}",
        "a".repeat(TEXT_PREFIX - 2)
    );
    let result = record(input.as_bytes());
    assert!(result.clipped);
    let value: Value = serde_json::from_slice(&result.bytes).unwrap();
    assert_eq!(value["lines"]["text"], "a".repeat(TEXT_PREFIX - 2));
}

#[test]
fn oversized_non_line_records_are_refused_at_safety_boundary() {
    let mut record = Record::new();
    for _ in 0..RECORD_CAP {
        record.byte(b'x').unwrap();
    }
    assert_eq!(
        record.byte(b'x'),
        Err("ripgrep JSON record exceeds safety maximum")
    );
}

#[test]
fn decoding_prefers_text_and_handles_base64_prefixes_and_invalid_utf8() {
    assert_eq!(decode(&json!({"text":"猫","bytes":"YQ=="})), "猫");
    assert_eq!(decode(&json!({"bytes":"YQ==x"})), "a");
    assert_eq!(decode(&json!({"bytes":"/w=="})), "\u{fffd}");
    for value in [json!({"bytes":"!!!!"}), json!({"text":3}), Value::Null] {
        assert_eq!(decode(&value), "");
    }
}
