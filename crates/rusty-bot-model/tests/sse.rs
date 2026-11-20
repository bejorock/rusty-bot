use rusty_bot_model::SseParser;

#[test]
fn parser_handles_arbitrary_byte_splits_and_utf8() {
    let wire = "data: {\"text\":\"你好\"}\r\n\r\ndata: [DONE]\n\n";
    for width in 1..=wire.len() {
        let mut parser = SseParser::new();
        let events: Vec<_> = wire
            .as_bytes()
            .chunks(width)
            .flat_map(|chunk| parser.push(chunk))
            .collect();
        assert_eq!(events, ["{\"text\":\"你好\"}", "[DONE]"]);
        assert_eq!(parser.finish(), None);
    }
}

#[test]
fn parser_joins_multiline_data_and_ignores_metadata() {
    let mut parser = SseParser::new();
    assert_eq!(
        parser
            .push(b": heartbeat\nevent: message\nid: 1\nretry: 1000\ndata: first\ndata:second\n\n"),
        ["first\nsecond"]
    );
    assert!(parser.push(b": another heartbeat\n\n").is_empty());
}

#[test]
fn parser_flushes_unterminated_final_line_exactly_once() {
    let mut parser = SseParser::new();
    assert!(parser.push(b"data: final").is_empty());
    assert_eq!(parser.finish().as_deref(), Some("final"));
    assert_eq!(parser.finish(), None);
}

#[test]
fn provider_configuration_debug_does_not_leak_api_key() {
    use rusty_bot_model::{OpenAiCompatConfig, OpenAiCompatProvider};
    let mut config = OpenAiCompatConfig::new(
        "https://example.invalid/v1/chat/completions"
            .parse()
            .unwrap(),
        "test",
    );
    config.api_key = Some(secrecy::SecretString::new("private-test-credential".into()));
    assert!(!format!("{config:?}").contains("private-test-credential"));
    let provider = OpenAiCompatProvider::new(config).unwrap();
    assert!(!format!("{provider:?}").contains("private-test-credential"));
}
