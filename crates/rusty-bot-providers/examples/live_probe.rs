//! Opt-in bounded live smoke probe. Credentials stay in a caller-owned JSON file.
//! Usage: cargo run -p rusty-bot-providers --example live_probe -- <keys.json> discover
//! Or replace `discover` with `generate <provider> <model> <default|off|on|adaptive>`.
use rusty_bot_messages::{ChatHistory, ConversationMessage, HistoryEntry, MessageAccumulator};
use rusty_bot_providers::ProviderCredential;
use rusty_bot_providers::{
    AnthropicProvider, ChatStreamTermination, DeepseekProvider, DiscoveryRequest,
    GenerationRequest, OpenAIProtocol, OpenAIProvider, OutputFormat, Provider, ProviderConfig,
    Reasoning, ReasoningEffort,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn provider(name: &str, model: &str, mode: &str, key: &str, structured: bool) -> Box<dyn Provider> {
    let endpoint = match name {
        "openai" => "https://api.openai.com/v1/responses",
        "openai-chat" => "https://api.openai.com/v1/chat/completions",
        "anthropic" => "https://api.anthropic.com/v1/messages",
        "deepseek" => "https://api.deepseek.com/chat/completions",
        "9router" => "https://router.arc-dev.work/v1/chat/completions",
        _ => panic!("unknown provider"),
    };
    let mut config = ProviderConfig::new(endpoint, model);
    config.timeout = Duration::from_secs(90);
    config.max_output_tokens = 2048;
    config.retry.retries = 0; // Keep paid smoke checks bounded; retries are tested offline.
    if structured {
        config.output_format = if name == "deepseek" {
            OutputFormat::JsonObject
        } else {
            OutputFormat::JsonSchema {
                name: "answer".into(),
                schema: serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
                strict: true,
            }
        };
    }
    if name == "9router" {
        config.chat_stream_termination = ChatStreamTermination::FinishReasonAndEof;
    }
    config.reasoning = match mode {
        "off" => Reasoning::Disabled,
        "on" if name == "anthropic" => Reasoning::Enabled {
            effort: None,
            budget_tokens: Some(1024),
        },
        "on" => Reasoning::Enabled {
            effort: Some(ReasoningEffort::Low),
            budget_tokens: None,
        },
        "adaptive" => Reasoning::Adaptive {
            effort: Some(ReasoningEffort::Low),
        },
        "default" => Reasoning::Default,
        _ => panic!("unknown reasoning mode"),
    };
    let key = ProviderCredential::new(key);
    match name {
        "openai" => Box::new(OpenAIProvider::with_api_key(config, key).unwrap()),
        "openai-chat" => Box::new(
            OpenAIProvider::with_protocol_and_api_key(config, OpenAIProtocol::ChatCompletions, key)
                .unwrap(),
        ),
        "anthropic" => Box::new(AnthropicProvider::with_api_key(config, key).unwrap()),
        "deepseek" => Box::new(DeepseekProvider::with_api_key(config, key).unwrap()),
        "9router" => Box::new(
            OpenAIProvider::with_protocol_and_api_key(config, OpenAIProtocol::ChatCompletions, key)
                .unwrap(),
        ),
        _ => unreachable!(),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() >= 3, "expected key-file and operation");
    let keys: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&args[1]).expect("read key file"))
            .expect("parse key file");
    if args[2] == "discover" {
        for name in ["openai", "anthropic", "deepseek", "9router"] {
            let p = provider(
                name,
                "discovery-only",
                "default",
                keys[name].as_str().expect("missing key"),
                false,
            );
            let now = Instant::now();
            match p
                .list_models(DiscoveryRequest::default(), CancellationToken::new())
                .await
            {
                Ok(page) => {
                    println!(
                        "{name} DISCOVERY OK count={} next_page={} elapsed_ms={}",
                        page.models.len(),
                        page.next_cursor.is_some(),
                        now.elapsed().as_millis()
                    );
                    for model in page.models.iter().take(100) {
                        println!(
                            "  {} input={:?} output={:?}",
                            model.id, model.max_input_tokens, model.max_output_tokens
                        );
                    }
                }
                Err(err) => println!("{name} DISCOVERY ERROR {err:?}"),
            }
        }
    } else if args[2] == "cross" {
        cross_provider_probe(&keys).await;
    } else {
        assert_eq!(args.len(), 6, "expected generate provider model mode");
        let name = &args[3];
        let p = provider(
            name,
            &args[4],
            &args[5],
            keys[if name == "openai-chat" {
                "openai"
            } else {
                name.as_str()
            }]
            .as_str()
            .expect("missing key"),
            args[2] == "structured",
        );
        let mut request = GenerationRequest {
            message_id: "live-answer".into(),
            history: ChatHistory {
                schema_version: 1,
                entries: vec![HistoryEntry::completed(
                    "live-user",
                    ConversationMessage::user("What is 17 + 25? Reply with only the number."),
                )],
            },
            tools: vec![],
        };
        if args[2] == "structured" {
            request.history.entries[0].message = ConversationMessage::user(
                "What is 17 + 25? Return a JSON object with integer field answer, for example {\"answer\":0}.",
            );
        }
        if args[2] == "image" {
            use base64::Engine;
            use rusty_bot_messages::{ContentBlock, MediaSource};
            // Deterministic vision fixture: two equally sized solid color panels.
            // The prompt does not disclose the colors, so success requires image input.
            let image = image::RgbImage::from_fn(256, 128, |x, _| {
                if x < 128 {
                    image::Rgb([255, 0, 0])
                } else {
                    image::Rgb([0, 0, 255])
                }
            });
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::ImageRgb8(image)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .unwrap();
            request.history.entries = vec![HistoryEntry::completed(
                "live-image-user",
                ConversationMessage::User { content: vec![
                    ContentBlock::text("Name the color of the left half and then the right half. Reply only LEFT_COLOR, RIGHT_COLOR."),
                    ContentBlock::Image { source: MediaSource::Inline {
                        mime_type: "image/png".into(),
                        base64: base64::engine::general_purpose::STANDARD.encode(bytes.into_inner()),
                    } },
                ] },
            )];
        }
        let (tx, mut rx) = mpsc::channel(32);
        let drain = tokio::spawn(async move {
            let mut accumulator = MessageAccumulator::default();
            let mut count = 0;
            while let Some(event) = rx.recv().await {
                accumulator
                    .apply(event)
                    .expect("valid provider event contract");
                count += 1;
            }
            (count, accumulator)
        });
        let now = Instant::now();
        let result = p.stream(&request, tx, CancellationToken::new()).await;
        let (events, accumulator) = drain.await.unwrap();
        match result {
            Ok(entry) => {
                assert_eq!(accumulator.response(), Some(&entry));
                let text: String = entry
                    .message
                    .content()
                    .iter()
                    .filter_map(|b| match b {
                        rusty_bot_messages::ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                if args[2] == "structured" {
                    // Probe assertion only; the provider returns text, not a parsed object.
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                        serde_json::json!({"answer":42})
                    );
                    println!("{name} STRUCTURED VERIFIED answer=42");
                }
                if args[2] == "image" {
                    let lower = text.to_ascii_lowercase();
                    assert!(
                        lower
                            .find("red")
                            .zip(lower.find("blue"))
                            .is_some_and(|(r, b)| r < b),
                        "expected red then blue, got {text:?}"
                    );
                    println!("{name} IMAGE VERIFIED red-left blue-right");
                }
                println!("{name} GENERATE OK model={} mode={} elapsed_ms={} events={} text={text:?} generation={:?}", args[4], args[5], now.elapsed().as_millis(), events, entry.generation);
                if args[2] == "replay" {
                    let mut next = request.clone();
                    next.message_id = "live-answer-2".into();
                    next.history.entries.push(entry);
                    next.history.entries.push(HistoryEntry::completed(
                        "live-user-2",
                        ConversationMessage::user(
                            "Add 1 to your previous answer. Reply with only the number.",
                        ),
                    ));
                    let (tx, mut rx) = mpsc::channel(32);
                    let drain = tokio::spawn(async move {
                        let mut acc = MessageAccumulator::default();
                        while let Some(event) = rx.recv().await {
                            acc.apply(event).unwrap();
                        }
                        acc
                    });
                    let result = p.stream(&next, tx, CancellationToken::new()).await;
                    let acc = drain.await.unwrap();
                    match result {
                        Ok(entry) => {
                            assert_eq!(acc.response(), Some(&entry));
                            let text: String = entry
                                .message
                                .content()
                                .iter()
                                .filter_map(|b| match b {
                                    rusty_bot_messages::ContentBlock::Text { text } => {
                                        Some(text.as_str())
                                    }
                                    _ => None,
                                })
                                .collect();
                            println!("{name} REPLAY OK text={text:?}");
                        }
                        Err(err) => println!("{name} REPLAY ERROR {err:?}"),
                    }
                }
            }
            Err(err) => println!(
                "{name} GENERATE ERROR model={} mode={} elapsed_ms={} events={} error={err:?}",
                args[4],
                args[5],
                now.elapsed().as_millis(),
                events
            ),
        }
    }
}

/// Carries generic history through all three vendors; no provider-native state is rewritten.
async fn cross_provider_probe(keys: &serde_json::Value) {
    let mut history = ChatHistory::default();
    for (index, (name, model, mode)) in [
        ("anthropic", "claude-haiku-4-5-20251001", "on"),
        ("openai", "gpt-5.4-nano", "off"),
        ("deepseek", "deepseek-flash", "on"),
        ("anthropic", "claude-haiku-4-5-20251001", "off"),
    ]
    .into_iter()
    .enumerate()
    {
        history.entries.push(HistoryEntry::completed(
            format!("cross-user-{index}"),
            ConversationMessage::user(if index == 0 {
                "Remember the codeword cedar. Reply only cedar."
            } else {
                "What codeword did I ask you to remember? Reply only the codeword."
            }),
        ));
        let p = provider(name, model, mode, keys[name].as_str().unwrap(), false);
        let req = GenerationRequest {
            message_id: format!("cross-answer-{index}"),
            history: history.clone(),
            tools: vec![],
        };
        let (tx, mut rx) = mpsc::channel(32);
        let drain = tokio::spawn(async move {
            let mut accumulator = MessageAccumulator::default();
            while let Some(event) = rx.recv().await {
                accumulator.apply(event).unwrap();
            }
            accumulator
        });
        let entry = p.stream(&req, tx, CancellationToken::new()).await;
        let accumulator = drain.await.unwrap();
        let entry = entry
            .unwrap_or_else(|error| panic!("{name} cross-provider generation failed: {error:?}"));
        assert_eq!(accumulator.response(), Some(&entry));
        let text: String = entry
            .message
            .content()
            .iter()
            .filter_map(|block| match block {
                rusty_bot_messages::ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            text.trim().trim_end_matches('.').to_ascii_lowercase(),
            "cedar"
        );
        println!("{name} CROSS VERIFIED turn={} codeword=cedar", index + 1);
        history.entries.push(entry);
    }
}
