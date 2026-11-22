//! Initial terminal host for the Temporal-only SDK. No tool execution yet.
use rusty_bot_core::ProviderCredential;
use rusty_bot_engine_temporal::{
    ChatMessage, Engine, EngineOptions, EngineResult, Session, TurnResult,
};
use rusty_bot_providers::{
    AnthropicProvider, ChatStreamTermination, DeepseekProvider, OpenAIProtocol, OpenAIProvider,
    Provider, ProviderConfig,
};
use std::{
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};

const TEMPORAL: &str = "http://100.88.152.92:7233";
const TASK_QUEUE: &str = "rusty-bot-chat-v3-rich";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ProviderChoice {
    OpenAi,
    Anthropic,
    Deepseek,
    #[default]
    Router,
}

impl ProviderChoice {
    fn queue_label(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Deepseek => "deepseek",
            Self::Router => "router",
        }
    }

    fn parse(value: &str) -> EngineResult<Self> {
        match value {
            "openai" => Ok(Self::OpenAi),
            "anthropic" => Ok(Self::Anthropic),
            "deepseek" => Ok(Self::Deepseek),
            "router" => Ok(Self::Router),
            _ => Err(USAGE.into()),
        }
    }

    fn settings(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::OpenAi => (
                "https://api.openai.com/v1/responses",
                "gpt-5.4-nano",
                "OPENAI_API_KEY",
            ),
            Self::Anthropic => (
                "https://api.anthropic.com/v1/messages",
                "claude-haiku-4-5-20251001",
                "ANTHROPIC_API_KEY",
            ),
            Self::Deepseek => (
                "https://api.deepseek.com/v1/chat/completions",
                "deepseek-flash",
                "DEEPSEEK_API_KEY",
            ),
            Self::Router => (
                "https://router.arc-dev.work/v1/chat/completions",
                "cx/gpt-6-luna",
                "NINEROUTER_API_KEY",
            ),
        }
    }

    fn build(self, model: &str, key: String) -> EngineResult<Arc<dyn Provider>> {
        let mut config = ProviderConfig::new(self.settings().0, model);
        config.max_output_tokens = 4096;
        config.timeout = Duration::from_secs(55);
        config.retry.retries = 0;
        if self == Self::Router {
            config.chat_stream_termination = ChatStreamTermination::FinishReasonAndEof;
        }
        let credential = ProviderCredential::new(key);
        Ok(match self {
            Self::OpenAi => Arc::new(OpenAIProvider::with_api_key(config, credential)?),
            Self::Anthropic => Arc::new(AnthropicProvider::with_api_key(config, credential)?),
            Self::Deepseek => Arc::new(DeepseekProvider::with_api_key(config, credential)?),
            Self::Router => Arc::new(OpenAIProvider::with_protocol_and_api_key(
                config,
                OpenAIProtocol::ChatCompletions,
                credential,
            )?),
        })
    }
}

// Encode UTF-8 bytes directly: stable across Rust versions and collision-free.
fn task_queue(provider: ProviderChoice, model: &str) -> EngineResult<String> {
    if model.trim().is_empty() || model.len() > 80 {
        return Err("model must be nonblank and at most 80 UTF-8 bytes".into());
    }
    use std::fmt::Write as _;
    let mut queue = format!("{TASK_QUEUE}-{}-", provider.queue_label());
    for byte in model.as_bytes() {
        write!(&mut queue, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(queue)
}

// Parse without mutating process environment (the Tokio runtime is multithreaded).
fn local_credentials() -> EngineResult<std::collections::BTreeMap<String, String>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    match dotenvy::from_path_iter(path) {
        Ok(entries) => entries
            .collect::<Result<_, _>>()
            .map_err(|_| "invalid CLI .env file (values redacted)".into()),
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            Ok(Default::default())
        }
        Err(_) => Err("cannot read CLI .env file (values redacted)".into()),
    }
}

fn credential(
    local: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> EngineResult<String> {
    let value = match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => local.get(name).cloned(),
        Err(_) => return Err(format!("invalid {name} (value redacted)").into()),
    };
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("set {name} in the environment or crates/rusty-bot-cli/.env").into())
}

#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Chat,
    Prompt(String),
    Help,
}

#[derive(Debug, PartialEq, Eq)]
struct Args {
    mode: Mode,
    session_id: Option<String>,
    provider: ProviderChoice,
    model: Option<String>,
}

const USAGE: &str = "usage: rusty-bot-cli [--session <id>] [--prompt \"message\"] [--provider openai|anthropic|deepseek|router] [--model <model>] [--help]";

fn parse_args(args: Vec<String>) -> EngineResult<Args> {
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        return Ok(Args {
            mode: Mode::Help,
            session_id: None,
            provider: ProviderChoice::default(),
            model: None,
        });
    }
    let mut args = args.into_iter();
    let mut session_id = None;
    let mut prompt = None;
    let mut provider = None;
    let mut model = None;
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--session" if session_id.is_none() => &mut session_id,
            "--prompt" if prompt.is_none() => &mut prompt,
            "--provider" if provider.is_none() => &mut provider,
            "--model" if model.is_none() => &mut model,
            _ => return Err(USAGE.into()),
        };
        let value = args.next().ok_or(USAGE)?;
        if value.trim().is_empty() || value.starts_with("--") {
            return Err(USAGE.into());
        }
        *slot = Some(value);
    }
    Ok(Args {
        mode: prompt.map_or(Mode::Chat, Mode::Prompt),
        session_id,
        provider: provider
            .as_deref()
            .map(ProviderChoice::parse)
            .transpose()?
            .unwrap_or_default(),
        model,
    })
}

fn session_id(explicit: Option<String>) -> String {
    explicit.unwrap_or_else(|| format!("rusty-bot-chat-{}", uuid::Uuid::new_v4()))
}

fn print_text(previous: &mut String, next: &str) -> io::Result<()> {
    if let Some(suffix) = next.strip_prefix(previous.as_str()) {
        print!("{suffix}");
    } else {
        print!("\n[reply replaced]\n{next}");
    }
    io::stdout().flush()?;
    previous.clear();
    previous.push_str(next);
    Ok(())
}

async fn turn(session: &Session, text: String) -> EngineResult<()> {
    let started = Instant::now();
    let update = session.send_message(text);
    tokio::pin!(update);
    let mut poll = tokio::time::interval(Duration::from_millis(250));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut visible = String::new();
    let mut first = None;
    loop {
        tokio::select! {
            result = &mut update => {
                match result? {
                    TurnResult::Completed(reply) => {
                        let elapsed = started.elapsed();
                        if first.is_none() && !reply.is_empty() { first = Some(elapsed); }
                        print_text(&mut visible, &reply)?;
                        println!();
                        eprintln!("[first visible {:.2}s; complete {:.2}s; {} bytes]", first.unwrap_or(elapsed).as_secs_f64(), elapsed.as_secs_f64(), reply.len());
                        return Ok(());
                    }
                    TurnResult::ModelFailed(error) => return Err(format!("model failed: {error}").into()),
                    TurnResult::Rejected(reason) => return Err(format!("turn rejected: {reason}").into()),
                    TurnResult::RollingOver => unreachable!("Session follows rollover internally"),
                }
            }
            _ = poll.tick() => {
                let snapshot = session.snapshot().await?;
                if snapshot.generating
                    && let Some(message) = snapshot.messages.last()
                    && let ChatMessage::Assistant(text) = &message.message
                    && !text.is_empty() && *text != visible
                {
                    first.get_or_insert_with(|| started.elapsed());
                    print_text(&mut visible, text)?;
                }
            }
        }
    }
}

async fn chat(session: &Session, mode: Mode) -> EngineResult<()> {
    if let Mode::Prompt(text) = mode {
        return turn(session, text).await;
    }
    eprintln!("Text-only chat. Enter a message; /quit or EOF closes the session. Ctrl-C exits.");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        print!("\nyou> ");
        io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else {
            break;
        };
        if line.trim() == "/quit" {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        print!("assistant> ");
        io::stdout().flush()?;
        // End on infrastructure/model failure rather than admitting another turn while
        // a failed client call might still have a running server-side Update.
        turn(session, line).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> EngineResult<()> {
    let Args {
        mode,
        session_id: requested_id,
        provider: provider_choice,
        model,
    } = parse_args(std::env::args().skip(1).collect())?;
    if mode == Mode::Help {
        println!(
            "{USAGE}\nNo flags: interactive text chat with a new UUID-based session ID. /quit or EOF closes it.\nUse --session <id> to create or reopen a named conversation; combine with --prompt for one turn.\nDefault provider: router (cx/gpt-6-luna). Select openai, anthropic, deepseek or router with --provider; override the provider default with --model.\nTemporal {TEMPORAL} (default namespace), queue {TASK_QUEUE}-<provider>-<hex-model>; PostgreSQL: rusty_bot_sessions. Models are limited to 80 UTF-8 bytes.\nThis makes real LLM requests; transcripts are stored in PostgreSQL and Temporal history."
        );
        return Ok(());
    }
    let (_, default_model, key_name) = provider_choice.settings();
    let model = model.unwrap_or_else(|| default_model.into());
    let queue = task_queue(provider_choice, &model)?;
    let credentials = local_credentials()?;
    let provider = provider_choice.build(&model, credential(&credentials, key_name)?)?;
    let postgres_password = credential(&credentials, "POSTGRES_PASSWORD")?;
    // Caller configuration, never part of SDK defaults or Temporal payloads.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with(
            sqlx::postgres::PgConnectOptions::new()
                .host("100.88.152.92")
                .port(55433)
                .username("postgres")
                .password(&postgres_password)
                .database("rusty_bot_sessions"),
        )
        .await?;
    let storage = Arc::new(rusty_bot_postgres::PgSessionStorage::new(pool));
    storage.migrate().await?;
    let id = session_id(requested_id);
    let engine = Engine::connect(
        EngineOptions {
            address: TEMPORAL.into(),
            namespace: "default".into(),
            task_queue: queue,
            idle_timeout: Duration::from_secs(300),
        },
        provider,
        storage,
    )
    .await?;
    let mut outcome: EngineResult<()> = Err("worker stopped unexpectedly".into());
    engine.run_until(async {
        outcome = async {
            let session = engine.start_session(id, model.clone()).await?;
            eprintln!("Session: {}\nTemporal: {TEMPORAL}; UI: http://100.88.152.92:8233\nProvider: {provider_choice:?}; model: {model}; Activity timeout: 115s", session.id());
            let result = tokio::select! {
                result = chat(&session, mode) => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    eprintln!("\nClosing session; an active turn may take up to its timeout to settle.");
                    Ok(())
                }
            };
            let closed = tokio::time::timeout(Duration::from_secs(180), session.close()).await;
            match closed {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => { if result.is_ok() { return Err(error); } eprintln!("Session close failed: {error}"); }
                Err(_) => { if result.is_ok() { return Err("session close timed out".into()); } eprintln!("Session close timed out; the between-turn idle timer remains active."); }
            }
            result
        }.await;
    }).await?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(args: &[&str]) -> EngineResult<Args> {
        parse_args(args.iter().map(|arg| (*arg).into()).collect())
    }

    #[test]
    fn bounded_cli_modes() {
        assert_eq!(
            parse(&[]).unwrap(),
            Args {
                mode: Mode::Chat,
                session_id: None,
                provider: ProviderChoice::Router,
                model: None
            }
        );
        for help in ["--help", "-h"] {
            assert_eq!(parse(&[help]).unwrap().mode, Mode::Help);
        }
        assert_eq!(
            parse(&["--prompt", "hello"]).unwrap(),
            Args {
                mode: Mode::Prompt("hello".into()),
                session_id: None,
                provider: ProviderChoice::Router,
                model: None
            }
        );
    }

    #[test]
    fn session_flag_works_for_chat_and_both_prompt_orders() {
        assert_eq!(
            parse(&["--session", "my-chat"]).unwrap(),
            Args {
                mode: Mode::Chat,
                session_id: Some("my-chat".into()),
                provider: ProviderChoice::Router,
                model: None
            }
        );
        for args in [
            ["--session", "my-chat", "--prompt", "hello"],
            ["--prompt", "hello", "--session", "my-chat"],
        ] {
            assert_eq!(
                parse(&args).unwrap(),
                Args {
                    mode: Mode::Prompt("hello".into()),
                    session_id: Some("my-chat".into()),
                    provider: ProviderChoice::Router,
                    model: None
                }
            );
        }
    }

    #[test]
    fn provider_and_model_selection() {
        for (name, choice, endpoint, default_model, key_name) in [
            (
                "openai",
                ProviderChoice::OpenAi,
                "https://api.openai.com/v1/responses",
                "gpt-5.4-nano",
                "OPENAI_API_KEY",
            ),
            (
                "anthropic",
                ProviderChoice::Anthropic,
                "https://api.anthropic.com/v1/messages",
                "claude-haiku-4-5-20251001",
                "ANTHROPIC_API_KEY",
            ),
            (
                "deepseek",
                ProviderChoice::Deepseek,
                "https://api.deepseek.com/v1/chat/completions",
                "deepseek-flash",
                "DEEPSEEK_API_KEY",
            ),
            (
                "router",
                ProviderChoice::Router,
                "https://router.arc-dev.work/v1/chat/completions",
                "cx/gpt-6-luna",
                "NINEROUTER_API_KEY",
            ),
        ] {
            let args = parse(&[
                "--model",
                "custom-model",
                "--provider",
                name,
                "--prompt",
                "hello",
            ])
            .unwrap();
            assert_eq!(args.provider, choice);
            assert_eq!(args.model.as_deref(), Some("custom-model"));
            assert_eq!(choice.settings(), (endpoint, default_model, key_name));
            assert!(choice.build(default_model, "test-only-key".into()).is_ok());
        }
        let defaults = parse(&[]).unwrap();
        assert_eq!(defaults.provider.settings().1, "cx/gpt-6-luna");
        assert_eq!(defaults.model, None);
        assert_eq!(
            parse(&["--model", "custom"]).unwrap().provider,
            ProviderChoice::Router
        );
        assert!(USAGE.contains("--provider openai|anthropic|deepseek|router"));
        assert!(USAGE.contains("--model <model>"));
    }

    #[test]
    fn rejects_missing_blank_duplicate_and_unknown_arguments() {
        for args in [
            vec!["--prompt"],
            vec!["--prompt", " "],
            vec!["--session"],
            vec!["--session", ""],
            vec!["--unknown"],
            vec!["my-chat"],
            vec!["--session", "--prompt", "hello"],
            vec!["--session", "a", "--session", "b"],
            vec!["--prompt", "a", "--prompt", "b"],
            vec!["--provider"],
            vec!["--provider", ""],
            vec!["--provider", "bogus"],
            vec!["--provider", "router", "--provider", "openai"],
            vec!["--model"],
            vec!["--model", "  "],
            vec!["--model", "--provider", "router"],
            vec!["--model", "a", "--model", "b"],
            vec!["--help", "--provider", "router"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
    }

    #[test]
    fn queues_are_stable_scoped_and_bounded() {
        assert_eq!(
            task_queue(ProviderChoice::Router, "cx/é").unwrap(),
            "rusty-bot-chat-v3-rich-router-63782fc3a9"
        );
        assert_ne!(
            task_queue(ProviderChoice::Router, "a").unwrap(),
            task_queue(ProviderChoice::OpenAi, "a").unwrap()
        );
        assert_ne!(
            task_queue(ProviderChoice::Router, "a").unwrap(),
            task_queue(ProviderChoice::Router, "b").unwrap()
        );
        let longest = task_queue(ProviderChoice::Anthropic, &"x".repeat(80)).unwrap();
        assert!(longest.len() <= 200);
        assert!(
            longest
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        );
        assert!(task_queue(ProviderChoice::Router, &"x".repeat(81)).is_err());
        assert!(task_queue(ProviderChoice::Router, &"é".repeat(41)).is_err());
        assert!(task_queue(ProviderChoice::Router, " ").is_err());
    }

    #[test]
    fn local_credentials_require_nonblank_values_without_printing_them() {
        let name = "RUSTY_BOT_CLI_TEST_UNUSED_CREDENTIAL_A30782";
        let mut local = std::collections::BTreeMap::new();
        assert!(credential(&local, name).is_err());
        local.insert(name.into(), "  ".into());
        assert!(credential(&local, name).is_err());
        local.insert(name.into(), "test-only-secret".into());
        assert_eq!(credential(&local, name).unwrap(), "test-only-secret");
    }

    #[test]
    fn explicit_id_is_preserved_and_default_is_random_uuid_v4() {
        assert_eq!(session_id(Some("my-chat".into())), "my-chat");
        let first = session_id(None);
        let second = session_id(None);
        assert_ne!(first, second);
        let uuid = uuid::Uuid::parse_str(first.strip_prefix("rusty-bot-chat-").unwrap()).unwrap();
        assert_eq!(uuid.get_version_num(), 4);
    }
}
