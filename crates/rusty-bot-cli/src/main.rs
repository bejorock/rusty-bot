//! Initial terminal host for the Temporal-only SDK. No tool execution yet.
use rusty_bot_engine_temporal::{
    ChatMessage, Engine, EngineOptions, EngineResult, Session, TurnResult,
};
use rusty_bot_model::{OpenAiCompatConfig, OpenAiCompatProvider};
use std::{
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};

const TEMPORAL: &str = "http://100.88.152.92:7233";
const ENDPOINT: &str = "https://router.arc-dev.work/v1/chat/completions";
const MODEL: &str = "cx/gpt-6-luna";
// Explicitly retained temporary test credential at the user's request.
const TEST_API_KEY: &str = "REMOVED_ROUTER_API_KEY";

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
}

const USAGE: &str = "usage: rusty-bot-cli [--session <id>] [--prompt \"message\"] [--help]";

fn parse_args(args: Vec<String>) -> EngineResult<Args> {
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        return Ok(Args {
            mode: Mode::Help,
            session_id: None,
        });
    }
    let mut args = args.into_iter();
    let mut session_id = None;
    let mut prompt = None;
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--session" if session_id.is_none() => &mut session_id,
            "--prompt" if prompt.is_none() => &mut prompt,
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
    } = parse_args(std::env::args().skip(1).collect())?;
    if mode == Mode::Help {
        println!(
            "{USAGE}\nNo flags: interactive text chat with a new UUID-based session ID. /quit or EOF closes it.\nUse --session <id> to create or reopen a named conversation; combine with --prompt for one turn.\nTest defaults: Temporal {TEMPORAL} (default namespace), model {MODEL}; PostgreSQL: rusty_bot_sessions.\nThis makes real LLM requests; transcripts are stored in PostgreSQL and Temporal history."
        );
        return Ok(());
    }
    let mut config = OpenAiCompatConfig::new(url::Url::parse(ENDPOINT)?, MODEL);
    config.api_key = Some(secrecy::SecretString::new(TEST_API_KEY.into()));
    config.max_retries = 0;
    config.request_timeout = Duration::from_secs(55);
    // Caller configuration, never part of SDK defaults or Temporal payloads.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with(
            sqlx::postgres::PgConnectOptions::new()
                .host("100.88.152.92")
                .port(55433)
                .username("postgres")
                .password("REMOVED_POSTGRES_PASSWORD")
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
            task_queue: "rusty-bot-chat-v2".into(),
            idle_timeout: Duration::from_secs(300),
        },
        Arc::new(OpenAiCompatProvider::new(config)?),
        storage,
    )
    .await?;
    let mut outcome: EngineResult<()> = Err("worker stopped unexpectedly".into());
    engine.run_until(async {
        outcome = async {
            let session = engine.start_session(id, MODEL.into()).await?;
            eprintln!("Session: {}\nTemporal: {TEMPORAL}; UI: http://100.88.152.92:8233\nModel: {MODEL}; Activity timeout: 60s", session.id());
            let result = tokio::select! {
                result = chat(&session, mode) => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    eprintln!("\nClosing session; an active turn may take up to its timeout to settle.");
                    Ok(())
                }
            };
            let closed = tokio::time::timeout(Duration::from_secs(75), session.close()).await;
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
                session_id: None
            }
        );
        for help in ["--help", "-h"] {
            assert_eq!(parse(&[help]).unwrap().mode, Mode::Help);
        }
        assert_eq!(
            parse(&["--prompt", "hello"]).unwrap(),
            Args {
                mode: Mode::Prompt("hello".into()),
                session_id: None
            }
        );
    }

    #[test]
    fn session_flag_works_for_chat_and_both_prompt_orders() {
        assert_eq!(
            parse(&["--session", "my-chat"]).unwrap(),
            Args {
                mode: Mode::Chat,
                session_id: Some("my-chat".into())
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
                    session_id: Some("my-chat".into())
                }
            );
        }
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
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
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
