# Rusty Bot

An embeddable AI coding-agent SDK for Rust. Connect a model to a workspace and let the SDK handle model requests, tools, session history, and progress updates.

This project provides libraries, not a CLI or web application.

## Requirements

- A current stable Rust toolchain.
- Linux or macOS for the local workspace adapter.
- A streaming, tool-calling model endpoint and any required credentials.
- `rg` (ripgrep) and `fd` on `PATH` for the local search tools.

> **The local adapter is not a sandbox.** Commands run with your application's privileges, and absolute paths outside the workspace are permitted. Use trusted inputs or provide your own isolation.

## Quick start

Create a host application and add these dependencies. Adjust the paths to this checkout:

```toml
[dependencies]
rusty-bot = { path = "../erica-rusty-bot/crates/rusty-bot" }
rusty-bot-model = { path = "../erica-rusty-bot/crates/rusty-bot-model" }
secrecy = "0.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Add this to `src/main.rs`. Replace the endpoint, model, workspace path, and credential with your application's configuration. Use the full Chat Completions URL; leave `api_key` as `None` if authentication is unnecessary.

```rust
use std::sync::Arc;
use rusty_bot::{
    Bot, ExecutionSelection, LocalWorkspaceProvider, ModelProvider, ModelSelection,
    SessionConfig, ToolFilter, WorkspaceKey, WorkspaceProvider, WorkspaceSpec,
};
use rusty_bot_model::{OpenAiCompatConfig, OpenAiCompatProvider};
use secrecy::SecretString;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::fs::canonicalize("/absolute/path/to/project")?;
    let model_id = "your-tool-capable-model".to_owned();
    let mut model = OpenAiCompatConfig::new(
        "https://api.openai.com/v1/chat/completions".parse()?,
        model_id.clone(),
    );
    // Obtain this through your application's credential mechanism.
    // Never commit real credentials.
    model.api_key = Some(SecretString::new("your-api-key".into()));
    let provider = Arc::new(OpenAiCompatProvider::new(model)?);
    let provider_id = provider.id().to_owned();
    let bot = Bot::builder(provider).build()?;

    let workspaces = LocalWorkspaceProvider::new(root.clone())?;
    let key = WorkspaceKey::new("demo");
    let workspace = workspaces.open(WorkspaceSpec {
        key: key.clone(),
        root: Some(root),
        execution: ExecutionSelection::None,
    }).await?;
    let session = bot.create_session(workspace, SessionConfig {
        model: ModelSelection { provider: provider_id, model_id },
        tools: ToolFilter::Only(vec![
            "read".into(), "ls".into(), "grep".into(), "find".into(),
        ]),
        ..SessionConfig::default()
    }).await?;
    println!("Save this session ID to reopen it: {:?}", session.id());

    let outcome = async {
        let mut run = session.prompt(
            "unique-request-id".into(),
            "Explain what this project does.".into(),
        ).await?;
        while let Some(frame) = run.next_event().await? {
            println!("{:?}: {}", frame.kind, frame.data);
        }
        run.wait().await
    }.await;

    let shutdown = session.shutdown().await;
    let release = workspaces.release(&key).await;
    let result = outcome?;
    shutdown?;
    release?;
    println!("Submission: {:?}; status: {:?}", result.submission, result.status);
    Ok(())
}
```

Run with `cargo run`. This makes real model requests and may incur charges. The example enables only reading/search tools, not file writes or shell commands; it is still not filesystem isolation.

## Sessions and results

- Use a unique request ID for each new prompt. Reuse it only to retry the same logical submission.
- `run.next_event().await?` reads persisted progress. Progress is provisional; read `session.entries(cursor, limit)` for committed conversation content. Start with `None` and follow each page's `next` cursor.
- `run.wait().await?` returns submission metadata and status, not a final-answer string. Check both `submission` and `status`.
- Use `prompt_content` for structured content such as images; image support requires explicit provider configuration.
- Call `run.cancel()` and then `wait()` to cancel and drain a run. Dropping an unfinished handle requests cancellation but does not wait for cleanup.
- Serialize operations within a session; overlapping use returns `SdkError::Busy`.
- Reopen a saved session with `bot.resume_session(session_id, workspace).await?`, using the same workspace and compatible provider/extensions.
- Call `session.shutdown().await` before releasing the workspace.

## Allow writes and shell commands

The built-in tools are `read`, `write`, `edit`, `ls`, `grep`, `find`, and `bash`.

To enable coding actions, include `write`, `edit`, and `bash` in the tool allowlist, and open the workspace with `ExecutionSelection::Local { env_id: None, cwd: Some(root.clone()) }`. `ToolFilter::All` exposes all registered tools.

Tool calls in a model round run concurrently, including writes and shell commands. Hosts and custom tools must synchronize conflicting effects. Model instructions are guidance, not an authorization boundary.

## Storage

By default, sessions are stored in `.rusty-bot/sessions.jsonl`, relative to the host process's current directory. Keep that location stable across restarts. Only one adapter may open the journal at a time; share it within the process. The journal contains sensitive prompts, source code, and tool output, and has no automatic retention policy.

Choose a different location with an existing parent directory:

```rust
let adapter = Arc::new(rusty_bot::FileSessionAdapter::open(journal_path)?);
let bot = Bot::builder(provider).session_adapter(adapter).build()?;
```

For ephemeral sessions, use `Arc::new(rusty_bot::MemoryStorage::new())`. Custom `SessionAdapter` implementations must support both transactional storage and progress-log operations. The optional `rusty-bot-postgres` storage implementation does not yet support the progress log and cannot be injected directly for SDK runs.

## Providers and customization

The `rusty-bot-model` crate provides `OpenAiCompatProvider`, `OpenAiResponsesProvider`, `AnthropicMessagesProvider`, and `GeminiGenerateContentProvider`. Their constructors and endpoint requirements differ; see their [API source](crates/rusty-bot-model/src).

Configure `Bot::builder(provider)` with:

- `.tool(Arc<dyn Tool>)` for custom tools.
- `.extension(Arc<dyn Extension>)` for tools, prompt sections, and lifecycle hooks.
- `.session_adapter(Arc<dyn SessionAdapter>)` for another session backend.
- `.compactor(Arc<dyn RequestCompactor>)` for opt-in context compaction.

Custom tools implement `Tool::spec` and async `Tool::execute`. Replay is unsafe by default; opt into safe replay only when repeating the action is genuinely safe.

## Development

```bash
cargo build --workspace --locked
cargo test --workspace --lib --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```
