# rusty-bot-cli

Terminal host for `rusty-bot-engine-temporal`. Requires Rust 1.92+, the configured Temporal and PostgreSQL services, and credentials for the selected LLM provider.

```sh
# If the local credentials file is not already present:
cp crates/rusty-bot-cli/.env.example crates/rusty-bot-cli/.env
# Fill in the PostgreSQL password and the selected provider's key, then run:
cargo run -p rusty-bot-cli
# A named conversation can be reopened across launches using the same configuration:
cargo run -p rusty-bot-cli -- --session my-conversation
cargo run -p rusty-bot-cli -- --session my-conversation --prompt "Continue our discussion"
# Select a provider and optionally override its default model:
cargo run -p rusty-bot-cli -- --provider openai --model gpt-5.4-nano --session openai-chat
cargo run -p rusty-bot-cli -- --provider anthropic --prompt "Hello"
```

Enter one message per line. Replies stream by reading the injected PostgreSQL adapter every 250 ms. Completed messages provide the next turn's context. `/quit`, EOF and Ctrl-C gracefully close the current execution; the stored session remains resumable. Without `--session`, each launch generates a new application session ID (`rusty-bot-chat-` followed by a random UUID v4). The CLI prints this ID so you can reuse it later. `--session` creates the conversation if it does not exist, or restores it if it does with matching configuration. No session environment variable is needed. Tools are not implemented yet.

## Provider selection

| `--provider` | Endpoint | Default model | Credential |
| --- | --- | --- | --- |
| `router` (default) | `https://router.arc-dev.work/v1/chat/completions` | `cx/gpt-6-luna` | `NINEROUTER_API_KEY` |
| `openai` | `https://api.openai.com/v1/responses` | `gpt-5.4-nano` | `OPENAI_API_KEY` |
| `anthropic` | `https://api.anthropic.com/v1/messages` | `claude-haiku-4-5-20251001` | `ANTHROPIC_API_KEY` |
| `deepseek` | `https://api.deepseek.com/v1/chat/completions` | `deepseek-flash` | `DEEPSEEK_API_KEY` |

`--model <model>` overrides the selected provider's default. Model availability, capabilities, endpoint compatibility and access depend on the provider and account. Reasoning uses the provider adapter's default; there are no CLI reasoning flags. The router uses Chat Completions and accepts a stream ending at EOF after a finish reason, for router compatibility.

The saved session model and configuration are immutable. Reopen a session only with the same provider and model; the model label and scoped queue are saved, but provider identity is not independently stored. You cannot switch an existing session to a different model in place. For a provider change, start a new session; to carry history into it, use the SDK's `start_session_with_history` with compatible complete history. The CLI only creates fresh sessions or reopens matching configurations and has no history import. Provider adapters support strict subsets of rich content and may reject history incompatible with the new provider rather than silently discarding it.

The CLI uses `rusty-bot-chat-v3-rich-<provider>-<hex-UTF8-model>` queues; model names must be nonblank and at most 80 UTF-8 bytes. Each provider/model pair has a deterministic separate queue, preventing differently selected CLI workers from polling the same queue. Sessions created on earlier unscoped queues, including `rusty-bot-chat-v3-rich` and `rusty-bot-chat-v2`, cannot be reopened by this CLI: use a new session ID. This is not an automatic history migration.

Queue scoping does not identify credentials, accounts, endpoints, or other provider settings. Temporal can dispatch work to any worker polling a queue. Callers deploying workers for different accounts, endpoints, or incompatible settings must choose separate queues and isolate their configuration; the SDK leaves queue selection to its caller.

## Temporary application defaults

- Temporal `http://100.88.152.92:7233`, namespace `default`, task queue scoped by selected provider and model.
- Temporal UI `http://100.88.152.92:8233`.
- PostgreSQL `100.88.152.92:55433`, dedicated database `rusty_bot_sessions`; adapter schema is initialized explicitly.
- Five-minute between-turn idle timeout, no fixed Workflow lifetime.
- User-provided credentials live in the Git-ignored `crates/rusty-bot-cli/.env` or the environment, never in SDK defaults, Workflow inputs or printed logs. The CLI reads that exact file regardless of the working directory; existing environment variables take precedence. The file is runtime configuration, not embedded in the executable. `POSTGRES_PASSWORD` and the selected provider's key are required.

No local Temporal server is started. `--help` does not connect. Calls to the LLM are real and potentially billable. First-visible/completion timings and output byte counts are client-observed, not model token metrics. Provider retries are disabled, the provider operation timeout is 55 seconds, the Activity timeout is 115 seconds (including a 60-second engine margin), and the CLI waits up to 180 seconds for session close.

The CLI, not the engine SDK, chooses PostgreSQL and constructs its connection pool. Other applications may inject a different implementation of `rusty-bot-session::SessionStorage`. Conversation content exists in PostgreSQL and Temporal history and needs appropriate access controls.
