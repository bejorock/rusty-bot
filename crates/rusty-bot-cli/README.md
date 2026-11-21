# rusty-bot-cli

Terminal host for `rusty-bot-engine-temporal`. Requires Rust 1.92+, the existing Temporal service, PostgreSQL and LLM router.

```sh
cargo run -p rusty-bot-cli
# A named conversation can be reopened across launches:
cargo run -p rusty-bot-cli -- --session my-conversation
cargo run -p rusty-bot-cli -- --session my-conversation --prompt "Continue our discussion"
```

Enter one message per line. Replies stream by reading the injected PostgreSQL adapter every 250 ms. Completed messages provide the next turn's context. `/quit`, EOF and Ctrl-C gracefully close the current execution; the stored session remains resumable. Without `--session`, each launch generates a new application session ID (`rusty-bot-chat-` followed by a random UUID v4). The CLI prints this ID so you can reuse it later. `--session` creates the conversation if it does not exist, or restores it if it does. No session environment variable is needed. Tools are not implemented yet.

## Temporary application defaults

- Temporal `http://100.88.152.92:7233`, namespace `default`, shared task queue `rusty-bot-chat-v2`.
- Temporal UI `http://100.88.152.92:8233`.
- PostgreSQL `100.88.152.92:55433`, dedicated database `rusty_bot_sessions`; adapter schema is initialized explicitly.
- Router `https://router.arc-dev.work/v1/chat/completions`, model `cx/gpt-6-luna`.
- Five-minute between-turn idle timeout, no fixed Workflow lifetime.
- User-provided test credentials are confined to `src/main.rs`, never Workflow inputs or printed logs. Replace hardcoded credentials with your application's secret configuration before production.

No local Temporal server is started. `--help` does not connect. Calls to the LLM are real and potentially billable. First-visible/completion timings and output byte counts are client-observed, not model token metrics. Provider retries are disabled, HTTP timeout is 55 seconds and Activity timeout is 60 seconds.

The CLI, not the engine SDK, chooses PostgreSQL and constructs its connection pool. Other applications may inject a different implementation of `rusty-bot-session::SessionStorage`. Conversation content exists in PostgreSQL and Temporal history and needs appropriate access controls. Do not run different model configurations on the same shared task queue.
