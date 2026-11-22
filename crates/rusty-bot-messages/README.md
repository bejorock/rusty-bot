# rusty-bot-messages

Provider-neutral LLM conversation types for Rust. Use them with
`rusty-bot-providers`, your own provider adapters, or an application that needs to
store and validate conversations without an agent framework.

No HTTP client, Tokio runtime, Temporal, session storage, or database dependency.
The library depends only on Serde, serde_json, and thiserror.

## Installation

```toml
[dependencies]
rusty-bot-messages = "0.1"
```

The first crates.io release is being prepared. Until publication, use a path or
Git dependency pointing to this workspace. The current tested minimum toolchain
is Rust 1.98; older compilers have not been validated.

## A conversation

```rust
use rusty_bot_messages::{ChatHistory, ConversationMessage, HistoryEntry};

let history = ChatHistory {
    schema_version: 1,
    entries: vec![
        HistoryEntry::completed("system:1", ConversationMessage::system("Be concise.")),
        HistoryEntry::completed("user:1", ConversationMessage::user("Hello")),
    ],
};
history.validate_complete()?;
# Ok::<(), rusty_bot_messages::HistoryError>(())
```

Messages support system, developer, user, assistant, and tool roles. Ordered
content blocks represent text, images, audio, video, files, JSON, reasoning,
tool calls, invalid tool calls, refusals, annotations, extensions, and native
replay state. Representing a block does not mean every provider supports it:
provider adapters must translate it or return an explicit error.

## Main types

- `ChatHistory`, `HistoryEntry`, and `ConversationMessage`: conversation data and
  stable message identities, independent of storage implementation.
- `ContentBlock` and `MediaSource`: multimodal content and inline, URL, or
  application-reference sources. This crate does not fetch or decode media.
- `GenerationMetadata`, `TokenUsage`, and `FinishReason`: provider response facts,
  distinct from the lifecycle `MessageStatus`.
- `ReplayState`: provider/model/format-bound opaque state for native continuation.
- `MessageStreamEvent` and `MessageAccumulator`: ordered streaming events,
  provisional drafts, and an authoritative final response.
- `PromptTemplate` and `ChatPromptTemplate`: strict named-variable substitution
  with escaped braces; no code execution or template expressions.

`validate_complete()` requires completed entries, unique message identities,
well-formed tool-call relationships, and no outstanding tool calls. These checks
are structural: they do not validate provider-specific JSON Schema subsets or
execute tools.

When importing exported history into a new `rusty-bot-session` session, call
`history.for_session_import()` to rebase entry IDs while preserving content and
metadata. The session layer reserves its own generated identity prefix.

## Safety and compatibility

Replay payloads can contain sensitive provider state. Their `Debug` output is
redacted, but serialized history contains the actual payload: protect stored JSON
and do not log conversation bodies. Redaction is not encryption.

Preserve native replay when it is compatible with the destination provider.
Cross-provider conversion is an adapter policy, not a promise that signed or
opaque reasoning is portable. Provisional streamed content is not a completed
response and malformed partial tool arguments must not be executed.

The JSON history schema is versioned independently from the crate version.
Legacy assistant `tool_calls` can be read into ordered content, but serialization
uses the canonical ordered-block representation.

## License

MIT. See `LICENSE`.
