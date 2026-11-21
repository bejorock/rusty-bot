# rusty-bot-session

SDK-owned application session contract, independent of Temporal and PostgreSQL.

The host injects `Arc<dyn SessionStorage>` into the Temporal engine. Implementations provide atomic `create`, `read` and `change` operations. `StoredSession::apply` defines the shared transitions for run ownership, request deduplication, ordered drafts, final results and graceful closure. Adapters must serialize changes to a session and persist the entire validated transition or nothing.

`MemorySessionStorage` is only process-local; use a shared adapter for distributed workers. `rusty-bot-postgres::PgSessionStorage` is the included PostgreSQL implementation. The application constructs its connection pool and explicitly initializes its schema. This crate has no credentials or database defaults.

`StoredSession::snapshot()` includes completed, generating and failed messages as a legacy text display. Use `history()` for the canonical transcript and `try_completed_context()` for a checked legacy text projection: it refuses rich initial history or canonical responses rather than dropping data. `completed_context()` is retained only for legacy text turns. Application session ID, assigned Workflow ID and owning Run ID are separate identities. Reopening an ended execution preserves the application session and its completed transcript.

Request IDs are application-provided idempotency keys. Retrying an ID with identical text returns its existing result; reusing it with different text is a conflict. A new run claims ownership and retires the previous owner, rejecting stale writers. Incomplete turns from the previous execution become failed, not silently replayed LLM calls.

## Provider-neutral history

`ChatHistory { schema_version: 1, entries }` is the client-facing typed transcript. `HistoryEntry` separates stable identity, optional turn ID, message, lifecycle status/failure and application metadata from optional generation facts: provider/model/response ID, finish reason, token usage and duration. An optional timestamp and application `artifact` are also preserved. Artifacts are not model content; a generation finish reason is not the same as transcript lifecycle status.

`ConversationMessage` supports system, developer, user, assistant and tool-result roles. Assistant content is **one ordered block list**: text, reasoning, tool calls, refusal, media and provider extensions retain their original order. `tool_calls()` is a derived view, not a second authority. Older serialized assistant messages with separate `tool_calls` are read by appending those calls after their content (their original interleaving cannot be recovered); mixed old/new call representations are rejected. New serialization writes only the ordered list.

Blocks support text, image, audio, video, file, JSON, displayable reasoning, tool calls, malformed non-executable tool calls, refusals and namespaced extensions. `Annotated` attaches an optional block ID, citations/annotations and replay material without duplicating content. `ReplayState` preserves opaque payloads with provider, optional model, format, version and a `required_for_replay` flag. Its debug output redacts the payload, but serialized storage still contains sensitive data. A provider adapter must explicitly check compatibility (`validate_replay_for`); required incompatible replay state is an error, never a silent drop. `text_only()` refuses non-text blocks.

Media uses `MediaSource::Inline` (MIME type/base64), `Url`, or an application-managed `Reference`. The schema does not fetch URLs, open files, decode media or enforce provider capabilities. Treat URLs/references as untrusted application data. `validate()` checks transcript structure and tool identities; `validate_complete()` also requires completed entries and no outstanding tool calls. Neither proves a provider can accept the content.

`SessionConfig::initial_history` stores immutable, rendered initial context. Its default is empty, including when reading older stored JSON. Initial entry IDs cannot use the reserved `turn:` prefix. `StoredSession::history()` appends turns with deterministic `turn:<id>:user/assistant` IDs, draft/failure status and failure details. `SessionChange::FinishResponse` can atomically commit a complete canonical assistant `HistoryEntry`, preserving blocks and generation metadata; identical retries are idempotent and conflicting overwrites are rejected. Legacy text results remain readable. The existing `snapshot()` remains a legacy text-only view. This increment does **not** translate rich messages into provider requests or enable multimodal/tool execution.

## Stream contract

`MessageStreamEvent` is transport-independent: it defines message start, ordered block start/delta/end, completion and failure. Events carry a stable message ID and contiguous sequence number. `MessageAccumulator` rejects invalid ordering/identity without changing its state. Raw partial tool-argument JSON is preview data, never an executable call; a malformed finished call becomes `InvalidToolCall`. A final completed `HistoryEntry` is authoritative, even when its content differs from the preview.

This is a contract and accumulator, **not a new live transport**. The current Temporal model Activity still streams legacy text drafts and commits text results. Provider translation, rich-response capture and tool execution are deferred; the Temporal admission guard refuses rich history rather than feeding a lossy projection to that model path.

## Reusable prompts

```rust
use rusty_bot_session::{PromptTemplate, PromptVariables, ConversationMessage};

let base = PromptTemplate::new("You are a {role}. Reply in {language}. {{literal braces}}")?;
let coding = base.partial(PromptVariables::from([
    ("role".into(), "coding assistant".into()),
]))?;
let rendered = coding.render(&PromptVariables::from([
    ("language".into(), "English".into()),
]))?;
let system = ConversationMessage::system(rendered);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Templates use `{identifier}` variables and `{{`/`}}` escaped braces. Missing/unknown variables and unsupported syntax are errors. `partial()` creates an immutable variation; rebinding or explicit render values override earlier bindings. Values are inserted literally, never recursively interpreted. Templates do not evaluate code or protect against prompt injection. `ChatPromptTemplate` combines role-specific text templates with literal typed messages, preserving multimodal examples verbatim. Render in the application before storing history; templates themselves are serializable for reuse.

The contract stores the entire transcript, including cumulative drafts, without pagination or retention policy. Applications must control access to this sensitive content. Structural additions use serde defaults for old text records; other serialization changes still require an explicit migration/versioning plan before production deployment.
