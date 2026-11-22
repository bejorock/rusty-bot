# rusty-bot-providers

Strict streaming adapters over `rusty-bot-messages` rich history. Independent of the
legacy `ModelProvider`, CLI, engine, and Temporal. No tool execution or persistence.

## Installation

```toml
[dependencies]
rusty-bot-providers = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-util = "0.7"
```

This is a standalone Tokio-based library: no agent engine, Temporal, session
storage, or database is required. Message types are available through
`rusty_bot_providers::messages`; a direct `rusty-bot-messages` dependency is optional.
The current tested minimum toolchain is Rust 1.98. Older compilers have not been
validated; the dependency set is not compatible with the former 1.85 declaration.
Version `0.1.0` is available on crates.io. The text embedding API described below
is an unreleased addition; use a path or Git dependency for it until the next release.

## API

- `OpenAIProvider::new(config, resolver)` selects **Responses**.
- `OpenAIProvider::with_protocol(config, OpenAIProtocol::ChatCompletions, resolver)`
  explicitly selects Chat Completions. There is no fallback.
- `AnthropicProvider::new(config, resolver)` uses Messages.
- `DeepseekProvider::new(config, resolver)` uses DeepSeek Chat Completions. No Gemini adapter.
- All implement `Provider::{build_request, stream, list_models}`. `build_request` performs the
  same strict, offline validation as `stream`, before credential resolution/HTTP.

`ProviderConfig::new(full_post_url, model)` requires a complete endpoint (for
example `https://api.openai.com/v1/responses`), not a base URL. Caller supplies an
`Arc<dyn rusty_bot_providers::CredentialResolver>`. Credentials are resolved per attempt.
The transport sends bearer authentication, or Anthropic `x-api-key` plus
`anthropic-version: 2023-06-01`. HTTPS is required except on loopback; redirects,
URL credentials, queries, and fragments are forbidden. Debug implementations
redact configuration, provider internals, and replay. Never log request bodies or
history: opaque replay is sensitive and is **not encrypted by this crate**.

```no_run
use std::sync::Arc;
use rusty_bot_providers::CredentialResolver;
use rusty_bot_providers::{GenerationRequest, OpenAIProvider, Provider, ProviderConfig};
use rusty_bot_providers::messages::{ChatHistory, ConversationMessage, HistoryEntry};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

async fn generate(resolver: Arc<dyn CredentialResolver>) -> Result<(), rusty_bot_providers::ModelError> {
    let provider = OpenAIProvider::new(
        ProviderConfig::new("https://api.openai.com/v1/responses", "caller-selected-model"),
        resolver,
    )?;
    let request = GenerationRequest {
        message_id: "answer:1".into(),
        history: ChatHistory {
            schema_version: 1,
            entries: vec![HistoryEntry::completed("user:1", ConversationMessage::user("Hello"))],
        },
        tools: vec![],
    };
    let (tx, mut rx) = mpsc::channel(32);
    // Drain concurrently: the bounded sink applies backpressure.
    let drain = tokio::spawn(async move { while let Some(event) = rx.recv().await { drop(event); } });
    let response = provider.stream(&request, tx, CancellationToken::new()).await?;
    drain.await.unwrap();
    assert_eq!(response.id, "answer:1");
    Ok(())
}
```

### Native structured output

Set `config.output_format` to `OutputFormat::JsonObject` or
`OutputFormat::JsonSchema { name, schema, strict }` to request a provider-native
constraint. The default `Text` sends no format constraint. Responses uses
`text.format`, Chat Completions uses `response_format`, Anthropic uses
`output_config.format` (only JSON Schema with `strict: true`), and DeepSeek
supports only `JsonObject`. Unsupported combinations fail offline; no prompt
emulation is performed. `stream` returns the authoritative rich response without parsing.
For checked provider-bound JSON, use `generate_structured::<T>(&provider, &request, cancel)`:
it uses the provider's configured format, parses only completed stop responses, validates
JSON Schema unchanged, and deserializes into `T`. Truncation, refusals, tool calls,
invalid JSON/schema/types are errors with no output text leaked. External schema retrieval
is disabled; internal references are supported. This is not a generic output-parser framework. Schema names must be 1–64
ASCII alphanumeric, underscore or hyphen characters, and schemas must be JSON
objects. Schemas are sent unchanged; model-specific schema support and generated
output validity remain model-dependent; use the checked helper when validity is required. Anthropic does not
send the name or strict flag, since its native format takes only the schema.
Adaptive reasoning effort and the Anthropic output format share `output_config`.

### Retries and total timeout

`config.retry` is a public `RetryPolicy`: by default it permits **3 retries after
the initial attempt**, with exponential backoff starting at 250 ms and capped
at 5 seconds. Set `config.retry.retries = 0` to disable retries; the initial and
maximum backoffs are also configurable. Discovery and generation retry only
retryable Network, Server, and RateLimited errors. Auth, input, protocol, and
cancellation errors are never retried. `Retry-After` (delta seconds or HTTP date)
is honored as a minimum wait, even when it exceeds the maximum backoff.

For generation, retries stop as soon as **any event** is delivered to the sink.
`Start` is delayed until the first decoded event (or successful completion), not
merely HTTP headers; bytes buffered without an event remain eligible for retry.
A failure after `Start` is returned without retry: discard provisional output.
Retrying a POST after a transient failure can still result in duplicate server-side
work or charges even if no event was delivered.

`config.timeout` is one total deadline per stream or discovery operation, not
per attempt: it includes credential resolution, requests, backoff, streaming,
and downstream event-sink backpressure. Cancellation interrupts all phases,
including backoff. Deadline expiry returns a non-retryable Network error.
Generation validates cloned input on a blocking worker under the same deadline.
The initial clone of the borrowed request is synchronous and non-preemptible;
bounded image decoding already in progress may finish after the caller is
cancelled, but cannot start HTTP afterward. `build_request` itself is an explicit
synchronous, offline operation and has no async timeout.

### Router compatibility: Chat stream termination

Chat Completions requires `[DONE]` by default. For a router that cleanly closes
HTTP after a finish-reason chunk without sending `[DONE]`, explicitly opt in:

```no_run
use rusty_bot_providers::{ChatStreamTermination, ProviderConfig};
let mut config = ProviderConfig::new("https://router.example/v1/chat/completions", "model");
config.chat_stream_termination = ChatStreamTermination::FinishReasonAndEof;
// Pass config to OpenAIProvider::with_protocol with OpenAIProtocol::ChatCompletions.
```

This accepts only successful HTTP EOF after a finish reason and normal stream
validation; missing finish reasons and transport errors still fail. It does not
change retry policy or fall back. Non-default policies are rejected for Responses, Anthropic,
and DeepSeek.

## Stream and history contract

Events start at sequence 1 with `Start`, then ordered block previews, and exactly
one `Completed` **only after HTTP EOF and terminal protocol validation**. Text and
visible reasoning stream incrementally. Tool calls stream as complete blocks
once closed; interleaved argument fragments are buffered, not executable.
`Completed.response` and the returned `HistoryEntry` are authoritative. Preview
text is not required to equal final response text.

On failure/cancellation/timeout, `stream` returns a sanitized `ModelError`; it does
not emit `Failed` into a potentially blocked sink. Discard previews on `Err`.
Dropping the receiver cancels when the next event is sent. Cancellation and the
whole-operation timeout also cover credential resolution, HTTP reads, and sink
backpressure. Default timeout is 120 seconds. No partial assistant entry is
returned. Caller owns the message ID and any persistence/timestamps.

Final metadata records provider, requested-model-bound replay, actual response
model/ID, mapped and raw finish reason, and optional token counts/details. Unknown
counts remain `None`. No token totals are invented. HTTP request ID is retained
only when the underlying safe transport accepts it. Transport errors never
include wire response bodies or reqwest details. Known credential encodings are
screened by the provider transport; this is not a general secret scanner.

## Deliberately bounded supported subset

- Schema-version-1 `ChatHistory` with unique IDs, completed entries, resolved tool
  calls, and valid rich messages. No silent filtering of draft/failed entries.
- System/user/assistant/tool roles; Developer only on OpenAI. Anthropic system
  text must precede all conversation entries. No synthetic role substitution.
- Text blocks, assistant function declarations/calls, text tool results. Tools
  use `ToolSpec` object schemas with automatic, disabled, required, or named choice
  where the API supports it. No execution, hosted tools, arbitrary provider
  option bags, or hooks.
- Static inline PNG/JPEG/WebP/GIF in **user** messages for OpenAI, Anthropic,
  and vision-capable DeepSeek models (for example `deepseek-flash`).
  Canonical base64, matching MIME/magic and full bounded decoding are checked;
  animation is refused. Aggregate decoded cap 6 MiB, dimensions at most 8192,
  with the core decoder's pixel/work limits. These are conservative SDK limits,
  not claims about maximum provider capabilities. HTTPS image URLs in user messages
  are passed to the provider without fetching them in the SDK (no credentials in
  URL authority, fragments, local HTTP, or arbitrary URI schemes). Remote format,
  dimensions and accessibility are the provider's responsibility. No reference fetching,
  audio, video, files, JSON blocks, or assistant media. Model-specific vision
  support remains a caller responsibility.
- Chat assistant text must have at most one text block before any tool calls;
  reasoning precedes text, which precedes tool calls. Unsupported ordering is
  refused, not flattened. Tool text results are at most one text block. Chat and
  Responses cannot encode `is_error: true` losslessly and refuse it; Anthropic
  preserves it. Anthropic preserves arbitrary text/tool block ordering.
- Unrecognized extensions, annotations/annotated wrappers, bare reasoning,
  caller-authored refusal outside portable fallback, and untranslatable replay are refused. Output text
  citations/annotations outside the subset are refused. Provider refusal output
  is retained for OpenAI. Unknown finish reasons fail closed.
- At most one Chat choice; indexed calls and provider output items are bounded
  to 128. Anthropic blocks and Responses output items must arrive sequentially;
  interleaving Responses items is refused. Anthropic permits one active block.
  SSE accepts CR, LF, CRLF, comments, multi-line data and split UTF-8; invalid
  UTF-8 is refused. Each SSE line/event is bounded to 4 MiB, transport to 16 MiB.

## Replay (version 1)

Every generated assistant entry ends in a required `ContentBlock::Replay`.
It binds provider, requested model, protocol format, and version 1:

| API | Format | Stored payload |
| --- | --- | --- |
| OpenAI Responses | `responses.output` | authoritative ordered output items |
| OpenAI Chat | `chat.message` | assembled assistant message |
| Anthropic | `messages.content` | complete native content array |
| DeepSeek | `chat.reasoning_message` | assembled message including `reasoning_content` |

For the exact same provider, model, format and version, projection must exactly
match the visible blocks: edited or malformed replay is refused before HTTP. The
trailing replay payload is sent back in its native shape, not reconstructed through
the legacy core `HistoryPart`. Same-target missing model identity or unsupported
version also fails closed. Do not manually modify replay or transplant it into
another assistant entry. Replay is integrity-checked against projection, not
cryptographically authenticated; application-supplied history remains trusted input.
To change same-target visible output, deliberately construct a supported plain message.

**Automatic portable fallback is lossy:** if the trailing replay identifies a
different provider, explicit model, or protocol format, it is not sent; visible
`Reasoning` blocks and the known opaque Anthropic redacted-thinking marker are
also removed, including when replay is marked required.
Text, tool calls and their order are retained where the destination can represent
them; visible refusal becomes ordinary assistant text. Other messages, including
user images, are translated using the same strict destination subset. A message
containing only dropped blocks is refused, not silently removed. Unsupported
ordering, unknown extensions, annotations, invalid tool calls, and other
untranslatable blocks still fail. This fallback does not validate the
foreign payload against its visible projection; applications must trust or verify
history before handing it to the adapter. No signatures or reasoning are sent to
an incompatible target. Portable assistant tool-call turns are refused for DeepSeek
unless thinking is disabled, and for Anthropic when thinking is explicitly enabled
(including adaptive thinking or a legacy thinking budget); Anthropic's default and
disabled modes remain supported. An incomplete or malformed
streamed Anthropic tool input is retained as a non-executable `InvalidToolCall`,
including its raw argument fragments; its replay payload stores those fragments as
an input string rather than a native tool-input object. Such entries are refused
for replay and must not be executed. Terminal and finish-reason validation still
applies.

Responses always sends `store: false` and
`include: ["reasoning.encrypted_content"]` and manually resubmits output items.
Reasoning output without encrypted content is refused. There are no
`previous_response_id` or server-owned conversation shortcuts. Summary text is
visible; encrypted reasoning remains opaque in replay.

Anthropic thinking/signature and redacted thinking are retained verbatim. Visible
thinking is `Reasoning`; redacted thinking occupies an ordered opaque extension
marker whose sensitive data lives only in redacted-debug replay. Thinking can be
enabled with `thinking_budget` (at least 1024 and below `max_output_tokens`). Adaptive thinking is also supported; server tools, citations, and new provider
block types are not.

DeepSeek `reasoning_content` is preserved and resubmitted on assistant turns,
**including tool-call turns followed by tool results**, following the
[thinking-mode tool-use contract](https://api-docs.deepseek.com/guides/thinking_mode).
On compatible native replay it is never replaced by display reasoning or stripped
on tool continuation; the portable fallback described above removes it.

## Validation

An opt-in live probe is available as `examples/live_probe.rs`. It reads a caller-owned
JSON file containing provider keys (`openai`, `anthropic`, `deepseek`, `9router`);
no credentials are embedded in the example. Run it with `<key-file> discover`,
`<key-file> generate <provider> <model> <default|off|on|adaptive>`, or use `replay`
instead of `generate` to check two-turn native replay. `openai-chat` selects Chat
Completions with the OpenAI key. The probe makes billable requests, uses short
arithmetic prompts, and explicitly enables clean-EOF compatibility for 9router.
Model IDs and endpoints in this example are test choices, not a supported-model catalog.

Offline unit fixtures cover strict translation, images/refusals, SSE framing,
stream ordering, metadata, authoritative final snapshots, opaque replay and
mismatches, malformed calls, cancellation, timeout and credential gating.
Only deterministic fake credentials/loopback fixtures are used; no live service,
keys, or database is needed. Run `cargo test -p rusty-bot-providers` and
`cargo clippy -p rusty-bot-providers --all-targets -- -D warnings`.

## Configuration and discovery

Use `from_base_url(base, model, resolver)` for each provider; the base includes
any version path (e.g. `https://api.openai.com/v1` or
`https://api.deepseek.com`). OpenAI offers `from_base_url_with_protocol`.
`with_api_key(config, ProviderCredential::new(key))` uses a redacted static
resolver; OpenAI also offers `with_protocol_and_api_key`. Neither reads the environment.
Full POST URL configurations derive `/models` only from recognized suffixes;
otherwise set `config.list_models_endpoint` explicitly. Redirects are disabled.

`config.reasoning` defaults to `Reasoning::Default` (omitted), and supports
`Disabled`, `Enabled { effort, budget_tokens }`, and `Adaptive { effort }`.
OpenAI accepts effort without budget, Anthropic enabled thinking requires a
budget of at least 1024 and less than max output; Anthropic adaptive supports
low/medium/high/xhigh/max. DeepSeek enabled supports low/high/max. The
legacy Anthropic `thinking_budget` is mutually exclusive with reasoning settings.
Optional `temperature` and `top_p` are validated; Anthropic thinking forbids
sampling and DeepSeek sampling requires explicitly disabled thinking.
`tool_choice` supports Auto, None, Required and Named, with declaration checks;
Anthropic active thinking cannot force a tool. These controls depend on API and
model support; a remote service may reject them. Effort availability is model-dependent;
no model-name catalog or capabilities are guessed. See the [OpenAI reasoning guide](https://developers.openai.com/api/docs/guides/reasoning) and [Anthropic model-list documentation](https://platform.claude.com/docs/en/api/models/list).

`list_models(DiscoveryRequest::default(), cancel)` fetches exactly one bounded
page. Anthropic supports `after` and `limit` and returns `next_cursor`; OpenAI
and DeepSeek reject pagination options. No capabilities are inferred from IDs.

`model.capabilities()` projects explicitly reported image, structured-output,
thinking-mode and effort-level support. Every boolean is `Option<bool>`: absent
means unknown, not false. Anthropic currently reports these; OpenAI and DeepSeek
model listings generally do not. The original `raw` metadata remains available.
Discovery is not a complete pricing/capability catalog or a guarantee that an
organization can use every advertised feature.

## Prompt caching and token accounting

`ProviderConfig.prompt_cache` is optional and never rewrites stored history.
Use `PromptCache::Anthropic { markers }` with `CacheMarker { message_id,
block_index, ttl }` to mark exact system/user text or user-image boundaries.
Up to four breakpoints are supported; TTL is optional (provider default), five
minutes or one hour, with longer-lived prefixes first. Assistant/tool/replay
markers are deliberately not supported yet.

`PromptCache::OpenAi` accepts an optional stable cache key and legacy retention
(`InMemory`/`TwentyFourHours`). Modern Responses caching uses
`OpenAiResponsesCache { mode, ttl, markers }`: implicit or explicit mode,
30-minute or 12-hour request TTL, and up to four explicit markers (three when
implicit mode also uses one slot). Modern and legacy retention cannot be mixed.
OpenAI per-breakpoint TTL overrides are not exposed yet. These options are
model-dependent and can be rejected remotely. DeepSeek caches prefixes
implicitly; manual cache controls are rejected instead of silently ignored.
Markers do not guarantee a hit or bypass a provider's minimum prefix length.

`usage::statistics(&generation.usage)` (pass the contained `TokenUsage`) exposes
input, output, total, reasoning, cache reads, cache writes, and cold-input counts.
Missing counters stay `None`, never an invented zero. Anthropic total input is
cold + read + write; output reasoning tokens are already part of output, not
additional output. Modern OpenAI cold input excludes both reads and writes.
Original usage is preserved in generation extras. These are actual returned
counts, not a preflight tokenizer: no exact local input estimate or pricing
calculation is promised. Prefix-cache accounting is based on reported token
counts, not local string-prefix comparisons.

### Local input-token estimates

For preflight sizing without a model call:

```rust,ignore
let estimate = estimate_input_tokens(&provider, &request, EstimationEncoding::Auto)?;
println!("estimated input: {}", estimate.estimated_input_tokens);
```

This uses `tiktoken-rs` locally. Auto selects a known OpenAI model encoding;
unknown models (including Claude/DeepSeek and unmapped router aliases) use an
explicitly labelled `o200k_base` proxy. `O200kBase` and `Cl100kBase` overrides
are also available. `model_encoding_known`, `used_fallback` and `encoding`
make the choice inspectable. No tokenizer files are downloaded at runtime.

The heuristic counts translated request JSON, including tool declarations,
output schemas and configuration overhead. Native chat framing is not JSON,
so even a known encoding does **not** make this a billed count or safe upper
bound. Image payloads/URLs and opaque replay fields are omitted rather than
counting their base64 or ciphertext as prompt text; their counts are reported
and `includes_all_input` becomes false. Hidden provider overhead is unknown.
Treat this as planning guidance, especially for non-OpenAI models, not a hard
context admission check. The synchronous estimator performs normal request
validation and CPU tokenization; cache/reuse it or offload it in async hosts.
It never overwrites actual returned `TokenUsage` or predicts cache hits.

## Convenience, observation and actionable errors

`generate(&provider, &request, cancel)` drains provisional events concurrently
and returns the unmodified authoritative entry, without requiring your own
channel consumer. `generate_structured::<T>` adds provider-bound JSON checking
as described above. Both remain ordinary Rust `Result` APIs.

`ObservedProvider::new(Arc<dyn Provider>, Arc<dyn Observer>)` emits operation
start, completion/failure, elapsed time, first-event latency, normalized usage
and error kind. It sends no prompts, response text, endpoints, keys or arbitrary
provider detail-map keys to observers. Callbacks must be fast/nonblocking and
must not panic. First event is not necessarily the first text token. Internal
retry attempts are not separate observations; dropping a future has no terminal
callback. There is no monitoring-service dependency.

Recognized provider codes classify billing, exhausted quota, missing model,
context overflow and permission failures separately from authentication,
rate limiting and server/network errors. HTTP status and a validated request ID
are retained when available. Known in-stream error events are classified too.
Billing/quota/input/permission failures do not retry; temporary rate limits and
server/network failures use the bounded retry policy before output starts.
Only allowlisted error codes are retained; arbitrary provider messages and raw
error bodies are never exposed. Unknown codes fall back to HTTP classification,
so message-only errors cannot always distinguish model/context/billing causes.
A normal output-token cutoff remains `FinishReason::Length`, not a transport
exception; typed structured generation refuses such incomplete output.

### Deferred tool controls

Hosted tools, incremental tool argument events, and live tool-use tests remain
deferred. Function declarations and provider strict/parallel request controls
are supported as described below.
No tool execution or Temporal integration is part of these provider helpers.
`max_input_tokens` is the server-reported input limit, not a context window;
missing limits remain `None`. Raw returned model metadata is retained; callers
should treat it as sensitive. Anthropic continuation cursors must match the last
returned model and advance beyond the requested cursor.
The list URL is caller-configured or locally derived, never server-provided.

### Tool request controls

`GenerationRequest.tools` remains the ordinary list of function `ToolSpec`s. Set
`ProviderConfig.tool_options` by declared tool name to request provider-side
strict schema mode, and optionally set `parallel_tool_calls` to control the
provider's tool-call generation. Neither setting executes tools, controls local
agent concurrency, rewrites schemas, or locally validates returned arguments.
Unset options are omitted; unknown names and explicit parallel settings without
tools are rejected offline. Provider/model schema support and compliance vary.

```rust
use rusty_bot_providers::{ProviderConfig, ToolOptions};
let mut config = ProviderConfig::new("https://api.openai.com/v1/responses", "model");
config.tool_options.insert("lookup".into(), ToolOptions { strict: Some(true) });
config.parallel_tool_calls = Some(false);
// Declare a tool named "lookup" in every request using this config.
```

Responses and Anthropic place `strict` on each tool; Chat Completions and
DeepSeek place it inside the function. Anthropic represents parallelism through
`tool_choice.disable_parallel_tool_use`, preserving auto/required/named choices.
An explicit parallel setting combined with Anthropic `ToolChoice::None` is
rejected because that choice does not accept the parallel-use field.
DeepSeek does not support the parallel control. DeepSeek `strict: true` requires
all declared tools to be strict and an explicitly chosen endpoint whose path ends
in `/beta/chat/completions` (e.g. `https://api.deepseek.com/beta/chat/completions`);
proxy hosts are accepted and endpoints are never rewritten. `strict: false` is
allowed on ordinary endpoints. DeepSeek named or required tool choices require
`reasoning = Reasoning::Disabled`; the default is treated as thinking enabled.

## Native batch jobs

`BatchProvider` and `BatchTranslator` are independent object-safe interfaces.
OpenAI Responses/Chat Completions and Anthropic Messages are supported; DeepSeek
returns `Unsupported`. Batch translation is offline and uses the same request
validation and replay rules as streaming. Configure the full generation endpoint
(e.g. `/v1/responses`, `/v1/chat/completions`, or `/v1/messages`); batch paths
are derived from it, including any proxy prefix. Custom IDs are unique ASCII
letters, numbers, `_` or `-`, at most 256 bytes for OpenAI and 64 bytes for Anthropic.

```no_run
use rusty_bot_providers::{BatchProvider, BatchTranslator, BatchRequest, BatchHandle};
use tokio_util::sync::CancellationToken;

async fn submit_and_remember(
    provider: &dyn BatchProvider,
    translator: &dyn BatchTranslator,
    items: &[BatchRequest],
) -> Result<BatchHandle, rusty_bot_providers::ModelError> {
    let _native_body = translator.build_submission(items)?; // optional offline preview
    let handle = provider.submit(items, CancellationToken::new()).await?;
    // Persist the entire serializable handle securely before returning. On restart,
    // restore it and call status/results/cancel with the same provider, model and
    // generation endpoint. Retain the same account/project context yourself.
    Ok(handle)
}
```

Submission is not retried: a timeout or network failure may mean the server
accepted a job even though its ID was not received. OpenAI uploads the input file
before creating the job, so failed or uncertain submission may leave an orphaned
upload; reconcile it with the provider before any manual resubmission. Results
refuse pending jobs. `BatchResults` returns the terminal job, available items,
and `unreported_ids`. Successful items contain a `Box<HistoryEntry>` in
`BatchOutcome::Success`; dereference it to access or move out the entry. OpenAI
failed, cancelled, or expired jobs can have missing
items (including validation failures with no files). `BatchJob::counts` is `None`
when OpenAI does not report counts, including for validation failures and jobs in
progress; no zeros or outcomes are invented. Completed OpenAI jobs and Anthropic
jobs require counts. Completed OpenAI jobs and ended Anthropic jobs require full ID and count
coverage; duplicate, unknown or malformed results always fail. Handles bind the
normalized generation endpoint as well as provider/protocol/model. Credentials
are never stored; the caller must ensure the same account/project context when
restoring a handle, which cannot be verified from the handle. SDK-side request
bodies and aggregate downloaded result files are bounded at 16 MiB; these are SDK
caps, not provider maxima. The SDK conservatively allows at most 50,000 items
across both APIs.
No list, delete or automatic recovery endpoint is provided. Never log raw batch
payloads or store handles in an untrusted location.

## Text embeddings

Embeddings use a separate model and endpoint, independently of generation settings.
`OpenAIEmbeddingProvider` implements `EmbeddingProvider`, whose
`build_embedding_request` validates and translates offline before resolving credentials.
The existing generation `OpenAIProvider` implements the trait but returns `Unsupported`:
construct the dedicated provider instead. Official Anthropic has no embedding model
(Voyage is a separate provider), and DeepSeek has no documented embedding endpoint;
their generation providers return `Unsupported` without resolving credentials.

```no_run
use rusty_bot_providers::{EmbeddingConfig, EmbeddingProvider, EmbeddingRequest,
    OpenAIEmbeddingProvider, ProviderCredential};
use tokio_util::sync::CancellationToken;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = EmbeddingConfig::new("https://api.openai.com/v1/embeddings", "text-embedding-3-small");
let key = ProviderCredential::new(std::env::var("OPENAI_API_KEY")?);
let provider = OpenAIEmbeddingProvider::with_api_key(config, key)?;
let request = EmbeddingRequest { input: vec!["hello".into()], dimensions: None, user: None };
let response = provider.embed(&request, CancellationToken::new()).await?;
assert_eq!(response.embeddings.len(), 1);
# Ok(()) }
```

Only nonempty text strings are supported (1–2048 inputs, 16 MiB encoded request and
response limits). Inputs are never split, truncated, or normalized; model-specific
token and dimensional limits are checked by the service. Vectors use float encoding;
the adapter validates indices, finite values, dimensions, model and reported usage.
Request/configuration and vector debug output is redacted, but serialized request and
response values remain sensitive. Timeouts cover validation, credential resolution,
network, response reading and retries; default retry policy is three retries in
addition to the first attempt. Bounded request validation and response parsing run
on the blocking pool; an already running CPU job may finish after cancellation,
but its result is discarded. The initial bounded clone is synchronous. Retries of
a POST can repeat billed work if an earlier response was lost. HTTPS is required
except for loopback fixtures. This API accepts `Arc<dyn EmbeddingProvider>` and
does not require a generation provider, agent engine, or vector database.
