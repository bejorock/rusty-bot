# rusty-bot-engine-temporal

Durable agent sessions using the official Temporal Rust SDK 1.2.0 (Rust 1.92+), with provider-neutral history and rich responses. The old local engine is unchanged.

## Application boundary

The application supplies **both** an `Arc<dyn rusty_bot_providers::Provider>` and `Arc<dyn SessionStorage>`. The provider receives canonical `GenerationRequest` history and streams events ending in an authoritative rich `HistoryEntry`; its supported content depends on the adapter, which rejects unsupported input instead of silently projecting it to text. See [`rusty-bot-providers`](../rusty-bot-providers/README.md) for configuration and supported subsets:

```rust,ignore
let storage = Arc::new(PgSessionStorage::new(application_pool));
storage.migrate().await?; // explicit; only in the application's chosen database
let engine = Engine::connect(options, provider, storage).await?;
let mut outcome = Ok(());
engine.run_until(async {
    outcome = async {
        let session = engine.start_session("my-conversation".into(), "my-model".into()).await?;
        session.send_request("stable-request-id".into(), "Hello".into()).await?;
        // Poll session.snapshot() concurrently for the generating reply.
        // Dropping this handle does not close the Workflow.
        Ok::<_, EngineError>(())
    }.await;
}).await?;
outcome?;
```

The sketch omits imports and application configuration. `run_until` takes a future returning `()`.  See the CLI for a complete runnable host.

- `rusty-bot-session`: SDK-owned storage contract and transcript types; no Temporal or database dependencies. `MemorySessionStorage` is process-local for tests.
- `rusty-bot-postgres::PgSessionStorage`: shared PostgreSQL adapter using an application-created SQLx pool. Credentials, TLS configuration, database creation and migrations belong to the application, never Workflow inputs.
- `EngineOptions`: application-supplied Temporal address, namespace, stable shared task queue and between-turn `idle_timeout`.
- `Engine::start_session(id, model)`: create or attach, rejecting incompatible configuration.
- `Engine::start_session_with_history(id, model, initial_history)`: store validated, immutable provider-neutral initial context. To copy an exported session history into a new session, pass `source.history().await?.for_session_import()`; this explicitly rebases every entry ID to `import:<index>` while preserving all other fields. Exported `turn:` IDs are reserved and otherwise rejected; supplied IDs are never silently rewritten. The history must be complete, and the chosen provider may explicitly reject unsupported content. This creates a new session, not a model change to the source session. Use `resume_session(id)` to reattach without resupplying initial history.
- `Session::history()`: read typed `ChatHistory` directly from the adapter, including initial messages, canonical responses and generation metadata, drafts and failure details. No live worker is required. `snapshot()` remains a legacy text display, not authoritative rich context. Schema and prompt-template types are re-exported by this crate; see [`rusty-bot-session`](../rusty-bot-session/README.md).
- `Engine::resume_session(id)`: attach to an open execution or start a fresh run restoring completed context from storage.
- `Session::snapshot()`: reads shared storage; works without a running Workflow worker.
- `Session::send_request(request_id, text)`: serialized turn with persistent request deduplication. Reuse the same ID and text when retrying an uncertain call. `send_message` generates a fresh ID.
- `Session::run_id()`: the run originally attached by this handle. `current_run_id().await?` resolves its current Continue-As-New successor. `id()` is the logical session and `workflow_id()` its stable Temporal identity.
- `Session::execution_status()`: reads Temporal's execution status (`ExecutionStatus`), not the transcript's `closing` flag.
- `Session::wait_closed()`: follows Continue-As-New successors and waits for final closure without requesting it. Failed/cancelled executions return an error.
- `Session::close()`: gracefully completes this execution and is safe to repeat after successful completion; the logical session remains stored and resumable. Concurrent close callers receive the same final snapshot.

## Durability

Application session IDs map to stable, independently assigned Workflow IDs. Adapter `create` atomically chooses that identity: concurrent candidates must all receive the same stored Workflow ID. The SDK uses Temporal's `UseExisting` conflict policy, so simultaneous create/resume calls attach to the same open run rather than replacing it. Only after completion may `AllowDuplicate` reuse the Workflow ID for a new run. **One logical session has at most one active run, not one run for its entire lifetime.**

Workers sharing sessions must use the same namespace, task queue, shared adapter data/keyspace (the same PostgreSQL database/table or Redis database/prefix), and matching injected provider configuration, including provider, model, endpoint and account. Provider configuration and credentials live in workers, not Workflow input; isolate differing configurations on separate task queues. When `Provider::model_id()` is declared, session creation and resume reject a mismatched model, and the Activity checks it again; an undeclared model ID cannot establish that binding. Separate databases, Redis prefixes, or unshared in-memory adapters are separate session domains; identical application IDs there do not provide global uniqueness. Do not delete/recreate a session's storage record while its Workflow is active. Direct manual starts outside the SDK are outside this admission contract.

Workflow replay owns execution state; storage owns the application transcript. Database I/O runs in Activities (or host API calls), never Workflow code. A begin record precedes model execution, and the final result is persisted before the Update returns. Transactions serialize session mutations. Completed request retries do not invoke the model again. Run ownership fences writes from previously claimed, retired runs.

Canonical cumulative drafts are persisted about every 100 ms; incomplete tool arguments and executable tool calls are not published as drafts. The authoritative final response, including generation metadata, replaces the draft; stale sequences/late drafts cannot replace a committed result. Failed/interrupted turns remain visible with `Failed` status but are excluded from future model context. Text Partial Signals also maintain the Workflow Query view; they still contribute to Temporal history.

The durable idle timer runs **between** turns. Expiry completes the execution rather than imposing a hard lifetime. Reopening starts a new run and restores completed conversation messages. Stopping the host worker does not delete sessions; timers need a worker available to process their completion.

Session handles follow only explicit **Continue-As-New links** from their original run, not whichever execution most recently reused the Workflow ID. After final `close().await?` (or idle `wait_closed().await?`), call `engine.resume_session(id)` for the next execution chain. An old handle can read the latest stored transcript but cannot send to or close that separately resumed chain. Resuming while the old run is still closing attaches to that run; wait for completion before resuming. The SDK never silently starts a new execution on send/close errors. The CLI continues to explicitly close on exit.

## History rollover

Between turns, the Workflow continues as new when Temporal recommends it, after **100 settled requests**, or after **8,000 history events**, whichever is first. The count includes deduplicated requests and failed settled turns. Thresholds can be lowered for tests or adjusted by the application:

```rust,ignore
let engine = Engine::connect(options, provider, storage).await?
    .with_rollover_policy(RolloverPolicy { max_turns: 100, max_history_events: 8_000 })?;
```

The policy is recorded in Workflow input and carried forward; changing workers does not alter an existing run's policy. Zero thresholds are rejected. New runs reload completed model context and request deduplication from the adapter, preserving session/Workflow identity while receiving a fresh Run ID. No transcript is copied into Continue-As-New input. Remaining inactivity time is carried as an absolute Workflow-time deadline, not renewed by rollover.

Rollover waits for active turns and Update handlers to finish. Updates reaching the rollover admission boundary receive an internal `RollingOver` response; `Session::send_request` follows the successor and retries with the same request ID. Close and wait operations also follow the chain. New runs claim adapter ownership and fence predecessor writes. An explicit close wins over rollover when processed before the transition.

These are **between-turn thresholds, not hard size guarantees**: one long response, many partial Signals, a large Activity result, or large model context can still exceed a per-run/payload limit before the next boundary. Transcript/context compaction and large-payload management remain separate work. Resolving an old handle traverses its retained successor links; it requires access to those histories and costs more RPCs as the chain grows.

## Run and test

```sh
cargo run -p rusty-bot-cli
cargo run -p rusty-bot-cli -- --session my-conversation --prompt "Hello"
cargo test -p rusty-bot-session -p rusty-bot-engine-temporal --locked
# Explicit external integration tests (use a dedicated database):
TEMPORAL_ADDRESS=http://your-temporal:7233 RUSTY_TEST_DATABASE_URL='postgres://...' \
  cargo test -p rusty-bot-engine-temporal --locked -- --ignored --nocapture
RUSTY_TEST_DATABASE_URL='postgres://...' \
  cargo test -p rusty-bot-postgres --test sessions --locked -- --ignored
```

Tests use fake models, unique session IDs and an existing Temporal service; no server is started/stopped. Successful PostgreSQL tests delete only their own rows. They cover concurrent create/resume from independent SDK hosts and PostgreSQL connections, idle completion, new-host restoration, request deduplication, stored drafts before completion, graceful close, and worker replay without repeating completed model Activities.

## Recovery during a turn

- A dropped client call does not cancel an accepted turn. Retry with the **same request ID and text** to obtain its saved result rather than generate again.
- Transient storage `Backend` failures retry with exponential backoff (250 ms initially, capped at 5 seconds), without an attempt limit. Each storage attempt has a 15-second timeout. A pending final commit holds the recorded model result in Temporal; worker restart/replay retries persistence, not generation. A commit whose acknowledgement was lost is safe to repeat.
- While persistence is unavailable, the Update and graceful close may remain pending; subsequent turns do not bypass the unfinished commit. Recovery requires the backend and a worker to become available. Configuration/conflict/not-found failures are not treated as transient outages.
- Model Activity execution is **not automatically retried** after an uncertain interruption. A worker shutdown during generation can require waiting for the Activity timeout, derived from the provider's declared operation timeout plus a 60-second margin. The turn is then saved as failed, excluded from model context, and further requests can proceed. Retrying that failed request ID reports its saved failure; use a new request ID only when deliberately requesting new generation. Provider-internal retries, if configured, are distinct from Temporal Activity retries.
- Resuming a terminated execution starts a new run. Its claim marks unfinished turns failed and fences the old owner. Already committed successful turns remain available and deduplicated. Terminating a Workflow does not guarantee immediate cancellation of external provider work already in flight.

These cases are tested with controlled providers and storage failures on an existing Temporal server. Worker restart tests rebuild the worker/cache, not the Temporal service or the database. They do not establish exactly-once external API execution or recovery from arbitrary infrastructure corruption.

## Provider and context limits

The live Activity passes validated canonical history to the injected provider, consumes its validated message stream, and commits the authoritative rich response. Provider adapters translate their supported subsets and explicitly reject unsupported content. The Temporal engine does not execute tool calls and rejects responses containing them. `Session::history()` reads the canonical transcript; the text-oriented send/snapshot APIs are display conveniences rather than a lossless rich-message API.

Model-context growth, context compaction, and oversized model request/response handling remain deferred. Continue-As-New is execution-history management, not model-context management.

## Current limits

No tools, context compaction or schema/data migration from earlier prototypes. Context and snapshots currently include the whole conversation; PostgreSQL stores one JSONB document per session and rewrites it for draft updates. This is a simple adapter, not a large-transcript optimization. Continue-As-New bounds accumulated history only at safe between-turn boundaries; it does not bound one turn, payload sizes, or the stored transcript.

The model Activity has one attempt and a start-to-close timeout equal to the provider's declared operation timeout plus 60 seconds. `Engine::connect` requires every provider, including a custom implementation, to declare a positive, representable `operation_timeout()`; it rejects a missing, zero or unrepresentable budget. The Activity timeout is recorded in Workflow input, so changes apply to new executions rather than changing an existing run's recorded budget. There is no exactly-once guarantee for an external LLM request across a crash before final persistence. Closing is graceful, not immediate cancellation. Session content is sensitive in both PostgreSQL and Temporal history; credentials are not included in Workflow input. The prototype's existing histories are not replay-compatible with this changed Workflow definition; deploy on a new task queue/version with matching provider configuration.
