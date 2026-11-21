# rusty-bot-redis

Redis adapter for `rusty_bot_session::SessionStorage`, interchangeable with
`MemorySessionStorage` and `PgSessionStorage`. The Temporal engine is unchanged:
pass `Arc::new(storage)` as its session adapter.

The application owns authentication, connection timeouts, TLS and the key prefix.
No credentials or server addresses are built into this crate.

```rust,no_run
use redis::{AsyncConnectionConfig, Client};
use rusty_bot_redis::RedisSessionStorage;
use std::time::Duration;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Caller supplies a redis:// or rediss:// URL; URL-encode username/password.
let client = Client::open(std::env::var("REDIS_URL")?)?;
let config = AsyncConnectionConfig::new()
    .set_connection_timeout(Some(Duration::from_secs(5)))
    .set_response_timeout(Some(Duration::from_secs(5)));
let connection = client
    .get_multiplexed_async_connection_with_config(&config).await?;
let storage = RedisSessionStorage::new(connection, "my-app".into())?;
// Engine::connect(options, provider, Arc::new(storage)).await?
# let _ = storage;
# Ok(()) }
```

TLS requires enabling the Redis driver's TLS feature in the application; this
crate enables only `tokio-comp`. Keep Redis private and require authentication.

## Atomicity and persistence

Each session is one JSON string key: `<prefix>:session:<hex-encoded-session-id>`.
Creation uses `SET NX`; updates run the shared Rust transition and commit using
a single-key Lua compare-and-set. Concurrent writers re-read and re-validate; no
process-local mutex, connection-local `WATCH`, or duplicated Lua state machine.
This preserves request deduplication, ownership fencing, draft ordering and
immutable final results, including exact `u64` values. Contention is retried up
to 64 times, then returned as a retryable backend error. Ambiguous network errors
are not silently retried by this adapter; callers may repeat idempotent operations.

Keys have **no TTL**. Use a distinct prefix per application/environment. ACLs must
permit `GET`, `SET`, and `EVAL` on that prefix. The adapter never scans, flushes,
changes server configuration or manages retention. `session_key(id)` exposes the
exact key for caller-managed deletion.

Acknowledgement means Redis accepted the write, **not** guaranteed disk or replica
persistence. Configure AOF/fsync, replication/backups and a non-evicting memory
policy appropriate to the transcript's durability requirements. An evicted key
is lost session history. This implementation rewrites the entire JSON document;
it is not a paginated or unbounded-transcript backend.

## Tests

```sh
cargo test -p rusty-bot-redis
# Explicitly allow writes to a UUID-scoped key on your chosen test Redis:
RUSTY_TEST_REDIS_URL='redis://user:URL_ENCODED_PASSWORD@host:6379/0' \
  cargo test -p rusty-bot-redis --test sessions -- --ignored
```

The opt-in test uses two independent connections and a fresh connection for
read-back. It checks concurrent creation, request deduplication, stale drafts,
final-result immutability, failed transition atomicity, owner fencing and context
restoration. It deletes only its own UUID-scoped key. The same contract sequence
also runs against memory without any Redis access.
