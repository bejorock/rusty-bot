//! PostgreSQL persistence for Rusty Bot.
//!
//! This crate owns the production `Storage` implementation, schema migrations,
//! durable event reads, and transient live notifications. There is no SQLite or
//! JSON production fallback.

pub mod events;
pub mod migrate;
pub mod storage;

pub use events::{
    live_payloads, PgEventListener, PgEventNotification, PgEventReader, PgLiveSink,
    MAX_LIVE_PAYLOAD_BYTES,
};
pub use migrate::{connect, migrate, MIGRATOR};
pub use storage::PgStorage;
