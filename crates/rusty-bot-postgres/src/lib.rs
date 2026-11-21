//! PostgreSQL session persistence for the Temporal engine.
//!
//! Implements the SDK's `rusty_bot_session::SessionStorage` contract. The host
//! application supplies a connection pool and explicitly initializes the schema;
//! this crate does not configure credentials or create databases.

pub mod sessions;

pub use sessions::PgSessionStorage;
