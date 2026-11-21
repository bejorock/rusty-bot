//! Redis implementation of the shared `SessionStorage` contract.
//! The caller owns connection credentials, TLS, timeouts and Redis persistence policy.
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use rusty_bot_session::{
    SessionChange, SessionStorage, StorageError, StorageResult, StoredSession,
};

// Compare bytes, not JSON decoded by Lua: preserve u64 sequences and exact Rust transitions.
const COMPARE_AND_SET: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
    return 1
end
return 0
"#;
const MAX_ATTEMPTS: usize = 64;

#[derive(Clone)]
pub struct RedisSessionStorage {
    connection: MultiplexedConnection,
    prefix: String,
}
impl RedisSessionStorage {
    /// Each session occupies one non-expiring string key under a caller-owned prefix.
    /// The supplied connection may be shared; no connection-local WATCH state is used.
    pub fn new(connection: MultiplexedConnection, prefix: String) -> StorageResult<Self> {
        validate_prefix(&prefix)?;
        Ok(Self { connection, prefix })
    }

    /// Exact key for a session, useful for application-managed deletion/retention.
    /// IDs are UTF-8 hex encoded so arbitrary IDs cannot collide or escape the prefix.
    pub fn session_key(&self, id: &str) -> String {
        session_key(&self.prefix, id)
    }

    async fn document(&self, key: &str) -> StorageResult<String> {
        let mut connection = self.connection.clone();
        redis::cmd("GET")
            .arg(key)
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(backend)?
            .ok_or(StorageError::NotFound)
    }
}
fn validate_prefix(prefix: &str) -> StorageResult<()> {
    if prefix.trim().is_empty() {
        return Err(StorageError::Conflict("blank Redis key prefix".into()));
    }
    Ok(())
}
fn session_key(prefix: &str, id: &str) -> String {
    use std::fmt::Write;
    let mut key = format!("{prefix}:session:");
    for byte in id.as_bytes() {
        write!(key, "{byte:02x}").expect("writing to String cannot fail");
    }
    key
}
fn backend(_: redis::RedisError) -> StorageError {
    // Redis errors can contain command arguments/server-supplied text. Never echo secrets
    // or serialized conversation content through the public error boundary.
    StorageError::Backend(
        "Redis command failed; check connectivity, authentication and ACLs".into(),
    )
}
fn decode(document: &str) -> StorageResult<StoredSession> {
    serde_json::from_str(document)
        .map_err(|_| StorageError::Backend("invalid Redis session document".into()))
}
fn encode(session: &StoredSession) -> StorageResult<String> {
    serde_json::to_string(session)
        .map_err(|_| StorageError::Backend("cannot serialize Redis session document".into()))
}

#[async_trait]
impl SessionStorage for RedisSessionStorage {
    async fn create(&self, session: StoredSession) -> StorageResult<StoredSession> {
        session.config.validate()?;
        let key = self.session_key(&session.config.id);
        let mut connection = self.connection.clone();
        let _: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(encode(&session)?)
            .arg("NX")
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        let old = decode(&self.document(&key).await?)?;
        if old.config != session.config {
            return Err(StorageError::Conflict(
                "session configuration mismatch".into(),
            ));
        }
        Ok(old)
    }

    async fn read(&self, id: &str) -> StorageResult<StoredSession> {
        decode(&self.document(&self.session_key(id)).await?)
    }

    async fn change(&self, id: &str, change: SessionChange) -> StorageResult<StoredSession> {
        let key = self.session_key(id);
        for _ in 0..MAX_ATTEMPTS {
            let original = self.document(&key).await?;
            let mut session = decode(&original)?;
            session.apply(&change)?;
            let mut connection = self.connection.clone();
            let applied: bool = redis::cmd("EVAL")
                .arg(COMPARE_AND_SET)
                .arg(1)
                .arg(&key)
                .arg(&original)
                .arg(encode(&session)?)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            if applied {
                return Ok(session);
            }
            // A different process won: re-read and re-validate against its committed state.
            tokio::task::yield_now().await;
        }
        Err(StorageError::Backend(
            "Redis session contention retry limit reached".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_unambiguous_for_unicode_and_delimiters() {
        assert_eq!(session_key("app", "a:é"), "app:session:613ac3a9");
        assert_ne!(session_key("app", "a"), session_key("app", "61"));
        assert!(validate_prefix(" \n").is_err());
        assert!(validate_prefix("app:test").is_ok());
    }

    #[test]
    fn corrupt_documents_and_backend_errors_do_not_leak_content() {
        let error = decode("{private conversation").unwrap_err().to_string();
        assert!(!error.contains("private"));
        let error = backend(redis::RedisError::from((redis::ErrorKind::Io, "secret")));
        assert!(!error.to_string().contains("secret"));
    }
}
