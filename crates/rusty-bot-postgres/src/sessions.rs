//! Shared session adapter implementing the Temporal engine's storage contract.
use async_trait::async_trait;
use rusty_bot_session::{
    SessionChange, SessionStorage, StorageError, StorageResult, StoredSession,
};
use sqlx::{types::Json, PgPool};

#[derive(Clone)]
pub struct PgSessionStorage {
    pool: PgPool,
}
impl PgSessionStorage {
    /// Caller owns pool construction, authentication, TLS policy and migrations.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    /// Explicit, additive schema initialization. Never creates a database automatically.
    pub async fn migrate(&self) -> StorageResult<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS rusty_bot_sessions (id TEXT PRIMARY KEY, document JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())")
            .execute(&self.pool).await.map_err(backend)?;
        Ok(())
    }
}
fn backend(error: sqlx::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}

#[async_trait]
impl SessionStorage for PgSessionStorage {
    async fn create(&self, session: StoredSession) -> StorageResult<StoredSession> {
        session.config.validate()?;
        sqlx::query("INSERT INTO rusty_bot_sessions(id, document) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
            .bind(&session.config.id).bind(Json(&session)).execute(&self.pool).await.map_err(backend)?;
        let old = self.read(&session.config.id).await?;
        if old.config != session.config {
            return Err(StorageError::Conflict(
                "session configuration mismatch".into(),
            ));
        }
        Ok(old)
    }
    async fn read(&self, id: &str) -> StorageResult<StoredSession> {
        sqlx::query_scalar::<_, Json<StoredSession>>(
            "SELECT document FROM rusty_bot_sessions WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .map(|v| v.0)
        .ok_or(StorageError::NotFound)
    }
    async fn change(&self, id: &str, change: SessionChange) -> StorageResult<StoredSession> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let Json(mut session) = sqlx::query_scalar::<_, Json<StoredSession>>(
            "SELECT document FROM rusty_bot_sessions WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(StorageError::NotFound)?;
        session.apply(&change)?;
        sqlx::query(
            "UPDATE rusty_bot_sessions SET document = $2, updated_at = now() WHERE id = $1",
        )
        .bind(id)
        .bind(Json(&session))
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(session)
    }
}
