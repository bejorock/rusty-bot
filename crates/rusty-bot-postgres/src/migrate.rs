//! Migration entry point.
//!
//! Applications explicitly call [`migrate`] against their own pool.

use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Connect to an explicitly supplied DSN with a ten-connection default.
/// For other pool options, construct a `PgPool` directly in the application.
pub async fn connect(dsn: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(dsn)
        .await
}

/// Apply all migrations. Idempotent: sqlx records applied versions.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
