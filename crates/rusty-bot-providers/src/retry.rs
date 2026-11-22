use crate::{ModelError, ModelErrorKind};
use std::time::{Duration, SystemTime};

/// Retry count is in addition to the initial attempt. Backoff is exponential,
/// capped at `max_backoff`; a server's Retry-After is never capped.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            retries: 3,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(5),
        }
    }
}
impl RetryPolicy {
    pub(crate) fn delay(&self, retry: u32, server: Option<Duration>) -> Duration {
        let factor = 1u32.checked_shl(retry).unwrap_or(u32::MAX);
        let backoff = self
            .initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff);
        server.map_or(backoff, |s| s.max(backoff))
    }
}
pub(crate) struct AttemptError {
    pub error: ModelError,
    pub retry_after: Option<Duration>,
}
impl From<ModelError> for AttemptError {
    fn from(error: ModelError) -> Self {
        Self {
            error,
            retry_after: None,
        }
    }
}
impl AttemptError {
    pub fn retryable(&self) -> bool {
        self.error.retryable
            && matches!(
                self.error.kind,
                ModelErrorKind::Network | ModelErrorKind::Server | ModelErrorKind::RateLimited
            )
    }
}
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
}

pub(crate) async fn sleep(delay: Duration) {
    if let Some(deadline) = tokio::time::Instant::now().checked_add(delay) {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}
