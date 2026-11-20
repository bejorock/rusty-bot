use std::time::{SystemTime, UNIX_EPOCH};

use crate::ids::TimestampMs;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> TimestampMs;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> TimestampMs {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as TimestampMs)
            .unwrap_or(0)
    }
}
