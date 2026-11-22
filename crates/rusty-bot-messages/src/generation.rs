//! Recorded generation facts, separate from message persistence status.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    Refusal,
    Aborted,
    Error,
    Other,
}

/// Counts are optional: unknown is not zero. Detail counts may overlap totals.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    #[serde(default)]
    pub input_details: BTreeMap<String, u64>,
    #[serde(default)]
    pub output_details: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationMetadata {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub response_id: Option<String>,
    pub finish_reason: Option<FinishReason>,
    pub raw_finish_reason: Option<String>,
    pub usage: Option<TokenUsage>,
    pub duration_ms: Option<u64>,
    /// Recorded response facts, not arbitrary options sent back to the model.
    #[serde(default)]
    pub extras: BTreeMap<String, Value>,
}

/// Opaque sensitive provider state. Not encrypted by the SDK and never portable by default.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayState {
    pub provider: String,
    pub model: Option<String>,
    pub format: String,
    pub version: u32,
    pub payload: Value,
    pub required_for_replay: bool,
}
impl std::fmt::Debug for ReplayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplayState([REDACTED])")
    }
}
impl ReplayState {
    /// Model-agnostic state uses None; format/version support must still be explicit.
    pub fn compatible_with(&self, provider: &str, model: &str, format: &str, version: u32) -> bool {
        self.provider == provider
            && self.model.as_deref().is_none_or(|m| m == model)
            && self.format == format
            && self.version == version
    }
    pub(crate) fn validate(&self) -> Result<(), crate::HistoryError> {
        if self.provider.trim().is_empty()
            || self.format.trim().is_empty()
            || self.version == 0
            || self.model.as_ref().is_some_and(|m| m.trim().is_empty())
        {
            return Err(crate::HistoryError("invalid replay state identity".into()));
        }
        Ok(())
    }
}
