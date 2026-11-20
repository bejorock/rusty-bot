use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::tool::Tool;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    pub key: String,
    pub content: String,
}

/// A Rust-native extension. Pi TypeScript extensions are not executed in this
/// process; that is a tracked parity gap, not silently emulated.
pub trait Extension: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn tools(&self) -> Vec<Arc<dyn Tool>>;
    fn sections(&self) -> Vec<PromptSection>;
    fn hooks(&self) -> Vec<Arc<dyn crate::Hook>> {
        Vec::new()
    }
    fn hook_config(&self, _hook_id: &str) -> serde_json::Value {
        serde_json::Value::Null
    }
    fn review_gates(&self) -> Vec<Arc<dyn crate::ReviewGate>> {
        Vec::new()
    }
    fn review_gate_config(&self, _gate_id: &str) -> serde_json::Value {
        serde_json::Value::Null
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("duplicate extension: {0}")]
    DuplicateExtension(String),
    #[error("invalid extension or hook identity: {0}")]
    InvalidIdentity(String),
    #[error("duplicate hook: {0}")]
    DuplicateHook(String),
    #[error("duplicate review gate: {0}")]
    DuplicateReviewGate(String),
    #[error("more than one review gate selected")]
    MultipleReviewGates,
    #[error("invalid prompt section key: {0}")]
    InvalidSectionKey(String),
    #[error("registry selection refers to unknown extension: {0}")]
    UnknownExtension(String),
}
