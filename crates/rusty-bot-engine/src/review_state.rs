//! Private persisted review foundation. Values can contain sensitive configuration.
use rusty_bot_core::{
    DocumentAddress, DocumentPoint, DocumentQuery, DocumentScope, PendingReview, ReviewAnswer,
    SessionError, SessionId, Storage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    registry::{Registry, ResolvedExtensions, ReviewGateDescriptor},
    session::SessionConfig,
};

pub(crate) const POLICY: &str = "rusty-bot.review.policy";
pub(crate) const PENDING: &str = "rusty-bot.review.pending";
pub(crate) const RECEIPT: &str = "rusty-bot.review.receipt";

pub(crate) fn receipt_address(id: SessionId, review: &rusty_bot_core::ReviewId) -> DocumentAddress {
    DocumentAddress::new(RECEIPT, DocumentScope::Session(id), &review.0)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ToolSnapshot {
    pub(crate) spec: rusty_bot_core::ToolSpec,
    pub(crate) replay: rusty_bot_core::ReplayPolicy,
    pub(crate) mode: rusty_bot_core::ExecutionMode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PolicySnapshot {
    version: u32,
    enabled: bool,
    gate: Option<ReviewGateDescriptor>,
    hooks: Vec<rusty_bot_core::HookDescriptor>,
    config: SessionConfig,
    provider: String,
    workspace: Value,
    tools: Vec<ToolSnapshot>,
    model_visible: Vec<String>,
}

impl PolicySnapshot {
    pub(crate) fn new(
        enabled: bool,
        config: &SessionConfig,
        resolved: &ResolvedExtensions,
        registry: &Registry,
        provider: &str,
        workspace: Value,
        hooks: Vec<rusty_bot_core::HookDescriptor>,
    ) -> Result<Self, SessionError> {
        let gate = if enabled {
            resolved
                .selected_review_gate()?
                .map(|g| g.descriptor.clone())
        } else {
            None
        };
        let tools = registry.resolve_tool_set(resolved, &config.tools, &config.model_tools);
        if enabled && registry.contains_tool("ask_user") {
            return Err(SessionError::InvalidConfig(
                "reserved review tool name".into(),
            ));
        }
        if enabled && config.model.provider != provider {
            return Err(SessionError::InvalidConfig(
                "review provider mismatch".into(),
            ));
        }
        Ok(Self {
            version: 1,
            enabled,
            gate,
            hooks,
            config: config.clone(),
            provider: provider.into(),
            workspace,
            tools: tools
                .tools
                .iter()
                .map(|t| ToolSnapshot {
                    spec: t.spec(),
                    replay: t.replay(),
                    mode: t.execution_mode(),
                })
                .collect(),
            model_visible: tools.model_visible,
        })
    }
}

pub(crate) fn address(kind: &str, id: SessionId) -> DocumentAddress {
    DocumentAddress::new(kind, DocumentScope::Session(id), "")
}

pub(crate) async fn check_policy(
    storage: &dyn Storage,
    id: SessionId,
    expected: &PolicySnapshot,
) -> Result<(), SessionError> {
    let stored = storage
        .get_document(&address(POLICY, id), DocumentPoint::Current)
        .await?;
    match stored {
        None if !expected.enabled => Ok(()),
        None => Err(SessionError::ReviewConfigMismatch),
        Some(doc) => {
            let stored: PolicySnapshot = serde_json::from_value(doc.value)
                .map_err(|_| SessionError::ReviewConfigMismatch)?;
            if stored.version != 1
                || stored.enabled != expected.enabled
                || (stored.enabled && stored != *expected)
            {
                return Err(SessionError::ReviewConfigMismatch);
            }
            Ok(())
        }
    }
}

pub(crate) async fn guard_manual(
    storage: &dyn Storage,
    id: SessionId,
    enabled: bool,
) -> Result<(), SessionError> {
    if enabled {
        return Err(SessionError::InvalidState(
            "review-enabled session requires the SDK review driver".into(),
        ));
    }
    let pending = storage
        .get_document(&address(PENDING, id), DocumentPoint::Current)
        .await?;
    if pending.is_some() {
        return Err(SessionError::InvalidState(
            "review state requires the SDK review driver".into(),
        ));
    }
    let receipts = storage
        .scan_documents(DocumentQuery {
            kind: Some(RECEIPT.into()),
            scope: Some(DocumentScope::Session(id)),
            key: None,
            at: DocumentPoint::Current,
            limit: usize::MAX,
        })
        .await?;
    // Storage scans are capped at 1,000 without pagination. Never assume a
    // potentially truncated receipt set contains no active continuation.
    if receipts.len() == 1000 {
        return Err(SessionError::InvalidState(
            "review receipt scan is incomplete".into(),
        ));
    }
    #[derive(Deserialize)]
    struct GuardReceipt {
        version: u32,
        disposition: ReceiptDisposition,
    }
    for doc in receipts {
        let receipt: GuardReceipt = serde_json::from_value(doc.value)
            .map_err(|_| SessionError::InvalidState("invalid review receipt".into()))?;
        if receipt.version != 1 || receipt.disposition == ReceiptDisposition::AcceptedInProgress {
            return Err(SessionError::InvalidState(
                "review state requires the SDK review driver".into(),
            ));
        }
    }
    Ok(())
}

/// Hash canonical JSON: object keys sorted recursively; arrays retain their order.
/// The resume path must also compare the structured binding, not only this digest.
#[allow(dead_code)]
pub(crate) fn fingerprint(value: &Value) -> Result<String, SessionError> {
    use sha2::{Digest, Sha256};
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> =
                    map.iter().map(|(k, v)| (k.clone(), canonical(v))).collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
            other => other.clone(),
        }
    }
    let bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| SessionError::Internal("review fingerprint serialization failed".into()))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// The independently checked inputs covered by the pending fingerprint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReviewBinding {
    pub pending: PendingReview,
    pub assistant_entry: rusty_bot_core::EntryId,
    pub original_call: rusty_bot_core::ToolCall,
    pub final_arguments: Value,
    pub tool: ToolSnapshot,
    pub policy: PolicySnapshot,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingRecord {
    pub version: u32,
    pub pending: PendingReview,
    pub assistant_entry: rusty_bot_core::EntryId,
    pub original_arguments: Value,
    pub final_arguments: Value,
    pub binding: ReviewBinding,
    pub fingerprint: String,
}
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReceiptRecord {
    pub version: u32,
    pub answer: ReviewAnswer,
    pub pending: PendingRecord,
    pub disposition: ReceiptDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_output: Option<rusty_bot_core::ToolOutput>,
    /// Immutable acceptance data; disposition intentionally changes as the run progresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<ReceiptIntegrity>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReceiptIntegrity {
    pub version: u32,
    pub digest: String,
}

impl ReceiptRecord {
    fn acceptance_digest(&self) -> Result<String, SessionError> {
        // Domain and format version prevent confusing this digest with the pending binding.
        fingerprint(&serde_json::json!({
            "domain": "rusty-bot.review.acceptance",
            "version": 1,
            "receipt_version": self.version,
            "pending": self.pending,
            "answer": self.answer,
            "result_output": self.result_output,
        }))
    }

    pub(crate) fn seal(&mut self) -> Result<(), SessionError> {
        self.integrity = Some(ReceiptIntegrity {
            version: 1,
            digest: self.acceptance_digest()?,
        });
        Ok(())
    }

    pub(crate) fn check_integrity(&self) -> Result<(), SessionError> {
        match &self.integrity {
            Some(seal) if seal.version == 1 && seal.digest == self.acceptance_digest()? => Ok(()),
            // Historical completed receipts remain readable; accepted continuations without a
            // binding cannot safely be replayed. Unknown present versions always fail closed.
            None if self.disposition != ReceiptDisposition::AcceptedInProgress => Ok(()),
            _ => Err(SessionError::InvalidState(
                "invalid review receipt integrity".into(),
            )),
        }
    }
}
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ReceiptDisposition {
    AcceptedInProgress,
    Completed,
    TerminalFailure,
}
