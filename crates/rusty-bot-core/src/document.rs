use serde::{Deserialize, Serialize};

use crate::ids::{ConversationId, DocumentId, SessionId, TaskId, TimestampMs};

/// Documents attach to a session, conversation, or task.
///
/// Serialized as `{"scope_kind":"session","id":"<uuid>"}` (or an integer id for
/// conversation/task). The representation is implemented manually because an
/// internally tagged enum cannot serialize a newtype variant whose payload is a
/// string, which is exactly how a UUID session id serializes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DocumentScope {
    Session(SessionId),
    Conversation(ConversationId),
    Task(TaskId),
}

impl DocumentScope {
    pub fn owner_id(&self) -> i64 {
        match self {
            DocumentScope::Session(id) => id.0.as_u128() as i64,
            DocumentScope::Conversation(id) => id.0,
            DocumentScope::Task(id) => id.0,
        }
    }
}

impl Serialize for DocumentScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        match self {
            DocumentScope::Session(id) => {
                map.serialize_entry("scope_kind", "session")?;
                map.serialize_entry("id", &id.0.to_string())?;
            }
            DocumentScope::Conversation(id) => {
                map.serialize_entry("scope_kind", "conversation")?;
                map.serialize_entry("id", &id.0)?;
            }
            DocumentScope::Task(id) => {
                map.serialize_entry("scope_kind", "task")?;
                map.serialize_entry("id", &id.0)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for DocumentScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "scope_kind", rename_all = "snake_case")]
        enum Helper {
            Session { id: String },
            Conversation { id: i64 },
            Task { id: i64 },
        }

        match Helper::deserialize(deserializer)? {
            Helper::Session { id } => uuid::Uuid::parse_str(&id)
                .map(|uuid| DocumentScope::Session(SessionId::from(uuid)))
                .map_err(serde::de::Error::custom),
            Helper::Conversation { id } => Ok(DocumentScope::Conversation(ConversationId(id))),
            Helper::Task { id } => Ok(DocumentScope::Task(TaskId(id))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentAddress {
    pub kind: String,
    pub scope: DocumentScope,
    /// Address key within the scope; empty for singletons.
    pub key: String,
    /// A family document changes its address key when rewritten.
    pub family: bool,
}

impl DocumentAddress {
    pub fn new(kind: impl Into<String>, scope: DocumentScope, key: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            scope,
            key: key.into(),
            family: false,
        }
    }

    pub fn family(mut self) -> Self {
        self.family = true;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentPoint {
    Current,
    Seq(crate::ids::Seq),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DocumentWrite {
    PutLatest {
        address: DocumentAddress,
        value: serde_json::Value,
    },
    PutRewindable {
        address: DocumentAddress,
        fork: DocumentFork,
        value: serde_json::Value,
    },
    /// v1 stores ops as JSON and applies them in application code:
    /// `{"set": value}` replaces the value, `{"merge": {...}}` shallow-merges an
    /// object, and any other value replaces the value directly.
    Delta {
        address: DocumentAddress,
        ops: serde_json::Value,
    },
    Retire {
        address: DocumentAddress,
    },
}

impl DocumentWrite {
    pub fn address(&self) -> &DocumentAddress {
        match self {
            DocumentWrite::PutLatest { address, .. }
            | DocumentWrite::PutRewindable { address, .. }
            | DocumentWrite::Delta { address, .. }
            | DocumentWrite::Retire { address } => address,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentFork {
    Initial,
    AsOf(crate::ids::Seq),
    Current,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentValue {
    pub id: DocumentId,
    pub address: DocumentAddress,
    pub version: u32,
    pub value: serde_json::Value,
    pub created_at: TimestampMs,
    pub retired_at: Option<TimestampMs>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocumentQuery {
    pub kind: Option<String>,
    pub scope: Option<DocumentScope>,
    pub key: Option<String>,
    pub at: DocumentPoint,
    pub limit: usize,
}
