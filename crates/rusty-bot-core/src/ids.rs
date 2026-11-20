use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A global numeric row identifier. All record kinds share one namespace.
pub type Id = i64;
/// A global commit sequence number.
pub type Seq = i64;
/// Epoch milliseconds.
pub type TimestampMs = i64;

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Id);

        impl $name {
            pub const fn get(self) -> Id {
                self.0
            }
        }

        impl From<Id> for $name {
            fn from(value: Id) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Id {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(
    /// Identifies a conversation (transcript lineage).
    ConversationId
);
id_type!(
    /// Identifies an immutable transcript entry.
    EntryId
);
id_type!(
    /// Identifies a durable task.
    TaskId
);
id_type!(
    /// Identifies a prompt submission.
    SubmissionId
);
id_type!(
    /// Identifies a document incarnation.
    DocumentId
);

/// A stable UUID identifies a session independently of its embedding application.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for SessionId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

/// A conversation fork parent reference. Forks are not wired in v1; the record
/// shape exists so stored history can be extended without a migration rewrite.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParentRef {
    pub conversation_id: ConversationId,
    pub at: EntryId,
}

/// Cross-reference from a conversation to the task that owns it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnerRef {
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
}
