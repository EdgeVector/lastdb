use crate::atom::provenance::Provenance;
use crate::schema::types::key_value::KeyValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Records a single field-level change within a mutation.
/// Stored at key "history:{molecule_uuid}:{timestamp_nanos_padded}"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationEvent {
    pub molecule_uuid: String,
    pub timestamp: DateTime<Utc>,
    pub field_key: FieldKey,
    pub old_atom_uuid: Option<String>,
    pub new_atom_uuid: String,
    /// Whether this row changed a tip or only kept a losing imported Put.
    #[serde(default)]
    pub kind: MutationEventKind,
    /// Molecule version at the time this event was recorded
    #[serde(default)]
    pub version: u64,
    /// Whether this event resulted from a merge conflict resolution.
    #[serde(default)]
    pub is_conflict: bool,
    /// The atom UUID that lost the conflict (if `is_conflict` is true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_loser_atom: Option<String>,
    /// Base64-encoded public key of the writer at the time of the mutation.
    #[serde(default)]
    pub writer_pubkey: String,
    /// Base64-encoded Ed25519 signature from the molecule at the time of the mutation.
    #[serde(default)]
    pub signature: String,
    /// Writer identity and verifiability info. Additive during the
    /// `projects/molecule-provenance-dag` migration: propagated from the
    /// originating `Mutation.provenance` when available; `None` otherwise
    /// (including for merge-conflict-originated events). Kept alongside
    /// `writer_pubkey` / `signature` until a follow-up PR removes them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    /// Original source order for an imported Put kept only as history.
    /// Older events omit it; the key digest is not a recoverable clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_order: Option<SourceMutationOrder>,
    /// The Delete that kept this imported Put out of the tip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppressed_by_delete: Option<SuppressedByDelete>,
    /// The tip that kept this imported Put out of the tip: the replayed write
    /// lost last-writer-wins to the tip that this node already held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppressed_by_tip: Option<SuppressedByTip>,
}

/// Old history rows are tip transitions. A suppressed Put keeps its atom,
/// but it never changed the local tip.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationEventKind {
    #[default]
    Transition,
    SuppressedPut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMutationOrder {
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
    /// Sync device that wrote the source Put. Empty when it is not known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin_device: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuppressedByDelete {
    pub mk_key: String,
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
}

/// The tip that won against a replayed Put (see `suppressed_by_tip`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuppressedByTip {
    pub atom_uuid: String,
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
    /// Sync device that wrote the winning tip. Empty when it is not known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin_device: String,
}

/// Identifies which slot in the molecule was changed.
///
/// Current flat object matching [`KeyValue`]. Older tagged key objects are refused.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldKey {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
}

impl FieldKey {
    #[must_use]
    pub fn single() -> Self {
        Self {
            hash: None,
            range: None,
        }
    }

    #[must_use]
    pub fn hash(hash: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: None,
        }
    }

    #[must_use]
    pub fn range(range: impl Into<String>) -> Self {
        Self {
            hash: None,
            range: Some(range.into()),
        }
    }

    #[must_use]
    pub fn hash_range(hash: impl Into<String>, range: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: Some(range.into()),
        }
    }

    /// True when this key identifies the same slot as `kv`.
    #[must_use]
    pub fn matches_key_value(&self, kv: &KeyValue) -> bool {
        self.hash.as_deref() == kv.hash.as_deref() && self.range.as_deref() == kv.range.as_deref()
    }

    /// True for the Single-schema (no key dimensions) slot.
    #[must_use]
    pub fn is_single(&self) -> bool {
        self.hash.is_none() && self.range.is_none()
    }
}

impl From<KeyValue> for FieldKey {
    fn from(kv: KeyValue) -> Self {
        Self {
            hash: kv.hash,
            range: kv.range,
        }
    }
}
