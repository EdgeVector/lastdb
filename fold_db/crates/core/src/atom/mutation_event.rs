use crate::atom::provenance::Provenance;
use crate::schema::types::key_value::KeyValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuppressedByDelete {
    pub mk_key: String,
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
}

/// Identifies which slot in the molecule was changed.
///
/// Unified shape matching [`KeyValue`]: `(hash, range)` with either component
/// optional. Serializes as a flat object; deserializes both the new form and
/// the legacy four-way enum (`Single` / `Hash` / `Range` / `HashRange`) so
/// on-disk mutation-event history remains readable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldKey {
    pub hash: Option<String>,
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

impl Serialize for FieldKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Flat KeyValue-shaped object — the unified on-the-wire form.
        #[derive(Serialize)]
        struct Flat<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            hash: &'a Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            range: &'a Option<String>,
        }
        Flat {
            hash: &self.hash,
            range: &self.range,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FieldKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        // New flat form: {"hash":..,"range":..} (either/both optional, including {})
        if let Some(obj) = value.as_object() {
            let has_legacy_tag = obj.contains_key("Single")
                || obj.contains_key("Hash")
                || obj.contains_key("Range")
                || obj.contains_key("HashRange");
            if !has_legacy_tag
                && (obj.is_empty()
                    || obj.contains_key("hash")
                    || obj.contains_key("range")
                    || obj.keys().all(|k| k == "hash" || k == "range"))
            {
                let hash = obj
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string);
                let range = obj
                    .get("range")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string);
                // Reject unknown keys in the flat form for safety.
                if obj.keys().all(|k| k == "hash" || k == "range") {
                    return Ok(Self { hash, range });
                }
            }
        }

        // Legacy externally-tagged enum forms.
        // "Single" | {"Single":null} | {"Hash":{"hash":"x"}} | ...
        if value.as_str() == Some("Single") {
            return Ok(Self::single());
        }
        if let Some(obj) = value.as_object() {
            if obj.contains_key("Single") {
                return Ok(Self::single());
            }
            if let Some(inner) = obj.get("Hash") {
                let hash = inner
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::custom("FieldKey::Hash missing hash"))?
                    .to_string();
                return Ok(Self::hash(hash));
            }
            if let Some(inner) = obj.get("Range") {
                let range = inner
                    .get("range")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::custom("FieldKey::Range missing range"))?
                    .to_string();
                return Ok(Self::range(range));
            }
            if let Some(inner) = obj.get("HashRange") {
                let hash = inner
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::custom("FieldKey::HashRange missing hash"))?
                    .to_string();
                let range = inner
                    .get("range")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::custom("FieldKey::HashRange missing range"))?
                    .to_string();
                return Ok(Self::hash_range(hash, range));
            }
        }

        Err(serde::de::Error::custom(format!(
            "unrecognized FieldKey form: {value}"
        )))
    }
}
