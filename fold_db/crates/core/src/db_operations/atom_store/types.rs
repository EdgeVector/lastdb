//! Molecule/atom storage types used by [`super::AtomStore`].

use crate::atom::{AtomEntry, KeyMetadata, MoleculeHashRange};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The one on-disk molecule shape after the one-molecule-kind unify.
///
/// Every field — including a `Single`-typed field, which stores its lone
/// atom under the empty `("", "")` key — persists as a keyed
/// [`MoleculeHashRange`] under `mk:` / `mh:`. The product path never loads
/// or parses legacy `ref:{M}` whole-molecule blobs (delete-train retired the
/// sync migrate-on-receive dual codec).
///
/// `MoleculeData` is a historical alias for call sites that still say
/// "molecule data"; prefer [`MoleculeHashRange`] in new code.
pub type MoleculeData = MoleculeHashRange;

impl MoleculeHashRange {
    /// Normalize 1-D slot orientation for Hash-only / Range-only fields.
    ///
    /// Hash-only slots are `(h, "")`; range-only are `("", r)`. A Range field
    /// reprojects mis-oriented hash-only slots to range-only (and vice versa
    /// for Hash). Correct slots and full HashRange entries are left alone.
    /// `None` leaves the molecule unchanged.
    pub(crate) fn retyped_to_slot(self, slot: Option<OneDSlot>) -> Self {
        match slot {
            Some(OneDSlot::Range) => {
                // Only reproject hash-only → range-only. Leave `("", r)` intact.
                let records: Vec<_> = self
                    .per_key_records()
                    .into_iter()
                    .map(|(h, r, e, meta)| {
                        if r.is_empty() && !h.is_empty() {
                            (EMPTY_KEY_COMPONENT.to_string(), h, e, meta)
                        } else {
                            (h, r, e, meta)
                        }
                    })
                    .collect();
                Self::from_per_key_records(
                    self.uuid().to_string(),
                    self.version(),
                    self.updated_at(),
                    records,
                )
            }
            Some(OneDSlot::Hash) => {
                // Only reproject range-only → hash-only. Leave `(h, "")` intact.
                let records: Vec<_> = self
                    .per_key_records()
                    .into_iter()
                    .map(|(h, r, e, meta)| {
                        if h.is_empty() && !r.is_empty() {
                            (r, EMPTY_KEY_COMPONENT.to_string(), e, meta)
                        } else {
                            (h, r, e, meta)
                        }
                    })
                    .collect();
                Self::from_per_key_records(
                    self.uuid().to_string(),
                    self.version(),
                    self.updated_at(),
                    records,
                )
            }
            None => self,
        }
    }
}

/// One-dimensional slot layout for narrowed Hash-only / Range-only filter loads
/// and for retyping 1-D slot orientation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OneDSlot {
    /// `(hash, "")` records.
    Hash,
    /// `("", range)` records.
    Range,
}

/// Filter/load layout for a field — independent of the on-disk header kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterLayout {
    /// 1-D hash-only or range-only slots on the unified key layout.
    OneD(OneDSlot),
    /// Full composite HashRange (and Single empty-slot) filter path.
    Composite,
}

/// Empty key component for hash-only or range-only slots in the unified layout.
pub(crate) const EMPTY_KEY_COMPONENT: &str = "";

/// Identifies a single key within a per-key molecule — the unit the live
/// write path persists. Unified shape matching [`KeyValue`] / [`FieldKey`]:
/// optional hash + optional range (missing components are empty string on disk).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ChangedKey {
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl ChangedKey {
    #[must_use]
    pub(crate) fn hash(hash: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: None,
        }
    }

    #[must_use]
    pub(crate) fn range(range: impl Into<String>) -> Self {
        Self {
            hash: None,
            range: Some(range.into()),
        }
    }

    #[must_use]
    pub(crate) fn hash_range(hash: impl Into<String>, range: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: Some(range.into()),
        }
    }

    /// On-disk hash segment (empty string when absent).
    #[must_use]
    pub(crate) fn disk_hash(&self) -> &str {
        self.hash.as_deref().unwrap_or(EMPTY_KEY_COMPONENT)
    }

    /// On-disk range segment (empty string when absent).
    #[must_use]
    pub(crate) fn disk_range(&self) -> &str {
        self.range.as_deref().unwrap_or(EMPTY_KEY_COMPONENT)
    }
}

/// One stored `mk:{M}:{key}` record: a key's atom entry plus its optional
/// per-key metadata. The key itself is encoded into the Sled key by
/// [`molecule_key_codec`], so it is not duplicated in the value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PerKeyRecord {
    pub entry: AtomEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<KeyMetadata>,
}

/// The immutable base value for one slot in a molecule generation.
///
/// `record=None` is a shadow tombstone. `shadow` is the live tip observed
/// while the generation was built. A later live tip must beat that exact
/// winner before it can revive the slot. This preserves concurrent writes
/// without retaining a molecule-wide write barrier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MoleculeGenerationSlot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<PerKeyRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<AtomEntry>,
}

/// The one mutable row in generation activation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MoleculeGenerationPointer {
    pub generation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_generation: Option<String>,
    pub activated_at_unix_nanos: u64,
}

/// A sparse slot deletion recorded after a generation cut.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MoleculeGenerationDelete {
    pub shadow: AtomEntry,
}

/// Derived `HashKey(hash)` fast-path marker.
///
/// A marker with `range` + `record` is authoritative only for hashes currently
/// known to have exactly one range. Missing markers and ambiguous markers both
/// fall back to the `mk:{M}:{hash}\0` prefix scan, so legacy data and multi-range
/// hashes keep full HashKey semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HashKeyLookupRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<PerKeyRecord>,
}

/// The molecule header stored at `mh:{M}`: the molecule-level
/// `(kind, version, updated_at)`, plus a "this molecule lives in the per-key
/// layout" marker (its mere presence).
///
/// The header is intentionally **O(1)**: the HashRange `update_order`
/// (molecule-global, one entry per key, drives `SampleN`) is NOT stored here —
/// it lives in the append log (`mord:{M}:{seq}` entries + the `moc:{M}` count).
/// That split is the linchpin of the filter-aware read path: a point/range
/// lookup reads only this tiny header + the matched `mk:` records, never the
/// O(field) order vector. Pre-split blobs (a HashRange header written before
/// this change) may still carry a `hash_range_order` field — serde ignores it
/// on read, and the next write re-stores the molecule under the split layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MoleculeHeader {
    /// Legacy `kind` field (Hash/Range/HashRange) is ignored on deserialize
    /// and omitted on serialize — storage is always unified HashRange.
    pub version: u64,
    pub updated_at: DateTime<Utc>,
}
