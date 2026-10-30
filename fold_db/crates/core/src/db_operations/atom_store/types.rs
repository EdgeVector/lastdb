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

/// Range coordinates in one transient query molecule, independent of its codec.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MoleculeRangeForm {
    /// Exact narrowed reads retain the caller's original range.
    Api,
    /// Ordered walks retain stored ranges until the field reader projects them.
    Storage,
}

// The slot layout values live in the molecule model so the schema field
// types can name them without a dependency on this store.
pub(crate) use crate::atom::molecule_slot::{
    ChangedKey, FilterLayout, OneDSlot, EMPTY_KEY_COMPONENT,
};

/// One stored `mk:{M}:{key}` record: a key's atom entry plus its optional
/// per-key metadata. The key itself is encoded into the storage key by
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
