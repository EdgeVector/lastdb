//! Molecule records of the atom store: the per-key row and the generation rows,
//! with the pure rules that merge them.

use crate::atom::{incoming_wins_lww, AtomEntry, KeyMetadata, MoleculeHashRange};
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

/// The live-write winner rule between two per-key rows.
pub(crate) fn incoming_record_wins(incoming: &PerKeyRecord, current: &PerKeyRecord) -> bool {
    incoming_wins_lww(incoming.entry.lww_key(), current.entry.lww_key())
}

/// Select one logical slot from the immutable base and the live change row.
pub(crate) fn merge_generation_slot(
    live: Option<PerKeyRecord>,
    base: Option<MoleculeGenerationSlot>,
) -> Option<PerKeyRecord> {
    match base {
        None
        | Some(MoleculeGenerationSlot {
            record: None,
            shadow: None,
        }) => live,
        Some(MoleculeGenerationSlot {
            record: Some(base), ..
        }) => match live {
            Some(live) if incoming_record_wins(&live, &base) => Some(live),
            _ => Some(base),
        },
        Some(MoleculeGenerationSlot {
            record: None,
            shadow: Some(shadow),
        }) => {
            live.filter(|candidate| incoming_wins_lww(candidate.entry.lww_key(), shadow.lww_key()))
        }
    }
}

pub(crate) fn apply_generation_delete(
    record: Option<PerKeyRecord>,
    deletion: Option<MoleculeGenerationDelete>,
) -> Option<PerKeyRecord> {
    match (record, deletion) {
        (Some(record), Some(deletion))
            if !incoming_wins_lww(record.entry.lww_key(), deletion.shadow.lww_key()) =>
        {
            None
        }
        (record, _) => record,
    }
}
