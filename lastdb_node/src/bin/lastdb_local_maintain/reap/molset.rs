//! The molecule sets of the plan: E1, L, D and the molecules the plan keeps.
//!
//! - E1: molecules that the drop receipts and the dropped names name.
//! - L: molecules that a live schema uses.
//! - D: the dead molecules whose rows the plan drops. D is E1 minus L, minus
//!   the molecules of a protein that has a member outside the dead set.
//!
//! Molecule ids have two spellings. Every set here is keyed by the 32-byte
//! digest of the id, never by the text.

use std::collections::{BTreeMap, BTreeSet};

use fold_db::atom::molecule_uuid::{encode_molecule_uuid_bytes, encode_molecule_uuid_hex};
use fold_db::db_operations::SchemaDropReceipt;
use fold_db::record_molecule::record_molecule_uuid;

use super::identities::Identities;
use super::keys::{mol_key, MolKey};
use super::proteins::ProteinIndex;
use super::tripwire::{name_drop_times, note_drop, RECEIPTLESS_DROP_MS};
use super::ReapError;

/// A set of molecules with every spelling seen for each.
pub(crate) type SpellingMap = BTreeMap<MolKey, BTreeSet<String>>;

/// Add `id` and its other spelling to `map`.
pub(crate) fn add_id(map: &mut SpellingMap, id: &str) {
    let key = mol_key(id);
    let spellings = map.entry(key).or_default();
    spellings.insert(id.to_string());
    if fold_db::atom::molecule_uuid::parse_molecule_uuid_bytes(id).is_some() {
        spellings.insert(encode_molecule_uuid_bytes(&key));
        spellings.insert(encode_molecule_uuid_hex(&key));
    }
}

/// A molecule kept out of D, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Kept {
    pub spellings: BTreeSet<String>,
    /// Free text: the live schema or the protein that causes the keep.
    pub detail: String,
}

/// Inputs of [`compute`].
pub(crate) struct Inputs<'a> {
    /// The receipts of the dropped names.
    pub receipts: &'a [&'a SchemaDropReceipt],
    /// The dropped names. Every spelling of each name is a name.
    pub ids: &'a Identities,
    /// L, used to subtract.
    pub live: &'a BTreeSet<MolKey>,
    /// L again, used by the final disjoint check. In production it is the same
    /// set as `live`. A test passes a different set to force the check.
    pub live_for_check: &'a BTreeSet<MolKey>,
    /// Which live schemas use each live molecule, for the keep list.
    pub live_owners: &'a BTreeMap<MolKey, Vec<String>>,
    pub proteins: &'a ProteinIndex,
}

/// The result of [`compute`].
#[derive(Debug, Default)]
pub(crate) struct MoleculeSets {
    pub e1: SpellingMap,
    pub dead: SpellingMap,
    pub shared_with_live: BTreeMap<MolKey, Kept>,
    pub protein_mixed: BTreeMap<MolKey, Kept>,
    /// The drop time in milliseconds of each molecule of E1. The tripwire reads it.
    pub drops: BTreeMap<MolKey, u64>,
}

/// Build E1 and D. Abort when D meets L.
pub(crate) fn compute(inputs: &Inputs<'_>) -> Result<MoleculeSets, ReapError> {
    let mut sets = MoleculeSets::default();
    for receipt in inputs.receipts {
        for id in &receipt.field_molecule_uuids {
            add_id(&mut sets.e1, id);
            note_drop(&mut sets.drops, id, receipt.dropped_at_unix_ms);
        }
    }
    let name_drops = name_drop_times(inputs.receipts, inputs.ids);
    for name in &inputs.ids.spellings {
        let record = record_molecule_uuid(name);
        add_id(&mut sets.e1, &record);
        let dropped = name_drops.get(name).copied().unwrap_or(RECEIPTLESS_DROP_MS);
        note_drop(&mut sets.drops, &record, dropped);
    }

    let mut dead: SpellingMap = BTreeMap::new();
    for (key, spellings) in &sets.e1 {
        if inputs.live.contains(key) {
            let owners = inputs.live_owners.get(key).cloned().unwrap_or_default();
            sets.shared_with_live.insert(
                *key,
                Kept {
                    spellings: spellings.clone(),
                    detail: format!("live:{}", owners.join(",")),
                },
            );
        } else {
            dead.insert(*key, spellings.clone());
        }
    }

    sets.protein_mixed = split_protein_mixed(&mut dead, inputs.proteins);
    sets.dead = dead;
    check_disjoint(&sets.dead, inputs.live_for_check)?;
    Ok(sets)
}

/// Gate: no dead molecule may be a live molecule.
pub(crate) fn check_disjoint(dead: &SpellingMap, live: &BTreeSet<MolKey>) -> Result<(), ReapError> {
    let overlap: Vec<String> = dead
        .iter()
        .filter(|(key, _)| live.contains(*key))
        .map(|(_, spellings)| spellings.iter().next().cloned().unwrap_or_default())
        .collect();
    if overlap.is_empty() {
        return Ok(());
    }
    Err(ReapError::abort(
        "D_MEETS_L",
        format!(
            "{} dead molecule(s) are also live, first: {}",
            overlap.len(),
            overlap.first().cloned().unwrap_or_default()
        ),
    ))
}

/// Move every dead molecule of a mixed protein out of `dead`.
///
/// A protein is mixed when it has a member outside `dead`. A molecule whose
/// back-reference names a protein with no record is kept too, because the
/// members of that protein are not known. Removing a molecule can make
/// another protein mixed, so the loop runs until nothing moves.
fn split_protein_mixed(dead: &mut SpellingMap, proteins: &ProteinIndex) -> BTreeMap<MolKey, Kept> {
    let mut kept: BTreeMap<MolKey, Kept> = BTreeMap::new();
    for (key, uuids) in &proteins.backrefs {
        if let Some(spellings) = dead.get(key) {
            let unknown: Vec<&String> = uuids
                .iter()
                .filter(|uuid| !proteins.members.contains_key(*uuid))
                .collect();
            if !unknown.is_empty() {
                kept.insert(
                    *key,
                    Kept {
                        spellings: spellings.clone(),
                        detail: format!(
                            "molprot_without_protein_record:{}",
                            unknown
                                .iter()
                                .map(|uuid| uuid.as_str())
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                    },
                );
            }
        }
    }
    for key in kept.keys() {
        dead.remove(key);
    }
    loop {
        let mut moved = false;
        for (uuid, members) in &proteins.members {
            let in_dead: Vec<MolKey> = members
                .iter()
                .filter(|key| dead.contains_key(*key))
                .copied()
                .collect();
            if in_dead.is_empty() || in_dead.len() == members.len() {
                continue;
            }
            for key in in_dead {
                if let Some(spellings) = dead.remove(&key) {
                    kept.insert(
                        key,
                        Kept {
                            spellings,
                            detail: format!("protein:{uuid}"),
                        },
                    );
                    moved = true;
                }
            }
        }
        if !moved {
            return kept;
        }
    }
}
