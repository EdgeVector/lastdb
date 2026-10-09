//! Point-get admission from [`laststore::LastStore::load_point`].
//!
//! Tests call this path. Product [`laststore::LastStore::get`] is unchanged.
//! This module does not import `Shard` and does not read
//! `LASTDB_LOGICAL_RESIDENT_SET`.

use super::logical_set::{AtomId, LogicalResidentSet, MoleculeId, ResidentKey, Tip};
use crate::atom::{atom_key_codec, molecule_key_codec, AtomEntry};
use laststore::{collections, LoadedPoint};

/// Loader that copies one storage record and drops an unpinned group.
pub trait PointLoader {
    /// One point load. `collection` is `tips` or `atoms`.
    fn load_point(
        &self,
        collection: &str,
        storage_key: &str,
    ) -> laststore::Result<Option<LoadedPoint>>;
}

#[allow(clippy::use_self)]
impl PointLoader for laststore::LastStore {
    fn load_point(
        &self,
        collection: &str,
        storage_key: &str,
    ) -> laststore::Result<Option<LoadedPoint>> {
        // Inherent LastStore::load_point. `Self::load_point` would recurse.
        laststore::LastStore::load_point(self, collection, storage_key)
    }
}

/// Result of one point admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointGetOutcome {
    /// No live tip. A dirty tombstone also yields this.
    Absent,
    /// One owned tip. `value` is the atom body when the caller asked for it
    /// and the body was present or loaded.
    Present { tip: Tip, value: Option<Vec<u8>> },
}

/// Failure from the loader or from a tip body that is not an atom entry.
#[derive(Debug)]
pub enum PointAdmitError {
    /// [`PointLoader::load_point`] failed.
    Loader(laststore::Error),
    /// Tip bytes were not a molecule per-key record or a bare atom entry.
    InvalidTipBody(String),
}

impl From<laststore::Error> for PointAdmitError {
    fn from(err: laststore::Error) -> Self {
        Self::Loader(err)
    }
}

impl std::fmt::Display for PointAdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loader(err) => write!(f, "point loader: {err}"),
            Self::InvalidTipBody(msg) => write!(f, "invalid tip body: {msg}"),
        }
    }
}

impl std::error::Error for PointAdmitError {}

impl LogicalResidentSet {
    /// Admit one molecule tip, and one atom body when `need_value` is set.
    ///
    /// A resident hit does not call the loader. A dirty tombstone returns
    /// absent and does not admit the disk tip. A tip hit with an atom miss
    /// loads that content id only. Sibling keys stay absent. The call does
    /// not install into [`crate::ResidentGraph`] and does not call
    /// [`crate::ResidentGraph::mark_key_set_complete`].
    pub fn get_point<L: PointLoader>(
        &mut self,
        loader: &L,
        molecule: MoleculeId,
        hash: &str,
        range: &str,
        need_value: bool,
    ) -> Result<PointGetOutcome, PointAdmitError> {
        if self.has_tombstone(molecule, hash, range) {
            return Ok(PointGetOutcome::Absent);
        }

        if let Some(tip) = self.tip(molecule, hash, range).cloned() {
            self.hold_tip(molecule, hash, range);
            if !need_value {
                return Ok(PointGetOutcome::Present { tip, value: None });
            }
            if let Some(value) = self.take_resident_atom(&tip.atom) {
                return Ok(PointGetOutcome::Present {
                    tip,
                    value: Some(value),
                });
            }
            let value = self.load_atom_body(loader, &tip.atom)?;
            return Ok(PointGetOutcome::Present { tip, value });
        }

        let storage_key = tip_storage_key(molecule, hash, range);
        let Some(loaded) = loader.load_point(collections::TIPS, &storage_key)? else {
            return Ok(PointGetOutcome::Absent);
        };
        let Some(body) = loaded.body.as_deref() else {
            return Ok(PointGetOutcome::Absent);
        };
        let tip = tip_from_body(body)?;
        self.store_tip_body(molecule, hash, range, body.to_vec());
        self.admit_tip(molecule, hash.to_string(), range.to_string(), tip.clone());
        if !need_value {
            return Ok(PointGetOutcome::Present { tip, value: None });
        }
        if let Some(value) = self.take_resident_atom(&tip.atom) {
            return Ok(PointGetOutcome::Present {
                tip,
                value: Some(value),
            });
        }
        let value = self.load_atom_body(loader, &tip.atom)?;
        Ok(PointGetOutcome::Present { tip, value })
    }

    fn take_resident_atom(&mut self, atom: &AtomId) -> Option<Vec<u8>> {
        let value = self.atom_body(atom)?.to_vec();
        self.touch(ResidentKey::Atom(atom.clone()));
        Some(value)
    }

    fn load_atom_body<L: PointLoader>(
        &mut self,
        loader: &L,
        atom: &AtomId,
    ) -> Result<Option<Vec<u8>>, PointAdmitError> {
        let storage_key = atom_storage_key(atom);
        let Some(loaded) = loader.load_point(collections::ATOMS, &storage_key)? else {
            return Ok(None);
        };
        let Some(body) = loaded.body else {
            return Ok(None);
        };
        self.admit_atom_body(atom.clone(), body.clone());
        Ok(Some(body))
    }
}

fn tip_storage_key(molecule: MoleculeId, hash: &str, range: &str) -> String {
    molecule_key_codec::hash_range_record_key(&molecule.storage_spelling(), hash, range)
}

fn atom_storage_key(atom: &AtomId) -> String {
    atom_key_codec::flat_key(atom.as_str())
}

fn tip_from_body(body: &[u8]) -> Result<Tip, PointAdmitError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| PointAdmitError::InvalidTipBody(err.to_string()))?;
    let entry_value = value.get("entry").cloned().unwrap_or(value);
    let entry: AtomEntry = serde_json::from_value(entry_value)
        .map_err(|err| PointAdmitError::InvalidTipBody(err.to_string()))?;
    Ok(Tip {
        atom: AtomId::new(entry.atom_uuid),
        written_at: entry.written_at,
        logical_counter: entry.logical_counter,
        device_id: entry.device_id,
        mutation_uuid: entry.mutation_uuid,
    })
}
