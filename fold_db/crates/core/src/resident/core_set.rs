//! Used logical paths over the actual molecule and atom model.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. The path map and recency list contain only
//! ownership metadata; current tips live in `MoleculeHashRange` itself.

use crate::atom::{Atom, AtomEntry, KeyMetadata, MoleculeHashRange};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};

mod admission;
mod keys;
mod lifecycle;
mod memory;
mod model;
mod sources;

pub(crate) use memory::CoreMemorySource;
pub(crate) use memory::PreparedCoreMemory;
pub use memory::{CoreMemoryStats, CoreMemoryValue};
pub(crate) use model::{CoreModelChange, CoreModelSnapshot, CoreModelStats, CoreModelUpdate};

/// Stable identity of one used field path. Its current atom is not its key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CorePath {
    pub(crate) scope: Option<String>,
    pub(crate) schema: String,
    pub(crate) field: String,
    pub(crate) molecule: String,
    pub(crate) hash: String,
    pub(crate) range: String,
}

/// Typed data prepared privately before one path enters the shared model.
#[derive(Clone)]
pub(crate) struct CoreWarmSlot {
    pub(crate) entry: AtomEntry,
    pub(crate) metadata: Option<KeyMetadata>,
    pub(crate) atom: Arc<Atom>,
    pub(crate) bytes: usize,
}

/// An immutable selected answer. Only the selected atom remains shared.
#[derive(Clone)]
pub(crate) struct CoreWarmSelection {
    pub(crate) entry: AtomEntry,
    pub(crate) metadata: Option<KeyMetadata>,
    pub(crate) atom: Arc<Atom>,
    pub(crate) bytes: usize,
}

/// An absent key is a complete answer; an unknown key still needs a fetch.
pub(crate) struct CoreWarmLookup {
    known: bool,
    selection: Option<CoreWarmSelection>,
}

impl CoreWarmLookup {
    fn unknown() -> Self {
        Self {
            known: false,
            selection: None,
        }
    }

    fn absent() -> Self {
        Self {
            known: true,
            selection: None,
        }
    }

    fn tip(selection: CoreWarmSelection) -> Self {
        Self {
            known: true,
            selection: Some(selection),
        }
    }

    pub(crate) fn is_unknown(&self) -> bool {
        !self.known
    }

    pub(crate) fn is_absent(&self) -> bool {
        self.known && self.selection.is_none()
    }

    pub(crate) fn into_tip(self) -> Option<CoreWarmSelection> {
        self.selection
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CoreCapacity {
    pub(crate) detail: &'static str,
}

impl std::fmt::Display for CoreCapacity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.detail)
    }
}

impl std::error::Error for CoreCapacity {}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct CoreWarmStats {
    pub(crate) paths: usize,
    pub(crate) molecules: usize,
    pub(crate) atoms: usize,
    pub(crate) held: usize,
    pub(crate) dirty: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) purged: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MoleculeKey {
    scope: Option<String>,
    molecule: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SlotKey {
    molecule: MoleculeKey,
    hash: String,
    range: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AtomKey {
    scope: Option<String>,
    uuid: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SourceKey {
    collection: String,
    key: String,
}

#[derive(Default)]
struct PathState {
    holds: u32,
    dirty: Option<u64>,
    known: bool,
    absent: bool,
    tip_authorized: bool,
    tick: u64,
}

struct SharedAtom {
    value: Arc<Atom>,
    bytes: usize,
}

/// One current model. The separate path list controls retention, not values.
#[derive(Default)]
pub(crate) struct CoreWarmSet {
    memory: memory::CoreMemory,
    model: model::CoreModel,
    molecules: HashMap<MoleculeKey, MoleculeHashRange>,
    // A content UUID does not identify the immutable metadata of a full atom.
    // The UUID buckets share only fully equal atoms and own no retained bytes.
    atoms: HashMap<AtomKey, Vec<Weak<SharedAtom>>>,
    slot_atoms: HashMap<SlotKey, Arc<SharedAtom>>,
    owned_slots: HashMap<SlotKey, Arc<model::CoreModelSlot>>,
    paths: HashMap<CorePath, PathState>,
    slot_owners: HashMap<SlotKey, HashSet<CorePath>>,
    order: BTreeMap<u64, CorePath>,
    clock: u64,
    slot_sources: HashMap<SlotKey, HashSet<SourceKey>>,
    source_slots: HashMap<SourceKey, HashSet<SlotKey>>,
    hits: u64,
    misses: u64,
    purged: u64,
}

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        CoreWarmStats {
            paths: self.paths.len(),
            molecules: self.molecules.len(),
            atoms: self.atoms.values().map(Vec::len).sum(),
            held: self.paths.values().filter(|state| state.holds > 0).count(),
            dirty: self
                .paths
                .values()
                .filter(|state| state.dirty.is_some())
                .count(),
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }

    /// Borrow the actual partial molecule for an existing synchronous filter.
    /// Its present slots alone do not establish complete range coverage.
    pub(crate) fn with_molecule<T>(
        &self,
        scope: Option<&str>,
        molecule: &str,
        inspect: impl FnOnce(&MoleculeHashRange) -> T,
    ) -> Option<T> {
        self.molecules
            .get(&MoleculeKey {
                scope: scope.map(str::to_owned),
                molecule: molecule.to_string(),
            })
            .map(inspect)
    }

    /// Absence requires the same publication boundary and source links as a tip.
    pub(crate) fn authorize_path(&mut self, path: &CorePath) {
        for owner in self.slot_owners.get(&path.slot()).into_iter().flatten() {
            if let Some(state) = self.paths.get_mut(owner) {
                state.tip_authorized = state.known;
            }
        }
    }

    pub(crate) fn path_authorized(&self, path: &CorePath) -> bool {
        self.paths
            .get(path)
            .is_some_and(|state| state.known && state.tip_authorized)
    }

    /// Control records can change visible tips without changing their bodies.
    pub(crate) fn clear_authorizations(&mut self) {
        for state in self.paths.values_mut() {
            state.tip_authorized = false;
        }
    }
}

impl CorePath {
    fn slot(&self) -> SlotKey {
        SlotKey {
            molecule: MoleculeKey {
                scope: self.scope.clone(),
                molecule: self.molecule.clone(),
            },
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }
}
