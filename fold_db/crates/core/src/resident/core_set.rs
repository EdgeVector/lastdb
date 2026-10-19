//! Used logical paths over the actual molecule and atom model.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. The path map and recency list contain only
//! ownership metadata; current tips live in `MoleculeHashRange` itself.

use crate::atom::{Atom, AtomEntry, KeyMetadata, MoleculeHashRange};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};

mod admission;
mod bytes;
mod charge;
mod keys;
mod lifecycle;
mod memory;
mod model;
mod sources;

pub use bytes::CoreWarmByteStats;
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

/// An authoritative current tip does not require its atom body to be loaded.
#[derive(Clone)]
pub(crate) struct CoreWarmTip {
    pub(crate) entry: AtomEntry,
    pub(crate) metadata: Option<KeyMetadata>,
}

/// A known tip permits key operations, while only a value can answer a body read.
pub(crate) enum CoreWarmLookup {
    Unknown,
    Absent,
    Tip(CoreWarmTip),
    Value(CoreWarmSelection),
}

impl CoreWarmLookup {
    fn unknown() -> Self {
        Self::Unknown
    }

    fn absent() -> Self {
        Self::Absent
    }

    fn value(selection: CoreWarmSelection) -> Self {
        Self::Value(selection)
    }

    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    pub(crate) fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    pub(crate) fn into_tip(self) -> Option<CoreWarmTip> {
        match self {
            Self::Tip(tip) => Some(tip),
            Self::Value(value) => Some(CoreWarmTip {
                entry: value.entry,
                metadata: value.metadata,
            }),
            Self::Unknown | Self::Absent => None,
        }
    }

    pub(crate) fn into_value(self) -> Option<CoreWarmSelection> {
        match self {
            Self::Value(value) => Some(value),
            Self::Unknown | Self::Absent | Self::Tip(_) => None,
        }
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
    pub(crate) tip_only_paths: usize,
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
    /// Byte charge of this path for the warm set gauge.
    charge: usize,
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
    bytes: bytes::WarmBytes,
}

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        let tip_only_paths = self
            .slot_owners
            .iter()
            .filter(|(slot, _)| {
                self.molecules
                    .get(&slot.molecule)
                    .and_then(|molecule| molecule.get_atom_entry(&slot.hash, &slot.range))
                    .is_some_and(|entry| {
                        self.slot_atoms
                            .get(*slot)
                            .is_none_or(|atom| atom.value.uuid() != entry.atom_uuid.as_str())
                    })
            })
            .flat_map(|(_, owners)| owners)
            .filter(|path| {
                self.paths
                    .get(*path)
                    .is_some_and(|state| state.known && !state.absent)
            })
            .count();
        CoreWarmStats {
            paths: self.paths.len(),
            tip_only_paths,
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
