//! Used logical paths over the actual molecule and atom model.
//!
//! The caller holds the publication lock before it locks this set. Disk I/O
//! never runs under the set lock. The path map and recency list contain only
//! ownership metadata; each slot holds its current tip and key metadata.
//! There is no second molecule copy (measured 2026-10-20: the copy cost about
//! 2.5 KB of each 7 KB small record, in text-keyed maps and key copies).

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
mod path_table;
mod slab;
mod source_keys;
mod sources;
mod tables;

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

/// Shared name strings: every slot of a molecule clones the same `Arc`s.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MoleculeKey {
    scope: Option<Arc<str>>,
    molecule: Arc<str>,
}

impl MoleculeKey {
    fn new(scope: Option<&str>, molecule: &str) -> Self {
        Self {
            scope: scope.map(Arc::from),
            molecule: Arc::from(molecule),
        }
    }
}

/// Stored once in the slot table; clones only bump reference counts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SlotKey {
    molecule: MoleculeKey,
    hash: Arc<str>,
    range: Arc<str>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AtomKey {
    scope: Option<Arc<str>>,
    uuid: String,
}

#[derive(Default)]
struct PathState {
    /// The interned slot this path shares with its aliases.
    slot: tables::SlotId,
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
    /// Complete key membership attached to a molecule. Tips live in slots.
    key_domains: HashMap<MoleculeKey, Arc<crate::resident::core_keys::CoreKeyDomain>>,
    // A content UUID does not identify the immutable metadata of a full atom.
    // The UUID buckets share only fully equal atoms and own no retained bytes.
    atoms: HashMap<AtomKey, Vec<Weak<SharedAtom>>>,
    // Interned paths. `order` and slot owner lists hold path ids.
    paths: path_table::PathTable,
    order: BTreeMap<u64, path_table::PathId>,
    clock: u64,
    // Slot owners, atoms, owned values, and source links, by interned id.
    slots: tables::SlotTable,
    sources: source_keys::SourceTable,
    hits: u64,
    misses: u64,
    purged: u64,
    bytes: bytes::WarmBytes,
}

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        let tip_only_paths = self
            .slots
            .iter()
            .filter(|(_, slot)| {
                slot.tip.as_ref().is_some_and(|entry| {
                    slot.atom
                        .as_ref()
                        .is_none_or(|atom| atom.value.uuid() != entry.atom_uuid.as_str())
                })
            })
            .flat_map(|(_, slot)| &slot.owners)
            .filter(|path| {
                self.paths
                    .state(**path)
                    .is_some_and(|state| state.known && !state.absent)
            })
            .count();
        CoreWarmStats {
            paths: self.paths.len(),
            tip_only_paths,
            molecules: self.slots.molecule_count(),
            atoms: self.atoms.values().map(Vec::len).sum(),
            held: self
                .paths
                .iter()
                .filter(|(_, state)| state.holds > 0)
                .count(),
            dirty: self
                .paths
                .iter()
                .filter(|(_, state)| state.dirty.is_some())
                .count(),
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }

    /// Absence requires the same publication boundary and source links as a tip.
    pub(crate) fn authorize_path(&mut self, path: &CorePath) {
        let Some(slot) = self.slot_of(path).and_then(|id| self.slots.get(id)) else {
            return;
        };
        for owner in &slot.owners {
            if let Some(state) = self.paths.state_mut(*owner) {
                state.tip_authorized = state.known;
            }
        }
    }

    pub(crate) fn path_authorized(&self, path: &CorePath) -> bool {
        self.path_state(path)
            .is_some_and(|state| state.known && state.tip_authorized)
    }

    /// Control records can change visible tips without changing their bodies.
    pub(crate) fn clear_authorizations(&mut self) {
        for (_, state) in self.paths.iter_mut() {
            state.tip_authorized = false;
        }
    }

    /// The slot of a path, or of its alias when this path is not warm. A
    /// warm path always has the slot of its own slot key.
    fn slot_of(&self, path: &CorePath) -> Option<tables::SlotId> {
        self.slots.find_path(path)
    }

    fn path_id(&self, path: &CorePath) -> Option<path_table::PathId> {
        self.paths.find(&self.slots, path)
    }

    fn path_state(&self, path: &CorePath) -> Option<&PathState> {
        self.paths.state(self.path_id(path)?)
    }
}

impl CorePath {
    /// A new key with fresh strings. Used to intern a slot and to group
    /// transient write effects; warm lookups use the interned id.
    fn slot_key(&self) -> SlotKey {
        SlotKey {
            molecule: MoleculeKey::new(self.scope.as_deref(), &self.molecule),
            hash: Arc::from(self.hash.as_str()),
            range: Arc::from(self.range.as_str()),
        }
    }
}
