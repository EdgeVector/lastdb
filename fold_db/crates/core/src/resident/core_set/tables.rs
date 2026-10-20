//! Interned slot table.
//!
//! Each slot key is stored once. Paths, owner lists, and source links refer
//! to a slot by a small integer id instead of a cloned key. The index holds
//! only ids, so a lookup by borrowed strings needs no temporary key. Source
//! keys live in `source_keys`, path names in `path_table`.

use super::path_table::PathId;
use super::*;
use hashbrown::HashTable;
use smallvec::SmallVec;
use std::hash::{BuildHasher, RandomState};

pub(super) type SlotId = u32;
pub(super) use super::source_keys::SourceId;

/// Everything the core keeps for one slot. The slot leaves with its last owner.
pub(super) struct SlotEntry {
    pub(super) key: SlotKey,
    /// Alias paths that share this slot.
    pub(super) owners: SmallVec<[PathId; 2]>,
    /// Distinct invalidation sources, at most `MAX_SLOT_SOURCES`.
    pub(super) sources: SmallVec<[SourceId; 8]>,
    pub(super) atom: Option<Arc<SharedAtom>>,
    pub(super) owned: Option<Arc<model::CoreModelSlot>>,
}

#[derive(Default)]
pub(super) struct SlotTable {
    index: HashTable<SlotId>,
    hasher: RandomState,
    entries: Vec<Option<SlotEntry>>,
    free: Vec<SlotId>,
    /// Canonical molecule names shared by every slot key, with slot counts.
    names: HashMap<MoleculeKey, usize>,
}

impl SlotTable {
    pub(super) fn find_path(&self, path: &CorePath) -> Option<SlotId> {
        self.find_parts(
            path.scope.as_deref(),
            &path.molecule,
            &path.hash,
            &path.range,
        )
    }

    fn find_parts(
        &self,
        scope: Option<&str>,
        molecule: &str,
        hash: &str,
        range: &str,
    ) -> Option<SlotId> {
        let digest = self.hasher.hash_one((scope, molecule, hash, range));
        let entries = &self.entries;
        self.index
            .find(digest, |id| {
                entries[*id as usize].as_ref().is_some_and(|entry| {
                    let key = &entry.key;
                    key.molecule.scope.as_deref() == scope
                        && key.molecule.molecule.as_ref() == molecule
                        && key.hash.as_ref() == hash
                        && key.range.as_ref() == range
                })
            })
            .copied()
    }

    pub(super) fn get(&self, id: SlotId) -> Option<&SlotEntry> {
        self.entries.get(id as usize)?.as_ref()
    }

    pub(super) fn get_mut(&mut self, id: SlotId) -> Option<&mut SlotEntry> {
        self.entries.get_mut(id as usize)?.as_mut()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (SlotId, &SlotEntry)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(id, entry)| Some((id as SlotId, entry.as_ref()?)))
    }

    /// Add a slot without owners. The caller adds the first owner at once.
    pub(super) fn insert(&mut self, mut key: SlotKey) -> SlotId {
        if let Some((canonical, _)) = self.names.get_key_value(&key.molecule) {
            key.molecule = canonical.clone();
        }
        *self.names.entry(key.molecule.clone()).or_insert(0) += 1;
        let digest = key_hash(&self.hasher, &key);
        let entry = SlotEntry {
            key,
            owners: SmallVec::new(),
            sources: SmallVec::new(),
            atom: None,
            owned: None,
        };
        let id = slab_insert(&mut self.entries, &mut self.free, entry);
        let (entries, hasher) = (&self.entries, &self.hasher);
        self.index
            .insert_unique(digest, id, |id| entry_hash(hasher, entries, *id));
        id
    }

    pub(super) fn remove(&mut self, id: SlotId) {
        let Some(entry) = self.entries.get_mut(id as usize).and_then(Option::take) else {
            return;
        };
        let digest = key_hash(&self.hasher, &entry.key);
        if let Ok(found) = self.index.find_entry(digest, |candidate| *candidate == id) {
            found.remove();
        }
        self.free.push(id);
        let unused = self
            .names
            .get_mut(&entry.key.molecule)
            .is_some_and(|count| {
                *count -= 1;
                *count == 0
            });
        if unused {
            self.names.remove(&entry.key.molecule);
        }
    }

    pub(super) fn try_reserve(&mut self, additional: usize) -> Result<(), CoreCapacity> {
        let (entries, hasher) = (&self.entries, &self.hasher);
        self.index
            .try_reserve(additional, |id| entry_hash(hasher, entries, *id))
            .map_err(|_| super::model::capacity())?;
        self.free
            .try_reserve(additional)
            .map_err(|_| super::model::capacity())?;
        self.entries
            .try_reserve(additional)
            .map_err(|_| super::model::capacity())
    }
}

fn key_hash(hasher: &RandomState, key: &SlotKey) -> u64 {
    hasher.hash_one((
        key.molecule.scope.as_deref(),
        key.molecule.molecule.as_ref(),
        key.hash.as_ref(),
        key.range.as_ref(),
    ))
}

fn entry_hash(hasher: &RandomState, entries: &[Option<SlotEntry>], id: SlotId) -> u64 {
    key_hash(
        hasher,
        &entries[id as usize]
            .as_ref()
            .expect("indexed core slot")
            .key,
    )
}

/// Reuse a freed id before growing the table.
pub(super) fn slab_insert<T>(entries: &mut Vec<Option<T>>, free: &mut Vec<u32>, entry: T) -> u32 {
    if let Some(id) = free.pop() {
        entries[id as usize] = Some(entry);
        return id;
    }
    let id = u32::try_from(entries.len()).expect("core table id space");
    entries.push(Some(entry));
    id
}
