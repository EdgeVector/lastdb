//! Interned warm paths.
//!
//! A warm path is its slot plus its schema and field names. Each name is
//! stored once, so a path entry is three ids and its state. The path index,
//! the recency map, and slot owner lists hold a 32-bit path id instead of a
//! copy of the path strings. `core_path` rebuilds the strings when a caller
//! needs a `CorePath` back.

use super::slab::Slab;
use super::tables::SlotTable;
use super::*;
use hashbrown::HashTable;
use std::hash::{BuildHasher, RandomState};

pub(super) type PathId = u32;
type NameId = u32;

struct PathEntry {
    schema: NameId,
    field: NameId,
    state: PathState,
}

#[derive(Default)]
pub(super) struct PathTable {
    index: HashTable<PathId>,
    hasher: RandomState,
    entries: Slab<PathEntry>,
    len: usize,
    names: NameTable,
}

impl PathTable {
    /// The id of a warm path. A path is warm only while its slot exists, so
    /// a missing slot or name means the path is not warm.
    pub(super) fn find(&self, slots: &SlotTable, path: &CorePath) -> Option<PathId> {
        let slot = slots.find_path(path)?;
        let schema = self.names.find(&path.schema)?;
        let field = self.names.find(&path.field)?;
        let hash = self.hasher.hash_one((slot, schema, field));
        let entries = &self.entries;
        self.index
            .find(hash, |id| {
                entries.get(*id).is_some_and(|entry| {
                    entry.state.slot == slot && entry.schema == schema && entry.field == field
                })
            })
            .copied()
    }

    pub(super) fn state(&self, id: PathId) -> Option<&PathState> {
        Some(&self.entries.get(id)?.state)
    }

    pub(super) fn state_mut(&mut self, id: PathId) -> Option<&mut PathState> {
        Some(&mut self.entries.get_mut(id)?.state)
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (PathId, &PathState)> {
        self.entries.iter().map(|(id, entry)| (id, &entry.state))
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (PathId, &mut PathState)> {
        self.entries
            .iter_mut()
            .map(|(id, entry)| (id, &mut entry.state))
    }

    pub(super) fn remove(&mut self, id: PathId) -> Option<PathState> {
        self.entries.get(id)?;
        let hash = entry_hash(&self.hasher, &self.entries, id);
        if let Ok(found) = self.index.find_entry(hash, |candidate| *candidate == id) {
            found.remove();
        }
        let entry = self.entries.take(id)?;
        self.len -= 1;
        self.names.release(entry.schema);
        self.names.release(entry.field);
        Some(entry.state)
    }
}

fn entry_hash(hasher: &RandomState, entries: &Slab<PathEntry>, id: PathId) -> u64 {
    let entry = entries.get(id).expect("indexed core path");
    hasher.hash_one((entry.state.slot, entry.schema, entry.field))
}

/// Schema and field names with use counts. A name leaves with its last path.
#[derive(Default)]
struct NameTable {
    index: HashMap<Arc<str>, NameId>,
    names: Vec<Option<(Arc<str>, usize)>>,
    free: Vec<NameId>,
}

impl NameTable {
    fn find(&self, name: &str) -> Option<NameId> {
        self.index.get(name).copied()
    }

    fn release(&mut self, id: NameId) {
        let Some((name, uses)) = self.names.get_mut(id as usize).and_then(Option::as_mut) else {
            return;
        };
        *uses -= 1;
        if *uses == 0 {
            self.index.remove(name.as_ref());
            self.names[id as usize] = None;
            self.free.push(id);
        }
    }
}
