//! Interned warm paths.
//!
//! A warm path is its slot plus its schema and field names. Each name is
//! stored once, so a path entry is three ids and its state. The path index,
//! the recency map, and slot owner lists hold a 32-bit path id instead of a
//! copy of the path strings. `core_path` rebuilds the strings when a caller
//! needs a `CorePath` back.

use super::tables::{slab_insert, SlotTable};
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
    entries: Vec<Option<PathEntry>>,
    free: Vec<PathId>,
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
                entries[*id as usize].as_ref().is_some_and(|entry| {
                    entry.state.slot == slot && entry.schema == schema && entry.field == field
                })
            })
            .copied()
    }

    pub(super) fn state(&self, id: PathId) -> Option<&PathState> {
        Some(&self.entries.get(id as usize)?.as_ref()?.state)
    }

    pub(super) fn state_mut(&mut self, id: PathId) -> Option<&mut PathState> {
        Some(&mut self.entries.get_mut(id as usize)?.as_mut()?.state)
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (PathId, &PathState)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(id, entry)| Some((id as PathId, &entry.as_ref()?.state)))
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (PathId, &mut PathState)> {
        self.entries
            .iter_mut()
            .enumerate()
            .filter_map(|(id, entry)| Some((id as PathId, &mut entry.as_mut()?.state)))
    }

    /// Add a path that is not warm. `state.slot` must be the slot of `path`.
    pub(super) fn insert(&mut self, path: &CorePath, state: PathState) -> PathId {
        let entry = PathEntry {
            schema: self.names.acquire(&path.schema),
            field: self.names.acquire(&path.field),
            state,
        };
        let hash = self
            .hasher
            .hash_one((entry.state.slot, entry.schema, entry.field));
        let id = slab_insert(&mut self.entries, &mut self.free, entry);
        let (entries, hasher) = (&self.entries, &self.hasher);
        self.index
            .insert_unique(hash, id, |id| entry_hash(hasher, entries, *id));
        self.len += 1;
        id
    }

    pub(super) fn remove(&mut self, id: PathId) -> Option<PathState> {
        self.entries.get(id as usize)?.as_ref()?;
        let hash = entry_hash(&self.hasher, &self.entries, id);
        if let Ok(found) = self.index.find_entry(hash, |candidate| *candidate == id) {
            found.remove();
        }
        let entry = self.entries[id as usize].take()?;
        self.free.push(id);
        self.len -= 1;
        self.names.release(entry.schema);
        self.names.release(entry.field);
        Some(entry.state)
    }
}

fn entry_hash(hasher: &RandomState, entries: &[Option<PathEntry>], id: PathId) -> u64 {
    let entry = entries[id as usize].as_ref().expect("indexed core path");
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

    fn acquire(&mut self, name: &str) -> NameId {
        if let Some(id) = self.find(name) {
            self.names[id as usize]
                .as_mut()
                .expect("interned core name")
                .1 += 1;
            return id;
        }
        let name: Arc<str> = Arc::from(name);
        let id = slab_insert(&mut self.names, &mut self.free, (Arc::clone(&name), 1));
        self.index.insert(name, id);
        id
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
