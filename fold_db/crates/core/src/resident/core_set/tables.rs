//! Interned slot table.
//!
//! Each slot key is stored once. Paths, owner lists, and source links refer
//! to a slot by a small integer id instead of a cloned key. Source keys live
//! in `source_keys`.

use super::*;
use smallvec::SmallVec;

pub(super) type SlotId = u32;
pub(super) use super::source_keys::SourceId;

/// Everything the core keeps for one slot. The slot leaves with its last owner.
pub(super) struct SlotEntry {
    pub(super) key: SlotKey,
    /// Alias paths that share this slot.
    pub(super) owners: SmallVec<[Arc<CorePath>; 1]>,
    /// Distinct invalidation sources, at most `MAX_SLOT_SOURCES`.
    pub(super) sources: SmallVec<[SourceId; 8]>,
    pub(super) atom: Option<Arc<SharedAtom>>,
    pub(super) owned: Option<Arc<model::CoreModelSlot>>,
}

#[derive(Default)]
pub(super) struct SlotTable {
    index: HashMap<SlotKey, SlotId>,
    entries: Vec<Option<SlotEntry>>,
    free: Vec<SlotId>,
    /// Canonical molecule names shared by every slot key, with slot counts.
    names: HashMap<MoleculeKey, usize>,
}

impl SlotTable {
    pub(super) fn find(&self, key: &SlotKey) -> Option<SlotId> {
        self.index.get(key).copied()
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
        let entry = SlotEntry {
            key: key.clone(),
            owners: SmallVec::new(),
            sources: SmallVec::new(),
            atom: None,
            owned: None,
        };
        let id = slab_insert(&mut self.entries, &mut self.free, entry);
        self.index.insert(key, id);
        id
    }

    pub(super) fn remove(&mut self, id: SlotId) {
        let Some(entry) = self.entries.get_mut(id as usize).and_then(Option::take) else {
            return;
        };
        self.index.remove(&entry.key);
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
        self.index
            .try_reserve(additional)
            .and_then(|_| self.free.try_reserve(additional))
            .map_err(|_| super::model::capacity())?;
        self.entries
            .try_reserve(additional)
            .map_err(|_| super::model::capacity())
    }
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
