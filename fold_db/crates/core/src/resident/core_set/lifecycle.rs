//! The separate recency list retains only logical path references.

use super::*;

impl CoreWarmSet {
    /// Release exact reservations. Unknown placeholders leave after their last hold.
    pub(crate) fn release_paths(&mut self, paths: &[CorePath]) {
        for path in paths {
            let forget = if let Some(state) = self.paths.get_mut(path) {
                state.holds = state.holds.saturating_sub(1);
                !state.known && state.holds == 0 && state.dirty.is_none()
            } else {
                false
            };
            if forget {
                self.drop_path(path);
            }
        }
    }

    /// A completed older flush cannot clear the sequence of a newer write.
    pub(crate) fn complete_flush(&mut self, sequence: u64) {
        self.complete_model_flush(sequence);
        let mut unknown = Vec::new();
        for (path, state) in &mut self.paths {
            if state.dirty.is_some_and(|dirty| dirty <= sequence) {
                state.dirty = None;
            }
            if !state.known && state.holds == 0 && state.dirty.is_none() {
                unknown.push(Arc::clone(path));
            }
        }
        for path in unknown {
            self.drop_path(&path);
        }
    }

    /// Check the entire reservation before any purge or path insertion.
    pub(super) fn make_room(&mut self, requested: &[CorePath]) -> Result<(), CoreCapacity> {
        let cap = super::super::resident_key_cap();
        let new_paths = requested
            .iter()
            .filter(|path| !self.paths.contains_key(*path))
            .count();
        if new_paths > cap {
            return Err(CoreCapacity {
                detail: "core warm batch exceeds the logical path cap",
            });
        }
        let needed = self
            .paths
            .len()
            .saturating_add(new_paths)
            .saturating_sub(cap);
        let requested: HashSet<_> = requested.iter().collect();
        let victims: Vec<_> = self
            .order
            .values()
            .filter(|path| !requested.contains(path.as_ref()))
            .filter(|path| {
                self.paths
                    .get(path.as_ref())
                    .is_some_and(|state| state.holds == 0 && state.dirty.is_none())
            })
            .take(needed)
            .cloned()
            .collect();
        if victims.len() < needed {
            return Err(CoreCapacity {
                detail: "all core warm path slots are held or unflushed",
            });
        }
        for path in victims {
            self.drop_path(&path);
            self.purged = self.purged.saturating_add(1);
        }
        Ok(())
    }

    pub(super) fn touch(&mut self, path: &CorePath) {
        if self.clock == u64::MAX {
            let paths: Vec<_> = self.order.values().cloned().collect();
            self.order.clear();
            self.clock = 0;
            for old in paths {
                self.clock += 1;
                self.paths.get_mut(&old).expect("ordered core path").tick = self.clock;
                self.order.insert(self.clock, old);
            }
        }
        let Some((shared, _)) = self.paths.get_key_value(path) else {
            return;
        };
        let shared = Arc::clone(shared);
        let state = self.paths.get_mut(path).expect("touched core path");
        self.order.remove(&state.tick);
        self.clock += 1;
        state.tick = self.clock;
        self.order.insert(self.clock, shared);
    }

    pub(super) fn drop_path(&mut self, path: &CorePath) {
        let Some(state) = self.paths.remove(path) else {
            return;
        };
        self.order.remove(&state.tick);
        self.release_path(state.charge);
        let id = state.slot;
        let last_owner = self.slots.get_mut(id).is_some_and(|slot| {
            slot.owners.retain(|owner| owner.as_ref() != path);
            slot.owners.is_empty()
        });
        if last_owner {
            self.release_slot(id);
            self.forget_slot_sources(id);
            self.forget_slot_value(id);
            self.slots.remove(id);
        }
    }

    pub(super) fn forget_slot_value(&mut self, id: tables::SlotId) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        slot.owned = None;
        let key = slot.key.clone();
        let (previous, empty) = match self.molecules.get_mut(&key.molecule) {
            Some(molecule) => (
                molecule.forget_loaded_slot(&key.hash, &key.range),
                molecule.is_empty() && molecule.core_keys().is_none(),
            ),
            None => return,
        };
        if empty {
            self.molecules.remove(&key.molecule);
        }
        if let Some(previous) = previous {
            self.unlink_atom(id, &previous.atom_uuid);
        }
    }

    pub(super) fn unlink_atom(&mut self, id: tables::SlotId, uuid: &str) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        slot.atom = None;
        let key = AtomKey {
            scope: slot.key.molecule.scope.clone(),
            uuid: uuid.to_string(),
        };
        let empty = self.atoms.get_mut(&key).is_some_and(|variants| {
            variants.retain(|atom| atom.strong_count() > 0);
            variants.is_empty()
        });
        if empty {
            self.atoms.remove(&key);
        }
    }
}
