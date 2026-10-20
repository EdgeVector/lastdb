//! The separate recency list retains only logical path references.

use super::*;

impl CoreWarmSet {
    /// Release exact reservations. Unknown placeholders leave after their last hold.
    pub(crate) fn release_paths(&mut self, paths: &[CorePath]) {
        for path in paths {
            let Some(id) = self.path_id(path) else {
                continue;
            };
            let state = self.paths.state_mut(id).expect("warm core path");
            state.holds = state.holds.saturating_sub(1);
            if !state.known && state.holds == 0 && state.dirty.is_none() {
                self.drop_id(id);
            }
        }
    }

    /// A completed older flush cannot clear the sequence of a newer write.
    pub(crate) fn complete_flush(&mut self, sequence: u64) {
        self.complete_model_flush(sequence);
        let mut unknown = Vec::new();
        for (id, state) in self.paths.iter_mut() {
            if state.dirty.is_some_and(|dirty| dirty <= sequence) {
                state.dirty = None;
            }
            if !state.known && state.holds == 0 && state.dirty.is_none() {
                unknown.push(id);
            }
        }
        for id in unknown {
            self.drop_id(id);
        }
    }

    /// Explicit pressure relief changes retention, not the stage-one admission
    /// policy. Shared or call-held atoms remain measured after their path leaves.
    pub(crate) fn purge_clean_to_bytes(&mut self, target: usize) -> (usize, usize, usize) {
        let before = self.byte_stats().used_bytes;
        let victims: Vec<_> = self
            .order
            .values()
            .filter(|id| {
                self.paths
                    .state(**id)
                    .is_some_and(|state| state.holds == 0 && state.dirty.is_none())
            })
            .copied()
            .collect();
        let mut purged = 0;
        for id in victims {
            if self.bytes.used() <= target {
                break;
            }
            let slot = self
                .paths
                .state(id)
                .and_then(|state| self.slots.get(state.slot));
            let allocations = [
                slot.and_then(|slot| slot.atom.as_ref())
                    .map(|atom| Arc::as_ptr(&atom.value) as usize),
                slot.and_then(|slot| slot.owned.as_ref())
                    .and_then(|owned| owned.value.as_ref())
                    .map(|value| Arc::as_ptr(&value.atom) as usize),
            ];
            self.drop_id(id);
            for allocation in allocations.into_iter().flatten() {
                self.bytes.release_dead_atom(allocation);
            }
            purged += 1;
        }
        self.purged = self.purged.saturating_add(purged as u64);
        (purged, before, self.byte_stats().used_bytes)
    }

    /// Check the entire reservation before any purge or path insertion.
    pub(super) fn make_room(&mut self, requested: &[CorePath]) -> Result<(), CoreCapacity> {
        let cap = super::super::resident_key_cap();
        let requested: Vec<_> = requested.iter().map(|path| self.path_id(path)).collect();
        let new_paths = requested.iter().filter(|id| id.is_none()).count();
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
        let requested: HashSet<_> = requested.into_iter().flatten().collect();
        let victims: Vec<_> = self
            .order
            .values()
            .filter(|id| !requested.contains(*id))
            .filter(|id| {
                self.paths
                    .state(**id)
                    .is_some_and(|state| state.holds == 0 && state.dirty.is_none())
            })
            .take(needed)
            .copied()
            .collect();
        if victims.len() < needed {
            return Err(CoreCapacity {
                detail: "all core warm path slots are held or unflushed",
            });
        }
        for id in victims {
            self.drop_id(id);
            self.purged = self.purged.saturating_add(1);
        }
        Ok(())
    }

    pub(super) fn touch(&mut self, path: &CorePath) {
        if let Some(id) = self.path_id(path) {
            self.touch_id(id);
        }
    }

    pub(super) fn touch_id(&mut self, id: path_table::PathId) {
        if self.clock == u64::MAX {
            let ids: Vec<_> = self.order.values().copied().collect();
            self.order.clear();
            self.clock = 0;
            for old in ids {
                self.clock += 1;
                self.paths.state_mut(old).expect("ordered core path").tick = self.clock;
                self.order.insert(self.clock, old);
            }
        }
        let Some(state) = self.paths.state_mut(id) else {
            return;
        };
        self.order.remove(&state.tick);
        self.clock += 1;
        state.tick = self.clock;
        self.order.insert(self.clock, id);
    }

    pub(super) fn drop_id(&mut self, path: path_table::PathId) {
        let Some(state) = self.paths.remove(path) else {
            return;
        };
        self.order.remove(&state.tick);
        self.release_path(state.charge);
        let id = state.slot;
        let last_owner = self.slots.get_mut(id).is_some_and(|slot| {
            slot.owners.retain(|owner| *owner != path);
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
