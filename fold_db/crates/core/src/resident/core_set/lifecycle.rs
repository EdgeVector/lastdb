//! The separate recency list retains only logical path references.

use super::charge::path_charge;
use super::*;

/// Victims selected per scan of the recency list.
const PURGE_BATCH: usize = 64;

impl CoreWarmSet {
    /// Release exact reservations, then trim records that the call no longer holds.
    /// Unknown placeholders leave after their last hold.
    pub(crate) fn release_paths(&mut self, paths: &[CorePath]) {
        for path in paths {
            let Some(id) = self.path_id(path) else {
                continue;
            };
            let state = self.paths.state_mut(id).expect("warm core path");
            state.holds = state.holds.saturating_sub(1);
            if !state.known && state.holds == 0 {
                self.drop_id(id);
            }
        }
        // The call drops its retained owners before this release. Sweep those
        // dead allocations first, so the purge charges only live values.
        if !self.fits(0) {
            self.bytes.sweep();
            self.trim_to_limit(0, &HashSet::new());
        }
    }

    /// Explicit pressure relief to a byte target below the configured limit.
    /// Shared or call-held atoms remain measured after their path leaves.
    pub(crate) fn purge_clean_to_bytes(&mut self, target: usize) -> (usize, usize, usize) {
        let before = self.byte_stats().used_bytes;
        let purged = self.purge_clean_until(target, &HashSet::new());
        (purged, before, self.byte_stats().used_bytes)
    }

    /// Make room for the requested paths under the byte limit. Never refuses:
    /// if only held paths remain, the new paths enter over the limit.
    /// Call release or a later admission purges again (design D5).
    pub(super) fn make_room(&mut self, requested: &[CorePath]) -> Result<(), CoreCapacity> {
        let new_paths = requested
            .iter()
            .filter(|path| self.path_id(path).is_none())
            .count();
        let incoming = new_paths * path_charge();
        // The common case fits: decide before any allocation.
        if self.fits(incoming) {
            return Ok(());
        }
        let keep: HashSet<_> = requested
            .iter()
            .filter_map(|path| self.path_id(path))
            .collect();
        self.trim_to_limit(incoming, &keep);
        Ok(())
    }

    /// Bring the charge back under the limit after an atom charge, which is
    /// known only at admission. The admitted path itself stays.
    pub(super) fn trim_after_admit(&mut self, path: &CorePath) {
        if self.fits(0) {
            return;
        }
        let keep: HashSet<_> = self.path_id(path).into_iter().collect();
        self.trim_to_limit(0, &keep);
    }

    fn fits(&self, incoming: usize) -> bool {
        self.bytes.used().saturating_add(incoming) <= crate::resident::core_warm_bytes()
    }

    /// Purge least recently used clean paths until `incoming` more bytes fit.
    fn trim_to_limit(&mut self, incoming: usize, keep: &HashSet<path_table::PathId>) {
        let limit = crate::resident::core_warm_bytes();
        if self.fits(incoming) {
            return;
        }
        self.bytes.sweep_if_stale();
        if self.bytes.used().saturating_add(incoming) <= limit {
            return;
        }
        self.purge_clean_until(limit.saturating_sub(incoming), keep);
        if self.bytes.used().saturating_add(incoming) > limit {
            self.bytes.note_over_limit();
        }
    }

    /// Remove clean, unheld paths oldest first until the charge is at or under
    /// `target`. Returns the number of paths removed.
    fn purge_clean_until(&mut self, target: usize, keep: &HashSet<path_table::PathId>) -> usize {
        let before = self.bytes.used();
        let mut purged = 0;
        // Bounded batches: the order list is not borrowed while paths leave.
        'scan: loop {
            let victims: Vec<_> = self
                .order
                .values()
                .filter(|id| !keep.contains(*id))
                .filter(|id| self.paths.state(**id).is_some_and(|state| state.holds == 0))
                .take(PURGE_BATCH)
                .copied()
                .collect();
            if victims.is_empty() {
                break;
            }
            for id in victims {
                if self.bytes.used() <= target {
                    break 'scan;
                }
                self.purge_path(id);
                purged += 1;
            }
        }
        self.purged = self.purged.saturating_add(purged as u64);
        self.bytes
            .note_purged(before.saturating_sub(self.bytes.used()));
        purged
    }

    /// Drop one path and release its atom charges if no other holder remains.
    fn purge_path(&mut self, id: path_table::PathId) {
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
        self.set_tip_charge(id, 0);
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        slot.owned = None;
        slot.metadata = None;
        slot.tip_source = None;
        slot.tip_wire_bytes = None;
        if let Some(previous) = slot.tip.take() {
            self.unlink_atom(id, &previous.atom_uuid);
        }
    }

    pub(super) fn unlink_atom(&mut self, id: tables::SlotId, uuid: &str) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        slot.atom = None;
        let key = AtomKey::new(slot.key.molecule.scope.clone(), uuid);
        let empty = self.atoms.get_mut(&key).is_some_and(|variants| {
            variants.retain(|atom| atom.strong_count() > 0);
            variants.is_empty()
        });
        if empty {
            self.atoms.remove(&key);
        }
    }
}
