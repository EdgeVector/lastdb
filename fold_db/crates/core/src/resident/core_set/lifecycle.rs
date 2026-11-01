//! The separate recency list retains only logical path references.

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

    fn fits(&self, incoming: usize) -> bool {
        self.bytes.used().saturating_add(incoming) <= crate::resident::core_warm_bytes()
    }

    /// Purge least recently used clean paths until `incoming` more bytes fit.
    pub(super) fn trim_to_limit(&mut self, incoming: usize, keep: &HashSet<path_table::PathId>) {
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

    /// Remove clean, unheld paths and records oldest first until the charge
    /// is at or under `target`. Returns the number of entries removed.
    fn purge_clean_until(&mut self, target: usize, keep: &HashSet<path_table::PathId>) -> usize {
        let before = self.bytes.used();
        let mut purged = 0;
        // Bounded batches: the order lists are not borrowed while entries
        // leave. One batch of each kind merges by tick; a full batch that
        // runs out first ends the round, so a later entry of that kind is
        // never skipped for a newer entry of the other kind.
        'scan: loop {
            let paths: Vec<_> = self
                .order
                .iter()
                .filter(|(_, id)| !keep.contains(*id))
                .filter(|(_, id)| self.paths.state(**id).is_some_and(|state| state.holds == 0))
                .take(PURGE_BATCH)
                .map(|(tick, id)| (*tick, *id))
                .collect();
            let records = self.record_victims(PURGE_BATCH);
            if paths.is_empty() && records.is_empty() {
                break;
            }
            let (paths_full, records_full) =
                (paths.len() == PURGE_BATCH, records.len() == PURGE_BATCH);
            let mut paths = paths.into_iter().peekable();
            let mut records = records.into_iter().peekable();
            loop {
                if self.bytes.used() <= target {
                    break 'scan;
                }
                let path_first = match (paths.peek(), records.peek()) {
                    (Some(path), Some(record)) => path.0 < record.0,
                    (Some(_), None) if !records_full => true,
                    (None, Some(_)) if !paths_full => false,
                    _ => break,
                };
                if path_first {
                    let (_, id) = paths.next().expect("peeked path victim");
                    self.purge_path(id);
                    purged += 1;
                } else {
                    let (_, key) = records.next().expect("peeked record victim");
                    if self.purge_record(&key) {
                        purged += 1;
                    }
                }
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
        if let Some(previous) = slot.tip.take() {
            self.unlink_atom(id, &previous.atom_uuid);
        }
    }

    pub(super) fn unlink_atom(&mut self, id: tables::SlotId, uuid: &str) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        if let Some(atom) = slot.atom.take() {
            self.bytes.detach_atom(Arc::as_ptr(&atom.value) as usize);
        }
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
