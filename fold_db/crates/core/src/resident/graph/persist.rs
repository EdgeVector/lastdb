//! Field writes, persist plans and dirty clearing.

use super::*;

impl ResidentGraph {
    // ── Persist plan (fidelity surface) ─────────────────────────────────

    /// Clear dirty for explicit non-tip keys after their durable put succeeds.
    ///
    /// Molecule tips are versioned by their current `atom_uuid`; use
    /// [`Self::clear_tip_dirty_if_current`] so an older deferred completion
    /// cannot clear a newer same-key write's dirty pin.
    pub(crate) fn mark_keys_persisted<I: IntoIterator<Item = DirtyKey>>(&self, keys: I) {
        for key in keys {
            debug_assert!(
                !matches!(&key, DirtyKey::MoleculeTip { .. }),
                "molecule tips need atom_uuid-aware dirty clearing"
            );
            if matches!(&key, DirtyKey::MoleculeTip { .. }) {
                continue;
            }
            self.clear_dirty(&key);
            if let DirtyKey::Atom(uuid) = &key {
                self.release_unreferenced_atom(uuid);
            }
        }
        self.enforce_budget();
    }

    /// Clear a tip only when its atom and resident revision are still current.
    ///
    /// The slot write lock spans the version checks and every keyed removal.
    /// A new apply publishes its revision before dirty state, so an older
    /// completion cannot clear or discharge the newer write.
    pub fn clear_tip_dirty_for_revision_if_current(
        &self,
        tip: &ResidentTip,
        resident_revision: u64,
    ) -> bool {
        self.clear_tip_dirty_if_current_revision(tip, Some(resident_revision))
    }

    pub(super) fn clear_tip_dirty_if_current_revision(
        &self,
        tip: &ResidentTip,
        resident_revision: Option<u64>,
    ) -> bool {
        let map_key = tip_map_key(&tip.molecule_uuid, &tip.hash, &tip.range);
        // Lock order: slots, then tips. The revision and the atom id must
        // still match before the body leaves. Retain slots through all keyed
        // cleanup; the next apply cannot publish its revision in that gap.
        let mut slots = self.slots.write().expect("resident slot lock");
        let Some(control) = slots.get_mut(&map_key) else {
            return false;
        };
        if resident_revision.is_some_and(|revision| control.resident_revision != revision) {
            return false;
        }
        let atom_uuid = {
            let mut tips = self.tips.write().expect("tips lock");
            let Some(current) = tips.get(&map_key) else {
                return false;
            };
            if current.atom_uuid != tip.atom_uuid {
                return false;
            }
            let atom_uuid = current.atom_uuid.clone();
            tips.remove(&map_key);
            control.state = ResidentSlotState::Absent;
            atom_uuid
        };
        self.invalidate_partition(&tip.molecule_uuid, &tip.hash);
        self.clear_dirty(&tip.dirty_key());
        self.clear_dirty(
            &ResidentMoleculeKey::new(tip.hash.clone(), tip.range.clone())
                .dirty_key(&tip.molecule_uuid),
        );
        self.discharge(&tip.dirty_key());
        self.remove_key_index_member(&tip.molecule_uuid, &tip.hash, &tip.range, true);
        // Budget enforcement can reacquire slots. Atom cleanup also stays
        // outside this short per-slot exclusion boundary.
        drop(slots);
        self.release_unreferenced_atom(&atom_uuid);
        self.enforce_budget();
        true
    }

    /// Drop an atom body once no graph tip names it and the body is clean.
    pub(super) fn release_unreferenced_atom(&self, uuid: &str) {
        let key = DirtyKey::Atom(uuid.to_string());
        if self.is_dirty(&key) {
            return;
        }
        let referenced = self
            .tips
            .read()
            .expect("tips lock")
            .values()
            .any(|tip| tip.atom_uuid == uuid);
        if referenced {
            return;
        }
        let removed = self
            .atoms
            .write()
            .expect("atoms lock")
            .remove(uuid)
            .is_some();
        if removed {
            self.discharge(&key);
        }
    }

    pub(super) fn clear_dirty(&self, key: &DirtyKey) {
        let mut dirty = self.dirty.write().expect("dirty lock");
        if dirty.remove(key) {
            self.metrics.record_persist_flushed();
        }
    }

    /// Forget a dirty marker without counting it as a successful persist.
    ///
    /// [`Self::clear_dirty`] means "the durable put landed" and records a
    /// flush. The destructive-purge paths below mean the opposite — the durable
    /// row is being hard-deleted, so the pending put must simply never happen —
    /// and recording those as flushes would inflate the persist metric with
    /// writes that were abandoned.
    pub(super) fn forget_dirty(&self, key: &DirtyKey) {
        self.dirty.write().expect("dirty lock").remove(key);
    }
}
