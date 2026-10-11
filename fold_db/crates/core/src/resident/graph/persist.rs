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
        }
        self.enforce_budget();
    }

    /// Clear a tip only when its atom and resident revision are still current.
    ///
    /// The slot read lock spans the revision check, the tip check, and both
    /// dirty clears. A new apply must publish its revision before dirty state,
    /// so it cannot lose its dirty marker in this critical section.
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
        let slots = self.slots.read().expect("resident slot lock");
        let Some(control) = slots.get(&map_key) else {
            return false;
        };
        if resident_revision.is_some_and(|revision| control.resident_revision != revision) {
            return false;
        }
        let tips = self.tips.read().expect("tips lock");
        if tips
            .get(&map_key)
            .is_none_or(|current| current.atom_uuid != tip.atom_uuid)
        {
            return false;
        }
        self.invalidate_partition(&tip.molecule_uuid, &tip.hash);
        self.clear_dirty(&tip.dirty_key());
        self.clear_dirty(
            &ResidentMoleculeKey::new(tip.hash.clone(), tip.range.clone())
                .dirty_key(&tip.molecule_uuid),
        );
        drop(tips);
        drop(slots);
        self.enforce_budget();
        true
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
