//! Tip and atom purge and eviction.

use super::*;

impl ResidentGraph {
    /// Hard-remove a tip from T0 for a **destructive purge**, dirty or not.
    ///
    /// Deliberately not [`Self::try_evict_tip`]. Eviction is a memory-pressure
    /// decision, so it refuses a dirty tip rather than drop an acked write that
    /// has not reached disk. A purge is the opposite situation: the durable row
    /// it would be persisted to is being erased, so refusing would leave the
    /// purged value resident — readable, and worse, eligible to be written back
    /// by the persist worker after the purge completed.
    ///
    /// Returns whether a tip was present.
    pub fn purge_tip(&self, molecule_uuid: &str, hash: &str, range: &str) -> bool {
        self.purge_tip_at_revision(molecule_uuid, hash, range, None)
    }

    /// Remove only the resident state owned by a completed purge ticket.
    /// A newer resident apply keeps its tip, marker, dirty pin, and turn.
    pub(crate) fn purge_tip_after_persist(&self, ticket: &PersistSlotRevision) -> bool {
        self.purge_tip_at_revision(
            &ticket.molecule_uuid,
            &ticket.disk_hash,
            &ticket.disk_range,
            Some(ticket.resident_revision),
        )
    }

    pub(super) fn purge_tip_at_revision(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        completed_revision: Option<u64>,
    ) -> bool {
        let key = DirtyKey::MoleculeTip {
            molecule_uuid: molecule_uuid.to_string(),
            hash: hash.to_string(),
            range: range.to_string(),
        };
        let k = tip_map_key(molecule_uuid, hash, range);
        // Lock order: readers, rehydrate flights, slots, dirty, then tips. Keep the
        // admission lock through removal so a new cold read cannot miss this
        // invalidation and install an old disk result.
        let readers = self.slot_readers.lock().expect("resident slot reader lock");
        let flights = self
            .rehydrate_flights
            .lock()
            .expect("resident rehydrate flight lock");
        let mut slots = self.slots.write().expect("resident slot lock");
        if let Some(completed) = completed_revision {
            if !slots.get(&k).is_some_and(|slot| {
                slot.resident_revision == completed && slot.durable_revision == completed
            }) {
                return false;
            }
        }
        self.invalidate_partition(molecule_uuid, hash);
        if flights.contains_key(&k) || readers.contains_key(&k) {
            let control = slots.entry(k.clone()).or_default();
            let was_clean = control.resident_revision == control.durable_revision;
            control.resident_revision = control.resident_revision.saturating_add(1);
            if was_clean {
                control.durable_revision = control.resident_revision;
            }
            control.state = ResidentSlotState::Absent;
        } else {
            slots.remove(&k);
        }
        self.forget_dirty(&key);
        let removed = self.tips.write().expect("tips lock").remove(&k).is_some();
        self.invalidate_partition(molecule_uuid, hash);
        if removed {
            self.discharge(&key);
            self.metrics.record_evicted(ResidentKind::MoleculeTip);
        }
        self.remove_key_index_member(molecule_uuid, hash, range, true);
        self.remove_key_tombstone(molecule_uuid, hash, range, true);
        self.invalidate_partition(molecule_uuid, hash);
        // Keep the revision guard through every keyed removal. A later apply
        // must not publish between the check and the index/marker cleanup.
        drop(slots);
        drop(flights);
        drop(readers);
        removed
    }

    /// Hard-remove an atom body from T0 for a **destructive purge**, dirty or
    /// not. See [`Self::purge_tip`] for why this is not an eviction.
    ///
    /// Call only for atoms the purge actually hard-deleted: atoms are
    /// content-addressed, so one still referenced by a sibling key must stay
    /// resident along with its durable row.
    ///
    /// Returns whether an atom was present.
    pub fn purge_atom(&self, uuid: &str) -> bool {
        let key = DirtyKey::Atom(uuid.to_string());
        self.forget_dirty(&key);
        let removed = self
            .atoms
            .write()
            .expect("atoms lock")
            .remove(uuid)
            .is_some();
        if removed {
            self.discharge(&key);
            self.metrics.record_evicted(ResidentKind::Atom);
        }
        removed
    }

    pub(crate) fn resident_field_ready(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Option<ResidentTip> {
        let tip = self.resolve_tip(molecule_uuid, hash, range)?;
        self.resolve_atom(&tip.value.atom_uuid)?;
        Some(tip.value)
    }

    /// Evict a clean tip; refuses if dirty.
    pub fn try_evict_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<bool, String> {
        let key = DirtyKey::MoleculeTip {
            molecule_uuid: molecule_uuid.to_string(),
            hash: hash.to_string(),
            range: range.to_string(),
        };
        let k = tip_map_key(molecule_uuid, hash, range);
        let readers = self.slot_readers.lock().expect("resident slot reader lock");
        let flights = self
            .rehydrate_flights
            .lock()
            .expect("resident rehydrate flight lock");
        let mut slots = self.slots.write().expect("resident slot lock");
        if self.is_dirty(&key) {
            return Err("refuse to evict dirty tip until persist acks".into());
        }
        if slots
            .get(&k)
            .is_some_and(|slot| slot.resident_revision != slot.durable_revision)
        {
            return Err("refuse to evict tip before its exact persist turn completes".into());
        }
        self.invalidate_partition(molecule_uuid, hash);
        if flights.contains_key(&k) || readers.contains_key(&k) {
            let control = slots.entry(k.clone()).or_default();
            control.resident_revision = control.resident_revision.saturating_add(1);
            // This is a clean invalidation, with no outstanding persist turn.
            // Keep both clocks equal so a later rehydrate stays evictable.
            control.durable_revision = control.resident_revision;
            control.state = ResidentSlotState::Absent;
        } else {
            slots.remove(&k);
        }
        let removed = self.tips.write().expect("tips lock").remove(&k).is_some();
        self.invalidate_partition(molecule_uuid, hash);
        drop(slots);
        drop(flights);
        drop(readers);
        if removed {
            self.discharge(&key);
        }
        Ok(removed)
    }
}
