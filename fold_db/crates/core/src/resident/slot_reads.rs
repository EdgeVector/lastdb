//! Cold reads retain a revision identity only for the duration of their I/O.

use super::{tip_map_key, ResidentGraph, ResidentSlotControl, ResidentSlotState};

pub(crate) struct SlotRead<'a> {
    graph: &'a ResidentGraph,
    key: String,
    snapshot: ResidentSlotControl,
}

impl std::ops::Deref for SlotRead<'_> {
    type Target = ResidentSlotControl;
    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl SlotRead<'_> {
    pub(crate) fn revisions(&self) -> (u64, u64) {
        (
            self.snapshot.resident_revision,
            self.snapshot.durable_revision,
        )
    }
}

impl Drop for SlotRead<'_> {
    fn drop(&mut self) {
        let mut readers = self
            .graph
            .slot_readers
            .lock()
            .expect("resident slot reader lock");
        let count = readers.get_mut(&self.key).expect("registered slot reader");
        *count -= 1;
        if *count != 0 {
            return;
        }
        readers.remove(&self.key);
        let flights = self
            .graph
            .rehydrate_flights
            .lock()
            .expect("resident rehydrate flight lock");
        let mut slots = self.graph.slots.write().expect("resident slot lock");
        if !flights.contains_key(&self.key)
            && slots
                .get(&self.key)
                .is_some_and(|slot| slot.state != ResidentSlotState::Ready)
        {
            slots.remove(&self.key);
        }
    }
}

impl ResidentGraph {
    /// Hold this guard from before durable I/O until after conditional install.
    /// Eviction can remove the value, but cannot recycle its revision identity.
    pub(crate) fn observe_slot(&self, molecule: &str, hash: &str, range: &str) -> SlotRead<'_> {
        let key = tip_map_key(molecule, hash, range);
        let mut readers = self.slot_readers.lock().expect("resident slot reader lock");
        let mut slots = self.slots.write().expect("resident slot lock");
        let snapshot = *slots.entry(key.clone()).or_default();
        *readers.entry(key.clone()).or_default() += 1;
        SlotRead {
            graph: self,
            key,
            snapshot,
        }
    }
}
