//! Revision observations retain their slot until the final observer leaves.

use std::sync::Arc;

use super::{tip_map_key, ResidentGraph, ResidentSlotControl};

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
        self.graph.release_slot_observation(&self.key);
    }
}

/// A prepared mutation owns this lease through revision validation and lane fill.
/// Clones share one observation; only the final clone releases the slot.
#[derive(Debug, Clone)]
pub(crate) struct OwnedSlotRead(Arc<OwnedSlotObservation>);

#[derive(Debug)]
struct OwnedSlotObservation {
    graph: Arc<ResidentGraph>,
    key: String,
    snapshot: ResidentSlotControl,
}

impl std::ops::Deref for OwnedSlotRead {
    type Target = ResidentSlotControl;
    fn deref(&self) -> &Self::Target {
        &self.0.snapshot
    }
}

impl OwnedSlotRead {
    pub(crate) fn revisions(&self) -> (u64, u64) {
        (self.resident_revision, self.durable_revision)
    }
}

impl Drop for OwnedSlotObservation {
    fn drop(&mut self) {
        self.graph.release_slot_observation(&self.key);
    }
}

impl ResidentGraph {
    /// Hold from before durable I/O until after conditional graph install.
    pub(crate) fn observe_slot(&self, molecule: &str, hash: &str, range: &str) -> SlotRead<'_> {
        let (key, snapshot) = self.register_slot_observation(molecule, hash, range);
        SlotRead {
            graph: self,
            key,
            snapshot,
        }
    }

    /// Hold a prepared revision across async preparation and apply-gate retries.
    pub(crate) fn observe_slot_owned(
        self: &Arc<Self>,
        molecule: &str,
        hash: &str,
        range: &str,
    ) -> OwnedSlotRead {
        let (key, snapshot) = self.register_slot_observation(molecule, hash, range);
        OwnedSlotRead(Arc::new(OwnedSlotObservation {
            graph: Arc::clone(self),
            key,
            snapshot,
        }))
    }

    fn register_slot_observation(
        &self,
        molecule: &str,
        hash: &str,
        range: &str,
    ) -> (String, ResidentSlotControl) {
        let key = tip_map_key(molecule, hash, range);
        let mut readers = self.slot_readers.lock().expect("resident slot reader lock");
        let mut slots = self.slots.write().expect("resident slot lock");
        let snapshot = *slots.entry(key.clone()).or_default();
        *readers.entry(key.clone()).or_default() += 1;
        (key, snapshot)
    }

    fn release_slot_observation(&self, key: &str) {
        let mut readers = self.slot_readers.lock().expect("resident slot reader lock");
        let count = readers.get_mut(key).expect("registered slot reader");
        *count -= 1;
        if *count != 0 {
            return;
        }
        readers.remove(key);
        let mut slots = self.slots.write().expect("resident slot lock");
        self.reclaim_idle_slot(&mut slots, key);
    }
}
