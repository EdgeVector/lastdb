//! Warm set byte charges. Stage 3 enforces the limit through the purge.
//!
//! Atoms are tracked by allocation (`Arc` pointer) with a weak reference, so an
//! atom stays charged while any holder keeps it, including a running call that
//! outlived the path's purge. A dead entry leaves the gauge at the next sweep.
//! Only atoms without a current slot can die, so sweeps visit that subset.

use super::charge::{atom_charge, path_charge, slot_charge, source_charge, tip_heap_charge};
use super::*;

/// Sweep dead atom entries after this many new tracks, to bound the map.
const SWEEP_EVERY: usize = 1024;
/// Minimum new atom tracks before a purge decision sweeps again.
const SWEEP_BEFORE_PURGE: usize = 64;

pub(super) struct TrackedAtom {
    atom: Weak<Atom>,
    bytes: usize,
    slots: usize,
    pending: Option<usize>,
}

#[derive(Default)]
pub(super) struct WarmBytes {
    atoms: HashMap<usize, TrackedAtom>,
    /// Unique allocation keys with no current slot; entries keep their position.
    pending: Vec<usize>,
    atom_bytes: usize,
    path_bytes: usize,
    slot_bytes: usize,
    link_bytes: usize,
    peak_bytes: usize,
    tracks_since_sweep: usize,
    purged_bytes: u64,
    over_limit_admissions: u64,
}

/// Charged warm set bytes. The purge keeps `used_bytes` at or under the limit
/// whenever clean, unheld paths remain.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CoreWarmByteStats {
    /// `LASTDB_CORE_WARM_BYTES`.
    pub limit_bytes: usize,
    pub used_bytes: usize,
    /// Highest charge seen at a sweep since start. Sweeps run every 1024 atom
    /// tracks and on each status read, after dead atoms leave the total.
    pub peak_bytes: usize,
    pub path_bytes: usize,
    pub atom_bytes: usize,
    /// Per-slot map entries, key copies, and current tip metadata heaps.
    pub slot_bytes: usize,
    /// Source links and interned source keys (the invalidation index).
    pub link_bytes: usize,
    /// Atom allocations that only a running call keeps.
    pub held_bytes: usize,
    /// Retired unflushed warm-path charge; disk-first publication keeps this zero.
    pub dirty_bytes: usize,
    /// The share of the warm byte limit available to unflushed writes.
    pub dirty_limit_bytes: usize,
    pub tracked_atoms: usize,
    /// Charged bytes that the purge removed since start.
    pub purged_bytes: u64,
    /// Admissions that left the charge over the limit because only held paths remained.
    pub over_limit_admissions: u64,
}

impl WarmBytes {
    pub(super) fn used(&self) -> usize {
        self.atom_bytes + self.path_bytes + self.slot_bytes + self.link_bytes
    }

    pub(super) fn note_purged(&mut self, bytes: usize) {
        self.purged_bytes = self.purged_bytes.saturating_add(bytes as u64);
    }

    pub(super) fn note_over_limit(&mut self) {
        self.over_limit_admissions = self.over_limit_admissions.saturating_add(1);
    }

    fn note_peak(&mut self) {
        self.peak_bytes = self.peak_bytes.max(self.used());
    }

    pub(super) fn sweep(&mut self) {
        let _timing = laststore::commit_timing::stage("memory_atom_sweep_us");
        laststore::commit_timing::count("memory_atom_sweep_calls", 1);
        laststore::commit_timing::count("memory_atom_sweep_entries", self.pending.len() as u64);
        let mut index = 0;
        while let Some(&key) = self.pending.get(index) {
            let tracked = self.atoms.get(&key).expect("tracked pending atom");
            if tracked.atom.strong_count() == 0 {
                self.remove_atom(key);
            } else {
                index += 1;
            }
        }
        self.tracks_since_sweep = 0;
        // Dead atoms leave first, so a freed allocation never inflates the peak.
        self.note_peak();
    }

    /// Sweep before a purge decision when enough new atoms arrived since the
    /// last sweep, so freed allocations do not trigger extra purging.
    pub(super) fn sweep_if_stale(&mut self) {
        if self.tracks_since_sweep >= SWEEP_BEFORE_PURGE {
            self.sweep();
        }
    }

    pub(super) fn release_dead_atom(&mut self, key: usize) {
        if self
            .atoms
            .get(&key)
            .is_some_and(|tracked| tracked.atom.strong_count() == 0)
        {
            self.remove_atom(key);
        }
    }

    /// One count per actual slot, regardless of how many paths own that slot.
    pub(super) fn attach_atom(&mut self, key: usize) {
        self.remove_pending(key);
        let tracked = self.atoms.get_mut(&key).expect("tracked slot atom");
        tracked.slots = tracked.slots.checked_add(1).expect("atom slot count");
    }

    pub(super) fn detach_atom(&mut self, key: usize) {
        let tracked = self.atoms.get_mut(&key).expect("tracked slot atom");
        tracked.slots = tracked.slots.checked_sub(1).expect("atom slot count");
        if tracked.slots == 0 && tracked.pending.is_none() {
            tracked.pending = Some(self.pending.len());
            self.pending.push(key);
        }
    }

    fn remove_pending(&mut self, key: usize) {
        let Some(index) = self
            .atoms
            .get_mut(&key)
            .and_then(|atom| atom.pending.take())
        else {
            return;
        };
        self.pending.swap_remove(index);
        if let Some(&moved) = self.pending.get(index) {
            self.atoms
                .get_mut(&moved)
                .expect("tracked pending atom")
                .pending = Some(index);
        }
    }

    fn remove_atom(&mut self, key: usize) {
        self.remove_pending(key);
        if let Some(tracked) = self.atoms.remove(&key) {
            self.atom_bytes -= tracked.bytes;
        }
    }
}

impl CoreWarmSet {
    /// Charge one new path. Returns the charge that `PathState` keeps.
    pub(super) fn charge_path(&mut self) -> usize {
        let charge = path_charge();
        self.bytes.path_bytes += charge;
        charge
    }

    pub(super) fn release_path(&mut self, charge: usize) {
        self.bytes.path_bytes -= charge;
    }

    /// Charge a slot when its first owner path arrives.
    pub(super) fn charge_slot(&mut self, id: tables::SlotId) {
        if let Some(slot) = self.slots.get(id) {
            self.bytes.slot_bytes += slot_charge(&slot.key);
        }
    }

    /// Release a slot when its last owner path leaves.
    pub(super) fn release_slot(&mut self, id: tables::SlotId) {
        if let Some(slot) = self.slots.get(id) {
            self.bytes.slot_bytes -= slot_charge(&slot.key);
        }
    }

    /// Measure the current tip after replacement. Immutable write-model copies
    /// keep their separate metadata reservations; this charges the molecule copy.
    pub(super) fn refresh_tip_charge(&mut self, id: tables::SlotId) {
        let charge = self.slots.get(id).map_or(0, |slot| {
            slot.tip
                .as_ref()
                .map_or(0, |entry| tip_heap_charge(entry, slot.metadata.as_deref()))
        });
        self.set_tip_charge(id, charge);
    }

    pub(super) fn set_tip_charge(&mut self, id: tables::SlotId, charge: usize) {
        if let Some(slot) = self.slots.get_mut(id) {
            self.bytes.slot_bytes -= slot.tip_charge;
            self.bytes.slot_bytes += charge;
            slot.tip_charge = charge;
        }
    }

    /// Charge an interned source key when its first link arrives.
    pub(super) fn charge_source(&mut self, packed_len: usize) {
        self.bytes.link_bytes += source_charge(packed_len);
    }

    /// Release an interned source key when its last link leaves.
    pub(super) fn release_source(&mut self, packed_len: usize) {
        self.bytes.link_bytes -= source_charge(packed_len);
    }

    pub(super) fn charge_link(&mut self, charge: usize) {
        self.bytes.link_bytes += charge;
    }

    pub(super) fn release_links(&mut self, charge: usize) {
        self.bytes.link_bytes -= charge;
    }

    /// Charge an atom allocation once. A reused address with a dead entry is a
    /// new allocation, so its old charge is replaced.
    pub(super) fn track_atom(&mut self, atom: &Arc<Atom>) {
        let key = Arc::as_ptr(atom) as usize;
        if let Some(tracked) = self.bytes.atoms.get(&key) {
            if tracked.atom.strong_count() > 0 {
                return;
            }
            self.bytes.remove_atom(key);
        }
        let bytes = atom_charge(atom);
        let pending = self.bytes.pending.len();
        self.bytes.pending.push(key);
        self.bytes.atoms.insert(
            key,
            TrackedAtom {
                atom: Arc::downgrade(atom),
                bytes,
                slots: 0,
                pending: Some(pending),
            },
        );
        self.bytes.atom_bytes += bytes;
        self.bytes.tracks_since_sweep += 1;
        if self.bytes.tracks_since_sweep >= SWEEP_EVERY {
            self.bytes.sweep();
        }
    }

    /// Sweep, then classify each live atom allocation as owned by the core or
    /// held only by calls. The walk is O(paths + atoms); status calls it.
    pub(crate) fn byte_stats(&mut self) -> CoreWarmByteStats {
        self.bytes.sweep();
        let mut owned = HashSet::new();
        for (_, slot) in self.slots.iter() {
            if let Some(shared) = &slot.atom {
                owned.insert(Arc::as_ptr(&shared.value) as usize);
            }
            if let Some(value) = slot.owned.as_ref().and_then(|owned| owned.value.as_ref()) {
                owned.insert(Arc::as_ptr(&value.atom) as usize);
            }
        }
        let held_bytes = self
            .bytes
            .atoms
            .iter()
            .filter(|(key, _)| !owned.contains(*key))
            .map(|(_, tracked)| tracked.bytes)
            .sum();
        let limit_bytes = crate::resident::core_warm_bytes();
        CoreWarmByteStats {
            limit_bytes,
            used_bytes: self.bytes.used(),
            peak_bytes: self.bytes.peak_bytes,
            path_bytes: self.bytes.path_bytes,
            atom_bytes: self.bytes.atom_bytes,
            slot_bytes: self.bytes.slot_bytes,
            link_bytes: self.bytes.link_bytes,
            held_bytes,
            dirty_bytes: 0,
            dirty_limit_bytes: crate::resident::core_warm_dirty_bytes(limit_bytes),
            tracked_atoms: self.bytes.atoms.len(),
            purged_bytes: self.bytes.purged_bytes,
            over_limit_admissions: self.bytes.over_limit_admissions,
        }
    }
}
