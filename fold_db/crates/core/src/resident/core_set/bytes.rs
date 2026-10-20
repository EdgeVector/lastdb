//! Warm set byte charges. Stage 3 enforces the limit through the purge.
//!
//! Atoms are tracked by allocation (`Arc` pointer) with a weak reference, so an
//! atom stays charged while any holder keeps it, including a running call that
//! outlived the path's purge. A dead entry leaves the gauge at the next sweep.

use super::charge::{atom_charge, path_charge, slot_charge, source_charge};
use super::*;

/// Sweep dead atom entries after this many new tracks, to bound the map.
const SWEEP_EVERY: usize = 1024;
/// Minimum new atom tracks before a purge decision sweeps again.
const SWEEP_BEFORE_PURGE: usize = 64;

pub(super) struct TrackedAtom {
    atom: Weak<Atom>,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct WarmBytes {
    atoms: HashMap<usize, TrackedAtom>,
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
    /// Per-slot map entries and key copies.
    pub slot_bytes: usize,
    /// Source links and interned source keys (the invalidation index).
    pub link_bytes: usize,
    /// Atom allocations that only a running call keeps.
    pub held_bytes: usize,
    /// Charges of unflushed write paths and their atoms.
    pub dirty_bytes: usize,
    /// The dirty share of the limit; not enforced in stage 1.
    pub dirty_limit_bytes: usize,
    pub tracked_atoms: usize,
    /// Charged bytes that the purge removed since start.
    pub purged_bytes: u64,
    /// Admissions that left the charge over the limit because only held or
    /// unflushed paths remained.
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

    fn sweep(&mut self) {
        let mut freed = 0;
        self.atoms.retain(|_, tracked| {
            let alive = tracked.atom.strong_count() > 0;
            if !alive {
                freed += tracked.bytes;
            }
            alive
        });
        self.atom_bytes -= freed;
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
            if let Some(tracked) = self.atoms.remove(&key) {
                self.atom_bytes -= tracked.bytes;
            }
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
            self.bytes.atom_bytes -= tracked.bytes;
        }
        let bytes = atom_charge(atom);
        self.bytes.atoms.insert(
            key,
            TrackedAtom {
                atom: Arc::downgrade(atom),
                bytes,
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
            dirty_bytes: self.dirty_bytes(),
            dirty_limit_bytes: crate::resident::core_warm_dirty_bytes(limit_bytes),
            tracked_atoms: self.bytes.atoms.len(),
            purged_bytes: self.bytes.purged_bytes,
            over_limit_admissions: self.bytes.over_limit_admissions,
        }
    }

    fn dirty_bytes(&self) -> usize {
        let mut atoms = HashSet::new();
        let mut bytes = 0;
        for (_, state) in self.paths.iter() {
            if state.dirty.is_none() {
                continue;
            }
            bytes += state.charge;
            let atom = self.slots.get(state.slot).and_then(|slot| {
                slot.owned
                    .as_ref()
                    .and_then(|owned| owned.value.as_ref())
                    .map(|value| Arc::as_ptr(&value.atom) as usize)
                    .or_else(|| {
                        slot.atom
                            .as_ref()
                            .map(|atom| Arc::as_ptr(&atom.value) as usize)
                    })
            });
            if let Some(key) = atom.filter(|key| atoms.insert(*key)) {
                bytes += self
                    .bytes
                    .atoms
                    .get(&key)
                    .map_or(0, |tracked| tracked.bytes);
            }
        }
        bytes
    }
}
