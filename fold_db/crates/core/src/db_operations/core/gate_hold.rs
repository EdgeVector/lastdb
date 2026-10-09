use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Node-lifetime accounting for how long molecule write gates are **held**.
///
/// The request-phase `molecule_gate` measures the opposite end of the same
/// lock: how long a writer *waited* to acquire it. Wait time alone cannot
/// distinguish the two situations that produce it, and they have opposite
/// fixes:
///
/// - **many writers, one hot key** — each holder is fast, the queue is deep.
///   The fix belongs to the caller's key layout (spread the key).
/// - **one slow holder** — the queue is shallow, but whoever holds the gate is
///   stalling inside it. PR 3 moved `restore_missing_molecules` ahead of the
///   apply gate so a cold disk read cannot extend the hold. A remaining stall
///   inside the gate is a write-path bug.
///
/// Measured on the live primary 2026-08-06: `molecule_gate` was 95% of
/// `kanban-probe` mutation wall time (729 s over 541 mutations) while every
/// sample in the recent ring showed single-digit *microseconds* of wait — a
/// bursty stall that the wait gauge alone could not attribute to either cause.
///
/// Counted for every acquisition in both resident modes: the guard is moved
/// into the deferred persist task under `LASTDB_RESIDENT_MODE=write`, so the
/// hold outlives the request that opened it and a per-request phase would
/// silently record zero there. A node-lifetime counter is honest in both.
#[derive(Debug, Default)]
pub struct MoleculeGateHoldStats {
    total_us: AtomicU64,
    count: AtomicU64,
    max_us: AtomicU64,
}

impl MoleculeGateHoldStats {
    /// Record one completed hold.
    pub(crate) fn record(&self, held: std::time::Duration) {
        let us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Snapshot the three counters.
    ///
    /// Read with three separate atomic loads, so a hold completing mid-read can
    /// land in `count` but not `total_us`. That skews an average by one sample
    /// and cannot produce an impossible reading, which is the right trade for
    /// keeping the recording side lock-free on the write path.
    #[must_use]
    pub fn snapshot(&self) -> MoleculeGateHold {
        MoleculeGateHold {
            total_us: self.total_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

/// A read of [`MoleculeGateHoldStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MoleculeGateHold {
    /// Summed hold time across every completed acquisition, microseconds.
    pub total_us: u64,
    /// How many gate acquisitions have been released.
    ///
    /// The denominator for a mean hold. Gates still held right now are absent
    /// from both this and `total_us` — an in-flight stall shows up when it
    /// ends, so a node wedged under a gate reports the stall late rather than
    /// never. `max_us` is what surfaces a single pathological hold.
    pub count: u64,
    /// Longest single hold observed, microseconds.
    pub max_us: u64,
}

impl super::DbOperations {
    /// Molecule write-gate hold accounting, for pairing with the
    /// `molecule_gate` wait phase.
    #[must_use]
    pub fn molecule_gate_hold(&self) -> MoleculeGateHold {
        self.molecule_gate_hold.snapshot()
    }

    /// The shared hold recorder, handed to each gate guard so it can book its
    /// own duration when it drops.
    #[must_use]
    pub(crate) fn molecule_gate_hold_stats(&self) -> Arc<MoleculeGateHoldStats> {
        Arc::clone(&self.molecule_gate_hold)
    }
}
