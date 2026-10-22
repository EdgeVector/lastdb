//! Lock-free phases and times for concurrent compaction.
use super::*;

#[derive(Default)]
pub(super) struct Progress {
    phase: AtomicU64,
    copy_millis: AtomicU64,
    swap_millis: AtomicU64,
    swap_max_millis: AtomicU64,
    rounds: AtomicU64,
    changed: AtomicU64,
    boundary: AtomicU64,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct NativeCompactionStatus {
    pub compaction_phase: String,
    pub compaction_copy_millis: u64,
    pub compaction_swap_millis: u64,
    pub compaction_swap_max_millis: u64,
    pub compaction_catch_up_rounds: u64,
    pub compaction_catch_up_records: u64,
    pub compaction_tier_boundary: u64,
}

pub(super) struct PhaseGuard(Arc<Progress>);

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        self.0.phase(0);
    }
}

impl Progress {
    pub(super) fn begin(self: &Arc<Self>) -> PhaseGuard {
        self.phase(1);
        PhaseGuard(Arc::clone(self))
    }

    pub(super) fn phase(&self, phase: u64) {
        self.phase.store(phase, Relaxed);
    }

    pub(super) fn completed(
        &self,
        receipt: &laststore::disk_pack::maintenance::MaintenanceReceipt,
        swap: u64,
    ) {
        self.copy_millis.store(receipt.copy_millis, Relaxed);
        self.swap_millis.store(swap, Relaxed);
        self.swap_max_millis.fetch_max(swap, Relaxed);
        self.rounds.store(receipt.catch_up_rounds, Relaxed);
        self.changed.store(receipt.catch_up_records, Relaxed);
        self.boundary.store(receipt.tier_boundary, Relaxed);
    }

    pub(super) fn status(&self) -> NativeCompactionStatus {
        let phase = match self.phase.load(Relaxed) {
            1 => "capture",
            2 => "copy",
            3 => "catch_up",
            4 => "swap",
            5 => "reclaim",
            _ => "idle",
        };
        NativeCompactionStatus {
            compaction_phase: phase.into(),
            compaction_copy_millis: self.copy_millis.load(Relaxed),
            compaction_swap_millis: self.swap_millis.load(Relaxed),
            compaction_swap_max_millis: self.swap_max_millis.load(Relaxed),
            compaction_catch_up_rounds: self.rounds.load(Relaxed),
            compaction_catch_up_records: self.changed.load(Relaxed),
            compaction_tier_boundary: self.boundary.load(Relaxed),
        }
    }
}
