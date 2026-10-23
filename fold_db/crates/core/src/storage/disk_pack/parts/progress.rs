//! Lock-free phases and times for concurrent compaction.
use super::*;

#[derive(Default)]
pub(in crate::storage::disk_pack) struct Progress {
    jobs: AtomicU64,
    retries: AtomicU64,
    phase: AtomicU64,
    copy_millis: AtomicU64,
    swap_millis: AtomicU64,
    swap_max_millis: AtomicU64,
    rounds: AtomicU64,
    changed: AtomicU64,
    boundary: AtomicU64,
    write_rate: AtomicU64,
    last_write: Mutex<Option<std::time::Instant>>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct NativeCompactionStatus {
    pub compaction_jobs: u64,
    pub compaction_publication_retries: u64,
    pub compaction_phase: String,
    pub compaction_copy_millis: u64,
    pub compaction_swap_millis: u64,
    pub compaction_swap_max_millis: u64,
    pub compaction_catch_up_rounds: u64,
    pub compaction_catch_up_records: u64,
    pub compaction_tier_boundary: u64,
}

pub(in crate::storage::disk_pack) struct PhaseGuard(Arc<Progress>);

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        let remaining = self.0.jobs.fetch_sub(1, Relaxed) - 1;
        self.0.phase(if remaining == 0 { 0 } else { 6 });
    }
}

impl Progress {
    pub(in crate::storage::disk_pack) fn observe_growth(&self, growth: u64) {
        if growth == 0 {
            return;
        }
        let now = std::time::Instant::now();
        let previous = self
            .last_write
            .lock()
            .expect("part growth clock")
            .replace(now);
        if let Some(previous) = previous {
            let micros = now.duration_since(previous).as_micros().max(1000);
            let rate = (u128::from(growth) * 1_000_000 / micros).min(u128::from(u64::MAX)) as u64;
            let old = self.write_rate.load(Relaxed);
            self.write_rate
                .store(old.saturating_mul(7).saturating_add(rate) / 8, Relaxed);
        }
    }

    pub(in crate::storage::disk_pack) fn start_before_exhaustion(
        &self,
        trigger: u64,
        minimum: u64,
    ) -> u64 {
        let growth = self
            .write_rate
            .load(Relaxed)
            .saturating_mul(self.copy_millis.load(Relaxed))
            / 1000;
        let headroom = laststore::disk_pack::MAX_DEPENDENCY_PARTS * 85 / 100;
        trigger.min(headroom.saturating_sub(growth).max(minimum))
    }

    pub(in crate::storage::disk_pack) fn begin(self: &Arc<Self>) -> PhaseGuard {
        self.jobs.fetch_add(1, Relaxed);
        self.phase(1);
        PhaseGuard(Arc::clone(self))
    }

    pub(in crate::storage::disk_pack) fn phase(&self, phase: u64) {
        self.phase.store(phase, Relaxed);
    }

    pub(in crate::storage::disk_pack) fn completed(
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
        self.retries.store(receipt.publication_retries, Relaxed);
    }

    pub(in crate::storage::disk_pack) fn status(&self) -> NativeCompactionStatus {
        let jobs = self.jobs.load(Relaxed);
        let phase = match (jobs, self.phase.load(Relaxed)) {
            (0, _) => "idle",
            (2.., _) => "multiple",
            (_, 1) => "capture",
            (_, 2) => "copy",
            (_, 3) => "catch_up",
            (_, 4) => "swap",
            (_, 5) => "reclaim",
            _ => "active",
        };
        NativeCompactionStatus {
            compaction_jobs: jobs,
            compaction_publication_retries: self.retries.load(Relaxed),
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
