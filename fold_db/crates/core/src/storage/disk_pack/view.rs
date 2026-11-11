//! One atomic disk root, immutable read view, diagnostics, and warm-memory relief.

use super::*;
use laststore::disk_run::home::RunHomeDiagnostics;

/// A read-only operational boundary; this method never wakes a flush worker.
#[derive(serde::Serialize)]
pub struct NativeDiskStatus {
    pub cleanup: NativeCleanupStatus,
    pub generation: u64,
    pub frontier: u64,
    pub accepted_batch: bool,
    pub accepted_batches: usize,
    pub accepted_sequence: u64,
    pub completed_sequence: u64,
    pub writer_failed: bool,
    pub cas_delete_recovery: Option<CasDeleteRecovery>,
    /// Decoded records in the warm set.
    pub warm_records: usize,
    pub core_warm_held: usize,
    pub core_warm_hits: u64,
    pub core_warm_misses: u64,
    pub core_warm_purged: u64,
    /// Warm set byte charge and the enforced clean-record retention limit.
    pub core_warm_bytes: crate::resident::CoreWarmByteStats,
}

#[derive(serde::Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private: Option<RunHomeDiagnostics>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryStatus {
    pub warm: crate::resident::CoreWarmByteStats,
    pub records: usize,
    pub held_records: usize,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryPurgeReport {
    pub records_purged: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub target_bytes: u64,
    pub target_met: bool,
    pub remaining: NativeMemoryStatus,
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct NativeReadView {
    pub(in crate::storage::disk_pack) current: physical::PhysicalRoot,
    pub(in crate::storage::disk_pack) revision: u64,
}

/// Generation and frontier of the last root that a commit or a merge installed.
pub(in crate::storage::disk_pack) struct LastRoot {
    generation: AtomicU64,
    frontier: AtomicU64,
}

impl LastRoot {
    pub(in crate::storage::disk_pack) fn new(root: &physical::PhysicalRoot) -> Self {
        Self {
            generation: AtomicU64::new(root.generation()),
            frontier: AtomicU64::new(root.frontier()),
        }
    }

    /// A commit and a merge adoption can race; the newer generation wins.
    pub(in crate::storage::disk_pack) fn observe(&self, generation: u64, frontier: u64) {
        if self.generation.fetch_max(generation, Ordering::Relaxed) <= generation {
            self.frontier.store(frontier, Ordering::Relaxed);
        }
    }

    fn get(&self) -> (u64, u64) {
        (
            self.generation.load(Ordering::Relaxed),
            self.frontier.load(Ordering::Relaxed),
        )
    }
}

impl PackRuntime {
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.home_identity()
    }

    /// Health probes read only this atomic; they never wait for a writer or read a record.
    pub fn requires_recovery(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let view = self.view.try_read().ok();
        let (generation, frontier) = view.as_deref().map_or_else(
            || self.last_root.get(),
            |view| (view.current.generation(), view.current.frontier()),
        );
        let accepted = queue.request_count();
        let (core, core_bytes) = {
            let mut core = self.core.lock().expect("native core status");
            (core.stats(), core.byte_stats())
        };
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation,
            frontier,
            accepted_batch: accepted > 0,
            accepted_batches: accepted,
            accepted_sequence: queue.sequence(),
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.requires_recovery(),
            cas_delete_recovery: self.physical.cas_delete_recovery,
            warm_records: core.records,
            core_warm_held: core.held,
            core_warm_hits: core.hits,
            core_warm_misses: core.misses,
            core_warm_purged: core.purged,
            core_warm_bytes: core_bytes,
        }
    }

    /// Physical totals are for independently captured home roots.
    pub async fn diagnostics_native(
        &self,
        include_inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        if include_inventory
            && self
                .queue
                .lock()
                .expect("native diagnostic queue")
                .request_count()
                > 0
        {
            return Err(StorageError::InvalidOperation(
                "storage inventory requires a checkpoint without accepted writes".into(),
            ));
        }
        let main = Arc::clone(&self.physical.home);
        let cas = Arc::clone(&self.physical.cas);
        let private = self
            .physical
            .shared
            .as_ref()
            .map(|shared| Arc::clone(&shared.private));
        tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            let main_cut = main.diagnostic_checkpoint(deadline).map_err(map)?;
            let cas_cut = cas.diagnostic_checkpoint(deadline).map_err(map)?;
            let private = private
                .as_ref()
                .map(|home| {
                    home.diagnostic_checkpoint(deadline)
                        .map(|cut| cut.finish(home, include_inventory, deadline))
                        .map_err(map)
                })
                .transpose()?;
            Ok(Some(NativeStorageDiagnostics {
                main: main_cut.finish(&main, include_inventory, deadline),
                cas: cas_cut.finish(&cas, include_inventory, deadline),
                private,
            }))
        })
        .await?
    }

    /// Keep held records and definitions.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, before, after) = core.purge_clean_to_bytes(target);
        let stats = core.stats();
        let remaining = NativeMemoryStatus {
            warm: core.byte_stats(),
            records: stats.records,
            held_records: stats.held,
        };
        drop(core);
        Ok(NativeMemoryPurgeReport {
            records_purged: purged as u64,
            bytes_before: before as u64,
            bytes_after: after as u64,
            target_bytes,
            target_met: after as u64 <= target_bytes,
            remaining,
        })
    }

    pub(in crate::storage::disk_pack) fn install_committed(
        &self,
        current: physical::PhysicalRoot,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let records = record_warm::committed_records(requests);
        let mut view = self.view.write().expect("native complete read view");
        if !hidden_changes.is_empty() {
            let revision = next_revision(&view)?;
            if requests.is_empty() {
                self.invalidate_changed_records(hidden_changes);
            }
            view.revision = revision;
        }
        for request in requests {
            self.invalidate_changed_records(&request.changed_coordinates());
        }
        if !records.is_empty() {
            self.core
                .lock()
                .expect("committed warm records")
                .admit_records(records);
        }
        let replaced = std::mem::replace(&mut view.current, current);
        drop(view);
        drop(replaced);
        self.adopt_merged_root()?;
        Ok(())
    }

    fn invalidate_changed_records(&self, changes: &[(String, String)]) {
        self.core
            .lock()
            .expect("core publication invalidation")
            .invalidate_records(changes);
    }
}

fn next_revision(view: &NativeReadView) -> StorageResult<u64> {
    view.revision
        .checked_add(1)
        .ok_or_else(|| StorageError::BackendError("native accepted revision exhausted".into()))
}

fn unavailable() -> StorageError {
    StorageError::BackendError("native warm set lock is unavailable".into())
}
