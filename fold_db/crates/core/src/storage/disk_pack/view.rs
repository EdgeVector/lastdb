//! One atomic disk root and immutable typed core read view.

use super::*;

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
    pub core_memory: crate::resident::CoreMemoryStats,
    pub(crate) core_model: crate::resident::CoreModelStats,
}

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) current: physical::PhysicalRoot,
    pub(super) revision: u64,
}

/// Generation and frontier of the last root that a commit or a merge
/// installed. Atomics only: a status read uses them while the view lock is
/// busy, so status never waits for an install.
pub(super) struct LastRoot {
    generation: AtomicU64,
    frontier: AtomicU64,
}

impl LastRoot {
    pub(super) fn new(root: &physical::PhysicalRoot) -> Self {
        Self {
            generation: AtomicU64::new(root.generation()),
            frontier: AtomicU64::new(root.frontier()),
        }
    }

    /// A commit and a merge adoption can race; the newer generation wins.
    pub(super) fn observe(&self, generation: u64, frontier: u64) {
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

    /// The same failure mark that refuses new record reads and writes.
    /// Health probes read only this atomic; they never wait for a writer or read a record.
    pub fn requires_recovery(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        // A status read must not wait behind an install, so it reports the
        // last installed root while the view lock is busy.
        let view = self.view.try_read().ok();
        let (generation, frontier) = view.as_deref().map_or_else(
            || self.last_root.get(),
            |view| (view.current.generation(), view.current.frontier()),
        );
        let accepted = queue.request_count();
        let (core, core_bytes, core_memory, core_model) = {
            let mut core = self.core.lock().expect("native core status");
            (
                core.stats(),
                core.byte_stats(),
                core.memory_stats(),
                core.model_stats(),
            )
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
            core_memory,
            core_model,
        }
    }

    /// The caller owns the write gate and complete physical/logical reservations.
    /// Captured core values retain their reservation until the last reader releases them.
    /// Old calls retain their typed values; the flush log is not their read state.
    pub(super) fn install_committed(
        &self,
        current: physical::PhysicalRoot,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        // Checked before the view lock; published under it with the root.
        let records = laststore::commit_timing::during("memory_record_check_us", || {
            record_warm::committed_records(requests)
        });
        let mut view = laststore::commit_timing::during("memory_view_lock_wait_us", || {
            self.view.write().expect("native complete read view")
        });
        if !hidden_changes.is_empty() {
            let revision = next_revision(&view)?;
            if requests.is_empty() {
                self.invalidate_changed_records(hidden_changes);
            }
            view.revision = revision;
        }
        for request in requests {
            // Later raw effects invalidate an earlier request's clean cache
            // even when the later request uses the disk-only memory fallback.
            laststore::commit_timing::during("memory_invalidation_us", || {
                self.invalidate_changed_records(&request.changed_coordinates());
            });
            if let Some(protection) = request
                .path_protection
                .lock()
                .expect("committed core paths")
                .as_ref()
            {
                let _timing = laststore::commit_timing::stage("memory_typed_install_us");
                protection.publish_clean()?;
            }
        }
        if !records.is_empty() {
            let _timing = laststore::commit_timing::stage("memory_record_install_us");
            self.core
                .lock()
                .expect("committed warm records")
                .admit_records(records);
        }
        let replaced = std::mem::replace(&mut view.current, current);
        drop(view);
        // The last drop of a root deletes its retired run files, so it
        // happens after the view lock.
        laststore::commit_timing::during("memory_old_root_release_us", || drop(replaced));
        // A merge can publish between the caller's read of `current` and
        // this install; take its root now, not at the next read.
        laststore::commit_timing::during("memory_merge_adoption_us", || self.adopt_merged_root());
        Ok(())
    }

    /// The view write lock excludes captures until these raw records and their
    /// epochs agree with the newly visible complete mutation.
    fn invalidate_changed_records(&self, changes: &[(String, String)]) {
        self.core
            .lock()
            .expect("core publication invalidation")
            .invalidate_sources(changes);
    }
}

fn next_revision(view: &NativeReadView) -> StorageResult<u64> {
    view.revision
        .checked_add(1)
        .ok_or_else(|| StorageError::BackendError("native accepted revision exhausted".into()))
}
