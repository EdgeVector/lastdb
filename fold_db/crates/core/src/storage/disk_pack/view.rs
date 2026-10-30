//! One atomic disk root and immutable typed core read view.

use super::*;

/// The database memory that a committed root publishes into. The store calls
/// it at fixed points of a commit and a read. It does not own or read the
/// records behind it; the database opens the store with its owner.
pub trait MemoryPublication: Send + Sync {
    /// Drop the memory records that depend on these changed coordinates.
    /// The store calls this under the view write lock, so no capture sees a
    /// new root until these raw records and their epochs agree with the
    /// newly visible complete mutation.
    fn invalidate(&self, changes: &[(String, String)]);

    /// A new owner of one read call's memory holds. The read context keeps
    /// it and drops it after its other fields.
    fn call_holds(&self) -> Arc<dyn std::any::Any + Send + Sync>;

    /// The database owner, for database code that reads its own memory.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// The disk facts of one status read. The memory owner adds its own facts.
pub(super) struct DiskStatus {
    pub(super) cleanup: NativeCleanupStatus,
    pub(super) generation: u64,
    pub(super) frontier: u64,
    pub(super) accepted_batches: usize,
    pub(super) accepted_sequence: u64,
    pub(super) completed_sequence: u64,
    pub(super) writer_failed: bool,
    pub(super) cas_delete_recovery: Option<CasDeleteRecovery>,
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

    /// Read the disk facts of one status read. `memory` reads the memory
    /// owner's facts while the queue lock is held, and the view read lock when
    /// it is free, so both sets of facts come from one status read.
    pub(super) fn status_disk<M>(&self, memory: impl FnOnce() -> M) -> (DiskStatus, M) {
        let queue = self.queue.lock().expect("native accepted queue");
        // A status read must not wait behind an install, so it reports the
        // last installed root while the view lock is busy.
        let view = self.view.try_read().ok();
        let (generation, frontier) = view.as_deref().map_or_else(
            || self.last_root.get(),
            |view| (view.current.generation(), view.current.frontier()),
        );
        let accepted_batches = queue.request_count();
        let facts = memory();
        let disk = DiskStatus {
            cleanup: self.status_cleanup(),
            generation,
            frontier,
            accepted_batches,
            accepted_sequence: queue.sequence(),
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.requires_recovery(),
            cas_delete_recovery: self.physical.cas_delete_recovery,
        };
        (disk, facts)
    }

    /// The caller owns the write gate and complete physical/logical reservations.
    /// Captured core values retain their reservation until the last reader releases them.
    /// Old calls retain their typed values; the flush log is not their read state.
    pub(super) fn install_committed(
        &self,
        current: physical::PhysicalRoot,
        hidden_changes: &[(String, String)],
        key_changes: &[(String, String, bool)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let mut view = laststore::commit_timing::during("memory_view_lock_wait_us", || {
            self.view.write().expect("native complete read view")
        });
        if !hidden_changes.is_empty() {
            let revision = next_revision(&view)?;
            if requests.is_empty() {
                self.memory.invalidate(hidden_changes);
            }
            laststore::commit_timing::during("memory_schema_keys_us", || {
                self.publish_schema_key_changes(key_changes.iter().map(
                    |(collection, key, deleted)| (collection.as_str(), key.as_str(), *deleted),
                ));
            });
            view.revision = revision;
        }
        for request in requests {
            // Later raw effects invalidate an earlier request's clean cache
            // even when the later request uses the disk-only memory fallback.
            laststore::commit_timing::during("memory_invalidation_us", || {
                self.memory.invalidate(&request.changed_coordinates());
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
}

fn next_revision(view: &NativeReadView) -> StorageResult<u64> {
    view.revision
        .checked_add(1)
        .ok_or_else(|| StorageError::BackendError("native accepted revision exhausted".into()))
}
