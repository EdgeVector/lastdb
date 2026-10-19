//! One atomic disk root and immutable typed core read view.

use super::*;

/// A read-only operational boundary; this method never wakes a flush worker.
#[derive(serde::Serialize)]
pub struct NativeDiskStatus {
    pub generation: u64,
    pub frontier: u64,
    pub accepted_batch: bool,
    pub accepted_batches: usize,
    pub accepted_sequence: u64,
    pub completed_sequence: u64,
    pub writer_failed: bool,
    pub warm_records: usize,
    pub warm_record_limit: usize,
    pub core_warm_paths: usize,
    pub core_warm_tip_only_paths: usize,
    pub core_warm_molecules: usize,
    pub core_warm_atoms: usize,
    pub core_warm_held: usize,
    pub core_warm_dirty: usize,
    pub core_warm_hits: u64,
    pub core_warm_misses: u64,
    pub core_warm_purged: u64,
    /// Warm set byte gauge (stage 1 of the byte limit design; not enforced).
    pub core_warm_bytes: crate::resident::CoreWarmByteStats,
    pub core_memory: crate::resident::CoreMemoryStats,
    pub(crate) core_model: crate::resident::CoreModelStats,
    pub schema_keys: schema_keys::SchemaKeyStatus,
}

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) current: Current,
    pub(super) accepted: Arc<Vec<Arc<transaction::PendingBatch>>>,
    pub(super) revision: u64,
    pub(super) sequence: u64,
}

impl PackRuntime {
    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let view = self.view.read().expect("native disk status view");
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
            generation: view.current.generation,
            frontier: view.current.frontier,
            accepted_batch: !view.accepted.is_empty(),
            accepted_batches: view.accepted.len(),
            accepted_sequence: queue.sequence(),
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.failed.load(Ordering::Acquire),
            warm_records: core.paths,
            warm_record_limit: crate::resident::resident_key_cap(),
            core_warm_paths: core.paths,
            core_warm_tip_only_paths: core.tip_only_paths,
            core_warm_molecules: core.molecules,
            core_warm_atoms: core.atoms,
            core_warm_held: core.held,
            core_warm_dirty: core.dirty,
            core_warm_hits: core.hits,
            core_warm_misses: core.misses,
            core_warm_purged: core.purged,
            core_warm_bytes: core_bytes,
            core_memory,
            core_model,
            schema_keys: self.schema_keys.status(),
        }
    }

    /// The caller owns the write gate and complete physical/logical reservations.
    /// Captured core values retain their reservation until the last reader releases them.
    pub(super) fn install_accepted(
        &self,
        batch: Arc<transaction::PendingBatch>,
        sequence: u64,
    ) -> StorageResult<()> {
        let changes = batch.changed_coordinates();
        let mut view = self.view.write().expect("native complete read view");
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native accepted view requires a healthy writer".into(),
            ));
        }
        let revision = next_revision(&view)?;
        self.invalidate_changed_records(&changes);
        if let Some(protection) = batch
            .path_protection
            .lock()
            .expect("native typed paths")
            .as_ref()
        {
            protection.publish(sequence)?;
        }
        let records = batch.latest_records();
        self.publish_schema_key_changes(records.iter().map(|record| {
            (
                record.collection.as_str(),
                record.key.as_str(),
                record.body.is_none(),
            )
        }));
        let mut accepted = view.accepted.as_ref().clone();
        accepted.push(batch);
        view.accepted = Arc::new(accepted);
        view.revision = revision;
        view.sequence = sequence;
        Ok(())
    }

    /// Old calls retain their typed values; the flush log is not their read state.
    pub(super) fn install_committed(
        &self,
        current: Current,
        hidden_changes: &[(String, String)],
        key_changes: &[(String, String, bool)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let mut view = self.view.write().expect("native complete read view");
        if !hidden_changes.is_empty() {
            let revision = next_revision(&view)?;
            self.invalidate_changed_records(hidden_changes);
            self.publish_schema_key_changes(
                key_changes.iter().map(|(collection, key, deleted)| {
                    (collection.as_str(), key.as_str(), *deleted)
                }),
            );
            view.revision = revision;
        }
        for request in requests {
            if let Some(protection) = request
                .path_protection
                .lock()
                .expect("committed core paths")
                .as_ref()
            {
                protection.publish_clean()?;
            }
        }
        let sequence = view.sequence;
        self.core
            .lock()
            .expect("core atomic flush completion")
            .complete_flush(sequence);
        view.current = current;
        view.accepted = Arc::new(Vec::new());
        Ok(())
    }

    /// The view write lock excludes captures until these raw records and their
    /// epochs agree with the newly visible complete mutation.
    fn invalidate_changed_records(&self, changes: &[(String, String)]) {
        logical_paths::controls::invalidate(
            &mut self.core.lock().expect("core publication invalidation"),
            changes,
        );
    }
}

fn next_revision(view: &NativeReadView) -> StorageResult<u64> {
    view.revision
        .checked_add(1)
        .ok_or_else(|| StorageError::BackendError("native accepted revision exhausted".into()))
}
