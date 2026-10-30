//! The database owns the warm set of one native runtime: its status, the
//! explicit relief of clean logical records, and the publication interface
//! that the store calls. No byte admission policy change.

use super::*;
use crate::resident::CoreWarmSet;

/// The owner of one native runtime's warm set. The database creates it and
/// opens the store with it. The store holds it only as a [`MemoryPublication`].
#[derive(Default)]
pub struct NativeWarmSet {
    core: Arc<Mutex<CoreWarmSet>>,
}

impl MemoryPublication for NativeWarmSet {
    fn invalidate(&self, changes: &[(String, String)]) {
        logical_paths::controls::invalidate(
            &mut self.core.lock().expect("core publication invalidation"),
            changes,
        );
    }

    fn call_holds(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new(read::core_holds::CoreCallHolds::new(Arc::clone(&self.core)))
    }

    fn open_batch(&self) -> Box<dyn BatchMemory> {
        Box::new(transaction::effects::NativeBatchMemory::new(Arc::clone(
            &self.core,
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

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
    pub warm_records: usize,
    /// Retired path-count limit; always 0. The limit is `core_warm_bytes.limit_bytes`.
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
    /// Warm set byte charge and the enforced clean-record retention limit.
    pub core_warm_bytes: crate::resident::CoreWarmByteStats,
    pub core_memory: crate::resident::CoreMemoryStats,
    pub(crate) core_model: crate::resident::CoreModelStats,
    pub schema_keys: schema_keys::SchemaKeyStatus,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryStatus {
    pub warm: crate::resident::CoreWarmByteStats,
    pub logical_records: usize,
    pub held_logical_records: usize,
    pub unflushed_logical_records: usize,
    /// Separate allowances; neither is part of the logical-record purge target.
    pub definition_bytes: usize,
    pub complete_key_bytes: usize,
    pub write_metadata_bytes: usize,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryPurgeReport {
    pub logical_records_purged: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub target_bytes: u64,
    pub target_met: bool,
    pub remaining: NativeMemoryStatus,
}

impl PackRuntime {
    /// The warm set behind this runtime's memory publication.
    pub(super) fn core(&self) -> &Arc<Mutex<CoreWarmSet>> {
        &self
            .memory
            .as_any()
            .downcast_ref::<NativeWarmSet>()
            .expect("native runtime memory is the LastDB warm set")
            .core
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let (disk, (core, core_bytes, core_memory, core_model, schema_keys)) =
            self.status_disk(|| {
                let (core, bytes, memory, model) = {
                    let mut core = self.core().lock().expect("native core status");
                    (
                        core.stats(),
                        core.byte_stats(),
                        core.memory_stats(),
                        core.model_stats(),
                    )
                };
                // Key status locks key entries, and a lease takes those
                // before the core, so it runs after the core lock is free.
                (core, bytes, memory, model, self.schema_keys.status())
            });
        NativeDiskStatus {
            cleanup: disk.cleanup,
            generation: disk.generation,
            frontier: disk.frontier,
            accepted_batch: disk.accepted_batches > 0,
            accepted_batches: disk.accepted_batches,
            accepted_sequence: disk.accepted_sequence,
            completed_sequence: disk.completed_sequence,
            writer_failed: disk.writer_failed,
            cas_delete_recovery: disk.cas_delete_recovery,
            warm_records: core.paths,
            warm_record_limit: 0,
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
            schema_keys,
        }
    }

    /// The publication boundary keeps all logical measurements on one view.
    pub fn memory_status(&self) -> StorageResult<NativeMemoryStatus> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core().lock().map_err(|_| unavailable())?;
        let measured = measure_core(&mut core);
        drop(core);
        Ok(measured.finish(self.schema_keys.status().bytes))
    }

    /// Keep held paths, unflushed paths, definitions, and complete key domains.
    /// A remaining held allocation can prevent the requested target.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core().lock().map_err(|_| unavailable())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, before, after) = core.purge_clean_to_bytes(target);
        let measured = measure_core(&mut core);
        drop(core);
        Ok(NativeMemoryPurgeReport {
            logical_records_purged: purged as u64,
            bytes_before: before as u64,
            bytes_after: after as u64,
            target_bytes,
            target_met: after as u64 <= target_bytes,
            remaining: measured.finish(self.schema_keys.status().bytes),
        })
    }
}

/// Core stats only. Complete key bytes stay outside this value: that status
/// locks schema key entries, and a lease holds those entries before the core.
struct MeasuredMemory {
    warm: crate::resident::CoreWarmByteStats,
    logical_records: usize,
    held_logical_records: usize,
    unflushed_logical_records: usize,
    definition_bytes: usize,
    write_metadata_bytes: usize,
}

fn measure_core(core: &mut CoreWarmSet) -> MeasuredMemory {
    let warm = core.byte_stats();
    let stats = core.stats();
    let definitions = core.memory_stats();
    let model = core.model_stats();
    MeasuredMemory {
        warm,
        logical_records: stats.paths,
        held_logical_records: stats.held,
        unflushed_logical_records: stats.dirty,
        definition_bytes: definitions.used_bytes,
        write_metadata_bytes: model.metadata_used_bytes,
    }
}

impl MeasuredMemory {
    fn finish(self, complete_key_bytes: usize) -> NativeMemoryStatus {
        NativeMemoryStatus {
            warm: self.warm,
            logical_records: self.logical_records,
            held_logical_records: self.held_logical_records,
            unflushed_logical_records: self.unflushed_logical_records,
            definition_bytes: self.definition_bytes,
            complete_key_bytes,
            write_metadata_bytes: self.write_metadata_bytes,
        }
    }
}

fn unavailable() -> StorageError {
    StorageError::BackendError("native logical memory lock is unavailable".into())
}
