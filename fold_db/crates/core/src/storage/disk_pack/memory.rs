//! Explicit relief of clean logical records; no byte admission policy change.

use super::*;

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
    /// The publication boundary keeps all logical measurements on one view.
    pub fn memory_status(&self) -> StorageResult<NativeMemoryStatus> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let measured = measure_core(&mut core);
        drop(core);
        Ok(measured.finish(0))
    }

    /// Keep held paths, unflushed paths, definitions, and complete key domains.
    /// A remaining held allocation can prevent the requested target.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
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
            remaining: measured.finish(0),
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

fn measure_core(core: &mut crate::resident::CoreWarmSet) -> MeasuredMemory {
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
