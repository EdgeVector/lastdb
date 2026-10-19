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
        let (warm, stats, definitions, model) = (
            core.byte_stats(),
            core.stats(),
            core.memory_stats(),
            core.model_stats(),
        );
        drop(core);
        Ok(NativeMemoryStatus {
            warm,
            logical_records: stats.paths,
            held_logical_records: stats.held,
            unflushed_logical_records: stats.dirty,
            definition_bytes: definitions.used_bytes,
            complete_key_bytes: self.schema_keys.status().bytes,
            write_metadata_bytes: model.metadata_used_bytes,
        })
    }

    /// Keep held paths, unflushed paths, definitions, and complete key domains.
    /// A remaining held allocation can prevent the requested target.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, before, after) = core.purge_clean_to_bytes(target);
        let (warm, stats, definitions, model) = (
            core.byte_stats(),
            core.stats(),
            core.memory_stats(),
            core.model_stats(),
        );
        drop(core);
        Ok(NativeMemoryPurgeReport {
            logical_records_purged: purged as u64,
            bytes_before: before as u64,
            bytes_after: after as u64,
            target_bytes,
            target_met: after as u64 <= target_bytes,
            remaining: NativeMemoryStatus {
                warm,
                logical_records: stats.paths,
                held_logical_records: stats.held,
                unflushed_logical_records: stats.dirty,
                definition_bytes: definitions.used_bytes,
                complete_key_bytes: self.schema_keys.status().bytes,
                write_metadata_bytes: model.metadata_used_bytes,
            },
        })
    }
}

fn unavailable() -> StorageError {
    StorageError::BackendError("native logical memory lock is unavailable".into())
}
