//! Explicit relief of clean warm records; no byte admission policy change.

use super::*;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryStatus {
    pub warm: crate::resident::CoreWarmByteStats,
    pub records: usize,
    pub held_records: usize,
    /// Separate allowances; neither is part of the warm record purge target.
    pub definition_bytes: usize,
    pub write_metadata_bytes: usize,
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

impl PackRuntime {
    /// The publication boundary keeps all warm set measurements on one view.
    pub fn memory_status(&self) -> StorageResult<NativeMemoryStatus> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        Ok(measure_core(&mut core))
    }

    /// Keep held records and definitions.
    /// A remaining held allocation can prevent the requested target.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, before, after) = core.purge_clean_to_bytes(target);
        let remaining = measure_core(&mut core);
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
}

fn measure_core(core: &mut crate::resident::CoreWarmSet) -> NativeMemoryStatus {
    let stats = core.stats();
    NativeMemoryStatus {
        warm: core.byte_stats(),
        records: stats.records,
        held_records: stats.held,
        definition_bytes: core.memory_stats().used_bytes,
        write_metadata_bytes: core.model_stats().metadata_used_bytes,
    }
}

fn unavailable() -> StorageError {
    StorageError::BackendError("native warm set lock is unavailable".into())
}
