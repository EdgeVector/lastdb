//! Reserve every typed effect before the complete publication boundary.

use super::*;
use crate::resident::{CoreModelChange, CoreModelUpdate, PreparedCoreMemory};
use std::collections::BTreeMap;

pub(crate) struct PathProtection {
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    keys: Vec<CorePath>,
    pending: Mutex<Option<CoreModelUpdate>>,
    memory_ready: bool,
}

pub(crate) struct PathPublication {
    pub definitions: Vec<PreparedCoreMemory>,
    pub owner: Option<Arc<crate::memory_budget::DeferReservation>>,
    pub changed_sources: Vec<(String, String)>,
    pub early: bool,
}

impl PathProtection {
    pub(crate) fn reserve(
        runtime: &PackRuntime,
        records: &[LogicalPathRecord],
        absences: &[LogicalPathAbsence],
        publication: PathPublication,
    ) -> StorageResult<Arc<Self>> {
        let bytes = records
            .iter()
            .map(LogicalPathRecord::retained_bytes)
            .chain(absences.iter().map(LogicalPathAbsence::bytes))
            .try_fold(0usize, |total, bytes| {
                total.checked_add(bytes).ok_or_else(capacity)
            })?;
        if bytes > 128 * 1024 * 1024 {
            return Err(capacity());
        }
        let mut changes = BTreeMap::new();
        // A disk-first fallback does not turn refused write memory into an
        // uncharged clean cache entry. Reads can fetch that value after commit.
        for record in records.iter().filter(|_| publication.owner.is_some()) {
            changes.insert(
                record.path.core(),
                prepared(
                    record.path.core(),
                    Some(record.slot.clone()),
                    record.source_dependencies(),
                )?,
            );
        }
        for absence in absences {
            if changes
                .insert(
                    absence.path.core(),
                    prepared(absence.path.core(), None, absence.source_dependencies())?,
                )
                .is_some()
            {
                return Err(StorageError::BackendError(
                    "native path has conflicting final typed effects".into(),
                ));
            }
        }
        let keys: Vec<_> = changes.keys().cloned().collect();
        let mut core = runtime.core.lock().expect("core write reservation");
        let keys = core.reserve_paths(&keys).map_err(|_| capacity())?;
        let changes: Vec<_> = changes.into_values().collect();
        let dirty = if publication.early
            && (publication.owner.is_some() || changes.is_empty())
            && super::completeness::covers(
                &changes,
                &publication.definitions,
                &publication.changed_sources,
            ) {
            let copied = changes
                .iter()
                .map(|change| CoreModelChange {
                    path: change.path.clone(),
                    slot: change.slot.clone(),
                    sources: change.sources.clone(),
                })
                .collect();
            core.prepare_dirty(
                copied,
                publication.definitions.clone(),
                publication.owner.as_ref(),
                &publication.changed_sources,
            )
            .ok()
        } else {
            None
        };
        let memory_ready = dirty.is_some();
        let Ok(update) = dirty.map_or_else(
            || core.prepare_clean(changes, publication.definitions, publication.owner.as_ref()),
            Ok,
        ) else {
            core.release_paths(&keys);
            return Err(capacity());
        };
        drop(core);
        Ok(Arc::new(Self {
            core: Arc::clone(&runtime.core),
            keys,
            pending: Mutex::new(Some(update)),
            memory_ready,
        }))
    }

    /// The caller retains the view write lock through the complete typed change.
    pub(crate) fn publish(&self, sequence: u64) -> StorageResult<()> {
        if !self.memory_ready {
            return Err(StorageError::BackendError(
                "disk-first core publication requires its durable root".into(),
            ));
        }
        let Some(update) = self.pending.lock().expect("core prepared paths").take() else {
            return Ok(());
        };
        self.core
            .lock()
            .expect("core write publication")
            .install_dirty(update, sequence);
        Ok(())
    }

    pub(crate) fn publish_clean(&self) -> StorageResult<()> {
        let Some(update) = self.pending.lock().expect("core prepared paths").take() else {
            return Ok(());
        };
        self.core
            .lock()
            .expect("core durable publication")
            .install_clean(&update);
        Ok(())
    }

    pub(crate) fn memory_ready(&self) -> bool {
        self.memory_ready
    }
}

impl Drop for PathProtection {
    fn drop(&mut self) {
        if let Ok(mut core) = self.core.lock() {
            core.release_paths(&self.keys);
        }
    }
}

fn prepared(
    path: CorePath,
    slot: Option<CoreWarmSlot>,
    (complete, mut sources): (bool, Vec<(String, String)>),
) -> StorageResult<CoreModelChange> {
    sources.sort();
    sources.dedup();
    if !complete || sources.len() > 64 {
        return Err(capacity());
    }
    Ok(CoreModelChange {
        path,
        slot,
        sources,
    })
}

fn capacity() -> StorageError {
    StorageError::WriteResourceLimit {
        detail: "native mutation cannot reserve its complete logical core paths".into(),
    }
}
