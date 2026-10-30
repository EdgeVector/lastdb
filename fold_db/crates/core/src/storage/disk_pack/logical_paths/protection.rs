//! Reserve every typed effect before the complete publication boundary.

use super::*;
use crate::resident::{CoreModelChange, CoreModelUpdate, PreparedCoreMemory};
use std::collections::BTreeMap;

pub(crate) struct PathProtection {
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    keys: Vec<CorePath>,
    pending: Mutex<Option<CoreModelUpdate>>,
}

pub(crate) struct PathPublication {
    pub definitions: Vec<PreparedCoreMemory>,
    pub owner: Option<Arc<crate::memory_budget::DeferReservation>>,
}

impl PathProtection {
    pub(crate) fn reserve(
        warm_set: &Arc<Mutex<crate::resident::CoreWarmSet>>,
        records: &[LogicalPathRecord],
        publication: PathPublication,
    ) -> StorageResult<Arc<Self>> {
        let bytes = records
            .iter()
            .map(LogicalPathRecord::retained_bytes)
            .try_fold(0usize, |total, bytes| {
                total.checked_add(bytes).ok_or_else(capacity)
            })?;
        if bytes > 128 * 1024 * 1024 {
            return Err(capacity());
        }
        let mut changes = BTreeMap::new();
        // A disk-first fallback does not turn refused write memory into an
        // uncharged clean cache entry. Reads can fetch that value after commit.
        for record in records
            .iter()
            .filter(|record| record.slot.is_none() || publication.owner.is_some())
        {
            changes.insert(
                record.path.core(),
                prepared(
                    record.path.core(),
                    record.slot.clone(),
                    record.source_dependencies(),
                )?,
            );
        }
        let keys: Vec<_> = changes.keys().cloned().collect();
        let mut core = warm_set.lock().expect("core write reservation");
        let keys = core.reserve_paths(&keys).map_err(|_| capacity())?;
        let changes: Vec<_> = changes.into_values().collect();
        let Ok(update) =
            core.prepare_clean(changes, publication.definitions, publication.owner.as_ref())
        else {
            core.release_paths(&keys);
            return Err(capacity());
        };
        drop(core);
        Ok(Arc::new(Self {
            core: Arc::clone(warm_set),
            keys,
            pending: Mutex::new(Some(update)),
        }))
    }

    /// The caller retains the view write lock through the complete typed change.
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
