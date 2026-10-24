//! Private typed effects join the same complete native publication.

use super::*;
use crate::resident::CoreMemorySource;
use crate::storage::disk_pack::logical_paths::LogicalPathRecord;
use std::any::Any;
use std::collections::HashSet;

pub(crate) fn stage_paths(
    runtime: &PackRuntime,
    records: Vec<LogicalPathRecord>,
) -> StorageResult<()> {
    let pending = current(runtime)?;
    let mut paths = pending.paths.lock().expect("native private paths");
    // Keep one final effect per core path, including present/absent replacements.
    let records: BTreeMap<_, _> = records
        .into_iter()
        .map(|record| (record.path.core(), record))
        .collect();
    check_room(&paths, records.values())?;
    let replaced: HashSet<_> = records.keys().cloned().collect();
    paths.retain(|record| !replaced.contains(&record.path.core()));
    paths.extend(records.into_values());
    Ok(())
}

pub(crate) fn stage_core_memory<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    owner: usize,
    name: String,
    value: T,
    sources: Vec<CoreMemorySource>,
    bytes: usize,
) -> StorageResult<()> {
    let pending = current(runtime)?;
    let key = (owner, name.clone());
    let prepared = runtime
        .core
        .lock()
        .expect("native typed definition preparation")
        .prepare_memory(owner, name, value, sources, bytes);
    let mut definitions = pending
        .definitions
        .lock()
        .expect("native private definitions");
    match prepared {
        Ok(prepared) if definitions.contains_key(&key) || definitions.len() < 4096 => {
            definitions.insert(key, prepared);
            Ok(())
        }
        _ => {
            definitions.remove(&key);
            drop(definitions);
            require_durable(runtime)
        }
    }
}

fn current(runtime: &PackRuntime) -> StorageResult<Arc<PendingBatch>> {
    PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("typed effects require a complete write scope".into())
    })
}

/// A later physical mutation supersedes earlier private typed facts. The writer
/// may stage its final typed winner afterward; an unmapped change stays unknown.
pub(super) fn forget_superseded(pending: &PendingBatch, operations: &[HomeMutation]) {
    let coordinates = super::super::logical_paths::controls::expanded_coordinates(
        &operations
            .iter()
            .map(|operation| (operation.collection.clone(), operation.key.clone()))
            .collect::<Vec<_>>(),
    );
    let changed: HashSet<_> = coordinates
        .iter()
        .map(|(collection, key)| (collection.as_str(), key.as_str()))
        .collect();
    pending
        .paths
        .lock()
        .expect("native private paths")
        .retain(|record| {
            !record
                .source_coordinates()
                .iter()
                .any(|(collection, key)| changed.contains(&(collection.as_str(), key.as_str())))
        });
    pending
        .definitions
        .lock()
        .expect("native private definitions")
        .retain(|_, definition| {
            !definition.sources().iter().any(|source| {
                coordinates.iter().any(|(collection, key)| {
                    *collection == source.collection
                        && source
                            .key
                            .as_ref()
                            .is_none_or(|source_key| source_key == key)
                })
            })
        });
}

fn check_room<'a>(
    paths: &[LogicalPathRecord],
    next_paths: impl Iterator<Item = &'a LogicalPathRecord>,
) -> StorageResult<()> {
    let mut sizes = BTreeMap::new();
    for record in paths {
        sizes.insert(record.path.core(), record.retained_bytes());
    }
    for record in next_paths {
        sizes.insert(record.path.core(), record.retained_bytes());
    }
    let bytes = sizes
        .values()
        .try_fold(0usize, |sum, bytes| sum.checked_add(*bytes));
    if sizes.len() > 10_000 || bytes.is_none_or(|bytes| bytes > 128 * 1024 * 1024) {
        return Err(StorageError::WriteResourceLimit {
            detail: "native typed effects exceed their path or byte cap".into(),
        });
    }
    Ok(())
}
