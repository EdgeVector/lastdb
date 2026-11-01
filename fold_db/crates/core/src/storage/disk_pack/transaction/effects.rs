//! Private typed effects join the same complete native publication.

use super::*;
use crate::resident::CoreMemorySource;
use std::any::Any;

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

/// Reuse only this request's already charged and source-validated definition.
/// Physical stages invalidate these entries through forget_superseded.
pub(crate) fn prepared_core_memory<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    owner: usize,
    name: &str,
) -> StorageResult<Option<Arc<crate::resident::CoreMemoryValue<T>>>> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return Ok(None);
    };
    let Some(context) = super::super::read::ReadContext::captured(runtime) else {
        return Ok(None);
    };
    context.require_prepared_view(&pending)?;
    let definitions = pending
        .definitions
        .lock()
        .expect("native private definitions");
    definitions
        .get(&(owner, name.to_owned()))
        .map(|definition| {
            definition.typed().ok_or_else(|| {
                StorageError::BackendError("native prepared definition type differs".into())
            })
        })
        .transpose()
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
