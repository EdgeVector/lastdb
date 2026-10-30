//! Private typed effects join the same complete native publication.
//!
//! The database owns these effects. The store keeps them with the batch as a
//! [`BatchMemory`] and calls them at fixed points; it does not read them.

use super::*;
use crate::resident::{CoreMemorySource, CoreWarmSet, PreparedCoreMemory};
use crate::storage::disk_pack::logical_paths::source::Sources;
use crate::storage::disk_pack::logical_paths::{
    LogicalPathRecord, PathProtection, PathPublication,
};
use std::any::Any;
use std::collections::HashSet;

/// The typed memory effects of one complete request.
pub(in crate::storage::disk_pack) struct NativeBatchMemory {
    core: Arc<Mutex<CoreWarmSet>>,
    raw_memory_reservation: Mutex<Option<Arc<crate::memory_budget::DeferReservation>>>,
    paths: Mutex<Vec<LogicalPathRecord>>,
    definitions: Mutex<BTreeMap<(usize, String), PreparedCoreMemory>>,
    path_protection: Mutex<Option<Arc<PathProtection>>>,
    source_receipts: Mutex<Option<Arc<Mutex<Sources>>>>,
}

impl NativeBatchMemory {
    pub(in crate::storage::disk_pack) fn new(core: Arc<Mutex<CoreWarmSet>>) -> Self {
        Self {
            core,
            raw_memory_reservation: Mutex::new(None),
            paths: Mutex::new(Vec::new()),
            definitions: Mutex::new(BTreeMap::new()),
            path_protection: Mutex::new(None),
            source_receipts: Mutex::new(None),
        }
    }
}

impl BatchMemory for NativeBatchMemory {
    /// A later physical mutation supersedes earlier private typed facts. The writer
    /// may stage its final typed winner afterward; an unmapped change stays unknown.
    fn forget_superseded(&self, operations: &[HomeMutation]) {
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
        self.paths
            .lock()
            .expect("native private paths")
            .retain(|record| {
                !record
                    .source_coordinates()
                    .iter()
                    .any(|(collection, key)| changed.contains(&(collection.as_str(), key.as_str())))
            });
        self.definitions
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

    fn path_bytes(&self) -> u64 {
        self.paths
            .lock()
            .expect("native private paths")
            .iter()
            .fold(0u64, |sum, record| {
                sum.saturating_add(record.retained_bytes() as u64)
            })
    }

    fn reserved_bytes(&self) -> Option<u64> {
        self.raw_memory_reservation
            .lock()
            .expect("native raw memory reservation")
            .as_ref()
            .map(|reservation| reservation.bytes())
    }

    fn is_empty(&self) -> bool {
        self.paths.lock().expect("native private paths").is_empty()
            && self
                .definitions
                .lock()
                .expect("native private definitions")
                .is_empty()
    }

    fn freeze(&self) {
        drop(
            self.source_receipts
                .lock()
                .expect("native write source receipts")
                .take(),
        );
    }

    fn protect(&self, reserved: bool) -> StorageResult<()> {
        // A late metadata stage must not reuse an insufficient memory owner.
        // The durable fallback invalidates/refetches records without an
        // uncharged cache.
        if !reserved {
            self.raw_memory_reservation
                .lock()
                .expect("native model reservation")
                .take();
        }
        let paths = std::mem::take(&mut *self.paths.lock().expect("native private paths"));
        let definitions =
            std::mem::take(&mut *self.definitions.lock().expect("native private definitions"))
                .into_values()
                .collect();
        let owner = self
            .raw_memory_reservation
            .lock()
            .expect("native model reservation")
            .clone();
        let publication = PathPublication { definitions, owner };
        let protection = match PathProtection::reserve(&self.core, &paths, publication) {
            Ok(protection) => protection,
            Err(StorageError::WriteResourceLimit { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        *self
            .path_protection
            .lock()
            .expect("native typed path protection") = Some(protection);
        Ok(())
    }

    fn publish(&self) -> StorageResult<()> {
        if let Some(protection) = self
            .path_protection
            .lock()
            .expect("committed core paths")
            .as_ref()
        {
            let _timing = laststore::commit_timing::stage("memory_typed_install_us");
            protection.publish_clean()?;
        }
        Ok(())
    }

    fn release(&self) {
        self.path_protection
            .lock()
            .expect("completed core write holds")
            .take();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The database effects that the current batch carries.
fn native(pending: &PendingBatch) -> &NativeBatchMemory {
    pending
        .memory
        .as_any()
        .downcast_ref::<NativeBatchMemory>()
        .expect("native batch memory is the LastDB warm set's")
}

pub(crate) fn stage_paths(
    runtime: &PackRuntime,
    records: Vec<LogicalPathRecord>,
) -> StorageResult<()> {
    let pending = current(runtime)?;
    let mut paths = native(&pending).paths.lock().expect("native private paths");
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
        .core()
        .lock()
        .expect("native typed definition preparation")
        .prepare_memory(owner, name, value, sources, bytes);
    let mut definitions = native(&pending)
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
    let definitions = native(&pending)
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

/// Only this typed quota can authorize read-held raw write bodies before disk.
pub(crate) fn retain_raw_memory_reservation(
    runtime: &PackRuntime,
    reservation: crate::memory_budget::DeferReservation,
) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native raw memory quota requires a write scope".into())
    })?;
    *native(&pending)
        .raw_memory_reservation
        .lock()
        .expect("native raw memory reservation") = Some(Arc::new(reservation));
    Ok(())
}

/// The current write's bounded source proofs, created on first use.
pub(in crate::storage::disk_pack) fn source_receipts(
    pending: &PendingBatch,
) -> Arc<Mutex<Sources>> {
    Arc::clone(
        native(pending)
            .source_receipts
            .lock()
            .expect("native write source receipts")
            .get_or_insert_with(|| Arc::new(Mutex::new(Sources::default()))),
    )
}

fn current(runtime: &PackRuntime) -> StorageResult<Arc<PendingBatch>> {
    PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("typed effects require a complete write scope".into())
    })
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
