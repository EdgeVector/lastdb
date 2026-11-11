//! One atomic disk root, immutable read view, diagnostics, and warm-memory relief.

use super::*;
use laststore::disk_run::home::RunHomeDiagnostics;
use std::sync::atomic::AtomicU64;

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
    /// Decoded records in the warm set.
    pub warm_records: usize,
    pub core_warm_held: usize,
    pub core_warm_hits: u64,
    pub core_warm_misses: u64,
    pub core_warm_purged: u64,
    /// Warm set byte charge and the enforced clean-record retention limit.
    pub core_warm_bytes: crate::resident::CoreWarmByteStats,
}

#[derive(serde::Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct NativeMemoryStatus {
    pub warm: crate::resident::CoreWarmByteStats,
    pub records: usize,
    pub held_records: usize,
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

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct NativeReadView {
    pub(in crate::storage::disk_pack) current: physical::PhysicalRoot,
    pub(in crate::storage::disk_pack) revision: u64,
}

/// Generation and frontier of the last root that a commit or a merge installed.
pub(in crate::storage::disk_pack) struct LastRoot {
    generation: AtomicU64,
    frontier: AtomicU64,
}

impl LastRoot {
    pub(in crate::storage::disk_pack) fn new(root: &physical::PhysicalRoot) -> Self {
        Self {
            generation: AtomicU64::new(root.generation()),
            frontier: AtomicU64::new(root.frontier()),
        }
    }

    /// A commit and a merge adoption can race; the newer generation wins.
    pub(in crate::storage::disk_pack) fn observe(&self, generation: u64, frontier: u64) {
        if self.generation.fetch_max(generation, Ordering::Relaxed) <= generation {
            self.frontier.store(frontier, Ordering::Relaxed);
        }
    }

    fn get(&self) -> (u64, u64) {
        (
            self.generation.load(Ordering::Relaxed),
            self.frontier.load(Ordering::Relaxed),
        )
    }
}

impl PackRuntime {
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.home_identity()
    }

    /// Health probes read only this atomic; they never wait for a writer or read a record.
    pub fn requires_recovery(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let view = self.view.try_read().ok();
        let (generation, frontier) = view.as_deref().map_or_else(
            || self.last_root.get(),
            |view| (view.current.generation(), view.current.frontier()),
        );
        let accepted = queue.request_count();
        let (core, core_bytes) = {
            let mut core = self.core.lock().expect("native core status");
            (core.stats(), core.byte_stats())
        };
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation,
            frontier,
            accepted_batch: accepted > 0,
            accepted_batches: accepted,
            accepted_sequence: queue.sequence(),
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.requires_recovery(),
            cas_delete_recovery: self.physical.cas_delete_recovery,
            warm_records: core.records,
            core_warm_held: core.held,
            core_warm_hits: core.hits,
            core_warm_misses: core.misses,
            core_warm_purged: core.purged,
            core_warm_bytes: core_bytes,
        }
    }

    /// Physical totals are for independently captured home roots.
    pub async fn diagnostics_native(
        &self,
        include_inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        if include_inventory
            && self
                .queue
                .lock()
                .expect("native diagnostic queue")
                .request_count()
                > 0
        {
            return Err(StorageError::InvalidOperation(
                "storage inventory requires a checkpoint without accepted writes".into(),
            ));
        }
        let main = Arc::clone(&self.physical.home);
        let cas = Arc::clone(&self.physical.cas);
        tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            let main_cut = main.diagnostic_checkpoint(deadline).map_err(map)?;
            let cas_cut = cas.diagnostic_checkpoint(deadline).map_err(map)?;
            Ok(Some(NativeStorageDiagnostics {
                main: main_cut.finish(&main, include_inventory, deadline),
                cas: cas_cut.finish(&cas, include_inventory, deadline),
            }))
        })
        .await?
    }

    /// Keep held records and definitions.
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let unavailable =
            || StorageError::BackendError("native warm set lock is unavailable".into());
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, before, after) = core.purge_clean_to_bytes(target);
        let stats = core.stats();
        let remaining = NativeMemoryStatus {
            warm: core.byte_stats(),
            records: stats.records,
            held_records: stats.held,
        };
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

    pub(in crate::storage::disk_pack) fn install_committed(
        &self,
        current: physical::PhysicalRoot,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let records = record_warm::committed_records(requests);
        let mut view = self.view.write().expect("native complete read view");
        if !hidden_changes.is_empty() {
            view.revision = view.revision.checked_add(1).ok_or_else(|| {
                StorageError::BackendError("native accepted revision exhausted".into())
            })?;
        }
        if !hidden_changes.is_empty() || !requests.is_empty() || !records.is_empty() {
            let mut core = self.core.lock().expect("committed warm records");
            if !hidden_changes.is_empty() && requests.is_empty() {
                core.invalidate_records(hidden_changes);
            }
            for request in requests {
                core.invalidate_records(&request.changed_coordinates());
            }
            if !records.is_empty() {
                core.admit_records(records);
            }
        }
        let replaced = std::mem::replace(&mut view.current, current);
        drop(view);
        drop(replaced);
        self.adopt_merged_root()
    }
}

pub(crate) mod record_warm {
    //! The record warm set of a home (decision D2).

    use super::*;
    use crate::resident::RecordCoordinate;
    use lastdb_model::record_value::StoredRecord;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

    /// The memory answer of one read by record key.
    pub(crate) struct WarmRecordRead {
        revision: u64,
        coordinates: Vec<RecordCoordinate>,
        found: Vec<Option<Arc<StoredRecord>>>,
    }

    impl WarmRecordRead {
        pub(crate) fn take_found(&mut self) -> Vec<Option<Arc<StoredRecord>>> {
            std::mem::take(&mut self.found)
        }
    }

    /// One record value that a native write request puts.
    pub(crate) struct WrittenRecord {
        coordinate: RecordCoordinate,
        record: Arc<StoredRecord>,
        bytes: usize,
        digest: u64,
    }

    impl WrittenRecord {
        pub(crate) fn new(key: &str, record: Arc<StoredRecord>, value: &[u8]) -> Self {
            Self {
                coordinate: kv::coordinate(crate::storage::format::MAIN_NAMESPACE, key.as_bytes()),
                record,
                bytes: value.len(),
                digest: digest(value),
            }
        }

        pub(in crate::storage::disk_pack) fn bytes(&self) -> usize {
            self.bytes
        }
    }

    impl PackRuntime {
        pub(crate) fn warm_records<K: AsRef<str>>(
            &self,
            keys: &[K],
        ) -> StorageResult<Option<WarmRecordRead>> {
            if transaction::PendingBatch::current(self).is_some() {
                return Ok(None);
            }
            let main = crate::storage::format::MAIN_NAMESPACE;
            let coordinates: Vec<_> = keys
                .iter()
                .map(|key| kv::coordinate(main, key.as_ref().as_bytes()))
                .collect();
            let (revision, found) = if let Some(context) = read::ReadContext::captured(self) {
                let view = self.view.read().expect("native record warm view");
                if view.revision != context.revision {
                    return Ok(None);
                }
                let mut call = context
                    .call
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let found = self
                    .core
                    .lock()
                    .expect("native record warm set")
                    .record_lookup(&coordinates);
                for (key, hit) in keys.iter().zip(&found) {
                    if let Some((_, bytes)) = hit {
                        context.account_memory_record(&mut call, key.as_ref().len(), *bytes)?;
                    }
                }
                (context.revision, found)
            } else {
                let view = self.view.read().expect("native record warm view");
                let found = self
                    .core
                    .lock()
                    .expect("native record warm set")
                    .record_lookup(&coordinates);
                (view.revision, found)
            };
            Ok(Some(WarmRecordRead {
                revision,
                coordinates,
                found: found
                    .into_iter()
                    .map(|hit| hit.map(|(record, _)| record))
                    .collect(),
            }))
        }

        pub(crate) fn admit_read_records(
            &self,
            read: &WarmRecordRead,
            records: Vec<(usize, Arc<StoredRecord>, usize)>,
        ) {
            if records.is_empty() {
                return;
            }
            let records = records
                .into_iter()
                .map(|(at, record, bytes)| (read.coordinates[at].clone(), record, bytes))
                .collect();
            let view = self.view.read().expect("native record warm view");
            if view.revision == read.revision {
                self.core
                    .lock()
                    .expect("native record warm set")
                    .admit_records(records);
            }
        }

        pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
            if let Some(pending) = transaction::PendingBatch::current(self) {
                pending.stage_warm_records(records, crate::resident::core_warm_bytes() / 4);
            }
        }
    }

    pub(in crate::storage::disk_pack) fn committed_records(
        requests: &[Arc<transaction::PendingBatch>],
    ) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
        let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
        for request in requests {
            let staged = request.take_warm_records();
            if latest.is_empty() && staged.is_empty() {
                continue;
            }
            for coordinate in request.changed_coordinates() {
                latest.remove(&coordinate);
            }
            for written in staged {
                let (collection, key) = &written.coordinate;
                if request.lookup(collection, key).is_some_and(|pending| {
                    pending.body.as_ref().is_some_and(|body| {
                        body.len() == written.bytes && digest(body) == written.digest
                    })
                }) {
                    latest.insert(written.coordinate, (written.record, written.bytes));
                }
            }
        }
        latest
            .into_iter()
            .map(|(coordinate, (record, bytes))| (coordinate, record, bytes))
            .collect()
    }

    fn digest(value: &[u8]) -> u64 {
        BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
    }
}
