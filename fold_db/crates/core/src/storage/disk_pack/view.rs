//! One atomic disk root, immutable read view, diagnostics, write-scope cost, and warm-memory relief.

use super::*;
use crate::resident::CoreWarmByteStats;
use laststore::disk_run::home::RunHomeDiagnostics;
use std::time::{Duration, Instant};

pub(crate) mod blocked {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    static WRITE_SCOPES: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Cost {
        pub(crate) blocked: Duration,
        pub(crate) scopes: u64,
    }

    tokio::task_local! { static JOB: Arc<(AtomicU64, AtomicU64)>; }

    pub(crate) async fn measure<F: Future>(work: F) -> (F::Output, Cost) {
        let job = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
        let out = JOB.scope(Arc::clone(&job), work).await;
        (
            out,
            Cost {
                blocked: Duration::from_micros(job.0.load(Ordering::Acquire)),
                scopes: job.1.load(Ordering::Acquire),
            },
        )
    }

    pub(in crate::storage::disk_pack) fn opened() {
        WRITE_SCOPES.fetch_add(1, Ordering::AcqRel);
        let _ = JOB.try_with(|j| j.1.fetch_add(1, Ordering::AcqRel));
    }

    pub(in crate::storage::disk_pack) fn note(blocked: Duration) {
        let us = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
        let _ = JOB.try_with(|j| j.0.fetch_add(us, Ordering::AcqRel));
    }

    pub(crate) fn write_scopes() -> u64 {
        WRITE_SCOPES.load(Ordering::Acquire)
    }
}

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
    pub core_warm_held: usize,
    pub core_warm_hits: u64,
    pub core_warm_misses: u64,
    pub core_warm_purged: u64,
    pub core_warm_bytes: CoreWarmByteStats,
}

#[derive(serde::Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeMemoryPurgeReport {
    pub records_purged: u64,
    pub target_met: bool,
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct NativeReadView {
    pub(in crate::storage::disk_pack) current: physical::V2Root,
    pub(in crate::storage::disk_pack) revision: u64,
}

impl PackRuntime {
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.physical.identity()
    }

    pub fn requires_recovery(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let (generation, frontier) = self.view.try_read().map_or_else(
            |_| {
                let r = self.physical.current_root();
                (r.generation(), r.frontier())
            },
            |v| (v.current.generation(), v.current.frontier()),
        );
        let accepted = queue.requests.len();
        let (core, core_warm_bytes) = {
            let mut core = self.core.lock().expect("native core status");
            (core.stats(), core.byte_stats())
        };
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation,
            frontier,
            accepted_batch: accepted > 0,
            accepted_batches: accepted,
            accepted_sequence: queue.next_sequence,
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.requires_recovery(),
            cas_delete_recovery: self.physical.cas_delete_recovery,
            warm_records: core.records,
            core_warm_held: core.held,
            core_warm_hits: core.hits,
            core_warm_misses: core.misses,
            core_warm_purged: core.purged,
            core_warm_bytes,
        }
    }

    pub async fn diagnostics_native(
        &self,
        include_inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        let has_queued = !self
            .queue
            .lock()
            .expect("native diagnostic queue")
            .requests
            .is_empty();
        if include_inventory && has_queued {
            return Err(StorageError::InvalidOperation(
                "storage inventory requires a checkpoint without accepted writes".into(),
            ));
        }
        let (main, cas) = (
            Arc::clone(&self.physical.home),
            Arc::clone(&self.physical.cas),
        );
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            Ok(Some(NativeStorageDiagnostics {
                main: main.diagnostics(include_inventory, deadline).map_err(map)?,
                cas: cas.diagnostics(include_inventory, deadline).map_err(map)?,
            }))
        })
        .await?
    }

    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let unavailable =
            || StorageError::BackendError("native warm set lock is unavailable".into());
        let _view = self.view.read().map_err(|_| unavailable())?;
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let (purged, _, after) =
            core.purge_clean_to_bytes(usize::try_from(target_bytes).unwrap_or(usize::MAX));
        Ok(NativeMemoryPurgeReport {
            records_purged: purged as u64,
            target_met: after as u64 <= target_bytes,
        })
    }

    pub(in crate::storage::disk_pack) fn install_committed(
        &self,
        current: physical::V2Root,
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
        drop((view, replaced));
        self.adopt_merged_root()
    }
}

pub(crate) mod record_warm {
    use super::*;
    use crate::resident::{core_warm_bytes, RecordCoordinate};
    use crate::storage::format::MAIN_NAMESPACE;
    use lastdb_model::record_value::StoredRecord;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    use std::sync::PoisonError;

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

    pub(crate) struct WrittenRecord(RecordCoordinate, Arc<StoredRecord>, usize, u64);

    impl WrittenRecord {
        pub(crate) fn new(key: &str, record: Arc<StoredRecord>, value: &[u8]) -> Self {
            Self(
                kv::coordinate(MAIN_NAMESPACE, key.as_bytes()),
                record,
                value.len(),
                digest(value),
            )
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
            let view = self.view.read().expect("native record warm view");
            let ctx = read::ReadContext::captured(self);
            if ctx.as_ref().is_some_and(|c| c.revision != view.revision) {
                return Ok(None);
            }
            let coords: Vec<_> = keys
                .iter()
                .map(|k| kv::coordinate(MAIN_NAMESPACE, k.as_ref().as_bytes()))
                .collect();
            let mut call = ctx
                .as_ref()
                .map(|c| c.call.lock().unwrap_or_else(PoisonError::into_inner));
            let found = self
                .core
                .lock()
                .expect("native record warm set")
                .record_lookup(&coords);
            if let (Some(ctx), Some(call)) = (&ctx, &mut call) {
                for (key, hit) in keys.iter().zip(&found) {
                    if let Some((_, bytes)) = hit {
                        ctx.account_memory_record(call, key.as_ref().len(), *bytes)?;
                    }
                }
            }
            Ok(Some(WarmRecordRead {
                revision: view.revision,
                coordinates: coords,
                found: found.into_iter().map(|h| h.map(|(r, _)| r)).collect(),
            }))
        }

        pub(crate) fn admit_read_records(
            &self,
            read: &WarmRecordRead,
            records: Vec<(usize, Arc<StoredRecord>, usize)>,
        ) {
            if !records.is_empty()
                && self.view.read().expect("native record warm view").revision == read.revision
            {
                let items = records
                    .into_iter()
                    .map(|(at, r, b)| (read.coordinates[at].clone(), r, b))
                    .collect();
                self.core
                    .lock()
                    .expect("native record warm set")
                    .admit_records(items);
            }
        }

        pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
            if let Some(pending) = transaction::PendingBatch::current(self) {
                let limit = core_warm_bytes() / 4;
                let mut staged = pending.warm_records.lock().expect("native warm records");
                for r in records {
                    let Some(b) = staged.1.checked_add(r.2).filter(|&b| b <= limit) else {
                        break;
                    };
                    staged.1 = b;
                    staged.0.push(r);
                }
            }
        }
    }

    pub(super) fn committed_records(
        requests: &[Arc<transaction::PendingBatch>],
    ) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
        let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
        for request in requests {
            let staged =
                std::mem::take(&mut *request.warm_records.lock().expect("native warm records")).0;
            if latest.is_empty() && staged.is_empty() {
                continue;
            }
            for coord in request.changed_coordinates() {
                latest.remove(&coord);
            }
            for WrittenRecord(coord, rec, bytes, dig) in staged {
                if request.lookup(&coord.0, &coord.1).is_some_and(|p| {
                    p.body
                        .as_ref()
                        .is_some_and(|b| b.len() == bytes && digest(b) == dig)
                }) {
                    latest.insert(coord, (rec, bytes));
                }
            }
        }
        latest.into_iter().map(|(c, (r, b))| (c, r, b)).collect()
    }

    fn digest(value: &[u8]) -> u64 {
        BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
    }
}
