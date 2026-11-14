//! Captured read roots, task-local scopes, and write-read caching.

mod ops;

use super::physical::V2Read;
use super::transaction::PendingBatch;
use super::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{MutexGuard, PoisonError};

pub(in crate::storage::disk_pack) fn unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(in crate::storage::disk_pack) type NativeRows = Vec<(String, Vec<u8>)>;
pub(in crate::storage::disk_pack) type Coordinate = (String, String);
type FailureSlot = Arc<Mutex<Option<RecordedFailure>>>;

#[derive(Clone)]
struct RecordedFailure {
    is_read_limit: bool,
    detail: String,
}

#[derive(Default)]
pub(super) struct WriteReadCache {
    pub(super) entries: BTreeMap<Coordinate, Option<Vec<u8>>>,
    pub(super) used_bytes: usize,
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct ReadContext {
    pub(in crate::storage::disk_pack) reader: Arc<Mutex<V2Read>>,
    pub(in crate::storage::disk_pack) accepted_batches: Arc<Vec<Arc<PendingBatch>>>,
    pub(in crate::storage::disk_pack) revision: u64,
    home_id: uuid::Uuid,
    failure: FailureSlot,
    write_owner: Option<Arc<PendingBatch>>,
    pub(super) write_reads: Option<Arc<Mutex<WriteReadCache>>>,
}

tokio::task_local! {
    static READ: Arc<ReadContext>;
    static ITEM: Option<FailureSlot>;
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn captured(rt: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone).ok().filter(|ctx| {
            ctx.home_id == rt.physical.home.identity()
                && ctx
                    .validate_owner(PendingBatch::current(rt).as_ref())
                    .is_ok()
        })
    }

    pub(in crate::storage::disk_pack) fn capture(
        rt: &Arc<PackRuntime>,
        write_owner: Option<Arc<PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if rt.requires_recovery() {
            return Err(StorageError::backend(
                "native publication failed; recovery required",
            ));
        }
        let prep = write_owner.is_some();
        let accepted_batches = if prep {
            let queue = rt.queue.lock().expect("native preparation prefix");
            Arc::new(queue.requests.iter().map(|(p, _)| Arc::clone(p)).collect())
        } else {
            Arc::default()
        };
        rt.adopt_merged_root()?;
        let view = rt.view.read().expect("native complete read view");
        let reader = Arc::new(Mutex::new(rt.physical.read(&view.root, prep)));
        let revision = view.revision;
        drop(view);
        Ok(Arc::new(Self {
            reader,
            accepted_batches,
            revision,
            home_id: rt.physical.home.identity(),
            failure: Arc::default(),
            write_owner,
            write_reads: prep.then(Arc::default),
        }))
    }

    pub(in crate::storage::disk_pack) async fn current(
        rt: &Arc<PackRuntime>,
    ) -> StorageResult<Arc<Self>> {
        if let Ok(ctx) = READ.try_with(Arc::clone) {
            if ctx.home_id != rt.physical.home.identity() {
                return Err(StorageError::invalid(
                    "read scope cannot change home identity",
                ));
            }
            ctx.validate_owner(PendingBatch::current(rt).as_ref())?;
            let mut next = (*ctx).clone();
            next.failure = ITEM
                .try_with(Clone::clone)
                .ok()
                .flatten()
                .unwrap_or_default();
            return Ok(Arc::new(next));
        }
        let rt = Arc::clone(rt);
        run_blocking(move || Self::capture(&rt, None)).await
    }

    pub(in crate::storage::disk_pack) fn remembered<T>(
        &self,
        res: StorageResult<T>,
    ) -> StorageResult<T> {
        if let Err(err) = &res {
            let is_read_limit = matches!(err, StorageError::ReadResourceLimit { .. });
            unpoison(&self.failure).get_or_insert_with(|| RecordedFailure {
                is_read_limit,
                detail: err.to_string(),
            });
        }
        res
    }

    pub(super) fn owns_write(&self, pending: Option<&Arc<PendingBatch>>) -> bool {
        matches!((&self.write_owner, pending), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }

    pub(super) fn validate_owner(&self, pending: Option<&Arc<PendingBatch>>) -> StorageResult<()> {
        if (self.write_owner.is_none() && pending.is_none()) || self.owns_write(pending) {
            return Ok(());
        }
        self.remembered(Err(StorageError::invalid(
            "read scope cannot change write owner",
        )))
    }

    pub(in crate::storage::disk_pack) fn error(&self) -> Option<StorageError> {
        let RecordedFailure {
            is_read_limit,
            detail,
        } = unpoison(&self.failure).clone()?;
        Some(if is_read_limit {
            StorageError::read_limit(detail)
        } else {
            StorageError::backend(detail)
        })
    }

    pub(in crate::storage::disk_pack) async fn in_scope<T>(
        self: &Arc<Self>,
        item: bool,
        fut: impl Future<Output = T>,
    ) -> T {
        let slot = item.then(|| Arc::clone(&self.failure));
        READ.scope(Arc::clone(self), ITEM.scope(slot, Box::pin(fut)))
            .await
    }

    pub(in crate::storage::disk_pack) fn account_memory_record(
        &self,
        call: &mut V2Read,
        key_bytes: usize,
        body_bytes: usize,
    ) -> StorageResult<()> {
        self.remembered((|| {
            let total = key_bytes
                .checked_add(body_bytes)
                .ok_or_else(|| StorageError::read_limit("record length overflow"))?;
            self.require_room(call, 1, total as u64)?;
            call.main
                .account_memory_record(key_bytes, body_bytes)
                .map_err(|e| call_error(e, call))
        })())
    }

    pub(super) fn require_room(&self, call: &V2Read, keys: u64, bytes: u64) -> StorageResult<()> {
        if let Some(err) = self.error() {
            return Err(err);
        }
        let (costs, limit) = (call.costs(), call.main.budget());
        let over_k = costs
            .examined_keys
            .checked_add(keys)
            .is_none_or(|n| n > limit.keys);
        let over_b = costs
            .budget_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > limit.bytes);
        if over_k || over_b {
            return Err(StorageError::read_limit(format!(
                "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                costs.examined_keys, limit.keys, costs.budget_bytes, limit.bytes,
            )));
        }
        Ok(())
    }
}

pub(super) fn call_error(error: laststore::Error, call: &V2Read) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (costs, limit) = (call.costs(), call.main.budget());
    StorageError::read_limit(format!(
        "native call: examined_keys={}/{}, charged_bytes={}/{}",
        costs.examined_keys, limit.keys, costs.budget_bytes, limit.bytes
    ))
}
