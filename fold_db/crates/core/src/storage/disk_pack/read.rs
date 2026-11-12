//! Captured read roots, write-read caching, and finite read-budget accounting.

use super::physical::{call_error, V2Read};
use super::transaction::{PendingBatch, PendingRecord};
use super::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::PoisonError;

const WRITE_READ_MAX_BYTES: usize = 1024 * 1024;
const WRITE_READ_MAX_VALUE_BYTES: usize = 16 * 1024;
type Coordinate = (String, String);

#[derive(Default)]
struct SavedWriteReads {
    values: BTreeMap<Coordinate, Option<Vec<u8>>>,
    bytes: usize,
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct ReadContext {
    pub(in crate::storage::disk_pack) call: Arc<Mutex<V2Read>>,
    identity: uuid::Uuid,
    failure: Arc<Mutex<Option<(bool, String)>>>,
    pub(in crate::storage::disk_pack) accepted: Arc<Vec<Arc<PendingBatch>>>,
    pub(in crate::storage::disk_pack) revision: u64,
    write_owner: Option<Arc<PendingBatch>>,
    write_reads: Option<Arc<Mutex<SavedWriteReads>>>,
}

tokio::task_local! {
    static READ: Arc<ReadContext>;
    static ITEM: Option<Arc<Mutex<Option<(bool, String)>>>>;
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn captured(runtime: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone).ok().filter(|ctx| {
            ctx.identity == runtime.native_home_id()
                && ctx
                    .validate_owner(PendingBatch::current(runtime).as_ref())
                    .is_ok()
        })
    }

    fn capture(
        runtime: &Arc<PackRuntime>,
        write_owner: Option<Arc<PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if runtime.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native disk or cache publication failed; complete recovery is required".into(),
            ));
        }
        let prep = write_owner.is_some();
        let accepted = if prep {
            runtime.preparation_prefix()
        } else {
            Arc::new(Vec::new())
        };
        runtime.adopt_merged_root()?;
        let (call, revision) = {
            let view = runtime.view.read().expect("native complete read view");
            (runtime.physical.read(&view.current, prep), view.revision)
        };
        Ok(Arc::new(Self {
            call: Arc::new(Mutex::new(call)),
            accepted,
            revision,
            identity: runtime.native_home_id(),
            failure: Arc::new(Mutex::new(None)),
            write_owner,
            write_reads: prep.then(|| Arc::new(Mutex::new(SavedWriteReads::default()))),
        }))
    }

    pub(in crate::storage::disk_pack) async fn current(
        runtime: &Arc<PackRuntime>,
    ) -> StorageResult<Arc<Self>> {
        if let Ok(ctx) = READ.try_with(Arc::clone) {
            if ctx.identity != runtime.native_home_id() {
                return Err(StorageError::InvalidOperation(
                    "one native read scope cannot change home identity".into(),
                ));
            }
            ctx.validate_owner(PendingBatch::current(runtime).as_ref())?;
            let failure = ITEM
                .try_with(Clone::clone)
                .ok()
                .flatten()
                .unwrap_or_else(|| Arc::new(Mutex::new(None)));
            return Ok(Arc::new(Self {
                failure,
                ..(*ctx).clone()
            }));
        }
        let rt = Arc::clone(runtime);
        run_blocking(move || Self::capture(&rt, None)).await
    }

    pub(in crate::storage::disk_pack) fn remembered<T>(
        &self,
        result: StorageResult<T>,
    ) -> StorageResult<T> {
        if let Err(err) = &result {
            self.failure
                .lock()
                .expect("native item failure")
                .get_or_insert_with(|| {
                    (
                        matches!(err, StorageError::ReadResourceLimit { .. }),
                        err.to_string(),
                    )
                });
        }
        result
    }

    pub(in crate::storage::disk_pack) fn owns_write(
        &self,
        pending: Option<&Arc<PendingBatch>>,
    ) -> bool {
        self.write_owner
            .as_ref()
            .zip(pending)
            .is_some_and(|(a, b)| Arc::ptr_eq(a, b))
    }

    pub(in crate::storage::disk_pack) fn validate_owner(
        &self,
        pending: Option<&Arc<PendingBatch>>,
    ) -> StorageResult<()> {
        if (self.write_owner.is_none() && pending.is_none()) || self.owns_write(pending) {
            return Ok(());
        }
        self.remembered(Err(StorageError::InvalidOperation(
            "native read scope cannot change its complete write owner".into(),
        )))
    }

    pub(in crate::storage::disk_pack) fn error(&self) -> Option<StorageError> {
        self.failure
            .lock()
            .expect("native item failure")
            .clone()
            .map(|(res, detail)| {
                if res {
                    StorageError::ReadResourceLimit { detail }
                } else {
                    StorageError::BackendError(detail)
                }
            })
    }

    pub(in crate::storage::disk_pack) fn overlay_record(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        key: &str,
    ) -> Option<Arc<PendingRecord>> {
        pending.and_then(|b| b.lookup(collection, key)).or_else(|| {
            self.accepted
                .iter()
                .rev()
                .find_map(|b| b.lookup(collection, key))
        })
    }

    pub(in crate::storage::disk_pack) fn overlay_rows(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let mut rows = BTreeMap::new();
        for batch in self.accepted.iter().chain(pending) {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        rows.into_values().collect()
    }

    pub(in crate::storage::disk_pack) fn account_memory_record(
        &self,
        call: &mut V2Read,
        key_bytes: usize,
        body_bytes: usize,
    ) -> StorageResult<()> {
        let res = key_bytes
            .checked_add(body_bytes)
            .ok_or_else(|| StorageError::ReadResourceLimit {
                detail: "native memory record length overflow".into(),
            })
            .and_then(|bytes| {
                self.require_room(call, 1, bytes as u64)?;
                call.main
                    .account_memory_record(key_bytes, body_bytes)
                    .map_err(|e| call_error(e, call))
            });
        self.remembered(res)
    }

    pub(super) fn require_room(&self, call: &V2Read, keys: u64, bytes: u64) -> StorageResult<()> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        let (costs, limits) = (call.costs(), call.main.budget());
        if costs
            .examined_keys
            .checked_add(keys)
            .is_none_or(|n| n > limits.keys)
            || costs
                .budget_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > limits.bytes)
        {
            return Err(StorageError::ReadResourceLimit {
                detail: format!(
                    "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                    costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
                ),
            });
        }
        Ok(())
    }

    pub(super) fn reuse_disk_value(
        &self,
        call: &mut V2Read,
        coordinate: &Coordinate,
    ) -> StorageResult<Option<Option<Vec<u8>>>> {
        let Some(reads) = &self.write_reads else {
            return Ok(None);
        };
        let value = reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values
            .get(coordinate)
            .cloned();
        if let Some(val) = &value {
            let len = val.as_ref().map_or(0, Vec::len);
            self.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), len)?;
        }
        Ok(value)
    }

    pub(super) fn remember_disk_value(&self, coordinate: &Coordinate, value: Option<&[u8]>) {
        let Some(reads) = &self.write_reads else {
            return;
        };
        let body_bytes = value.map_or(0, <[u8]>::len);
        if body_bytes > WRITE_READ_MAX_VALUE_BYTES {
            return;
        }
        let mut saved = reads.lock().unwrap_or_else(PoisonError::into_inner);
        let overhead =
            std::mem::size_of::<(Coordinate, Option<Vec<u8>>)>() + 8 * std::mem::size_of::<usize>();
        let bytes = coordinate.0.len() + coordinate.1.len() + body_bytes + overhead;
        if bytes <= WRITE_READ_MAX_BYTES.saturating_sub(saved.bytes)
            && !saved.values.contains_key(coordinate)
        {
            saved
                .values
                .insert(coordinate.clone(), value.map(<[u8]>::to_vec));
            saved.bytes += bytes;
        }
    }
}

/// One native read boundary shared by every child query or worker in a call.
#[derive(Clone)]
pub struct PackReadScope {
    context: Arc<ReadContext>,
}

impl PackReadScope {
    pub(crate) async fn for_runtime(runtime: &Arc<PackRuntime>) -> StorageResult<Self> {
        Ok(Self {
            context: ReadContext::current(runtime).await?,
        })
    }

    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let item = Some(Arc::clone(&self.context.failure));
        READ.scope(
            Arc::clone(&self.context),
            ITEM.scope(item, Box::pin(future)),
        )
        .await
    }

    pub async fn run_shared<T>(&self, future: impl Future<Output = T>) -> T {
        READ.scope(
            Arc::clone(&self.context),
            ITEM.scope(None, Box::pin(future)),
        )
        .await
    }

    pub async fn run_result<T, E>(&self, future: impl Future<Output = Result<T, E>>) -> Result<T, E>
    where
        E: From<StorageError>,
    {
        let result = self.run(future).await;
        match self.context.error() {
            Some(err) => Err(err.into()),
            None => result,
        }
    }
}

pub(in crate::storage::disk_pack) async fn write_scope<T>(
    runtime: Arc<PackRuntime>,
    pending: Arc<PendingBatch>,
    future: impl Future<Output = T>,
) -> StorageResult<T> {
    let context = run_blocking(move || ReadContext::capture(&runtime, Some(pending))).await?;
    PackReadScope { context }
        .run_result(async { Ok(future.await) })
        .await
}
