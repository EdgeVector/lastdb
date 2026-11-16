//! Write-request execution scopes, read scopes, staged metadata publications, and callbacks.

use super::{PendingBatch, WRITE};
use crate::storage::disk_pack::{read, run_blocking, PackRuntime, StorageError, StorageResult};
use crate::storage::format::require_final_record_sizes;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

pub(crate) mod traits {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum KvMutation {
        Put { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }

    impl KvMutation {
        pub(crate) fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
            Self::Put {
                key: key.into(),
                value: value.into(),
            }
        }

        pub(crate) fn delete(key: impl Into<Vec<u8>>) -> Self {
            Self::Delete { key: key.into() }
        }

        pub(crate) fn key(&self) -> &[u8] {
            let (Self::Put { key, .. } | Self::Delete { key }) = self;
            key
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(crate) struct YieldedScan {
        pub(crate) rows: Vec<(Vec<u8>, Vec<u8>)>,
        pub(crate) resume: Option<Vec<u8>>,
    }
}

pub(crate) mod blocked {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::{future::Future, sync::Arc, time::Duration};

    static WRITE_SCOPES: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Cost {
        pub(crate) blocked: Duration,
        pub(crate) scopes: u64,
    }

    #[derive(Default)]
    struct ScopeJob {
        blocked_micros: AtomicU64,
        scopes: AtomicU64,
    }

    tokio::task_local! { static JOB: Arc<ScopeJob>; }

    pub(crate) async fn measure<F: Future>(work: F) -> (F::Output, Cost) {
        let job = Arc::new(ScopeJob::default());
        let out = JOB.scope(Arc::clone(&job), work).await;
        let blocked = Duration::from_micros(job.blocked_micros.load(Ordering::Acquire));
        (
            out,
            Cost {
                blocked,
                scopes: job.scopes.load(Ordering::Acquire),
            },
        )
    }

    pub(in crate::storage::disk_pack) fn opened() {
        WRITE_SCOPES.fetch_add(1, Ordering::AcqRel);
        let _ = JOB.try_with(|job| job.scopes.fetch_add(1, Ordering::AcqRel));
    }

    pub(in crate::storage::disk_pack) fn note(blocked: Duration) {
        let us = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
        let _ = JOB.try_with(|job| job.blocked_micros.fetch_add(us, Ordering::AcqRel));
    }

    pub(crate) fn write_scopes() -> u64 {
        WRITE_SCOPES.load(Ordering::Acquire)
    }
}

pub(crate) mod publication {
    use crate::storage::disk_pack::read::{unpoison, ReadContext};
    use crate::storage::disk_pack::transaction::PendingBatch;
    use crate::storage::disk_pack::{PackRuntime, StorageError, StorageResult};
    use std::any::Any;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    type CommitAction = Box<dyn FnOnce() -> StorageResult<()> + Send>;
    type CachedValue = Arc<dyn Any + Send + Sync>;

    #[derive(Default)]
    struct StagedPublications {
        entries: BTreeMap<(usize, String), (CachedValue, CommitAction)>,
        total_staged: usize,
    }

    #[derive(Default)]
    pub(in crate::storage::disk_pack) struct PendingPublications {
        inner: Mutex<StagedPublications>,
    }

    impl PendingPublications {
        pub(in crate::storage::disk_pack) fn is_empty(&self) -> StorageResult<bool> {
            Ok(unpoison(&self.inner).entries.is_empty())
        }

        pub(in crate::storage::disk_pack) fn finish(&self) -> StorageResult<()> {
            let mut first_err = None;
            for (_, (_, action)) in std::mem::take(&mut *unpoison(&self.inner)).entries {
                if let Err(err) = action() {
                    first_err.get_or_insert(err);
                }
            }
            first_err.map_or(Ok(()), Err)
        }
    }

    pub(crate) fn publish<T: Any + Send + Sync>(
        runtime: &PackRuntime,
        cache: usize,
        name: String,
        value: T,
        action: impl FnOnce() -> StorageResult<()> + Send + 'static,
    ) -> StorageResult<()> {
        let Some(pending) = PendingBatch::current(runtime) else {
            return action();
        };
        let mut staged = unpoison(&pending.publications.inner);
        if staged.total_staged >= 4096 {
            return Err(StorageError::invalid(
                "native mutation exceeds 4096 metadata publications",
            ));
        }
        staged
            .entries
            .insert((cache, name), (Arc::new(value), Box::new(action)));
        staged.total_staged += 1;
        Ok(())
    }

    fn cast<T: Any + Send + Sync>(value: &CachedValue) -> StorageResult<Arc<T>> {
        Arc::clone(value)
            .downcast::<T>()
            .map_err(|_| StorageError::backend("metadata cache type differs"))
    }

    pub(crate) fn get<T: Any + Send + Sync>(
        runtime: &PackRuntime,
        cache: usize,
        name: &str,
    ) -> StorageResult<Option<Arc<T>>> {
        let key = (cache, name.to_string());
        for batch in write_views(runtime).into_iter().rev() {
            if let Some((value, _)) = unpoison(&batch.publications.inner).entries.get(&key) {
                return cast(value).map(Some);
            }
        }
        Ok(None)
    }

    pub(crate) fn entries<T: Any + Send + Sync>(
        runtime: &PackRuntime,
        cache: usize,
    ) -> StorageResult<Vec<(String, Arc<T>)>> {
        let mut out = BTreeMap::new();
        for batch in write_views(runtime) {
            for ((owner, name), (value, _)) in &unpoison(&batch.publications.inner).entries {
                if *owner == cache {
                    out.insert(name.clone(), cast(value)?);
                }
            }
        }
        Ok(out.into_iter().collect())
    }

    fn write_views(runtime: &PackRuntime) -> Vec<Arc<PendingBatch>> {
        let Some(pending) = PendingBatch::current(runtime) else {
            return Vec::new();
        };
        let mut views = ReadContext::captured(runtime)
            .map_or_else(Vec::new, |ctx| (*ctx.accepted_batches).clone());
        views.push(pending);
        views
    }
}

pub(crate) async fn scope<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let (gate, held_from) = (runtime.enter_prepare().await, Instant::now());
    let pending = Arc::new(PendingBatch::new(&runtime));
    let (read_rt, write_pend) = (Arc::clone(&runtime), Arc::clone(&pending));
    let context =
        run_blocking(move || read::ReadContext::capture(&read_rt, Some(write_pend))).await?;
    let result = PackReadScope { context }
        .run_result(async { Ok(WRITE.scope(Arc::clone(&pending), future).await) })
        .await;
    if !pending.is_empty().unwrap_or(false) {
        blocked::opened();
    }
    if let Some(detail) = pending
        .mutation_limit
        .lock()
        .expect("native mutation limit")
        .clone()
    {
        return Err(StorageError::mutation_limit(detail).into());
    }
    let result = result??;
    {
        let data = pending.lock_mutations();
        let rows = data.index.values().filter_map(|&at| {
            let row = &data.ops[at];
            Some((
                row.collection.as_str(),
                row.key.as_str(),
                row.body.as_ref()?.len(),
            ))
        });
        require_final_record_sizes(data.max_body_bytes, rows)?;
    }
    if pending.is_empty()? {
        let target = runtime.seal_queue();
        drop(gate);
        runtime.wait_for_queue(target).await?;
        blocked::note(held_from.elapsed());
        return Ok(result);
    }
    pending.frozen.store(true, Ordering::Release);
    let ops: Vec<_> = pending
        .lock_mutations()
        .ops
        .iter()
        .map(|row| (**row).clone())
        .collect();
    let pub_gate = Arc::clone(&runtime.write_gate).lock_owned().await;
    let owner = Arc::clone(&runtime);
    let (prepared, gate, pub_gate) =
        run_blocking(move || Ok((owner.prepare_queued(ops)?, gate, pub_gate))).await?;
    let sequence = runtime.accept_queued(pending, prepared)?;
    drop((pub_gate, gate));
    runtime.wait_for_queue(sequence).await?;
    blocked::note(held_from.elapsed());
    Ok(result)
}

#[derive(Clone)]
pub struct PackReadScope {
    context: Arc<read::ReadContext>,
}

impl PackReadScope {
    pub(crate) async fn for_runtime(rt: &Arc<PackRuntime>) -> StorageResult<Self> {
        Ok(Self {
            context: read::ReadContext::current(rt).await?,
        })
    }

    pub async fn run_shared<T>(&self, fut: impl Future<Output = T>) -> T {
        self.context.in_scope(false, fut).await
    }

    pub async fn run_result<T, E: From<StorageError>>(
        &self,
        fut: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let res = self.context.in_scope(true, fut).await;
        self.context.error().map_or(res, |err| Err(err.into()))
    }
}

pub(crate) fn after_commit(
    runtime: &PackRuntime,
    action: impl Future<Output = StorageResult<()>> + Send + 'static,
) -> StorageResult<()> {
    let batch = PendingBatch::current(runtime)
        .ok_or_else(|| StorageError::invalid("post-commit work requires preparation scope"))?;
    batch.lock_after_commit().push(Box::pin(action));
    Ok(())
}

impl PackRuntime {
    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        match self.view.try_write() {
            Ok(mut v) => drop(crate::storage::disk_pack::physical::adopt(
                &mut v,
                self.physical.current_root(),
            )),
            Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.failed.store(true, Ordering::Release);
                let msg = "read view lock poisoned; restart process to reopen home";
                tracing::error!(msg);
                return Err(StorageError::backend(msg));
            }
        }
        Ok(())
    }
}
