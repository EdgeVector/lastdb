//! Pending batch staging, write request scopes, write-scope cost, and post-commit callbacks.

use super::record_warm::WrittenRecord;
use super::run_commit::{account_ops, batch_bytes};
use super::*;
use crate::storage::format::records::require_final_record_sizes;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

mod accepted_slot;
pub(crate) use super::queue::publication;
pub(in crate::storage) use accepted_slot::{has_accepted, stage_accepted, take_accepted_notes};
pub(crate) use scope as request_scope;

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
        let blocked = Duration::from_micros(job.0.load(Ordering::Acquire));
        (
            out,
            Cost {
                blocked,
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

type AfterCommit = Pin<Box<dyn Future<Output = StorageResult<()>> + Send>>;
type PendingData = (
    Vec<Arc<HomeMutation>>,
    BTreeMap<(String, String), usize>,
    u64,
    usize,
);

pub(in crate::storage) struct PendingBatch {
    identity: uuid::Uuid,
    data: Mutex<PendingData>,
    pub(in crate::storage::disk_pack) publications: publication::PendingPublications,
    accepted: Mutex<Vec<crate::storage::disk_backup::accepted::Note>>,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    preparation_limit: Mutex<Option<String>>,
    pub(in crate::storage::disk_pack) operation_limit: usize,
    pub(super) warm_records: Mutex<(Vec<WrittenRecord>, usize)>,
}

tokio::task_local! { static WRITE: Arc<PendingBatch>; }

impl PendingBatch {
    fn new(runtime: &PackRuntime) -> Self {
        Self {
            identity: runtime.native_home_id(),
            data: Mutex::new(PendingData::default()),
            publications: publication::PendingPublications::default(),
            accepted: Mutex::new(Vec::new()),
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            preparation_limit: Mutex::new(None),
            operation_limit: runtime.batch_operation_limit(),
            warm_records: Mutex::new((Vec::new(), 0)),
        }
    }

    pub(in crate::storage) fn current(runtime: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|b| b.identity == runtime.native_home_id())
    }

    fn is_empty(&self) -> StorageResult<bool> {
        let empty = self.data.lock().expect("native pending batch").0.is_empty()
            && self
                .after_commit
                .lock()
                .expect("native post-commit work")
                .is_empty()
            && !self.has_accepted_notes();
        Ok(empty && self.publications.is_empty()?)
    }

    pub(in crate::storage::disk_pack) fn lookup(
        &self,
        col: &str,
        key: &str,
    ) -> Option<Arc<HomeMutation>> {
        let d = self.data.lock().expect("native pending batch");
        d.1.get(&(col.to_string(), key.to_string()))
            .map(|&at| Arc::clone(&d.0[at]))
    }

    pub(in crate::storage::disk_pack) fn matching(
        &self,
        col: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<HomeMutation>> {
        let d = self.data.lock().expect("native pending batch");
        d.1.iter()
            .filter(|((name, key), _)| {
                name == col && key.as_bytes() >= start && key.as_bytes() < end
            })
            .map(|(_, &at)| Arc::clone(&d.0[at]))
            .collect()
    }

    fn remember_limit(&self, error: StorageError) -> StorageError {
        if let StorageError::MutationLimit { detail } = &error {
            self.preparation_limit
                .lock()
                .expect("native complete mutation limit")
                .get_or_insert_with(|| detail.clone());
        }
        error
    }

    pub(in crate::storage::disk_pack) fn stage(
        &self,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<()> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(StorageError::invalid("native accepted batch is immutable"));
        }
        batch_bytes(&operations, self.operation_limit).map_err(|e| self.remember_limit(e))?;
        let mut d = self.data.lock().expect("native pending batch");
        let (cur, inc, lim) = (d.0.len(), operations.len(), self.operation_limit);
        if cur.checked_add(inc).is_none_or(|c| c > lim) {
            let mut uniq: BTreeSet<_> = d.1.keys().map(|(c, k)| (c.as_str(), k.as_str())).collect();
            uniq.extend(
                operations
                    .iter()
                    .map(|op| (op.collection.as_str(), op.key.as_str())),
            );
            let detail = format!(
                "complete native mutation exceeds {lim} storage operations: current={cur}, incoming={inc}, projected={}, projected_unique={}",
                cur.saturating_add(inc),
                uniq.len()
            );
            return Err(self.remember_limit(StorageError::mutation_limit(detail)));
        }
        let lim_msg = "complete native mutation exceeds pending memory allowance";
        let (bytes, max_body) =
            account_ops(&operations, d.2, d.3, PENDING_MEMORY_BYTES - 256, lim_msg)
                .map_err(|e| self.remember_limit(e))?;
        d.0.try_reserve(operations.len())
            .map_err(|_| StorageError::backend("native mutation allocation refused"))?;
        for op in operations {
            let at = d.0.len();
            d.1.insert((op.collection.clone(), op.key.clone()), at);
            d.0.push(Arc::new(op));
        }
        (d.2, d.3) = (bytes, max_body);
        Ok(())
    }

    pub(in crate::storage::disk_pack) fn changed_coordinates(&self) -> Vec<(String, String)> {
        let d = self.data.lock().expect("native pending changes");
        d.1.keys().cloned().collect()
    }

    pub(in crate::storage::disk_pack) async fn deliver(&self) -> StorageResult<()> {
        let mut actions: VecDeque<_> =
            std::mem::take(&mut *self.after_commit.lock().expect("native post-commit work")).into();
        while let Some(action) = actions.pop_front() {
            if let Err(error) = action.await {
                self.after_commit
                    .lock()
                    .expect("native post-commit work")
                    .extend(actions);
                return Err(error);
            }
        }
        Ok(())
    }
}

pub(crate) async fn scope<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let gate = runtime.enter_prepare().await;
    let held_from = Instant::now();
    let pending = Arc::new(PendingBatch::new(&runtime));
    let (r_rt, w_pend) = (Arc::clone(&runtime), Arc::clone(&pending));
    let context = run_blocking(move || read::ReadContext::capture(&r_rt, Some(w_pend))).await?;
    let result = PackReadScope(context)
        .run_result(async { Ok(WRITE.scope(Arc::clone(&pending), future).await) })
        .await;
    if !pending.is_empty().unwrap_or(false) {
        blocked::opened();
    }
    let lim = pending
        .preparation_limit
        .lock()
        .expect("native complete mutation limit")
        .clone();
    if let Some(detail) = lim {
        return Err(StorageError::mutation_limit(detail).into());
    }
    let result = result??;
    {
        let d = pending.data.lock().expect("native final format admission");
        let rows = d.1.values().filter_map(|&at| {
            let r = &d.0[at];
            Some((
                r.collection.as_str(),
                r.key.as_str(),
                r.body.as_ref()?.len(),
            ))
        });
        require_final_record_sizes(d.3, rows)?;
    }
    if pending.is_empty()? {
        let target = runtime.seal_queue();
        drop(gate);
        runtime.wait_for_queue(target).await?;
        blocked::note(held_from.elapsed());
        return Ok(result);
    }
    pending.frozen.store(true, Ordering::Release);
    let operations: Vec<_> = {
        let d = pending.data.lock().expect("native pending batch");
        d.0.iter().map(|r| (**r).clone()).collect()
    };
    let pub_gate = Arc::clone(&runtime.write_gate).lock_owned().await;
    let owner = Arc::clone(&runtime);
    let (prepared, gate, pub_gate) =
        run_blocking(move || Ok((owner.prepare_queued(operations)?, gate, pub_gate))).await?;
    let sequence = runtime.accept_queued(pending, prepared)?;
    drop((pub_gate, gate));
    runtime.wait_for_queue(sequence).await?;
    blocked::note(held_from.elapsed());
    Ok(result)
}

#[derive(Clone)]
pub struct PackReadScope(pub(super) Arc<read::ReadContext>);

impl PackReadScope {
    pub(crate) async fn for_runtime(rt: &Arc<PackRuntime>) -> StorageResult<Self> {
        Ok(Self(read::ReadContext::current(rt).await?))
    }

    pub async fn run_shared<T>(&self, fut: impl Future<Output = T>) -> T {
        self.0.in_scope(false, fut).await
    }

    pub async fn run_result<T, E: From<StorageError>>(
        &self,
        fut: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let res = self.0.in_scope(true, fut).await;
        self.0.error().map_or(res, |err| Err(err.into()))
    }
}

pub(crate) fn after_commit(
    runtime: &PackRuntime,
    action: impl Future<Output = StorageResult<()>> + Send + 'static,
) -> StorageResult<()> {
    PendingBatch::current(runtime)
        .ok_or_else(|| {
            StorageError::invalid("native post-commit work requires one complete preparation scope")
        })?
        .after_commit
        .lock()
        .expect("native post-commit work")
        .push(Box::pin(action));
    Ok(())
}
