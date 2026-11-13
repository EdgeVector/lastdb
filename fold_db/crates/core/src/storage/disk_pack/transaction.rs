//! Pending batch staging, write request scopes, traits, and post-commit callbacks.

use super::*;
use crate::storage::format::require_final_record_sizes;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::{MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use record_warm::WrittenRecord;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::{future::Future, pin::Pin, sync::MutexGuard, time::Instant};

mod accepted_slot;
pub(crate) use super::physical::publication;
pub(crate) use super::run_node::record_warm;
pub(in crate::storage) use accepted_slot::{has_accepted, stage_accepted, take_accepted_notes};
pub(crate) use scope as request_scope;

pub mod traits {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum KvMutation {
        Put { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }
    impl KvMutation {
        pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
            Self::Put {
                key: key.into(),
                value: value.into(),
            }
        }
        pub fn delete(key: impl Into<Vec<u8>>) -> Self {
            Self::Delete { key: key.into() }
        }
        pub fn key(&self) -> &[u8] {
            let (Self::Put { key, .. } | Self::Delete { key }) = self;
            key
        }
    }
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct YieldedScan {
        pub rows: Vec<(Vec<u8>, Vec<u8>)>,
        pub resume: Option<Vec<u8>>,
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

type Coord = (String, String);
type AfterCommit = Pin<Box<dyn Future<Output = StorageResult<()>> + Send>>;
type PendingData = (Vec<Arc<HomeMutation>>, BTreeMap<Coord, usize>, u64, usize);
type PendingMeta = (uuid::Uuid, Mutex<Vec<AfterCommit>>, Mutex<Option<String>>);

pub(in crate::storage) struct PendingBatch {
    meta: PendingMeta,
    data: Mutex<PendingData>,
    pub(in crate::storage::disk_pack) publications: publication::PendingPublications,
    accepted: Mutex<Vec<crate::storage::disk_backup::accepted::Note>>,
    frozen: AtomicBool,
    pub(in crate::storage::disk_pack) operation_limit: usize,
    pub(in crate::storage::disk_pack) warm_records: Mutex<(Vec<WrittenRecord>, usize)>,
}

tokio::task_local! { static WRITE: Arc<PendingBatch>; }

impl PendingBatch {
    fn new(rt: &PackRuntime) -> Self {
        let meta = (
            rt.physical.home.identity(),
            Mutex::default(),
            Mutex::default(),
        );
        Self {
            meta,
            data: Mutex::default(),
            publications: publication::PendingPublications::default(),
            accepted: Mutex::default(),
            frozen: AtomicBool::new(false),
            operation_limit: rt.batch_operation_limit(),
            warm_records: Mutex::default(),
        }
    }
    fn d(&self) -> MutexGuard<'_, PendingData> {
        self.data.lock().expect("native pending batch")
    }
    fn post(&self) -> MutexGuard<'_, Vec<AfterCommit>> {
        self.meta.1.lock().expect("native post-commit work")
    }
    pub(in crate::storage) fn current(rt: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|b| b.meta.0 == rt.physical.home.identity())
    }
    fn is_empty(&self) -> StorageResult<bool> {
        let empty = self.d().0.is_empty() && self.post().is_empty() && !self.has_accepted_notes();
        Ok(empty && self.publications.is_empty()?)
    }
    pub(in crate::storage::disk_pack) fn lookup(
        &self,
        c: &str,
        k: &str,
    ) -> Option<Arc<HomeMutation>> {
        let d = self.d();
        d.1.get(&(c.into(), k.into()))
            .map(|&at| Arc::clone(&d.0[at]))
    }
    pub(in crate::storage::disk_pack) fn matching(
        &self,
        col: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<HomeMutation>> {
        let d = self.d();
        d.1.iter()
            .filter(|((n, k), _)| n == col && k.as_bytes() >= start && k.as_bytes() < end)
            .map(|(_, &at)| Arc::clone(&d.0[at]))
            .collect()
    }
    fn remember_limit(&self, error: StorageError) -> StorageError {
        if let StorageError::MutationLimit { detail } = &error {
            self.meta
                .2
                .lock()
                .expect("native complete mutation limit")
                .get_or_insert_with(|| detail.clone());
        }
        error
    }
    pub(in crate::storage::disk_pack) fn stage(&self, ops: Vec<HomeMutation>) -> StorageResult<()> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(StorageError::invalid("native accepted batch is immutable"));
        }
        batch_bytes(&ops, self.operation_limit).map_err(|e| self.remember_limit(e))?;
        let mut d = self.d();
        let (cur, inc, lim) = (d.0.len(), ops.len(), self.operation_limit);
        if cur.checked_add(inc).is_none_or(|c| c > lim) {
            let mut uniq: BTreeSet<_> = d.1.keys().map(|(c, k)| (c.as_str(), k.as_str())).collect();
            uniq.extend(
                ops.iter()
                    .map(|op| (op.collection.as_str(), op.key.as_str())),
            );
            let proj = cur.saturating_add(inc);
            let detail = format!(
                "complete native mutation exceeds {lim} storage operations: current={cur}, incoming={inc}, projected={proj}, projected_unique={}",
                uniq.len()
            );
            return Err(self.remember_limit(StorageError::mutation_limit(detail)));
        }
        let msg = "complete native mutation exceeds pending memory allowance";
        let (bytes, max_body) = account_ops(&ops, d.2, d.3, PENDING_MEMORY_BYTES - 256, msg)
            .map_err(|e| self.remember_limit(e))?;
        d.0.try_reserve(ops.len())
            .map_err(|_| StorageError::backend("mutation alloc refused"))?;
        for op in ops {
            let at = d.0.len();
            d.1.insert((op.collection.clone(), op.key.clone()), at);
            d.0.push(Arc::new(op));
        }
        (d.2, d.3) = (bytes, max_body);
        Ok(())
    }
    pub(in crate::storage::disk_pack) fn changed_coordinates(&self) -> Vec<Coord> {
        self.d().1.keys().cloned().collect()
    }
    pub(in crate::storage::disk_pack) async fn deliver(&self) -> StorageResult<()> {
        let mut actions: VecDeque<_> = std::mem::take(&mut *self.post()).into();
        while let Some(action) = actions.pop_front() {
            if let Err(error) = action.await {
                self.post().extend(actions);
                return Err(error);
            }
        }
        Ok(())
    }
}

fn account_ops(
    ops: &[HomeMutation],
    mut bytes: u64,
    mut max_body: usize,
    cap: u64,
    msg: &'static str,
) -> StorageResult<(u64, usize)> {
    for op in ops {
        let body = op.body.as_ref().map_or(0, Vec::len);
        if op.collection.is_empty()
            || op.collection.len() > 65531
            || 2 + op.collection.len() + op.key.len() > MAX_RUN_KEY_BYTES
            || body > MAX_RUN_VALUE_BYTES
        {
            return Err(StorageError::invalid(
                "collection, key, or value exceeds format bounds",
            ));
        }
        max_body = max_body.max(body);
        let add = (160 + op.collection.len() + 2 * op.key.len() + body) as u64;
        bytes = bytes
            .checked_add(add)
            .filter(|&b| b <= cap)
            .ok_or_else(|| StorageError::mutation_limit(msg))?;
    }
    Ok((bytes, max_body))
}

pub(in crate::storage::disk_pack) fn batch_bytes(
    ops: &[HomeMutation],
    limit: usize,
) -> StorageResult<u64> {
    if ops.len() > limit {
        return Err(StorageError::mutation_limit(format!(
            "complete native batch exceeds {limit} operations"
        )));
    }
    let msg = "batch exceeds pending memory allowance";
    account_ops(ops, 256, 0, PENDING_MEMORY_BYTES, msg).map(|(b, _)| b)
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
    let (r_rt, w_pend) = (Arc::clone(&runtime), Arc::clone(&pending));
    let context = run_blocking(move || read::ReadContext::capture(&r_rt, Some(w_pend))).await?;
    let result = PackReadScope(context)
        .run_result(async { Ok(WRITE.scope(Arc::clone(&pending), future).await) })
        .await;
    if !pending.is_empty().unwrap_or(false) {
        blocked::opened();
    }
    if let Some(d) = pending
        .meta
        .2
        .lock()
        .expect("native mutation limit")
        .clone()
    {
        return Err(StorageError::mutation_limit(d).into());
    }
    let result = result??;
    {
        let d = pending.d();
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
    let ops: Vec<_> = pending.d().0.iter().map(|r| (**r).clone()).collect();
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
    let batch = PendingBatch::current(runtime)
        .ok_or_else(|| StorageError::invalid("post-commit work requires preparation scope"))?;
    batch.post().push(Box::pin(action));
    Ok(())
}

impl PackRuntime {
    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        match self.view.try_write() {
            Ok(mut v) => drop(super::physical::adopt(&mut v, self.physical.current_root())),
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
