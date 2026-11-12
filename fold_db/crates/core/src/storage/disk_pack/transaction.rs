//! Pending batch staging, write request scopes, and post-commit callbacks.

use super::record_warm::WrittenRecord;
use super::run_commit::{account_ops, batch_bytes};
use super::*;
use crate::storage::format::records::require_final_record_sizes;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

mod accepted_slot;
pub(crate) use super::queue::publication;
pub(crate) use super::view::blocked;
pub(in crate::storage) use accepted_slot::{has_accepted, stage_accepted, take_accepted_notes};
pub(crate) use scope as request_scope;

type AfterCommit = Pin<Box<dyn Future<Output = StorageResult<()>> + Send>>;

#[derive(Default)]
struct PendingData {
    operations: Vec<Arc<HomeMutation>>,
    latest: BTreeMap<(String, String), usize>,
    bytes: u64,
    max_body_bytes: usize,
}

pub(in crate::storage) struct PendingBatch {
    identity: uuid::Uuid,
    data: Mutex<PendingData>,
    pub(in crate::storage::disk_pack) publications: publication::PendingPublications,
    accepted: Mutex<Vec<crate::storage::disk_backup::accepted::Note>>,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    pub(super) flush_blocked_us: AtomicU64,
    preparation_limit: Mutex<Option<String>>,
    pub(in crate::storage::disk_pack) operation_limit: usize,
    pub(super) warm_records: Mutex<(Vec<WrittenRecord>, usize)>,
}

tokio::task_local! {
    static WRITE: Arc<PendingBatch>;
}

impl PendingBatch {
    fn new(runtime: &PackRuntime) -> Self {
        Self {
            identity: runtime.native_home_id(),
            data: Mutex::new(PendingData::default()),
            publications: publication::PendingPublications::default(),
            accepted: Mutex::new(Vec::new()),
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            flush_blocked_us: AtomicU64::new(0),
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
        let no_ops = self
            .data
            .lock()
            .expect("native pending batch")
            .operations
            .is_empty();
        let no_cb = self
            .after_commit
            .lock()
            .expect("native post-commit work")
            .is_empty();
        Ok(no_ops && no_cb && !self.has_accepted_notes() && self.publications.is_empty()?)
    }

    pub(in crate::storage::disk_pack) fn lookup(
        &self,
        collection: &str,
        key: &str,
    ) -> Option<Arc<HomeMutation>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .get(&(collection.to_string(), key.to_string()))
            .map(|&at| Arc::clone(&data.operations[at]))
    }

    pub(in crate::storage::disk_pack) fn matching(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<HomeMutation>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .iter()
            .filter(|((name, key), _)| {
                name == collection && key.as_bytes() >= start && key.as_bytes() < end
            })
            .map(|(_, &at)| Arc::clone(&data.operations[at]))
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
            return Err(StorageError::InvalidOperation(
                "native accepted batch is immutable".into(),
            ));
        }
        batch_bytes(&operations, self.operation_limit).map_err(|e| self.remember_limit(e))?;
        let mut data = self.data.lock().expect("native pending batch");
        let count = data
            .operations
            .len()
            .checked_add(operations.len())
            .filter(|&c| c <= self.operation_limit)
            .ok_or_else(|| {
                self.remember_limit(refusal(&data, &operations, self.operation_limit))
            })?;
        let (bytes, max_body) = account_ops(
            &operations,
            data.bytes,
            data.max_body_bytes,
            PENDING_MEMORY_BYTES - 256,
            "complete native mutation exceeds pending memory allowance",
        )
        .map_err(|e| self.remember_limit(e))?;
        data.operations
            .try_reserve(count - data.operations.len())
            .map_err(|_| StorageError::BackendError("native mutation allocation refused".into()))?;
        for op in operations {
            let at = data.operations.len();
            data.latest
                .insert((op.collection.clone(), op.key.clone()), at);
            data.operations.push(Arc::new(op));
        }
        (data.bytes, data.max_body_bytes) = (bytes, max_body);
        Ok(())
    }

    fn freeze_operations(&self) -> Vec<HomeMutation> {
        self.frozen.store(true, Ordering::Release);
        self.data
            .lock()
            .expect("native pending batch")
            .operations
            .iter()
            .map(|r| (**r).clone())
            .collect()
    }

    pub(in crate::storage::disk_pack) fn changed_coordinates(&self) -> Vec<(String, String)> {
        self.data
            .lock()
            .expect("native pending changes")
            .latest
            .keys()
            .cloned()
            .collect()
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

fn refusal(data: &PendingData, ops: &[HomeMutation], limit: usize) -> StorageError {
    let mut unique: BTreeSet<_> = data
        .latest
        .keys()
        .map(|(c, k)| (c.as_str(), k.as_str()))
        .collect();
    unique.extend(
        ops.iter()
            .map(|op| (op.collection.as_str(), op.key.as_str())),
    );
    let (cur, inc) = (data.operations.len(), ops.len());
    StorageError::MutationLimit {
        detail: format!(
            "complete native mutation exceeds {limit} storage operations: current={cur}, incoming={inc}, projected={}, projected_unique={}",
            cur.saturating_add(inc),
            unique.len()
        ),
    }
}

pub(crate) async fn scope<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    scope_inner(runtime, future, false).await
}

pub(crate) async fn background_scope<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    scope_inner(runtime, future, true).await
}

async fn scope_inner<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl Future<Output = Result<T, E>>,
    background: bool,
) -> Result<T, E> {
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let gate = runtime.enter_prepare().await;
    let held_from = Instant::now();
    let pending = Arc::new(PendingBatch::new(&runtime));
    let (r_rt, w_pend) = (Arc::clone(&runtime), Arc::clone(&pending));
    let context = run_blocking(move || read::ReadContext::capture(&r_rt, Some(w_pend))).await?;
    let result = PackReadScope { context }
        .run_result(async { Ok(WRITE.scope(Arc::clone(&pending), future).await) })
        .await;
    if !pending.is_empty().unwrap_or(false) {
        blocked::opened();
    }
    if let Some(detail) = pending
        .preparation_limit
        .lock()
        .expect("native complete mutation limit")
        .clone()
    {
        return Err(StorageError::MutationLimit { detail }.into());
    }
    let result = result??;
    {
        let d = pending.data.lock().expect("native final format admission");
        require_final_record_sizes(
            d.max_body_bytes,
            d.latest.values().filter_map(|&at| {
                let r = &d.operations[at];
                Some((
                    r.collection.as_str(),
                    r.key.as_str(),
                    r.body.as_ref()?.len(),
                ))
            }),
        )?;
    }
    if pending.is_empty()? {
        let target = runtime.seal_queue();
        drop(gate);
        runtime.wait_for_queue(target).await?;
        blocked::note(held_from.elapsed());
        return Ok(result);
    }
    let operations = pending.freeze_operations();
    let pub_gate = Arc::clone(&runtime.write_gate).lock_owned().await;
    let owner = Arc::clone(&runtime);
    let (prepared, gate, pub_gate) =
        run_blocking(move || Ok((owner.prepare_queued(operations)?, gate, pub_gate))).await?;
    let timing = Arc::clone(&pending);
    let sequence = runtime.accept_queued(pending, prepared)?;
    drop((pub_gate, gate));
    let held = held_from.elapsed();
    runtime.wait_for_queue(sequence).await?;
    let flush_wait = Duration::from_micros(timing.flush_blocked_us.load(Ordering::Acquire));
    blocked::note(if background {
        held.saturating_add(flush_wait)
    } else {
        held_from.elapsed()
    });
    Ok(result)
}

pub(crate) fn after_commit(
    runtime: &PackRuntime,
    action: impl Future<Output = StorageResult<()>> + Send + 'static,
) -> StorageResult<()> {
    PendingBatch::current(runtime)
        .ok_or_else(|| {
            StorageError::InvalidOperation(
                "native post-commit work requires one complete preparation scope".into(),
            )
        })?
        .after_commit
        .lock()
        .expect("native post-commit work")
        .push(Box::pin(action));
    Ok(())
}
