//! Pending batch staging and memory accounting for native write scopes.

use super::*;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::{MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::{future::Future, pin::Pin, sync::MutexGuard};

mod accepted_slot;
mod scope;

pub(crate) use super::{publication, record_warm};
pub(in crate::storage) use accepted_slot::{has_accepted, stage_accepted, take_accepted_notes};
pub use scope::PackReadScope;
pub(crate) use scope::{after_commit, blocked, scope, scope as request_scope, traits};

type Coord = (String, String);
type AfterCommit = Pin<Box<dyn Future<Output = StorageResult<()>> + Send>>;

#[derive(Default)]
struct PendingMutations {
    ops: Vec<Arc<HomeMutation>>,
    index: BTreeMap<Coord, usize>,
    pending_bytes: u64,
    max_body_bytes: usize,
}

pub(in crate::storage) struct PendingBatch {
    home_id: uuid::Uuid,
    after_commit: Mutex<Vec<AfterCommit>>,
    mutation_limit: Mutex<Option<String>>,
    mutations: Mutex<PendingMutations>,
    pub(in crate::storage::disk_pack) publications: publication::PendingPublications,
    accepted: Mutex<Vec<crate::storage::disk_backup::accepted::Note>>,
    frozen: AtomicBool,
    pub(in crate::storage::disk_pack) operation_limit: usize,
    pub(in crate::storage::disk_pack) warm_records: Mutex<record_warm::StagedWarmRecords>,
}

tokio::task_local! { static WRITE: Arc<PendingBatch>; }

impl PendingBatch {
    fn new(rt: &PackRuntime) -> Self {
        Self {
            home_id: rt.physical.home.identity(),
            after_commit: Mutex::default(),
            mutation_limit: Mutex::default(),
            mutations: Mutex::default(),
            publications: publication::PendingPublications::default(),
            accepted: Mutex::default(),
            frozen: AtomicBool::new(false),
            operation_limit: rt.batch_operation_limit(),
            warm_records: Mutex::default(),
        }
    }

    fn lock_mutations(&self) -> MutexGuard<'_, PendingMutations> {
        self.mutations.lock().expect("native pending batch")
    }

    fn lock_after_commit(&self) -> MutexGuard<'_, Vec<AfterCommit>> {
        self.after_commit.lock().expect("native post-commit work")
    }

    pub(in crate::storage) fn current(rt: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|batch| batch.home_id == rt.physical.home.identity())
    }

    fn is_empty(&self) -> StorageResult<bool> {
        let empty = self.lock_mutations().ops.is_empty()
            && self.lock_after_commit().is_empty()
            && !self.has_accepted_notes();
        Ok(empty && self.publications.is_empty()?)
    }

    pub(in crate::storage::disk_pack) fn lookup(
        &self,
        col: &str,
        key: &str,
    ) -> Option<Arc<HomeMutation>> {
        let data = self.lock_mutations();
        data.index
            .get(&(col.into(), key.into()))
            .map(|&at| Arc::clone(&data.ops[at]))
    }

    pub(in crate::storage::disk_pack) fn matching(
        &self,
        col: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<HomeMutation>> {
        let data = self.lock_mutations();
        data.index
            .iter()
            .filter(|((ns, key), _)| ns == col && key.as_bytes() >= start && key.as_bytes() < end)
            .map(|(_, &at)| Arc::clone(&data.ops[at]))
            .collect()
    }

    fn remember_limit(&self, error: StorageError) -> StorageError {
        if let StorageError::MutationLimit { detail } = &error {
            self.mutation_limit
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
        let mut data = self.lock_mutations();
        let (cur, inc, lim) = (data.ops.len(), ops.len(), self.operation_limit);
        if cur.checked_add(inc).is_none_or(|c| c > lim) {
            let mut uniq: BTreeSet<_> = data
                .index
                .keys()
                .map(|(c, k)| (c.as_str(), k.as_str()))
                .collect();
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
        let (pending_bytes, max_body_bytes) = account_ops(
            &ops,
            data.pending_bytes,
            data.max_body_bytes,
            PENDING_MEMORY_BYTES - 256,
            msg,
        )
        .map_err(|e| self.remember_limit(e))?;
        data.ops
            .try_reserve(ops.len())
            .map_err(|_| StorageError::backend("mutation alloc refused"))?;
        for op in ops {
            let at = data.ops.len();
            data.index
                .insert((op.collection.clone(), op.key.clone()), at);
            data.ops.push(Arc::new(op));
        }
        data.pending_bytes = pending_bytes;
        data.max_body_bytes = max_body_bytes;
        Ok(())
    }

    pub(in crate::storage::disk_pack) fn changed_coordinates(&self) -> Vec<Coord> {
        self.lock_mutations().index.keys().cloned().collect()
    }

    pub(in crate::storage::disk_pack) async fn deliver(&self) -> StorageResult<()> {
        let mut actions: VecDeque<_> = std::mem::take(&mut *self.lock_after_commit()).into();
        while let Some(action) = actions.pop_front() {
            if let Err(error) = action.await {
                self.lock_after_commit().extend(actions);
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
