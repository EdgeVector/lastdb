//! Bounded complete requests share one generation-bound stored publication.

use super::run_commit::PreparedCommit;
use super::*;
use std::time::Instant;
use tokio::sync::OwnedMutexGuard;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;

#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
}

pub(in crate::storage) struct WriteGuard {
    _preparation: OwnedMutexGuard<()>,
    _publication: OwnedMutexGuard<()>,
}

#[derive(Default)]
pub(super) struct WriteQueue {
    pub(super) requests: Vec<(Arc<transaction::PendingBatch>, u64)>,
    prepared: Option<PreparedCommit>,
    pub(super) next_sequence: u64,
    sealed: bool,
}

impl PackRuntime {
    pub fn subscribe_durable_writes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.completed.subscribe()
    }

    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.seal_queue();
        self.wait_for_queue(target_sequence).await?;
        let cur = &self.view.read().expect("native durable flush view").current;
        Ok(NativeFlushReceipt {
            generation: cur.generation(),
            frontier: cur.frontier(),
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    pub(in crate::storage::disk_pack) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        self.wait_for_queue(self.seal_queue()).await
    }

    pub(in crate::storage) async fn enter_write(self: &Arc<Self>) -> WriteGuard {
        loop {
            let gate = self.enter_prepare().await;
            let target = self.seal_queue();
            if target <= *self.completed.borrow() || self.failed.load(Ordering::Acquire) {
                return WriteGuard {
                    _preparation: gate,
                    _publication: Arc::clone(&self.write_gate).lock_owned().await,
                };
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(super) async fn enter_prepare(self: &Arc<Self>) -> OwnedMutexGuard<()> {
        loop {
            let gate = Arc::clone(&self.prepare_gate).lock_owned().await;
            let target = {
                let q = self.queue.lock().expect("native accepted queue");
                q.sealed.then_some(q.next_sequence)
            };
            let Some(target) = target.filter(|_| !self.failed.load(Ordering::Acquire)) else {
                return gate;
            };
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(super) fn preparation_prefix(&self) -> Arc<Vec<Arc<transaction::PendingBatch>>> {
        let q = self.queue.lock().expect("native preparation prefix");
        Arc::new(q.requests.iter().map(|(p, _)| Arc::clone(p)).collect())
    }

    pub(super) fn prepare_queued(
        &self,
        incoming: Vec<HomeMutation>,
    ) -> StorageResult<Option<PreparedCommit>> {
        let q = self.queue.lock().expect("native accepted queue");
        if q.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::WriteResourceLimit {
                detail: "native accepted queue reached 1024 requests; retry after flush".into(),
            });
        }
        (!incoming.is_empty())
            .then(|| self.prepare_commit(incoming, q.prepared.as_ref()))
            .transpose()
    }

    pub(super) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<transaction::PendingBatch>,
        prepared: Option<PreparedCommit>,
    ) -> StorageResult<u64> {
        let mut q = self.queue.lock().expect("native accepted queue");
        let seq = q
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::BackendError("native sequence exhausted".into()))?;
        let first = q.requests.is_empty();
        if let (Some(inc), Some(prev)) = (&prepared, q.prepared.as_mut()) {
            prev.operations
                .try_reserve(inc.operations.len())
                .map_err(|_| StorageError::WriteResourceLimit {
                    detail: "native prefix operation allocation refused".into(),
                })?;
        }
        q.requests.push((pending, seq));
        q.next_sequence = seq;
        if let Some(mut inc) = prepared {
            if let Some(mut prev) = q.prepared.take() {
                prev.operations.append(&mut inc.operations);
                inc.operations = prev.operations;
                inc.space.absorb(prev.space);
            }
            q.prepared = Some(inc);
        }
        q.sealed |= q.requests.len() == MAX_REQUESTS;
        drop(q);
        if first {
            let rt = Arc::clone(self);
            tokio::spawn(async move { rt.run_queue().await }.in_current_span());
        }
        Ok(seq)
    }

    pub(super) fn seal_queue(&self) -> u64 {
        let mut q = self.queue.lock().expect("native accepted queue");
        q.sealed |= !q.requests.is_empty();
        q.next_sequence
    }

    pub(super) async fn wait_for_queue(&self, target: u64) -> StorageResult<()> {
        let mut completed = self.completed.subscribe();
        loop {
            if self.failed.load(Ordering::Acquire) {
                return Err(StorageError::BackendError(
                    "native accepted queue requires complete recovery after publication failure"
                        .into(),
                ));
            }
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            completed.changed().await.map_err(|_| {
                StorageError::BackendError(
                    "native accepted queue owner stopped before its durable result".into(),
                )
            })?;
        }
    }

    async fn run_queue(self: Arc<Self>) {
        let prep = Arc::clone(&self.prepare_gate).lock_owned().await;
        self.seal_queue();
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
        let (gate_from, rt) = (Instant::now(), Arc::clone(&self));
        let result = run_blocking(move || {
            let (_prep, _gate) = (prep, gate);
            rt.commit_queue()
                .inspect_err(|_| rt.failed.store(true, Ordering::Release))
        })
        .await;
        match result {
            Ok((seq, requests)) => {
                for req in &requests {
                    if let Err(error) = req.deliver().await {
                        self.fail_queue();
                        tracing::error!(%error, "native durable request post-commit delivery requires recovery");
                        return;
                    }
                }
                {
                    let mut q = self.queue.lock().expect("native accepted queue");
                    let us = u64::try_from(gate_from.elapsed().as_micros()).unwrap_or(u64::MAX);
                    for r in &requests {
                        r.flush_blocked_us.store(us, Ordering::Release);
                    }
                    q.sealed = false;
                    q.requests.clear();
                }
                let signal = self.completed.clone();
                drop((requests, self));
                signal.send_modify(|done| *done = (*done).max(seq));
            }
            Err(error) => {
                self.fail_queue();
                tracing::error!(%error, "native complete accepted prefix requires recovery");
            }
        }
    }

    fn fail_queue(&self) {
        self.failed.store(true, Ordering::Release);
        self.completed.send_modify(|_| {});
    }

    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<transaction::PendingBatch>>)> {
        let (prepared, hidden, requests) = {
            let mut q = self.queue.lock().expect("native accepted queue");
            q.sealed = true;
            let hidden = q
                .requests
                .iter()
                .flat_map(|(p, _)| p.changed_coordinates())
                .collect::<Vec<_>>();
            let reqs = q.requests.iter().map(|(p, _)| Arc::clone(p)).collect();
            (q.prepared.take(), hidden, reqs)
        };
        if let Some(prepared) = prepared {
            self.commit_prepared(prepared, &hidden, &requests)?;
        } else {
            if transaction::has_accepted(&requests) {
                return Err(StorageError::InvalidOperation(
                    "accepted notes require a native commit".into(),
                ));
            }
            self.prepare_accepted(self.physical.current_root().frontier(), &[])?;
            let cur = self.view.read().expect("empty prefix root").current.clone();
            self.install_committed(cur, &hidden, &requests)?;
        }
        let q = self.queue.lock().expect("native accepted queue");
        for (pending, _) in &q.requests {
            pending.publications.finish()?;
        }
        Ok((
            q.requests.last().map_or(q.next_sequence, |(_, seq)| *seq),
            requests,
        ))
    }
}

pub(crate) mod publication {
    use super::{Arc, Mutex, PackRuntime, StorageError, StorageResult};
    use crate::storage::disk_pack::{read::ReadContext, transaction::PendingBatch};
    use std::any::Any;
    use std::collections::BTreeMap;

    type Action = Box<dyn FnOnce() -> StorageResult<()> + Send>;
    type Value = Arc<dyn Any + Send + Sync>;
    type ChangeMap = (BTreeMap<(usize, String), (Value, Action)>, usize);

    #[derive(Default)]
    pub(in crate::storage::disk_pack) struct PendingPublications(Mutex<ChangeMap>);

    impl PendingPublications {
        pub(in crate::storage::disk_pack) fn is_empty(&self) -> StorageResult<bool> {
            Ok(self.lock()?.0.is_empty())
        }

        fn lock(&self) -> StorageResult<std::sync::MutexGuard<'_, ChangeMap>> {
            self.0.lock().map_err(|_| {
                StorageError::BackendError("native metadata publication lock is poisoned".into())
            })
        }

        fn stage(&self, key: (usize, String), val: Value, act: Action) -> StorageResult<()> {
            let mut c = self.lock()?;
            if c.1 >= 4096 {
                return Err(StorageError::InvalidOperation(
                    "native mutation exceeds 4096 metadata publications".into(),
                ));
            }
            c.0.insert(key, (val, act));
            c.1 += 1;
            Ok(())
        }

        pub(in crate::storage::disk_pack) fn finish(&self) -> StorageResult<()> {
            let mut err = None;
            for (_, (_, act)) in std::mem::take(&mut *self.lock()?).0 {
                if let Err(e) = act() {
                    err.get_or_insert(e);
                }
            }
            err.map_or(Ok(()), Err)
        }
    }

    pub(crate) fn publish<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
        name: String,
        val: T,
        act: impl FnOnce() -> StorageResult<()> + Send + 'static,
    ) -> StorageResult<()> {
        match PendingBatch::current(rt) {
            Some(p) => p
                .publications
                .stage((cache, name), Arc::new(val), Box::new(act)),
            None => act(),
        }
    }

    fn downcast<T: Any + Send + Sync>(val: &Value) -> StorageResult<Arc<T>> {
        Arc::clone(val)
            .downcast::<T>()
            .map_err(|_| StorageError::BackendError("native metadata cache type differs".into()))
    }

    pub(crate) fn get<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
        name: &str,
    ) -> StorageResult<Option<Arc<T>>> {
        let key = (cache, name.to_string());
        for p in write_views(rt).into_iter().rev() {
            if let Some((val, _)) = p.publications.lock()?.0.get(&key) {
                return downcast(val).map(Some);
            }
        }
        Ok(None)
    }

    pub(crate) fn entries<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
    ) -> StorageResult<Vec<(String, Arc<T>)>> {
        let mut out = BTreeMap::new();
        for p in write_views(rt) {
            for ((owner, name), (val, _)) in &p.publications.lock()?.0 {
                if *owner == cache {
                    out.insert(name.clone(), downcast(val)?);
                }
            }
        }
        Ok(out.into_iter().collect())
    }

    fn write_views(rt: &PackRuntime) -> Vec<Arc<PendingBatch>> {
        let Some(p) = PendingBatch::current(rt) else {
            return Vec::new();
        };
        let mut v =
            ReadContext::captured(rt).map_or_else(Vec::new, |c| c.accepted.as_ref().clone());
        v.push(p);
        v
    }
}
