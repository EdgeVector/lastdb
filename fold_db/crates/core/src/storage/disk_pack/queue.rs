//! Bounded complete requests share one generation-bound stored publication.

use super::run_commit::PreparedCommit;
use super::*;
use tokio::sync::OwnedMutexGuard;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;

fn berr(m: &str) -> StorageError {
    StorageError::BackendError(m.into())
}

#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
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
        let cur = &self.view.read().expect("native durable flush view").0;
        Ok(NativeFlushReceipt {
            generation: cur.generation(),
            frontier: cur.frontier(),
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    pub(in crate::storage) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        self.wait_for_queue(self.seal_queue()).await
    }

    pub(in crate::storage) async fn enter_write(
        self: &Arc<Self>,
    ) -> (OwnedMutexGuard<()>, OwnedMutexGuard<()>) {
        loop {
            let gate = self.enter_prepare().await;
            let target = self.seal_queue();
            if target <= *self.completed.borrow() || self.requires_recovery() {
                return (gate, Arc::clone(&self.write_gate).lock_owned().await);
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
            let Some(target) = target.filter(|_| !self.requires_recovery()) else {
                return gate;
            };
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(super) fn prepare_queued(
        &self,
        incoming: Vec<HomeMutation>,
    ) -> StorageResult<Option<PreparedCommit>> {
        let q = self.queue.lock().expect("native accepted queue");
        if q.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::write_limit(
                "native accepted queue reached 1024 requests; retry after flush",
            ));
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
            .ok_or_else(|| berr("native sequence exhausted"))?;
        let first = q.requests.is_empty();
        if let (Some(inc), Some(prev)) = (&prepared, q.prepared.as_mut()) {
            prev.0.try_reserve(inc.0.len()).map_err(|_| {
                StorageError::write_limit("native prefix operation allocation refused")
            })?;
        }
        q.requests.push((pending, seq));
        q.next_sequence = seq;
        if let Some(mut inc) = prepared {
            if let Some(mut prev) = q.prepared.take() {
                prev.0.append(&mut inc.0);
                inc.0 = prev.0;
                inc.2.absorb(prev.2);
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
            if self.requires_recovery() {
                return Err(berr(
                    "native accepted queue requires complete recovery after publication failure",
                ));
            }
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            completed.changed().await.map_err(|_| {
                berr("native accepted queue owner stopped before its durable result")
            })?;
        }
    }

    async fn run_queue(self: Arc<Self>) {
        let prep = Arc::clone(&self.prepare_gate).lock_owned().await;
        self.seal_queue();
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
        let rt = Arc::clone(&self);
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
        let (prepared, hidden, requests, seq) = {
            let mut q = self.queue.lock().expect("native accepted queue");
            q.sealed = true;
            let hidden: Vec<_> = q
                .requests
                .iter()
                .flat_map(|(p, _)| p.changed_coordinates())
                .collect();
            let reqs: Vec<_> = q.requests.iter().map(|(p, _)| Arc::clone(p)).collect();
            let seq = q.requests.last().map_or(q.next_sequence, |(_, s)| *s);
            (q.prepared.take(), hidden, reqs, seq)
        };
        if let Some(prepared) = prepared {
            self.commit_prepared(prepared, &hidden, &requests)?;
        } else {
            if transaction::has_accepted(&requests) {
                return Err(StorageError::invalid(
                    "accepted notes require a native commit",
                ));
            }
            self.prepare_accepted(self.physical.current_root().frontier(), &[])?;
            let cur = self.view.read().expect("empty prefix root").0.clone();
            self.install_committed(cur, &hidden, &requests)?;
        }
        for pending in &requests {
            pending.publications.finish()?;
        }
        Ok((seq, requests))
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
            self.0
                .lock()
                .map_err(|_| StorageError::backend("native metadata publication lock is poisoned"))
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
        let Some(p) = PendingBatch::current(rt) else {
            return act();
        };
        let mut c = p.publications.lock()?;
        if c.1 >= 4096 {
            return Err(StorageError::invalid(
                "native mutation exceeds 4096 metadata publications",
            ));
        }
        c.0.insert((cache, name), (Arc::new(val), Box::new(act)));
        c.1 += 1;
        Ok(())
    }

    fn downcast<T: Any + Send + Sync>(val: &Value) -> StorageResult<Arc<T>> {
        Arc::clone(val)
            .downcast::<T>()
            .map_err(|_| StorageError::backend("native metadata cache type differs"))
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
