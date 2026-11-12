//! Bounded complete requests share one generation-bound stored publication.

use super::run_commit::PreparedCommit;
use super::*;
use std::time::Instant;
use tokio::sync::OwnedMutexGuard;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;

/// Durable native boundary after all earlier accepted batches finish.
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

pub(super) struct QueuedRequest {
    pub(super) pending: Arc<transaction::PendingBatch>,
    pub(super) sequence: u64,
}

#[derive(Default)]
pub(super) struct WriteQueue {
    pub(super) requests: Vec<QueuedRequest>,
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
        let view = self.view.read().expect("native durable flush view");
        Ok(NativeFlushReceipt {
            generation: view.current.generation(),
            frontier: view.current.frontier(),
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    pub(in crate::storage::disk_pack) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        let target = self.seal_queue();
        self.wait_for_queue(target).await
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

    /// A sealed prefix excludes new preparation until its durable result.
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
        Arc::new(q.requests.iter().map(|r| Arc::clone(&r.pending)).collect())
    }

    /// Quote only new operations; the accepted prefix retains its owned quota.
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

    /// No await separates accepted publication from transfer to the queue owner.
    pub(super) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<transaction::PendingBatch>,
        prepared: Option<PreparedCommit>,
    ) -> StorageResult<u64> {
        let mut q = self.queue.lock().expect("native accepted queue");
        let sequence = q.next_sequence.checked_add(1).ok_or_else(|| {
            StorageError::BackendError("native accepted sequence exhausted".into())
        })?;
        let first = q.requests.is_empty();
        if let (Some(inc), Some(prev)) = (&prepared, q.prepared.as_mut()) {
            prev.operations
                .try_reserve(inc.operations.len())
                .map_err(|_| StorageError::WriteResourceLimit {
                    detail: "native prefix operation allocation refused".into(),
                })?;
        }
        q.requests.push(QueuedRequest { pending, sequence });
        q.next_sequence = sequence;
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
        Ok(sequence)
    }

    pub(super) fn seal_queue(&self) -> u64 {
        let mut q = self.queue.lock().expect("native accepted queue");
        q.sealed |= !q.requests.is_empty();
        q.next_sequence
    }

    /// A flush waits for its captured prefix, not a moving queue tail.
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
        let gate_from = Instant::now();
        let rt = Arc::clone(&self);
        let result = run_blocking(move || {
            let (_prep, _gate) = (prep, gate);
            rt.commit_queue()
                .inspect_err(|_| rt.failed.store(true, Ordering::Release))
        })
        .await;
        match result {
            Ok((sequence, requests)) => {
                for req in &requests {
                    if let Err(error) = req.deliver().await {
                        self.failed.store(true, Ordering::Release);
                        self.completed.send_modify(|_| {});
                        tracing::error!(%error, "native durable request post-commit delivery requires recovery");
                        return;
                    }
                }
                {
                    let mut q = self.queue.lock().expect("native accepted queue");
                    let blocked = gate_from.elapsed();
                    for req in &requests {
                        req.note_flush_blocked(blocked);
                    }
                    q.requests.clear();
                    q.sealed = false;
                }
                let signal = self.completed.clone();
                drop((requests, self));
                signal.send_modify(|done| *done = (*done).max(sequence));
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                self.completed.send_modify(|_| {});
                tracing::error!(%error, "native complete accepted prefix requires recovery");
            }
        }
    }

    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<transaction::PendingBatch>>)> {
        let (prepared, hidden, requests) = {
            let mut q = self.queue.lock().expect("native accepted queue");
            q.sealed = true;
            let hidden = q
                .requests
                .iter()
                .flat_map(|r| r.pending.changed_coordinates())
                .collect();
            let requests = q.requests.iter().map(|r| Arc::clone(&r.pending)).collect();
            (q.prepared.take(), hidden, requests)
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
            let cur = self
                .view
                .read()
                .expect("native empty prefix root")
                .current
                .clone();
            self.install_committed(cur, &hidden, &requests)?;
        }
        let q = self.queue.lock().expect("native accepted queue");
        for req in &q.requests {
            req.pending.finish_publications()?;
        }
        Ok((
            q.requests.last().map_or(q.next_sequence, |r| r.sequence),
            requests,
        ))
    }
}
