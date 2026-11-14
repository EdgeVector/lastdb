//! Accepted write-queue batching, sealing, and post-commit delivery.

mod publish;

use super::transaction::PendingBatch;
use super::*;
use std::sync::MutexGuard;
use tokio::sync::OwnedMutexGuard;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;
type GatePair = (OwnedMutexGuard<()>, OwnedMutexGuard<()>);

#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
}

pub(in crate::storage::disk_pack) struct PreparedCommit {
    mutations: Vec<HomeMutation>,
    pending_bytes: u64,
    space: SpaceReservation,
}

#[derive(Default)]
pub(in crate::storage) struct WriteQueue {
    pub(in crate::storage) requests: Vec<(Arc<PendingBatch>, u64)>,
    pub(in crate::storage) next_sequence: u64,
    prepared: Option<PreparedCommit>,
    sealed: bool,
}

impl PackRuntime {
    fn lock_queue(&self) -> MutexGuard<'_, WriteQueue> {
        self.queue.lock().expect("native accepted queue")
    }

    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.seal_queue();
        self.wait_for_queue(target_sequence).await?;
        let cur = &self.view.read().expect("native durable flush view").root;
        let completed_sequence = *self.completed.borrow();
        Ok(NativeFlushReceipt {
            generation: cur.generation(),
            frontier: cur.frontier(),
            target_sequence,
            completed_sequence,
        })
    }

    pub(in crate::storage) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if PendingBatch::current(self).is_none() {
            self.wait_for_queue(self.seal_queue()).await?;
        }
        Ok(())
    }

    pub(in crate::storage) async fn enter_write(self: &Arc<Self>) -> GatePair {
        loop {
            let (gate, target) = (self.enter_prepare().await, self.seal_queue());
            let done = *self.completed.borrow();
            if target <= done || self.requires_recovery() {
                return (gate, Arc::clone(&self.write_gate).lock_owned().await);
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(in crate::storage::disk_pack) async fn enter_prepare(
        self: &Arc<Self>,
    ) -> OwnedMutexGuard<()> {
        loop {
            let gate = Arc::clone(&self.prepare_gate).lock_owned().await;
            let target = {
                let q = self.lock_queue();
                (q.sealed && !self.requires_recovery()).then_some(q.next_sequence)
            };
            let Some(target) = target else { return gate };
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(in crate::storage::disk_pack) fn prepare_queued(
        &self,
        ops: Vec<HomeMutation>,
    ) -> StorageResult<Option<PreparedCommit>> {
        let q = self.lock_queue();
        if q.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::write_limit(
                "accepted queue reached 1024 requests; retry after flush",
            ));
        }
        (!ops.is_empty())
            .then(|| self.prepare_commit(ops, q.prepared.as_ref()))
            .transpose()
    }

    pub(in crate::storage::disk_pack) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<PendingBatch>,
        prepared: Option<PreparedCommit>,
    ) -> StorageResult<u64> {
        let mut q = self.lock_queue();
        let seq = q
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::backend("sequence exhausted"))?;
        let first = q.requests.is_empty();
        if let (Some(inc), Some(prev)) = (&prepared, q.prepared.as_mut()) {
            prev.mutations
                .try_reserve(inc.mutations.len())
                .map_err(|_| StorageError::write_limit("prefix alloc refused"))?;
        }
        q.requests.push((pending, seq));
        q.next_sequence = seq;
        if let Some(mut inc) = prepared {
            if let Some(mut prev) = q.prepared.take() {
                prev.mutations.append(&mut inc.mutations);
                inc.mutations = prev.mutations;
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

    pub(in crate::storage::disk_pack) fn seal_queue(&self) -> u64 {
        let mut q = self.lock_queue();
        q.sealed |= !q.requests.is_empty();
        q.next_sequence
    }

    pub(in crate::storage::disk_pack) async fn wait_for_queue(
        &self,
        target: u64,
    ) -> StorageResult<()> {
        let mut completed = self.completed.subscribe();
        while !self.requires_recovery() {
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            if completed.changed().await.is_err() {
                return Err(StorageError::backend(
                    "accepted queue owner stopped before durable result",
                ));
            }
        }
        Err(StorageError::backend(
            "accepted queue requires recovery after publication failure",
        ))
    }

    async fn run_queue(self: Arc<Self>) {
        let prep = Arc::clone(&self.prepare_gate).lock_owned().await;
        self.seal_queue();
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
        let rt = Arc::clone(&self);
        let res = run_blocking(move || {
            let _held = (prep, gate);
            rt.commit_queue()
                .inspect_err(|_| rt.failed.store(true, Ordering::Release))
        })
        .await;
        let fail = || {
            self.failed.store(true, Ordering::Release);
            self.completed.send_modify(|_| {});
        };
        let Ok((seq, requests)) = res.inspect_err(|error| {
            fail();
            tracing::error!(%error, "native complete accepted prefix requires recovery");
        }) else {
            return;
        };
        for req in &requests {
            if let Err(error) = req.deliver().await {
                fail();
                tracing::error!(%error, "native durable request post-commit delivery requires recovery");
                return;
            }
        }
        {
            let mut q = self.lock_queue();
            q.sealed = false;
            q.requests.clear();
        }
        let signal = self.completed.clone();
        drop((requests, self));
        signal.send_modify(|done| *done = (*done).max(seq));
    }

    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<PendingBatch>>)> {
        let (prepared, requests, seq) = {
            let mut q = self.lock_queue();
            q.sealed = true;
            let reqs: Vec<_> = q.requests.iter().map(|(p, _)| Arc::clone(p)).collect();
            let seq = q.requests.last().map_or(q.next_sequence, |(_, s)| *s);
            (q.prepared.take(), reqs, seq)
        };
        let hidden: Vec<_> = requests
            .iter()
            .flat_map(|p| p.changed_coordinates())
            .collect();
        if let Some(prepared) = prepared {
            self.commit_prepared(prepared, &hidden, &requests)?;
        } else {
            if transaction::has_accepted(&requests) {
                return Err(StorageError::invalid(
                    "accepted notes require a native commit",
                ));
            }
            self.prepare_accepted(self.physical.current_root().frontier(), &[])?;
            let cur = self.view.read().expect("empty prefix root").root.clone();
            self.install_committed(cur, &hidden, &requests)?;
        }
        for pending in &requests {
            pending.publications.finish()?;
        }
        Ok((seq, requests))
    }
}
