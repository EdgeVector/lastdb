//! Commit admission, write queue batching, and durable publication.

use super::physical::{V2Home, V2Root, CAS_COLLECTION};
use super::transaction::{batch_bytes, PendingBatch};
use super::*;
use crate::storage::format::{NAMESPACES, REGISTRY};
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::home::CheckpointKind::CasDelete;
use laststore::space::reserve_space;
use mutation_log::{CommitRole, Recording};
use std::collections::BTreeMap;
use std::sync::MutexGuard;
use tokio::sync::OwnedMutexGuard;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;
type GatePair = (OwnedMutexGuard<()>, OwnedMutexGuard<()>);
type Coord = (String, String);

#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
}

pub(super) struct PreparedCommit(Vec<HomeMutation>, u64, SpaceReservation);

#[derive(Default)]
pub(in crate::storage) struct WriteQueue {
    pub(in crate::storage) requests: Vec<(Arc<PendingBatch>, u64)>,
    pub(in crate::storage) next_sequence: u64,
    state: (Option<PreparedCommit>, bool),
}

impl PackRuntime {
    fn q(&self) -> MutexGuard<'_, WriteQueue> {
        self.queue.lock().expect("native accepted queue")
    }
    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.seal_queue();
        self.wait_for_queue(target_sequence).await?;
        let cur = &self.view.read().expect("native durable flush view").0;
        let completed_sequence = *self.completed.borrow();
        let (generation, frontier) = (cur.generation(), cur.frontier());
        Ok(NativeFlushReceipt {
            generation,
            frontier,
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
    pub(super) async fn enter_prepare(self: &Arc<Self>) -> OwnedMutexGuard<()> {
        loop {
            let gate = Arc::clone(&self.prepare_gate).lock_owned().await;
            let target = {
                let q = self.q();
                (q.state.1 && !self.requires_recovery()).then_some(q.next_sequence)
            };
            let Some(target) = target else { return gate };
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }
    pub(super) fn prepare_queued(
        &self,
        ops: Vec<HomeMutation>,
    ) -> StorageResult<Option<PreparedCommit>> {
        let q = self.q();
        if q.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::write_limit(
                "accepted queue reached 1024 requests; retry after flush",
            ));
        }
        (!ops.is_empty())
            .then(|| self.prepare_commit(ops, q.state.0.as_ref()))
            .transpose()
    }
    pub(super) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<PendingBatch>,
        prepared: Option<PreparedCommit>,
    ) -> StorageResult<u64> {
        let mut q = self.q();
        let seq = q
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::backend("sequence exhausted"))?;
        let first = q.requests.is_empty();
        if let (Some(inc), Some(prev)) = (&prepared, q.state.0.as_mut()) {
            prev.0
                .try_reserve(inc.0.len())
                .map_err(|_| StorageError::write_limit("prefix alloc refused"))?;
        }
        q.requests.push((pending, seq));
        q.next_sequence = seq;
        if let Some(mut inc) = prepared {
            if let Some(mut prev) = q.state.0.take() {
                prev.0.append(&mut inc.0);
                inc.0 = prev.0;
                inc.2.absorb(prev.2);
            }
            q.state.0 = Some(inc);
        }
        q.state.1 |= q.requests.len() == MAX_REQUESTS;
        drop(q);
        if first {
            let rt = Arc::clone(self);
            tokio::spawn(async move { rt.run_queue().await }.in_current_span());
        }
        Ok(seq)
    }
    pub(super) fn seal_queue(&self) -> u64 {
        let mut q = self.q();
        q.state.1 |= !q.requests.is_empty();
        q.next_sequence
    }
    pub(super) async fn wait_for_queue(&self, target: u64) -> StorageResult<()> {
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
            let mut q = self.q();
            q.state.1 = false;
            q.requests.clear();
        }
        let signal = self.completed.clone();
        drop((requests, self));
        signal.send_modify(|done| *done = (*done).max(seq));
    }
    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<PendingBatch>>)> {
        let (prepared, requests, seq) = {
            let mut q = self.q();
            q.state.1 = true;
            let reqs: Vec<_> = q.requests.iter().map(|(p, _)| Arc::clone(p)).collect();
            let seq = q.requests.last().map_or(q.next_sequence, |(_, s)| *s);
            (q.state.0.take(), reqs, seq)
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
            let cur = self.view.read().expect("empty prefix root").0.clone();
            self.install_committed(cur, &hidden, &requests)?;
        }
        for pending in &requests {
            pending.publications.finish()?;
        }
        Ok((seq, requests))
    }
    pub(super) fn commit(&self, ops: Vec<HomeMutation>) -> StorageResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let prepared = self.prepare_commit(ops, None)?;
        let chg: Vec<_> = prepared
            .0
            .iter()
            .map(|o| (o.collection.clone(), o.key.clone()))
            .collect();
        self.commit_prepared(prepared, &chg, &[])
    }
    fn prepare_commit(
        &self,
        ops: Vec<HomeMutation>,
        prev: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedCommit> {
        let ns_only = ops.iter().all(|o| {
            o.collection == REGISTRY
                && o.key.starts_with(NAMESPACES)
                && o.body.as_deref() == Some(&[1])
        });
        if self.fresh_initialization_pending() && !ns_only {
            return Err(creation_refusal(
                "creation holds durable proofs; source writes must wait",
            ));
        }
        if self.requires_recovery() {
            return Err(StorageError::backend(
                "writer requires reopen after failed publication",
            ));
        }
        let limit = self.batch_operation_limit();
        let incoming = batch_bytes(&ops, limit)?;
        let bytes = if let Some(p) = prev {
            let ok = p.0.len().checked_add(ops.len()).is_some_and(|n| n <= limit);
            let sum = p.1.checked_add(incoming).and_then(|b| b.checked_sub(256));
            sum.filter(|&b| ok && b <= PENDING_MEMORY_BYTES)
                .ok_or_else(|| {
                    StorageError::write_limit("accepted prefix limit reached; retry after flush")
                })?
        } else {
            incoming
        };
        let cas_res = cas_delete::reserved_space(&ops, prev.map(|p| p.0.as_slice()))?;
        let reserved = bytes.saturating_add(cas_res);
        let space = match prev {
            Some(p) => p.2.reserve_extension(&self.space_policy, reserved),
            None => reserve_space(self.physical.home.path(), &self.space_policy, reserved),
        }
        .map_err(map)?;
        Ok(PreparedCommit(ops, bytes, space))
    }
    fn commit_prepared(
        &self,
        PreparedCommit(ops, _, space): PreparedCommit,
        hidden: &[Coord],
        reqs: &[Arc<PendingBatch>],
    ) -> StorageResult<()> {
        let (v2, before) = (&self.physical, self.physical.current_root().frontier());
        let notes = transaction::take_accepted_notes(reqs);
        self.prepare_accepted(before, &notes)?;
        let split = !notes.is_empty() && ops.iter().any(|o| o.collection == CAS_COLLECTION);
        if split {
            self.arm_accepted_split()?;
        }
        let result = (|| {
            cas_delete::require_clear(v2)?;
            let has_main = ops.iter().any(|o| o.collection != CAS_COLLECTION);
            let mut log = has_main.then(|| self.mutation_log.begin(v2)).flatten();
            commit_split(v2, ops, log.as_mut())?;
            if let Some(log) = log {
                log.finish(v2);
            }
            if split {
                self.note_accepted_split(before)?;
            }
            self.publish_accepted(before, v2.current_root().frontier())
        })();
        if let Err(error) = result {
            self.fail_accepted(before);
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        self.install_committed(v2.current_root(), hidden, reqs)?;
        drop(space);
        self.failed.store(false, Ordering::Release);
        self.spawn_merges();
        Ok(())
    }
    fn install_committed(
        &self,
        cur: V2Root,
        hidden: &[Coord],
        reqs: &[Arc<PendingBatch>],
    ) -> StorageResult<()> {
        let records = record_warm::committed_records(reqs);
        let mut view = self.view.write().expect("native complete read view");
        if !hidden.is_empty() {
            view.1 = view
                .1
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("revision exhausted"))?;
        }
        if !hidden.is_empty() || !reqs.is_empty() || !records.is_empty() {
            let mut core = self.core.lock().expect("committed warm records");
            if reqs.is_empty() {
                core.invalidate_records(hidden);
            } else {
                for r in reqs {
                    core.invalidate_records(&r.changed_coordinates());
                }
            }
            if !records.is_empty() {
                core.admit_records(records);
            }
        }
        drop((std::mem::replace(&mut view.0, cur), view));
        self.adopt_merged_root()
    }
}

fn commit_split(
    v2: &V2Home,
    ops: Vec<HomeMutation>,
    mut log: Option<&mut Recording<'_>>,
) -> StorageResult<()> {
    let mut commit_main = |ops: Vec<HomeMutation>| -> StorageResult<()> {
        let committed = v2.home.commit(ops).map_err(map)?;
        if let Some(j) = log.as_mut() {
            j.retain(CommitRole::Main, &committed);
        }
        Ok(())
    };
    let is_cas = |op: &HomeMutation| op.collection == CAS_COLLECTION;
    if !ops.iter().any(is_cas) {
        return commit_main(ops);
    }
    let (cas, main): (Vec<_>, _) = ops.into_iter().partition(is_cas);
    let last: BTreeMap<_, _> = cas.into_iter().map(|op| (op.key.clone(), op)).collect();
    let (puts, deletes): (Vec<_>, _) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        commit_main(main)?;
    }
    if !deletes.is_empty() {
        v2.cas.commit(deletes).map_err(map)?;
        v2.home.clear_checkpoint(CasDelete).map_err(map)?;
    }
    Ok(())
}
