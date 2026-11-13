//! Commit admission, publication, background merge worker, and root adoption.

use super::physical::{V2Home, V2Root, CAS_COLLECTION};
use super::*;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::home::{CheckpointKind, RunHome};
use laststore::space::{reserve_space, SpacePolicy};
use std::collections::BTreeMap;
use std::sync::{TryLockError, Weak};
use std::time::{Duration, Instant};

pub(super) struct PreparedCommit {
    pub(super) operations: Vec<HomeMutation>,
    pub(super) batch_bytes: u64,
    pub(super) space: SpaceReservation,
}

impl PackRuntime {
    pub(super) fn commit(&self, operations: Vec<HomeMutation>) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let prepared = self.prepare_commit(operations, None)?;
        let changes: Vec<_> = prepared
            .operations
            .iter()
            .map(|op| (op.collection.clone(), op.key.clone()))
            .collect();
        self.commit_prepared(prepared, &changes, &[])
    }

    pub(super) fn prepare_commit(
        &self,
        operations: Vec<HomeMutation>,
        previous: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedCommit> {
        let only_ns = operations.iter().all(|op| {
            op.collection == REGISTRY
                && op.key.starts_with(NAMESPACES)
                && op.body.as_deref() == Some(&[1])
        });
        if self.fresh_initialization_pending() && !only_ns {
            return Err(creation_refusal(
                "creation already holds its durable proofs; source writes must wait",
            ));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native writer requires a snapshot reopen after a failed publication".into(),
            ));
        }
        RunHome::check_mutations(&operations).map_err(map)?;
        let batch_bytes = prefix_batch_bytes(previous, &operations, self.batch_operation_limit())?;
        let intent =
            cas_delete::reserved_space(&operations, previous.map(|p| p.operations.as_slice()))?;
        let reserved = batch_bytes.saturating_add(intent);
        let space = match previous {
            Some(prev) => prev.space.reserve_extension(&self.space_policy, reserved),
            None => reserve_space(self.physical.home.path(), &self.space_policy, reserved),
        }
        .map_err(map)?;
        Ok(PreparedCommit {
            operations,
            batch_bytes,
            space,
        })
    }

    pub(super) fn commit_prepared(
        &self,
        PreparedCommit {
            operations, space, ..
        }: PreparedCommit,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let frontier_before = self.physical.current_root().frontier();
        let notes = transaction::take_accepted_notes(requests);
        self.prepare_accepted(frontier_before, &notes)?;
        let split =
            !notes.is_empty() && operations.iter().any(|op| op.collection == CAS_COLLECTION);
        if split {
            self.arm_accepted_split()?;
        }
        let v2 = &self.physical;
        let result = (|| {
            cas_delete::require_clear(v2)?;
            let mut journal = operations
                .iter()
                .any(|op| op.collection != CAS_COLLECTION)
                .then(|| self.mutation_log.begin(v2))
                .flatten();
            commit_split(v2, operations, journal.as_mut())?;
            if let Some(journal) = journal {
                journal.finish(v2);
            }
            if split {
                self.note_accepted_split(frontier_before)?;
            }
            self.publish_accepted(frontier_before, v2.current_root().frontier())
        })();
        if let Err(error) = result {
            self.fail_accepted(frontier_before);
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        self.install_committed(v2.current_root(), hidden_changes, requests)?;
        drop(space);
        self.failed.store(false, Ordering::Release);
        self.spawn_merges();
        Ok(())
    }

    pub(super) fn resume_merges(&self) {
        if self.physical.home.needs_merge() || self.physical.cas.needs_merge() {
            self.spawn_merges();
        }
    }

    fn spawn_merges(&self) {
        let v2 = &self.physical;
        if v2
            .retry_after
            .lock()
            .expect("disk run merge retry")
            .is_some_and(|at| Instant::now() < at)
            || v2.merging.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
        let (view, merging, stop, retry, policy) = (
            Arc::clone(&self.view),
            Arc::clone(&v2.merging),
            Arc::clone(&v2.stop),
            Arc::clone(&v2.retry_after),
            self.space_policy.clone(),
        );
        match std::thread::Builder::new()
            .name("lastdb-run-merge".into())
            .spawn(move || merge_until_settled(&homes, &view, &merging, &stop, &retry, &policy))
        {
            Ok(h) => {
                if let Some(prev) = v2.merger.lock().expect("disk run merge thread").replace(h) {
                    let _ = prev.join();
                }
            }
            Err(error) => {
                v2.merging.store(false, Ordering::Release);
                tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
            }
        }
    }

    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        match self.view.try_write() {
            Ok(mut view) => drop(adopt(&mut view, self.physical.current_root())),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(_)) => {
                self.failed.store(true, Ordering::Release);
                let msg =
                    "native read view lock is poisoned; restart the process to reopen the home";
                tracing::error!(msg);
                return Err(StorageError::BackendError(msg.into()));
            }
        }
        Ok(())
    }
}

pub(in crate::storage::disk_pack) fn account_ops(
    operations: &[HomeMutation],
    mut bytes: u64,
    mut max_body: usize,
    cap: u64,
    limit_detail: &'static str,
) -> StorageResult<(u64, usize)> {
    for op in operations {
        if op.collection.len() > 65531 || op.key.len() > 65535 {
            return Err(StorageError::InvalidOperation(
                "native collection or key exceeds format bounds".into(),
            ));
        }
        let body = op.body.as_ref().map_or(0, Vec::len);
        max_body = max_body.max(body);
        bytes = bytes
            .checked_add((160 + op.collection.len() + 2 * op.key.len() + body) as u64)
            .filter(|&b| b <= cap)
            .ok_or_else(|| StorageError::MutationLimit {
                detail: limit_detail.into(),
            })?;
    }
    Ok((bytes, max_body))
}

pub(in crate::storage::disk_pack) fn batch_bytes(
    operations: &[HomeMutation],
    operation_limit: usize,
) -> StorageResult<u64> {
    if operations.len() > operation_limit {
        return Err(StorageError::MutationLimit {
            detail: format!("complete native batch exceeds {operation_limit} operations"),
        });
    }
    account_ops(
        operations,
        256,
        0,
        PENDING_MEMORY_BYTES,
        "complete native batch exceeds pending memory allowance",
    )
    .map(|(bytes, _)| bytes)
}

fn prefix_batch_bytes(
    previous: Option<&PreparedCommit>,
    incoming: &[HomeMutation],
    limit: usize,
) -> StorageResult<u64> {
    let bytes = batch_bytes(incoming, limit)?;
    let Some(prev) = previous else {
        return Ok(bytes);
    };
    let ops = prev.operations.len().checked_add(incoming.len());
    let total = prev
        .batch_bytes
        .checked_add(bytes)
        .and_then(|b| b.checked_sub(256));
    match (ops, total) {
        (Some(n), Some(b)) if n <= limit && b <= PENDING_MEMORY_BYTES => Ok(b),
        _ => Err(StorageError::WriteResourceLimit {
            detail: "native accepted prefix reached its operation or byte limit; retry after flush"
                .into(),
        }),
    }
}

fn adopt(view: &mut view::NativeReadView, latest: V2Root) -> Option<V2Root> {
    (view.current.frontier() == latest.frontier()
        && view.current.generation() < latest.generation())
    .then(|| std::mem::replace(&mut view.current, latest))
}

fn commit_split(
    v2: &V2Home,
    operations: Vec<HomeMutation>,
    mut journal: Option<&mut mutation_log::Recording<'_>>,
) -> StorageResult<()> {
    let mut commit_main = |ops: Vec<HomeMutation>| -> StorageResult<()> {
        let committed = v2.home.commit(ops).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
        Ok(())
    };
    if !operations.iter().any(|op| op.collection == CAS_COLLECTION) {
        return commit_main(operations);
    }
    let (cas, main): (Vec<_>, _) = operations
        .into_iter()
        .partition(|op| op.collection == CAS_COLLECTION);
    let mut last: BTreeMap<_, _> = cas.into_iter().map(|op| (op.key.clone(), op)).collect();
    physical::queue_new_uploads(&mut last);
    let (puts, deletes): (Vec<_>, _) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        commit_main(main)?;
    }
    if !deletes.is_empty() {
        v2.cas
            .commit(physical::with_releases(deletes))
            .map_err(map)?;
        v2.home
            .clear_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?;
    }
    Ok(())
}

fn merge_until_settled(
    homes: &[Weak<RunHome>; 2],
    view: &RwLock<view::NativeReadView>,
    merging: &AtomicBool,
    stop: &AtomicBool,
    retry_after: &Mutex<Option<Instant>>,
    policy: &SpacePolicy,
) {
    let cas_policy = SpacePolicy::default();
    loop {
        'steps: while !stop.load(Ordering::Acquire) {
            let mut merged = false;
            for (idx, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                match home.merge_step_with_space(if idx == 0 { policy } else { &cas_policy }) {
                    Ok(true) => {
                        drop(home);
                        if let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) {
                            let latest = V2Root {
                                main: main.current_root(),
                                cas: cas.current_root(),
                            };
                            drop(view.write().ok().and_then(|mut v| adopt(&mut v, latest)));
                        }
                        wait_for_held_runs(weak, stop);
                        merged = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, "disk run merge failed");
                        *retry_after.lock().expect("disk run merge retry") =
                            Some(Instant::now() + Duration::from_secs(30));
                        merging.store(false, Ordering::Release);
                        return;
                    }
                }
            }
            if !merged {
                break;
            }
        }
        merging.store(false, Ordering::Release);
        let needed = !stop.load(Ordering::Acquire)
            && homes
                .iter()
                .filter_map(Weak::upgrade)
                .any(|h| h.needs_merge());
        if !needed || merging.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

fn wait_for_held_runs(home: &Weak<RunHome>, stop: &AtomicBool) {
    let (until, mut limit) = (Instant::now() + Duration::from_secs(2), None);
    while let Some(home) = home.upgrade() {
        let cap = *limit.get_or_insert_with(|| (home.run_totals().2 / 16).max(1 << 20));
        if home.retired_bytes() <= cap || stop.load(Ordering::Acquire) || Instant::now() >= until {
            return;
        }
        drop(home);
        std::thread::sleep(Duration::from_millis(2));
    }
}

impl Drop for V2Home {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(Some(handle)) = self.merger.get_mut().map(Option::take) {
            let _ = handle.join();
        }
    }
}
