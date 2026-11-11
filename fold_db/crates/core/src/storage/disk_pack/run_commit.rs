//! Commit admission, publication, background merge worker, and root adoption.

use super::physical::{PhysicalRoot, V2Home, V2Root, CAS_COLLECTION};
use super::*;
use laststore::disk_run::home::{CheckpointKind, RunHome};
use laststore::space::SpacePolicy;

const MERGE_RETRY: std::time::Duration = std::time::Duration::from_secs(30);
const HELD_SHARE: u64 = 16;
const MIN_HELD_BYTES: u64 = 1 << 20;
const HELD_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const HELD_POLL: std::time::Duration = std::time::Duration::from_millis(2);

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
        self.validate_initialization_write(&operations)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native writer requires a snapshot reopen after a failed publication".into(),
            ));
        }
        RunHome::check_mutations(&operations).map_err(map)?;
        let batch_bytes = prefix_batch_bytes(previous, &operations, self.batch_operation_limit())?;
        let intent_keys = cas_delete::key_bytes(&operations)?
            + previous
                .map(|p| cas_delete::key_bytes(&p.operations))
                .transpose()?
                .unwrap_or(0);
        let reserved_bytes = batch_bytes.saturating_add(cas_delete::space(intent_keys)?);
        let v2 = &self.physical;
        let space = match previous {
            Some(prev) => prev
                .space
                .reserve_extension(&self.space_policy, reserved_bytes),
            None => {
                laststore::space::reserve_space(v2.home.path(), &self.space_policy, reserved_bytes)
            }
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
        prepared: PreparedCommit,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let PreparedCommit {
            operations, space, ..
        } = prepared;
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
            Ok(())
        })();
        if let Err(error) = result {
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        let root = v2.current_root();
        let (generation, frontier) = (root.generation(), root.frontier());
        self.install_committed(root, hidden_changes, requests)?;
        self.last_root.observe(generation, frontier);
        drop(space);
        self.failed.store(false, Ordering::Release);
        start_merges(
            v2,
            Arc::clone(&self.view),
            Arc::clone(&self.last_root),
            &self.space_policy,
        );
        Ok(())
    }

    pub(super) fn resume_merges(&self) {
        let v2 = &self.physical;
        if v2.home.needs_merge() || v2.cas.needs_merge() {
            start_merges(
                v2,
                Arc::clone(&self.view),
                Arc::clone(&self.last_root),
                &self.space_policy,
            );
        }
    }

    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        let mut view = match self.view.try_write() {
            Ok(view) => view,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.failed.store(true, Ordering::Release);
                tracing::error!(
                    "native read view lock is poisoned; restart the process to reopen the home"
                );
                return Err(StorageError::BackendError(
                    "native read view lock is poisoned; restart the process to reopen the home"
                        .into(),
                ));
            }
        };
        let replaced = adopt(&mut view, self.physical.current_root(), &self.last_root);
        drop(view);
        drop(replaced);
        Ok(())
    }
}

fn prefix_batch_bytes(
    previous: Option<&PreparedCommit>,
    incoming: &[HomeMutation],
    limit: usize,
) -> StorageResult<u64> {
    let bytes = transaction::batch_bytes(incoming, limit)?;
    let Some(previous) = previous else {
        return Ok(bytes);
    };
    let operations = previous.operations.len().checked_add(incoming.len());
    let bytes = previous
        .batch_bytes
        .checked_add(bytes)
        .and_then(|b| b.checked_sub(256));
    if operations.is_none_or(|count| count > limit)
        || bytes
            .is_none_or(|b| b > lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES)
    {
        return Err(StorageError::WriteResourceLimit {
            detail: "native accepted prefix reached its operation or byte limit; retry after flush"
                .into(),
        });
    }
    Ok(bytes.expect("checked native prefix bytes"))
}

fn adopt(
    view: &mut view::NativeReadView,
    latest: V2Root,
    last_root: &view::LastRoot,
) -> Option<PhysicalRoot> {
    let held = &view.current;
    if held.frontier() != latest.frontier() || held.generation() >= latest.generation() {
        return None;
    }
    last_root.observe(latest.generation(), latest.frontier());
    Some(std::mem::replace(&mut view.current, latest))
}

fn commit_split(
    v2: &V2Home,
    operations: Vec<HomeMutation>,
    mut journal: Option<&mut mutation_log::Recording<'_>>,
) -> StorageResult<()> {
    if !operations.iter().any(|op| op.collection == CAS_COLLECTION) {
        let committed = v2.home.commit(operations).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
        return Ok(());
    }
    let (cas, main): (Vec<_>, Vec<_>) = operations
        .into_iter()
        .partition(|op| op.collection == CAS_COLLECTION);
    let mut last = std::collections::BTreeMap::new();
    for op in cas {
        last.insert(op.key.clone(), op);
    }
    blob_backup::queue_new_uploads(&mut last);
    let (puts, deletes): (Vec<_>, Vec<_>) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        let committed = v2.home.commit(main).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
    }
    if !deletes.is_empty() {
        v2.cas
            .commit(blob_backup::with_releases(deletes))
            .map_err(map)?;
        v2.home
            .clear_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?;
    }
    Ok(())
}

fn start_merges(
    v2: &V2Home,
    view: Arc<RwLock<view::NativeReadView>>,
    last_root: Arc<view::LastRoot>,
    policy: &SpacePolicy,
) {
    let waiting = v2
        .retry_after
        .lock()
        .expect("disk run merge retry")
        .is_some_and(|at| std::time::Instant::now() < at);
    if waiting || v2.merging.swap(true, Ordering::AcqRel) {
        return;
    }
    let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
    let (merging, stop, retry_after, policy) = (
        Arc::clone(&v2.merging),
        Arc::clone(&v2.stop),
        Arc::clone(&v2.retry_after),
        policy.clone(),
    );
    match std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || {
            merge_until_settled(
                &homes,
                &view,
                &last_root,
                &merging,
                &stop,
                &retry_after,
                &policy,
            );
        }) {
        Ok(handle) => {
            if let Some(earlier) = v2
                .merger
                .lock()
                .expect("disk run merge thread")
                .replace(handle)
            {
                let _ = earlier.join();
            }
        }
        Err(error) => {
            v2.merging.store(false, Ordering::Release);
            tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
        }
    }
}

fn merge_until_settled(
    homes: &[std::sync::Weak<RunHome>; 2],
    view: &RwLock<view::NativeReadView>,
    last_root: &view::LastRoot,
    merging: &AtomicBool,
    stop: &AtomicBool,
    retry_after: &Mutex<Option<std::time::Instant>>,
    policy: &SpacePolicy,
) {
    let cas_policy = SpacePolicy::default();
    loop {
        'steps: while !stop.load(Ordering::Acquire) {
            let mut merged = false;
            for (index, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                let space = if index == 0 { policy } else { &cas_policy };
                match home.merge_step_with_space(space) {
                    Ok(true) => {
                        drop(home);
                        if let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) {
                            let latest = V2Root {
                                main: main.current_root(),
                                cas: cas.current_root(),
                            };
                            drop(
                                view.write()
                                    .ok()
                                    .and_then(|mut v| adopt(&mut v, latest, last_root)),
                            );
                        }
                        wait_for_held_runs(weak, stop);
                        merged = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, "disk run merge failed");
                        *retry_after.lock().expect("disk run merge retry") =
                            Some(std::time::Instant::now() + MERGE_RETRY);
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
                .any(|h| h.upgrade().is_some_and(|h| h.needs_merge()));
        if !needed || merging.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

fn wait_for_held_runs(home: &std::sync::Weak<RunHome>, stop: &AtomicBool) {
    let until = std::time::Instant::now() + HELD_WAIT;
    let mut limit = None;
    while let Some(home) = home.upgrade() {
        let budget = *limit
            .get_or_insert_with(|| (home.status().file_bytes / HELD_SHARE).max(MIN_HELD_BYTES));
        if home.retired_bytes() <= budget
            || stop.load(Ordering::Acquire)
            || std::time::Instant::now() >= until
        {
            return;
        }
        drop(home);
        std::thread::sleep(HELD_POLL);
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
