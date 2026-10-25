//! Commit and background merge on a v2 run home.
//!
//! A commit writes one level-0 run and installs its root as the readable
//! state. A merge publishes a root with the same frontier, so it holds the
//! same data; reads adopt it the next time they capture a root, and the
//! merged input files go when the last read of the older root ends.
//!
//! The CAS files have their own run home. A commit that holds CAS changes
//! writes up to three runs in this order: new CAS files, then the other
//! records, then CAS deletes. A crash between them can leave a CAS file that
//! nothing references, but never a reference to a CAS file that is not there.

use super::physical::{Physical, PhysicalRoot, V2Home, CAS_COLLECTION};
use super::*;
use laststore::disk_run::home::RunHome;

/// Hidden record changes and key-membership changes of one commit.
type Changes<'a> = (&'a [(String, String)], &'a [(String, String, bool)]);

impl PackRuntime {
    /// Admit one v2 batch, or one extension of an accepted prefix. Key and
    /// value limits and disk space are checked here, so an oversized write
    /// or a full disk is refused alone and never fails a commit.
    pub(super) fn prepare_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        previous: Option<&write::PreparedCommit>,
    ) -> StorageResult<write::PreparedCommit> {
        RunHome::check_mutations(&operations).map_err(map)?;
        let limit = self.batch_operation_limit();
        let batch_bytes = write::prefix_batch_bytes(previous, &operations, limit)?;
        // A run file holds about the batch bytes; a merge reuses the space
        // of the runs it replaces once their last read ends.
        let space = match previous {
            Some(previous) => previous
                .space()
                .reserve_extension(&self.space_policy, batch_bytes),
            None => laststore::disk_pack::maintenance::reserve_space(
                v2.home.path(),
                &self.space_policy,
                batch_bytes,
            ),
        }
        .map_err(map)?;
        Ok(write::PreparedCommit::for_run(
            operations,
            batch_bytes,
            space,
        ))
    }

    /// The caller holds the write gate. An error fails the runtime, as a
    /// failed v1 publication does: accepted writes did not become durable.
    pub(super) fn commit_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        space: SpaceReservation,
        (hidden_changes, key_changes): Changes<'_>,
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        if let Err(error) = commit_split(v2, operations) {
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(map(error));
        }
        // The current roots hold this commit; a merge root holds the same data.
        let root = v2.current_root();
        let _timing = laststore::commit_timing::stage("memory_publication_us");
        let (generation, frontier) = (root.generation(), root.frontier());
        self.install_committed(
            PhysicalRoot::V2(root),
            hidden_changes,
            key_changes,
            requests,
        )?;
        self.parts.observe_root(generation, frontier);
        drop(space);
        self.failed.store(false, Ordering::Release);
        start_merges(v2);
        Ok(())
    }

    /// Replace the readable root with a merged root of the same frontier.
    /// It never waits: a busy view keeps its root until the next capture.
    pub(super) fn adopt_merged_root(&self) {
        let Physical::V2(v2) = &self.physical else {
            return;
        };
        let latest = v2.current_root();
        let Ok(mut view) = self.view.try_write() else {
            return;
        };
        if let PhysicalRoot::V2(held) = &view.current {
            if held.frontier() == latest.frontier() && held.generation() < latest.generation() {
                self.parts
                    .observe_root(latest.generation(), latest.frontier());
                view.current = PhysicalRoot::V2(latest);
            }
        }
    }
}

/// Commit one batch across the main and CAS homes in the safe order. For a
/// CAS key named twice in the batch, the later operation wins, as in one
/// run home commit.
fn commit_split(v2: &V2Home, operations: Vec<HomeMutation>) -> laststore::Result<()> {
    if !operations.iter().any(|op| op.collection == CAS_COLLECTION) {
        return v2.home.commit(operations).map(drop);
    }
    let (cas, main): (Vec<_>, Vec<_>) = operations
        .into_iter()
        .partition(|op| op.collection == CAS_COLLECTION);
    let mut last = std::collections::BTreeMap::new();
    for op in cas {
        last.insert(op.key.clone(), op);
    }
    let (puts, deletes): (Vec<_>, Vec<_>) = last.into_values().partition(|op| op.body.is_some());
    for (home, batch) in [(&v2.cas, puts), (&v2.home, main), (&v2.cas, deletes)] {
        if !batch.is_empty() {
            home.commit(batch)?;
        }
    }
    Ok(())
}

/// Run merge steps on one background thread until no level is full. The
/// thread holds the home only during a step. Closing the node store stops
/// the thread and waits for its current step, so the home and its owner lock
/// are free when the close returns.
fn start_merges(v2: &V2Home) {
    let waiting = v2
        .retry_after
        .lock()
        .expect("disk run merge retry")
        .is_some_and(|at| std::time::Instant::now() < at);
    if waiting || v2.merging.swap(true, Ordering::AcqRel) {
        return;
    }
    let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
    let merging = Arc::clone(&v2.merging);
    let stop = Arc::clone(&v2.stop);
    let retry_after = Arc::clone(&v2.retry_after);
    let spawned = std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || merge_until_settled(&homes, &merging, &stop, &retry_after));
    match spawned {
        Ok(handle) => {
            let mut merger = v2.merger.lock().expect("disk run merge thread");
            // The earlier thread cleared the mark, so it is at its end.
            if let Some(earlier) = merger.replace(handle) {
                let _ = earlier.join();
            }
        }
        Err(error) => {
            v2.merging.store(false, Ordering::Release);
            tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
        }
    }
}

/// The wait before a new merge thread after a failed merge, so a merge that
/// fails each time (a full disk) does not start a thread on each commit.
const MERGE_RETRY: std::time::Duration = std::time::Duration::from_secs(30);

fn merge_until_settled(
    homes: &[std::sync::Weak<RunHome>],
    merging: &AtomicBool,
    stop: &AtomicBool,
    retry_after: &Mutex<Option<std::time::Instant>>,
) {
    loop {
        'steps: while !stop.load(Ordering::Acquire) {
            let mut merged = false;
            for home in homes {
                let Some(home) = home.upgrade() else {
                    break 'steps;
                };
                match home.merge_step() {
                    Ok(Some(_)) => merged = true,
                    Ok(None) => {}
                    Err(error) => {
                        // A failed merge leaves its inputs in force; a poisoned
                        // home refuses the next commit, which fails the runtime.
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
        // A commit between the last step and the clear saw the mark set and
        // started no thread. Take the mark again when a level became full.
        let full = !stop.load(Ordering::Acquire)
            && homes.iter().any(|home| {
                home.upgrade().is_some_and(|home| {
                    home.status()
                        .levels
                        .iter()
                        .any(|(runs, _, _)| *runs >= laststore::disk_run::home::MERGE_WIDTH)
                })
            });
        if !full || merging.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

impl Drop for V2Home {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let handle = self.merger.get_mut().map(Option::take);
        if let Ok(Some(handle)) = handle {
            let _ = handle.join();
        }
    }
}
