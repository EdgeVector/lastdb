//! Commit and background merge on a v2 run home.
//!
//! A commit writes one level-0 run and installs its root as the readable
//! state. A merge publishes a root with the same frontier, so it holds the
//! same data; reads adopt it the next time they capture a root, and the
//! merged input files go when the last read of the older root ends.

use super::physical::{Physical, PhysicalRoot, V2Home};
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
        let commit = match v2.home.commit(operations) {
            Ok(commit) => commit,
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *self.failed_write.lock().expect("native failed write owner") = Some(space);
                return Err(map(error));
            }
        };
        let _timing = laststore::commit_timing::stage("memory_publication_us");
        self.install_committed(
            PhysicalRoot::V2(commit.root),
            hidden_changes,
            key_changes,
            requests,
        )?;
        self.parts.observe_root(commit.generation, commit.frontier);
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
        let latest = v2.home.current_root();
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

/// Run merge steps on one background thread until no level is full. The
/// thread holds the home only during a step. Closing the node store stops
/// the thread and waits for its current step, so the home and its owner lock
/// are free when the close returns.
fn start_merges(v2: &V2Home) {
    if v2.merging.swap(true, Ordering::AcqRel) {
        return;
    }
    let home = Arc::downgrade(&v2.home);
    let merging = Arc::clone(&v2.merging);
    let stop = Arc::clone(&v2.stop);
    let spawned = std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || merge_until_settled(&home, &merging, &stop));
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

fn merge_until_settled(home: &std::sync::Weak<RunHome>, merging: &AtomicBool, stop: &AtomicBool) {
    loop {
        while !stop.load(Ordering::Acquire) {
            let Some(home) = home.upgrade() else { break };
            match home.merge_step() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(error) => {
                    // A failed merge leaves its inputs in force; a poisoned
                    // home refuses the next commit, which fails the runtime.
                    tracing::warn!(%error, "disk run merge failed");
                    merging.store(false, Ordering::Release);
                    return;
                }
            }
        }
        merging.store(false, Ordering::Release);
        // A commit between the last step and the clear saw the mark set and
        // started no thread. Take the mark again when a level became full.
        let full = !stop.load(Ordering::Acquire)
            && home.upgrade().is_some_and(|home| {
                home.status()
                    .levels
                    .iter()
                    .any(|(runs, _, _)| *runs >= laststore::disk_run::home::MERGE_WIDTH)
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
