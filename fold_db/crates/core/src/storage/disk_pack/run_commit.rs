//! Commit and background merge on a v2 run home.
//!
//! A commit writes one level-0 run and installs its root as the readable
//! state. A merge publishes a root with the same frontier, so it holds the
//! same data; reads adopt it the next time they capture a root, and the
//! merged input files go when the last read of the older root ends.

use super::physical::{Physical, PhysicalRoot, V2Home};
use super::*;
use laststore::disk_run::home::RunHome;

impl PackRuntime {
    /// The caller holds the write gate. An error fails the runtime, as a
    /// failed v1 publication does: accepted writes did not become durable.
    pub(super) fn commit_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        hidden_changes: &[(String, String)],
        key_changes: &[(String, String, bool)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let commit = match v2.home.commit(operations) {
            Ok(commit) => commit,
            Err(error) => {
                self.failed.store(true, Ordering::Release);
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
                view.current = PhysicalRoot::V2(latest);
            }
        }
    }
}

/// Run merge steps on one background thread until no level is full. The
/// thread holds the home only during a step, so a closed node store releases
/// the home and its owner lock when the current step ends.
fn start_merges(v2: &V2Home) {
    if v2.merging.swap(true, Ordering::AcqRel) {
        return;
    }
    let home = Arc::downgrade(&v2.home);
    let merging = Arc::clone(&v2.merging);
    let spawned = std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || {
            merge_until_settled(&home);
            merging.store(false, Ordering::Release);
        });
    if let Err(error) = spawned {
        v2.merging.store(false, Ordering::Release);
        tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
    }
}

fn merge_until_settled(home: &std::sync::Weak<RunHome>) {
    while let Some(home) = home.upgrade() {
        match home.merge_step() {
            Ok(Some(_)) => {}
            Ok(None) => return,
            Err(error) => {
                // A failed merge leaves its inputs in force; a poisoned home
                // refuses the next commit, which fails the runtime.
                tracing::warn!(%error, "disk run merge failed");
                return;
            }
        }
    }
}
