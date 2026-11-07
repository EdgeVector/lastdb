//! Commit and background merge on a v2 run home.
//!
//! A commit writes one level-0 run and installs its root as the readable
//! state. A merge publishes a root with the same frontier, so it holds the
//! same data. The merge thread installs it after each step, and reads also
//! adopt it when they capture a root. The merged input files go when the
//! last read of the older root ends, so the disk does not keep them until
//! the next read.
//!
//! A read holds its root for its whole call, and a write request holds one
//! while it prepares, so the thread waits before its next step while such
//! reads keep more than 1/16 of a home in replaced runs. Without the wait a
//! base merge rewrote much of the base during one request, and the disk v2
//! node proof peaked at 2.03x to 2.06x of its record bytes on macOS.
//!
//! The CAS files have their own run home. A commit that holds CAS changes
//! writes up to three runs in this order: new CAS files, then the other
//! records, then CAS deletes. A crash between them can leave a CAS file that
//! nothing references, but never a reference to a CAS file that is not there.

use super::physical::{PhysicalRoot, V2Home, V2Root, CAS_COLLECTION};
use super::*;
use laststore::disk_run::home::RunHome;

mod merge;
use merge::start_merges;

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
        let intent_keys = cas_delete::key_bytes(&operations)?
            + previous
                .map(write::PreparedCommit::cas_delete_key_bytes)
                .transpose()?
                .unwrap_or(0);
        let intent_bytes = cas_delete::space(intent_keys)?;
        let reserved_bytes = if let Some(shared) = &v2.shared {
            let current = shared.scope.read().expect("shared disk scope");
            shared_layout::Scope::with_writes(
                &current,
                previous
                    .into_iter()
                    .flat_map(write::PreparedCommit::operations)
                    .chain(&operations),
            )?;
            // The encrypted write intent and the final native files coexist.
            batch_bytes.saturating_mul(3)
        } else {
            batch_bytes.saturating_add(intent_bytes)
        };
        // A run file holds about the batch bytes; a merge reuses the space
        // of the runs it replaces once their last read ends.
        let space = match previous {
            Some(previous) => previous
                .space()
                .reserve_extension(&self.space_policy, reserved_bytes),
            None => {
                laststore::space::reserve_space(v2.home.path(), &self.space_policy, reserved_bytes)
            }
        }
        .map_err(map)?;
        Ok(write::PreparedCommit::for_run(
            operations,
            batch_bytes,
            space,
        ))
    }

    /// The caller holds the write gate. An error fails the runtime: accepted
    /// writes did not become durable.
    pub(super) fn commit_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        space: SpaceReservation,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let result = (|| {
            cas_delete::require_clear(v2)?;
            if let Some(shared) = &v2.shared {
                shared_layout::journal::write(v2, shared, operations)
            } else {
                // The log holds main-home runs only. A CAS-only batch (blob
                // upload markers, releases) makes no log entry.
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
            }
        })();
        if let Err(error) = result {
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        // The current roots hold this commit; a merge root holds the same data.
        let root = v2.current_root();
        let publication_timing = laststore::commit_timing::stage("memory_publication_us");
        let (generation, frontier) = (root.generation(), root.frontier());
        self.install_committed(root, hidden_changes, requests)?;
        self.last_root.observe(generation, frontier);
        drop(publication_timing);
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

    /// Start the merge thread when a home needs a merge, as after a crash
    /// during a base merge. Otherwise each commit starts it.
    pub(super) fn resume_merges(&self) {
        let v2 = &self.physical;
        if v2.home.needs_merge()
            || v2.cas.needs_merge()
            || v2
                .shared
                .as_ref()
                .is_some_and(|shared| shared.private.needs_merge())
        {
            start_merges(
                v2,
                Arc::clone(&self.view),
                Arc::clone(&self.last_root),
                &self.space_policy,
            );
        }
    }

    /// Replace the readable root with a merged root of the same frontier.
    /// It never waits: a busy view keeps its root until the next capture.
    pub(in crate::storage) fn adopt_merged_root(&self) {
        let replaced = self.view.try_write().ok().and_then(|mut view| {
            let latest = self.physical.current_root();
            adopt(&mut view, latest, &self.last_root)
        });
        drop(replaced);
    }
}

/// Install `latest` when it holds the same commits as the view's root and is
/// newer: a merge root. Returns the replaced root. Drop it after the view
/// lock: its last drop deletes retired run files.
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

/// Commit one batch across the main and CAS homes in the safe order. For a
/// CAS key named twice in the batch, the later operation wins, as in one
/// run home commit.
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
    let (puts, deletes): (Vec<_>, Vec<_>) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    // CAS runs are not logged: record blobs upload by path, not as log files.
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        let committed = v2.home.commit(main).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
    }
    // Main publication is durable. A later failure is an uncertain commit,
    // never a logical refusal. Fail the runtime and retain the authenticated
    // intent; reopen finishes it before any reader or writer can reuse a key.
    if !deletes.is_empty() {
        v2.cas.commit(deletes).map_err(map)?;
        v2.home.clear_cas_delete_checkpoint().map_err(map)?;
    }
    Ok(())
}
