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
//! node proof peaked at 2.03x to 2.06x of its atom bytes on macOS.
//!
//! The CAS files have their own run home. A commit that holds CAS changes
//! writes up to three runs in this order: new CAS files, then the other
//! records, then CAS deletes. A crash between them can leave a CAS file that
//! nothing references, but never a reference to a CAS file that is not there.

use super::physical::{PhysicalRoot, V2Home, V2Root, CAS_COLLECTION};
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
        let intent_keys = cas_delete::key_bytes(&operations)?
            + previous
                .map(write::PreparedCommit::cas_delete_key_bytes)
                .transpose()?
                .unwrap_or(0);
        let intent_bytes = cas_delete::space(intent_keys)?;
        let reserved_bytes = batch_bytes.saturating_add(intent_bytes);
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
        (hidden_changes, key_changes): Changes<'_>,
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let result = (|| {
            cas_delete::require_clear(v2)?;
            let mut journal = self.mutation_log.begin(v2);
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
        // The current roots hold this commit; a merge root holds the same data.
        let root = v2.current_root();
        let publication_timing = laststore::commit_timing::stage("memory_publication_us");
        let (generation, frontier) = (root.generation(), root.frontier());
        self.install_committed(root, hidden_changes, key_changes, requests)?;
        self.last_root.observe(generation, frontier);
        drop(publication_timing);
        drop(space);
        self.failed.store(false, Ordering::Release);
        start_merges(v2, Arc::clone(&self.view), Arc::clone(&self.last_root));
        Ok(())
    }

    /// Start the merge thread when a home needs a merge, as after a crash
    /// during a base merge. Otherwise each commit starts it.
    pub(super) fn resume_merges(&self) {
        let v2 = &self.physical;
        if v2.home.needs_merge() || v2.cas.needs_merge() {
            start_merges(v2, Arc::clone(&self.view), Arc::clone(&self.last_root));
        }
    }

    /// Replace the readable root with a merged root of the same frontier.
    /// It never waits: a busy view keeps its root until the next capture.
    pub(super) fn adopt_merged_root(&self) {
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
    for (home, batch, role) in [
        (&v2.cas, puts, mutation_log::CommitRole::CasAdd),
        (&v2.home, main, mutation_log::CommitRole::Main),
    ] {
        if !batch.is_empty() {
            let committed = home.commit(batch).map_err(map)?;
            if let Some(log) = journal.as_mut() {
                log.retain(role, &committed);
            }
        }
    }
    // Main publication is durable. A later failure is an uncertain commit,
    // never a logical refusal. Fail the runtime and retain the authenticated
    // intent; reopen finishes it before any reader or writer can reuse a key.
    if !deletes.is_empty() {
        let committed = v2.cas.commit(deletes).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::CasDelete, &committed);
        }
        v2.home.clear_cas_delete_checkpoint().map_err(map)?;
    }
    Ok(())
}

/// Run merge steps on one background thread until no home needs a merge.
/// The thread holds a home only during a step, and installs the merged root
/// in `view` after each step. Closing the node store stops the thread and
/// waits for its current step, so the home and its owner lock are free when
/// the close returns.
fn start_merges(
    v2: &V2Home,
    view: Arc<RwLock<view::NativeReadView>>,
    last_root: Arc<view::LastRoot>,
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
    let merging = Arc::clone(&v2.merging);
    let stop = Arc::clone(&v2.stop);
    let retry_after = Arc::clone(&v2.retry_after);
    let spawned = std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || {
            let adopted = Adopted { view, last_root };
            merge_until_settled(&homes, &adopted, &merging, &stop, &retry_after);
        });
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

/// The thread waits before its next step while the replaced runs that reads
/// still hold are more than this share of the home's run files.
const HELD_SHARE: u64 = 16;
/// The smallest wait budget, so a small home does not wait at each step.
const MIN_HELD_BYTES: u64 = 1 << 20;
/// The longest wait for reads: a longer read keeps its runs, and the merge
/// goes on.
const HELD_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const HELD_POLL: std::time::Duration = std::time::Duration::from_millis(2);

/// Where the merge thread installs merged roots.
struct Adopted {
    view: Arc<RwLock<view::NativeReadView>>,
    last_root: Arc<view::LastRoot>,
}

impl Adopted {
    /// Install the current roots of both homes. It waits for the view: the
    /// thread holds no other lock, and a read holds the view only briefly.
    fn install(&self, homes: &[std::sync::Weak<RunHome>]) {
        let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) else {
            return;
        };
        let latest = V2Root {
            main: main.current_root(),
            cas: cas.current_root(),
        };
        let replaced = self
            .view
            .write()
            .ok()
            .and_then(|mut view| adopt(&mut view, latest, &self.last_root));
        drop(replaced);
    }
}

fn merge_until_settled(
    homes: &[std::sync::Weak<RunHome>; 2],
    adopted: &Adopted,
    merging: &AtomicBool,
    stop: &AtomicBool,
    retry_after: &Mutex<Option<std::time::Instant>>,
) {
    loop {
        'steps: while !stop.load(Ordering::Acquire) {
            let mut merged = false;
            for (home_number, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                let (result, report) = laststore::commit_timing::measure(|| {
                    let result = home.merge_step();
                    if matches!(&result, Ok(Some(_))) {
                        drop(home);
                        laststore::commit_timing::during("v2_merge_root_adoption_us", || {
                            adopted.install(homes);
                        });
                        // Adoption releases the view's previous root before
                        // the existing pacing wait for readers' older roots.
                        laststore::commit_timing::during("v2_merge_held_read_wait_us", || {
                            wait_for_held_runs(weak, stop);
                        });
                    }
                    result
                });
                super::timing::merge(report, &result, home_number);
                match result {
                    Ok(Some(_)) => {
                        merged = true;
                    }
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
        // started no thread. Take the mark again when a home needs a merge.
        let needed = !stop.load(Ordering::Acquire)
            && homes
                .iter()
                .any(|home| home.upgrade().is_some_and(|home| home.needs_merge()));
        if !needed || merging.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

/// Wait while reads keep more than 1/[`HELD_SHARE`] of `home` in runs that
/// merges replaced, for at most [`HELD_WAIT`]. The thread holds the home
/// only for each check, and a close ends the wait.
fn wait_for_held_runs(home: &std::sync::Weak<RunHome>, stop: &AtomicBool) {
    let until = std::time::Instant::now() + HELD_WAIT;
    let mut limit = None;
    while let Some(home) = home.upgrade() {
        let budget = *limit
            .get_or_insert_with(|| (home.status().file_bytes() / HELD_SHARE).max(MIN_HELD_BYTES));
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
        let handle = self.merger.get_mut().map(Option::take);
        if let Ok(Some(handle)) = handle {
            let _ = handle.join();
        }
    }
}
