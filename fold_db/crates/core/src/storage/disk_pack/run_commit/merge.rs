//! Merge and adopt equivalent roots independently of the writer.

use super::*;
use laststore::space::SpacePolicy;

/// Run merge steps on one background thread until no home needs a merge.
/// The thread holds a home only during a step, and installs the merged root
/// in `view` after each step. Closing the node store stops the thread and
/// waits for its current step, so the home and its owner lock are free when
/// the close returns.
pub(super) fn start_merges(
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
    let mut homes = vec![Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
    let scope = v2.shared.as_ref().map(|shared| {
        homes.push(Arc::downgrade(&shared.private));
        Arc::clone(&shared.scope)
    });
    let merging = Arc::clone(&v2.merging);
    let stop = Arc::clone(&v2.stop);
    let retry_after = Arc::clone(&v2.retry_after);
    let policy = policy.clone();
    let spawned = std::thread::Builder::new()
        .name("lastdb-run-merge".into())
        .spawn(move || {
            let adopted = Adopted {
                view,
                last_root,
                scope,
            };
            merge_until_settled(&homes, &adopted, &merging, &stop, &retry_after, &policy);
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
    scope: Option<Arc<RwLock<Arc<shared_layout::Scope>>>>,
}

impl Adopted {
    /// Install the current roots of both homes. It waits for the view: the
    /// thread holds no other lock, and a read holds the view only briefly.
    fn install(&self, homes: &[std::sync::Weak<RunHome>]) {
        let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) else {
            return;
        };
        let private = homes.get(2).and_then(std::sync::Weak::upgrade);
        if self.scope.is_some() && private.is_none() {
            return;
        }
        let latest = V2Root {
            main: main.current_root(),
            cas: cas.current_root(),
            private: private.map(|home| home.current_root()),
            scope: self
                .scope
                .as_ref()
                .map(|scope| Arc::clone(&scope.read().expect("shared disk scope"))),
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
    homes: &[std::sync::Weak<RunHome>],
    adopted: &Adopted,
    merging: &AtomicBool,
    stop: &AtomicBool,
    retry_after: &Mutex<Option<std::time::Instant>>,
    policy: &SpacePolicy,
) {
    let cas_policy = SpacePolicy::default();
    loop {
        'steps: while !stop.load(Ordering::Acquire) {
            let mut merged = false;
            for (home_number, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                let (result, report) = laststore::commit_timing::measure(|| {
                    let space = if home_number == 0 {
                        policy
                    } else {
                        &cas_policy
                    };
                    let result = home.merge_step_with_space(space);
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
