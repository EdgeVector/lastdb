//! Merge and adopt equivalent roots independently of the writer.

use super::physical::{V2Home, V2Root};
use super::run_commit::adopt;
use super::*;
use laststore::disk_run::home::RunHome;
use laststore::space::SpacePolicy;

pub(in crate::storage::disk_pack) fn start_merges(
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

const MERGE_RETRY: std::time::Duration = std::time::Duration::from_secs(30);
const HELD_SHARE: u64 = 16;
const MIN_HELD_BYTES: u64 = 1 << 20;
const HELD_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const HELD_POLL: std::time::Duration = std::time::Duration::from_millis(2);

struct Adopted {
    view: Arc<RwLock<view::NativeReadView>>,
    last_root: Arc<view::LastRoot>,
    scope: Option<Arc<RwLock<Arc<shared_layout::Scope>>>>,
}

impl Adopted {
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
                let space = if home_number == 0 {
                    policy
                } else {
                    &cas_policy
                };
                let result = home.merge_step_with_space(space);
                if matches!(&result, Ok(true)) {
                    drop(home);
                    adopted.install(homes);
                    wait_for_held_runs(weak, stop);
                }
                match result {
                    Ok(true) => {
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
                .any(|home| home.upgrade().is_some_and(|home| home.needs_merge()));
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
