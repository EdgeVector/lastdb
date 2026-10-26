//! Join independent private tree barriers before any receipt or root publication.

use super::live_parts::Changes;
use super::locator_forest::{LocatorChanges, LocatorOutput};
use super::tree_edit::TreeEdit;
use super::TreeRoot;
use crate::{commit_timing as timing, Result};

pub(super) type DirectoryOutput = (TreeRoot, u64, Vec<u32>, Changes);
type Outputs = (Vec<LocatorOutput>, DirectoryOutput);

#[cfg(target_os = "linux")]
const PARALLEL_LINUX_FINISH: bool = true;

pub(super) fn finish(locators: LocatorChanges, directory: TreeEdit) -> Result<Outputs> {
    // Apple barriers use thread-local ordering state; keep that path on its caller.
    #[cfg(target_os = "linux")]
    if PARALLEL_LINUX_FINISH {
        return parallel(locators, directory);
    }
    serial(locators, directory)
}

fn serial(locators: LocatorChanges, directory: TreeEdit) -> Result<Outputs> {
    let locators = finish_locators(locators)?;
    Ok((locators, finish_directory(directory)?))
}

fn finish_locators(locators: LocatorChanges) -> Result<Vec<LocatorOutput>> {
    timing::during("locator_finish_us", || locators.finish())
}

fn finish_directory(directory: TreeEdit) -> Result<DirectoryOutput> {
    timing::during("directory_finish_us", || directory.finish_with_changes())
}

#[cfg(target_os = "linux")]
fn parallel(locators: LocatorChanges, directory: TreeEdit) -> Result<Outputs> {
    // One worker per publication, with a fixed stack. The handoff permits a
    // serial fallback if the OS cannot start it; no new write refusal is needed.
    let locators = std::sync::Mutex::new(Some(locators));
    let inherit = timing::capturing();
    std::thread::scope(|scope| {
        let worker = std::thread::Builder::new()
            .name("metadata-finish".into())
            .stack_size(256 * 1024)
            .spawn_scoped(scope, || {
                let locators = locators
                    .lock()
                    .expect("private tree handoff")
                    .take()
                    .expect("one owner");
                timing::measure_child(inherit, || finish_locators(locators))
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(_) => {
                let locators = locators
                    .lock()
                    .expect("private tree handoff")
                    .take()
                    .expect("worker did not start");
                return serial(locators, directory);
            }
        };
        let directory = finish_directory(directory);
        // Always join, including a directory error. Neither result can publish
        // until both full file barriers and their own directory barriers finish.
        let (locators, report) = worker
            .join()
            .map_err(|_| crate::Error::Config("private metadata finish worker panicked".into()))?;
        timing::merge_child(report);
        Ok((locators?, directory?))
    })
}
