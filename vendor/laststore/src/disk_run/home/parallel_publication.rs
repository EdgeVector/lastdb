//! Overlap independent durability prerequisites, never CURRENT publication.

use super::commit::overflow;
use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::commit_timing as timing;
use crate::disk_run::PreparedRun;
use crate::{Error, Result};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

impl RunHome {
    /// The caller holds commit. Start run durability before waiting for publish,
    /// retaining its overlap with a concurrent compaction's publication.
    pub(super) fn install_prepared(
        &self,
        file: Arc<RunFile>,
        run: PreparedRun,
    ) -> Result<Arc<Manifest>> {
        let _prerequisites = timing::stage("v2_parallel_prerequisites_us");
        let mut run = Some(run);
        let parallel = std::thread::scope(|scope| {
            let worker = match std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, || {
                    let run = run.take().expect("unclaimed private run");
                    let from = Instant::now();
                    let (result, report) = timing::measure(|| run.finish().map(|_| ()));
                    (result, report, from, Instant::now())
                }) {
                Ok(worker) => worker,
                Err(_) => return None,
            };
            let _publish = timing::during("v2_publish_lock_wait_us", || {
                self.publish.lock().unwrap_or_else(|e| e.into_inner())
            });
            let from = Instant::now();
            let staged = timing::during("v2_current_staging_us", || self.stage_new_run(&file));
            let until = Instant::now();
            let result = timing::during("v2_run_join_wait_us", || worker.join());
            let durable = match result {
                Ok((result, report, run_from, run_until)) => {
                    let overlap = until
                        .min(run_until)
                        .saturating_duration_since(from.max(run_from));
                    timing::absorb_joined(report, overlap);
                    result
                }
                Err(_) => Err(Error::Config("disk run: durability worker panicked".into())),
            };
            if let Err(error) = durable {
                if let Ok((_, path)) = staged {
                    // CURRENT still names the old root. This path names only
                    // the successfully staged, private temporary manifest.
                    let _ = fs::remove_file(path);
                }
                return Some(Err(error));
            }
            // Both prerequisites succeeded. Retain the publication lock
            // through CURRENT rename, directory sync, and root installation.
            Some(staged.and_then(|(next, path)| self.publish_staged(next, &path)))
        });
        match parallel {
            Some(result) => result,
            None => {
                // The failed spawn consumed nothing. Preserve the original
                // serial path without adding a foreground availability failure.
                timing::count("v2_parallel_worker_fallbacks", 1);
                run.take().expect("unclaimed private run").finish()?;
                let _publish = timing::during("v2_publish_lock_wait_us", || {
                    self.publish.lock().unwrap_or_else(|e| e.into_inner())
                });
                let (next, path) = self.stage_new_run(&file)?;
                self.publish_staged(next, &path)
            }
        }
    }

    /// Called with publish held. The private manifest can name an unfinished
    /// run, but the caller must join successful durability before its rename.
    fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        self.refuse_if_poisoned()?;
        let prepare = timing::stage("v2_manifest_prepare_us");
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        next.frontier = next.frontier.checked_add(1).ok_or_else(overflow)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        drop(prepare);
        let path = next.stage(&self.root, &self.key)?;
        Ok((next, path))
    }
}
