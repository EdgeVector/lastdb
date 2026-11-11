//! Overlap independent durability prerequisites, never CURRENT publication.

use super::commit::overflow;
use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::disk_run::PreparedRun;
use crate::{Error, Result};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

impl RunHome {
    /// The caller holds commit. Start run durability before waiting for publish,
    /// retaining its overlap with a concurrent compaction's publication.
    pub(super) fn install_prepared(
        &self,
        file: Arc<RunFile>,
        run: PreparedRun,
    ) -> Result<Arc<Manifest>> {
        let mut run = Some(run);
        let parallel = std::thread::scope(|scope| {
            let worker = match std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, || {
                    run.take()
                        .expect("unclaimed private run")
                        .finish()
                        .map(|_| ())
                }) {
                Ok(worker) => worker,
                Err(_) => return None,
            };
            let _publish = match self.publish_lock() {
                Ok(guard) => guard,
                Err(error) => {
                    // Drop joins this worker. A panic there would hide the lock error.
                    let _ = worker.join();
                    return Some(Err(error));
                }
            };
            let staged = self.stage_new_run(&file);
            let durable = match worker.join() {
                Ok(result) => result,
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
                run.take().expect("unclaimed private run").finish()?;
                let _publish = self.publish_lock()?;
                let (next, path) = self.stage_new_run(&file)?;
                self.publish_staged(next, &path)
            }
        }
    }

    /// Called with publish held. The private manifest can name an unfinished
    /// run, but the caller must join successful durability before its rename.
    fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        next.frontier = next.frontier.checked_add(1).ok_or_else(overflow)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root, &self.key)?;
        Ok((next, path))
    }
}
