//! Overlap independent durability prerequisites, never CURRENT publication.

use super::manifest::Manifest;
use super::RunHome;
use crate::commit_timing as timing;
use crate::disk_run::PreparedRun;
use crate::{Error, Result};
use std::fs;
use std::sync::Arc;
use std::time::Instant;

impl RunHome {
    /// The caller holds commit and publish. A staged CURRENT is private: its
    /// rename cannot start until the run file AND its directory are durable.
    pub(super) fn install_prepared(
        &self,
        next: Manifest,
        run: PreparedRun,
    ) -> Result<Arc<Manifest>> {
        let staged = {
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
                let from = Instant::now();
                let staged = timing::during("v2_current_staging_us", || {
                    next.stage(&self.root, &self.key)
                });
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
                    if let Ok(path) = staged {
                        // CURRENT still names the old root. Only this owned
                        // temporary manifest may be removed on this branch.
                        let _ = fs::remove_file(path);
                    }
                    return Some(Err(error));
                }
                Some(staged)
            });
            match parallel {
                Some(staged) => staged?,
                None => {
                    // Thread creation is optional. The scope released its
                    // borrow, and no worker consumed or changed this run.
                    timing::count("v2_parallel_worker_fallbacks", 1);
                    run.take().expect("unclaimed private run").finish()?;
                    next.stage(&self.root, &self.key)?
                }
            }
        };
        // Both durable prerequisites succeeded. Rename and root-directory
        // sync retain the existing poison-on-uncertainty publication boundary.
        self.publish_staged(next, &staged)
    }
}
