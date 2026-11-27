//! The automatic rollback of the switch: stop whatever runs, put the links and the job
//! file back, start the old build and verify it against the preflight snapshot.

use std::path::PathBuf;
use std::time::Duration;

use super::procs;
use super::service;
use super::state::{self, atomic_symlink};
use super::switch::{note, poll, Outcome, Progress, Switch};

impl Switch<'_> {
    /// Go back to the old pair: stop whatever runs, put the links and the job file
    /// back, start, and verify the old build against the preflight snapshot.
    pub(super) fn roll_back(
        &self,
        progress: &mut Progress,
        reason: &str,
        step: &'static str,
    ) -> (&'static str, Outcome) {
        note(&format!("rolling back: {reason}"));
        let running = service::service_pid(self.label).ok().flatten();
        if service::is_loaded(self.label).unwrap_or(false) {
            if let Err(e) = service::stop(self.label) {
                note(&format!("stop during rollback: {e}"));
            }
        }
        if let Some(pid) = running.filter(|pid| *pid != progress.snap.pid) {
            let limit = Duration::from_secs(self.cfg.stop_timeout_secs);
            let gone = poll(limit, Duration::from_secs(1), || {
                if procs::alive(pid) {
                    Err(format!("pid {pid} is still running"))
                } else {
                    Ok(())
                }
            });
            if let Err(e) = gone {
                return (
                    step,
                    Outcome::needs_human(format!(
                        "{reason}. Rollback could not stop the new daemon: {e}. Nothing was killed or deleted; both program folders are on disk"
                    )),
                );
            }
        }
        if progress.switched {
            let restore = |link: PathBuf, old: &Option<PathBuf>| match old {
                Some(target) => atomic_symlink(&link, target),
                None => match std::fs::remove_file(&link) {
                    Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.to_string()),
                    _ => Ok(()),
                },
            };
            let result = restore(self.paths.current(), &progress.old_current)
                .and_then(|()| restore(self.paths.previous(), &progress.old_previous));
            if let Err(e) = result {
                return (
                    step,
                    Outcome::needs_human(format!(
                        "{reason}. Rollback could not restore the links: {e}"
                    )),
                );
            }
        }
        if progress.rewrote {
            if let Err(e) = service::restore_definition(self.label, &self.backup_file()) {
                return (
                    step,
                    Outcome::needs_human(format!(
                        "{reason}. Rollback could not restore the job file: {e}"
                    )),
                );
            }
        }
        if let Err(e) = service::start(self.label) {
            return (
                step,
                Outcome::needs_human(format!(
                    "{reason}. Rollback could not start the old build: {e}"
                )),
            );
        }
        let old = poll(Duration::from_secs(180), Duration::from_secs(2), || {
            self.verify_once(progress, &self.from, false)
        });
        if let Err(e) = old {
            return (
                step,
                Outcome::needs_human(format!(
                    "{reason}. The old build did not verify after rollback: {e}"
                )),
            );
        }
        if !progress.switched || self.adopt || self.to == self.from {
            return (
                step,
                Outcome::rolled_back(format!("the old build {} runs again: {reason}.", self.from)),
            );
        }
        if let Err(e) = state::add_rejected(self.paths, &self.to) {
            note(&format!("cannot record the rejected version: {e}"));
        }
        (
            step,
            Outcome::rolled_back(format!(
                "rolled back to {}: {reason}. {} is rejected and automatic runs skip it.",
                self.from, self.to
            )),
        )
    }
}
