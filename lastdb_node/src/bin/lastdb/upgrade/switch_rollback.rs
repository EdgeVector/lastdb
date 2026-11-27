//! The automatic rollback of the switch, and the restart of the old job after a stop that
//! did not finish. Nothing here kills a process, deletes a home or removes a program.

use std::path::PathBuf;
use std::time::Duration;

use super::daemon;
use super::procs;
use super::service;
use super::state::{self, atomic_symlink};
use super::switch::{note, poll, Outcome, Progress, Switch};
use super::switch_post::SampleCheck;

impl Switch<'_> {
    /// Go back to the old pair: stop whatever runs, put the links and the job file back,
    /// start, and verify the old build. When the new build ran, it goes to `rejected`
    /// first, whatever the rollback's outcome, so no automatic run tries it again.
    pub(super) fn roll_back(
        &self,
        progress: &Progress,
        reason: &str,
        step: &'static str,
    ) -> (&'static str, Outcome) {
        note(&format!("rolling back: {reason}"));
        let rejected = progress.started_new && self.to != self.from;
        if rejected {
            if let Err(e) = state::add_rejected(self.paths, &self.to) {
                note(&format!("cannot record the rejected version: {e}"));
            }
        }
        let fail = |what: String| {
            let tail = if rejected {
                format!(" {} is rejected.", self.to)
            } else {
                String::new()
            };
            (
                step,
                Outcome::needs_human(format!(
                    "{reason}. {what}. Nothing was killed or deleted; both program folders are on disk.{tail}"
                )),
            )
        };
        if let Err(e) = self.stop_new(progress) {
            return fail(e);
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
                return fail(format!("Rollback could not restore the links: {e}"));
            }
        }
        if progress.rewrote {
            if let Err(e) = service::restore_definition(self.label, &self.backup_file()) {
                return fail(format!("Rollback could not restore the job file: {e}"));
            }
        }
        if let Err(e) = self.start_old() {
            return fail(format!("Rollback could not start the old build: {e}"));
        }
        // The new build may have taken writes, so the sample key must read, not match.
        let old = poll(Duration::from_secs(180), Duration::from_secs(2), || {
            self.verify_once(progress, &self.from, false, SampleCheck::Readable)
        });
        if let Err(e) = old {
            return fail(format!("The old build did not verify after rollback: {e}"));
        }
        let message = if rejected {
            format!(
                "rolled back to {}: {reason}. {} is rejected and automatic runs skip it.",
                self.from, self.to
            )
        } else {
            format!("the old build {} runs again: {reason}.", self.from)
        };
        (step, Outcome::rolled_back(message))
    }

    /// Step 5 did not finish: the old process outlived the stop bound, or something still
    /// answers on the socket. The links and the job file are untouched, so the job still
    /// names the old build. Start it again and fail; never kill. Needs a human only when
    /// the old job cannot be started again.
    pub(super) fn restart_old(&self, progress: &Progress, reason: &str) -> (&'static str, Outcome) {
        let (pid, label, from) = (progress.snap.pid, self.label, &self.from);
        let by_hand = || service::start_command(label);
        if procs::alive(pid) {
            let own = service::is_loaded(label).unwrap_or(false)
                && service::service_pid(label).ok().flatten() == Some(pid);
            if own {
                // The bound covers the job's own stop window, so a stop under way would
                // have ended by now: this stop never took effect.
                daemon::cancel_stop(&self.socket());
                return (
                    "stop",
                    Outcome::refused(format!(
                        "{reason}. The stop did not take effect: the daemon still runs under {label} and takes writes again; nothing changed"
                    )),
                );
            }
            return match service::start_queued(label) {
                Ok(()) => (
                    "stop",
                    Outcome::rolled_back(format!(
                        "{reason}. A start of the old build {from} is queued and runs once pid {pid} exits; nothing switched"
                    )),
                ),
                Err(e) => (
                    "stop",
                    Outcome::needs_human(format!(
                        "{reason}. The job is stopping and nothing will start it again ({e}). When pid {pid} has exited, run '{}' to start {from} again; nothing switched",
                        by_hand()
                    )),
                ),
            };
        }
        note(&format!("{reason}; starting the old build {from} again"));
        if let Err(e) = self.start_old() {
            return (
                "stop",
                Outcome::needs_human(format!(
                    "{reason}. The old build {from} could not be started again: {e}. Run '{}'; nothing switched",
                    by_hand()
                )),
            );
        }
        let old = poll(Duration::from_secs(180), Duration::from_secs(2), || {
            self.verify_once(progress, from, false, SampleCheck::Readable)
        });
        match old {
            Ok(_) => (
                "stop",
                Outcome::rolled_back(format!(
                    "{reason}. The old daemon exited late; the old build {from} runs again and nothing switched"
                )),
            ),
            Err(e) => (
                "stop",
                Outcome::needs_human(format!(
                    "{reason}. The old build {from} was started again but did not verify: {e}; nothing switched"
                )),
            ),
        }
    }

    /// Stop whatever the job runs now, and wait within the stop bound for it to exit.
    fn stop_new(&self, progress: &Progress) -> Result<(), String> {
        let running = service::service_pid(self.label).ok().flatten();
        if running.is_some() || service::is_loaded(self.label).unwrap_or(false) {
            if let Err(e) = service::stop(self.label, self.stop_wait()) {
                note(&format!(
                    "stop during rollback: {e}; waiting for the daemon to exit"
                ));
            }
        }
        let Some(pid) = running.filter(|pid| *pid != progress.snap.pid) else {
            return Ok(());
        };
        let limit = self.stop_wait();
        poll(limit, Duration::from_secs(1), || {
            if procs::alive(pid) {
                Err(format!("pid {pid} still runs"))
            } else {
                Ok(())
            }
        })
        .map_err(|e| {
            format!(
                "Rollback could not stop the new daemon: {e} after {} seconds",
                limit.as_secs()
            )
        })
    }

    /// Start the job. A start that fails while the job already runs a live process counts
    /// as started; the verify that follows checks what runs.
    fn start_old(&self) -> Result<(), String> {
        let Err(e) = service::start(self.label) else {
            return Ok(());
        };
        let runs = service::service_pid(self.label)
            .ok()
            .flatten()
            .is_some_and(procs::alive);
        if runs {
            note(&format!("{e}; the job runs already"));
            Ok(())
        } else {
            Err(e)
        }
    }
}
