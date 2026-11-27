//! The safe switch: design section 8, steps 3 to 9. A failure before the stop changes
//! nothing. A failure after it rolls back to the old pair. Nothing here kills a
//! process, deletes a home, or skips a check.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::config::Config;
use super::daemon::{Sample, Snapshot};
use super::service;
use super::state::{self, Attempt, Paths};
use super::switch_post::{SampleCheck, StopFail};

/// The exit codes of contract section 10, and what to write in `history.jsonl`.
pub(super) struct Outcome {
    pub(super) code: i32,
    pub(super) result: &'static str,
    pub(super) message: String,
}

impl Outcome {
    pub(super) fn upgraded(message: impl Into<String>) -> Self {
        Self {
            code: 0,
            result: "upgraded",
            message: message.into(),
        }
    }

    pub(super) fn refused(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            result: "refused",
            message: message.into(),
        }
    }

    pub(super) fn rolled_back(message: impl Into<String>) -> Self {
        Self {
            code: 3,
            result: "rolled-back",
            message: message.into(),
        }
    }

    pub(super) fn needs_human(message: impl Into<String>) -> Self {
        Self {
            code: 4,
            result: "needs-human",
            message: message.into(),
        }
    }
}

pub(super) fn note(text: &str) {
    eprintln!("lastdb upgrade: {text}");
}

pub(super) struct Switch<'a> {
    pub(super) paths: &'a Paths,
    pub(super) cfg: &'a Config,
    pub(super) label: &'a str,
    /// The program folder that runs now, which becomes `previous`.
    pub(super) from: String,
    /// The staged program folder to run next.
    pub(super) to: String,
    pub(super) trigger: &'static str,
    pub(super) accept_behind: bool,
    /// `service adopt`: the job file is rewritten between the stop and the start.
    pub(super) adopt: bool,
    /// A rollback to `previous`: on success the `from` version goes to `rejected`.
    pub(super) reject_from: bool,
}

pub(super) struct Progress {
    pub(super) snap: Snapshot,
    pub(super) sample: Option<Sample>,
    /// The `acked` the new daemon must reach when cloud sync is on.
    pub(super) ack_target: Option<u64>,
    /// What the links named before the switch, to put back on a rollback.
    pub(super) old_current: Option<PathBuf>,
    pub(super) old_previous: Option<PathBuf>,
    /// A link may have changed: a rollback puts both back as preflight read them.
    pub(super) switched: bool,
    /// The job file backup exists and the file may have changed: a rollback restores it.
    pub(super) rewrote: bool,
    /// The new build was started, so a failure from here on is the new build's.
    pub(super) started_new: bool,
}

pub(super) fn poll<T>(
    limit: Duration,
    every: Duration,
    mut check: impl FnMut() -> Result<T, String>,
) -> Result<T, String> {
    let end = Instant::now() + limit;
    loop {
        match check() {
            Ok(v) => return Ok(v),
            Err(e) if Instant::now() >= end => return Err(e),
            Err(_) => std::thread::sleep(every),
        }
    }
}

impl Switch<'_> {
    pub(super) fn socket(&self) -> PathBuf {
        self.paths.socket()
    }

    pub(super) fn backup_file(&self) -> PathBuf {
        self.paths.dir().join("job-definition.before-adopt")
    }

    /// The bound on one stop: the longer of `stop_timeout_secs` and the job's own stop
    /// window, plus a margin. Every stop and every wait for an exit uses it.
    pub(super) fn stop_wait(&self) -> Duration {
        Duration::from_secs(service::stop_wait_secs(
            self.label,
            self.cfg.stop_timeout_secs,
        ))
    }

    fn finish(&self, attempt: &Attempt, step: &str, outcome: Outcome) -> Outcome {
        if let Err(e) = attempt.finish(self.paths, outcome.result, step, &outcome.message) {
            note(&format!("cannot write history: {e}"));
        }
        outcome
    }

    /// Steps 3 to 9. `to` is staged and verified already.
    pub(super) fn run(&self) -> Outcome {
        let attempt = Attempt::begin(&self.from, &self.to, self.trigger);
        let (step, outcome) = self.steps();
        // Automatic runs stop after one failure per version (design section 12): a run that
        // got past the drain and failed is not tried again until a person clears it. A
        // refusal in preflight or the drain changed nothing and may succeed next hour.
        let automatic = self.trigger != "manual";
        let past_drain = !matches!(step, "preflight" | "drain" | "finish");
        if automatic && past_drain && outcome.code != 0 && self.to != self.from {
            if let Err(e) = state::add_rejected(self.paths, &self.to) {
                note(&format!("cannot record the rejected version: {e}"));
            }
        }
        self.finish(&attempt, step, outcome)
    }

    fn steps(&self) -> (&'static str, Outcome) {
        let mut progress = match self.preflight_and_drain() {
            Ok(progress) => progress,
            Err(done) => return done,
        };

        // Step 5: strict stop. `bootout` may have unloaded the job and sent SIGTERM even
        // when the client failed or ran out of time, so any error here counts as a stop in
        // progress: wait for the process, then start the old job again if it is not
        // switched. Nothing is killed.
        note("stopping the service");
        if let Err(e) = service::stop(self.label, self.stop_wait()) {
            note(&format!("{e}; waiting for the daemon to exit"));
        }
        match self.wait_stopped(&progress.snap) {
            Ok(()) => {}
            Err(StopFail::StillRunning(m)) => return self.restart_old(&progress, &m),
            Err(StopFail::NoReceipt(m)) => {
                return self.roll_back(&progress, &m, "stop");
            }
        }

        // Step 6: switch the links, then start.
        if let Err(e) = self.flip(&mut progress) {
            return self.roll_back(&progress, &e, "switch");
        }
        note("starting the service");
        progress.started_new = true;
        if let Err(e) = service::start(self.label) {
            return self.roll_back(&progress, &e, "switch");
        }

        // Step 7: verify.
        let verified = poll(Duration::from_secs(180), Duration::from_secs(2), || {
            self.verify_once(&progress, &self.to, true, SampleCheck::SameHash)
        });
        let new_pid = match verified {
            Ok(pid) => pid,
            Err(e) => return self.roll_back(&progress, &format!("verify failed: {e}"), "verify"),
        };
        note(&format!("verified: pid {new_pid} runs {}", self.to));

        // Step 8: soak.
        if let Err(e) = self.soak(&progress, new_pid) {
            return self.roll_back(&progress, &format!("soak: {e}"), "soak");
        }

        // Step 9: finish.
        if self.reject_from {
            if let Err(e) = state::add_rejected(self.paths, &self.from) {
                note(&format!("cannot record the rejected version: {e}"));
            }
        }
        super::stage::prune(self.paths);
        let message = format!("{} now runs {} (was {}).", self.label, self.to, self.from);
        if self.reject_from {
            // A rollback the person asked for is a success. An automatic one is
            // reported as a rollback.
            let code = if self.trigger == "manual" { 0 } else { 3 };
            return (
                "finish",
                Outcome {
                    code,
                    result: "rolled-back",
                    message,
                },
            );
        }
        ("finish", Outcome::upgraded(message))
    }
}
