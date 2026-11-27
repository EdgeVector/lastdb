//! The safe switch: design section 8, steps 3 to 9. A failure before the stop changes
//! nothing. A failure after it rolls back to the old pair. Nothing here kills a
//! process, deletes a home, or skips a check.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::daemon::{self, Drain, Sample, Snapshot};
use super::procs;
use super::service;
use super::stage::build_matches;
use super::state::{self, atomic_symlink, Attempt, Config, Paths};

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

struct Progress {
    snap: Snapshot,
    sample: Option<Sample>,
    /// The `acked` the new daemon must reach when cloud sync is on.
    ack_target: Option<u64>,
    /// What the links named before the switch, to put back on a rollback.
    old_current: Option<PathBuf>,
    old_previous: Option<PathBuf>,
    switched: bool,
    rewrote: bool,
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
    fn socket(&self) -> PathBuf {
        self.paths.socket()
    }

    fn backup_file(&self) -> PathBuf {
        self.paths.dir().join("job-definition.before-adopt")
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
        self.finish(&attempt, step, outcome)
    }

    fn steps(&self) -> (&'static str, Outcome) {
        // Step 3: preflight. Nothing changes if it fails.
        if let Err(e) = service::stop_time_ok(self.label, self.cfg.stop_timeout_secs) {
            return ("preflight", Outcome::refused(e));
        }
        let pid = match service::service_pid(self.label) {
            Ok(pid) => pid,
            Err(e) => return ("preflight", Outcome::refused(e)),
        };
        let snap = match daemon::preflight(&self.socket(), pid) {
            Ok(snap) => snap,
            Err(e) => {
                return (
                    "preflight",
                    Outcome::refused(format!("preflight failed: {e}")),
                )
            }
        };
        note(&format!(
            "preflight passed: pid {}, build {}, spool head {} acked {}",
            snap.pid, snap.build, snap.head, snap.acked
        ));

        // Step 4: drain. After this the daemon refuses writes until it exits or the
        // hold ends, so every refusal below cancels the hold.
        let drained = match daemon::prepare_stop(&self.socket(), &snap) {
            Drain::Done(d) => d,
            Drain::Unsupported(d) => {
                note("this daemon has no prepare-stop; using the sampled status");
                d
            }
            Drain::Failed(e) => {
                daemon::cancel_stop(&self.socket());
                return ("drain", Outcome::refused(e));
            }
        };
        if drained.cloud == "behind" && !self.accept_behind {
            daemon::cancel_stop(&self.socket());
            let missing = drained.head.saturating_sub(drained.acked);
            return (
                "drain",
                Outcome::refused(format!(
                    "{missing} entries are not in the cloud backup yet ({}); nothing changed. Run again later, or pass --accept-backup-behind to accept a smaller cloud recovery point",
                    drained.detail
                )),
            );
        }
        let sample = match &snap.sample {
            None => None,
            Some(s) => {
                match daemon::record_hash(&self.socket(), &s.schema, &s.hash, s.range.as_deref()) {
                    Ok(record_hash) => Some(Sample {
                        record_hash,
                        ..s.clone()
                    }),
                    Err(e) => {
                        daemon::cancel_stop(&self.socket());
                        return (
                            "drain",
                            Outcome::refused(format!("cannot read the sample record: {e}")),
                        );
                    }
                }
            }
        };
        let cloud_on = drained.cloud != "off" && snap.cloud_connected;
        let mut progress = Progress {
            ack_target: (cloud_on && drained.cloud != "behind").then_some(drained.head),
            snap,
            sample,
            old_current: std::fs::read_link(self.paths.current()).ok(),
            old_previous: std::fs::read_link(self.paths.previous()).ok(),
            switched: false,
            rewrote: false,
        };

        // Step 5: strict stop.
        note("stopping the service");
        if let Err(e) = service::stop(self.label) {
            if service::service_pid(self.label).ok().flatten() == Some(progress.snap.pid) {
                daemon::cancel_stop(&self.socket());
                return (
                    "stop",
                    Outcome::refused(format!("{e}; the daemon still runs and nothing changed")),
                );
            }
        }
        match self.wait_stopped(&progress.snap) {
            Ok(()) => {}
            Err(StopFail::StillRunning(m)) => {
                return ("stop", Outcome::needs_human(m));
            }
            Err(StopFail::NoReceipt(m)) => {
                return self.roll_back(&mut progress, &m, "stop");
            }
        }

        // Step 6: switch the links, then start.
        if let Err(e) = self.flip(&mut progress) {
            return self.roll_back(&mut progress, &e, "switch");
        }
        note("starting the service");
        if let Err(e) = service::start(self.label) {
            return self.roll_back(&mut progress, &e, "switch");
        }

        // Step 7: verify.
        let verified = poll(Duration::from_secs(180), Duration::from_secs(2), || {
            self.verify_once(&progress, &self.to, true)
        });
        let new_pid = match verified {
            Ok(pid) => pid,
            Err(e) => {
                return self.roll_back(&mut progress, &format!("verify failed: {e}"), "verify")
            }
        };
        note(&format!("verified: pid {new_pid} runs {}", self.to));

        // Step 8: soak.
        if let Err(e) = self.soak(&progress, new_pid) {
            return self.roll_back(&mut progress, &format!("soak: {e}"), "soak");
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

    fn flip(&self, progress: &mut Progress) -> Result<(), String> {
        let relative = |version: &str| PathBuf::from("programs").join(version);
        if self.paths.program(&self.from).is_dir() {
            atomic_symlink(&self.paths.previous(), &relative(&self.from))?;
        }
        atomic_symlink(&self.paths.current(), &relative(&self.to))?;
        progress.switched = true;
        if self.adopt {
            service::rewrite_definition(
                self.label,
                &self.paths.home,
                self.cfg.stop_timeout_secs,
                &self.backup_file(),
            )?;
            progress.rewrote = true;
        }
        Ok(())
    }

    /// The process is gone, the socket does not answer, and the exact receipt for the
    /// recorded pid and start time is on disk. On a timeout nothing is killed.
    fn wait_stopped(&self, snap: &Snapshot) -> Result<(), StopFail> {
        let socket = self.socket();
        let limit = Duration::from_secs(self.cfg.stop_timeout_secs);
        let gone = poll(limit, Duration::from_secs(1), || {
            if procs::alive(snap.pid) || UnixStream::connect(&socket).is_ok() {
                Err(format!(
                    "pid {} is still running after {} seconds",
                    snap.pid, self.cfg.stop_timeout_secs
                ))
            } else {
                Ok(())
            }
        });
        if let Err(e) = gone {
            return Err(StopFail::StillRunning(format!(
                "{e}. The daemon was not killed. Wait for it to exit, then run 'lastdb upgrade --status'"
            )));
        }
        let receipt = self.paths.home.join(".shutdown_flush_ready");
        let text = std::fs::read_to_string(&receipt).map_err(|e| {
            StopFail::NoReceipt(format!("the stopped node left no flush receipt: {e}"))
        })?;
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|_| StopFail::NoReceipt("the flush receipt is not valid JSON".into()))?;
        let exact = v["version"].as_u64() == Some(1)
            && v["pid"].as_u64() == Some(u64::from(snap.pid))
            && v["start_ts"].as_u64() == Some(snap.start_ts)
            && v["flush_ok"].as_bool() == Some(true);
        if exact {
            Ok(())
        } else {
            Err(StopFail::NoReceipt(
                "the flush receipt does not match the stopped pid and start time".into(),
            ))
        }
    }

    /// One pass of the post-start checks. `new_build` is the version the daemon must
    /// report; `must_differ` requires a pid other than the one that was stopped.
    fn verify_once(
        &self,
        progress: &Progress,
        expect: &str,
        must_differ: bool,
    ) -> Result<u32, String> {
        let socket = self.socket();
        if !daemon::health_ok(&socket) {
            return Err("the daemon does not answer /health".into());
        }
        let (pid, _, build) = daemon::boot_identity(&socket)?;
        if service::service_pid(self.label)? != Some(pid) {
            return Err("the daemon on the socket is not the service's process".into());
        }
        if must_differ && pid == progress.snap.pid {
            return Err("the old process still answers".into());
        }
        if !build_matches(&build, expect) {
            return Err(format!("the daemon reports build {build}, not {expect}"));
        }
        if daemon::writer_failed(&socket)? {
            return Err("the writer has failed".into());
        }
        let id = crate::get_path(
            &socket,
            "/api/system/auto-identity",
            Duration::from_secs(10),
        )
        .and_then(|raw| crate::parse_json_response(&raw, "identity"))?;
        if id["user_hash"].as_str() != Some(progress.snap.user_hash.as_str()) {
            return Err("the owner user hash changed".into());
        }
        if let Some(sample) = &progress.sample {
            let now = daemon::record_hash(
                &socket,
                &sample.schema,
                &sample.hash,
                sample.range.as_deref(),
            )?;
            if now != sample.record_hash {
                return Err("the sample record reads differently than before the stop".into());
            }
        }
        Ok(pid)
    }

    /// Every five seconds: same pid, healthy, writer not failed, memory under the
    /// limit. With cloud sync on, `acked` must reach the pre-stop head.
    fn soak(&self, progress: &Progress, pid: u32) -> Result<(), String> {
        let socket = self.socket();
        let end = Instant::now() + Duration::from_secs(self.cfg.soak_secs);
        let mut acked_ok = progress.ack_target.is_none();
        loop {
            let (now_pid, _, _) = daemon::boot_identity(&socket)?;
            if now_pid != pid {
                return Err(format!("the daemon restarted (pid {pid} became {now_pid})"));
            }
            if !daemon::health_ok(&socket) {
                return Err("health failed".into());
            }
            if daemon::writer_failed(&socket)? {
                return Err("the writer failed".into());
            }
            if let Some(mb) = procs::rss_mb(pid) {
                if mb > self.cfg.rss_limit_mb {
                    return Err(format!(
                        "memory {mb} MB is above the {} MB limit",
                        self.cfg.rss_limit_mb
                    ));
                }
            }
            if let Some(target) = progress.ack_target {
                let v = crate::get_path(&socket, "/api/status", Duration::from_secs(10))
                    .and_then(|raw| crate::parse_json_response(&raw, "status"))?;
                if v["status"]["spool"]["acked"].as_u64().unwrap_or(0) >= target {
                    acked_ok = true;
                }
            }
            if Instant::now() >= end {
                break;
            }
            std::thread::sleep(Duration::from_secs(5));
        }
        if acked_ok {
            Ok(())
        } else {
            Err("the cloud backup did not reach the position it had before the stop".into())
        }
    }

    /// Go back to the old pair: stop whatever runs, put the links and the job file
    /// back, start, and verify the old build against the preflight snapshot.
    fn roll_back(
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

enum StopFail {
    /// The process did not exit within `stop_timeout_secs`.
    StillRunning(String),
    /// It exited, but the receipt is missing or wrong. The old build is started again.
    NoReceipt(String),
}
