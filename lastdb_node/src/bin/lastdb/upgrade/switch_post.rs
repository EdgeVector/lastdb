//! Steps 5 to 8 of the switch: the strict stop, the links, the post-start checks and
//! the soak.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::checks::build_matches;
use super::daemon::{self, Snapshot};
use super::procs;
use super::service;
use super::state::atomic_symlink;
use super::switch::{poll, Progress, Switch};

/// Why the stop did not finish cleanly.
pub(super) enum StopFail {
    /// The process did not exit within the stop bound. The old job is started again.
    StillRunning(String),
    /// It exited, but the receipt is missing or wrong. The old build is started again.
    NoReceipt(String),
}

/// What the post-start check asks of the sample record.
#[derive(Clone, Copy)]
pub(super) enum SampleCheck {
    /// The record reads exactly as it did after the drain: the new build after the switch.
    SameHash,
    /// The key reads at all: the old build after a rollback, since the new build may have
    /// taken writes that changed the record.
    Readable,
}

impl Switch<'_> {
    /// Point `previous` at the old pair and `current` at the new one; on adopt, rewrite the
    /// job file. Each flag is set before its change, so a failure part way is undone by
    /// the rollback: both links go back as preflight read them, and the job file comes
    /// back from its backup.
    pub(super) fn flip(&self, progress: &mut Progress) -> Result<(), String> {
        let relative = |version: &str| PathBuf::from("programs").join(version);
        progress.switched = true;
        if self.paths.program(&self.from).is_dir() {
            atomic_symlink(&self.paths.previous(), &relative(&self.from))?;
        }
        atomic_symlink(&self.paths.current(), &relative(&self.to))?;
        if self.adopt {
            service::backup_definition(self.label, &self.backup_file())?;
            progress.rewrote = true;
            service::rewrite_definition(
                self.label,
                &self.paths.home,
                self.cfg.stop_timeout_secs,
                &self.backup_file(),
            )?;
        }
        Ok(())
    }

    /// The process is gone, the socket does not answer, and the exact receipt for the
    /// recorded pid and start time is on disk. The wait is bounded by [`Switch::stop_wait`];
    /// on a timeout nothing is killed.
    pub(super) fn wait_stopped(&self, snap: &Snapshot) -> Result<(), StopFail> {
        let socket = self.socket();
        let limit = self.stop_wait();
        let gone = poll(limit, Duration::from_secs(1), || {
            if procs::alive(snap.pid) || UnixStream::connect(&socket).is_ok() {
                Err(format!(
                    "pid {} is still running after {} seconds",
                    snap.pid,
                    limit.as_secs()
                ))
            } else {
                Ok(())
            }
        });
        if let Err(e) = gone {
            return Err(StopFail::StillRunning(format!("{e}; it was not killed")));
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

    /// One pass of the post-start checks. `expect` is the version the daemon must report;
    /// `must_differ` requires a pid other than the one that was stopped; `sample_check` says
    /// what the sample record must show.
    pub(super) fn verify_once(
        &self,
        progress: &Progress,
        expect: &str,
        must_differ: bool,
        sample_check: SampleCheck,
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
            if matches!(sample_check, SampleCheck::SameHash) && now != sample.record_hash {
                return Err("the sample record reads differently than before the stop".into());
            }
        }
        Ok(pid)
    }

    /// Every five seconds: same pid, healthy, writer not failed, memory under the
    /// limit. With cloud sync on, `acked` must reach the pre-stop head.
    pub(super) fn soak(&self, progress: &Progress, pid: u32) -> Result<(), String> {
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
}
