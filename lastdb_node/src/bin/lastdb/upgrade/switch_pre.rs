//! Steps 3 and 4 of the switch: preflight and drain.

use super::daemon::{self, Drain, Sample, Snapshot};
use super::service;
use super::switch::{note, Outcome, Progress, Switch};

impl Switch<'_> {
    /// Steps 3 and 4: preflight, then the drain. A failure here changes nothing, and
    /// every refusal after the drain cancels the hold so the daemon takes writes again.
    pub(super) fn preflight_and_drain(&self) -> Result<Progress, (&'static str, Outcome)> {
        // Step 3: preflight. Nothing changes if it fails.
        if let Err(e) = service::stop_time_ok(self.label, self.cfg.stop_timeout_secs) {
            return Err(("preflight", Outcome::refused(e)));
        }
        let pid =
            service::service_pid(self.label).map_err(|e| ("preflight", Outcome::refused(e)))?;
        let snap = daemon::preflight(&self.socket(), pid).map_err(|e| {
            (
                "preflight",
                Outcome::refused(format!("preflight failed: {e}")),
            )
        })?;
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
                return Err(("drain", Outcome::refused(e)));
            }
        };
        if drained.cloud == "behind" && !self.accept_behind {
            daemon::cancel_stop(&self.socket());
            let missing = drained.head.saturating_sub(drained.acked);
            return Err((
                "drain",
                Outcome::refused(format!(
                    "{missing} entries are not in the cloud backup yet ({}); nothing changed. Run again later, or pass --accept-backup-behind to accept a smaller cloud recovery point",
                    drained.detail
                )),
            ));
        }
        let sample = match self.reread_sample(&snap) {
            Ok(sample) => sample,
            Err(e) => {
                daemon::cancel_stop(&self.socket());
                return Err((
                    "drain",
                    Outcome::refused(format!("cannot read the sample record: {e}")),
                ));
            }
        };
        let cloud_on = drained.cloud != "off" && snap.cloud_connected;
        Ok(Progress {
            ack_target: (cloud_on && drained.cloud != "behind").then_some(drained.head),
            snap,
            sample,
            old_current: std::fs::read_link(self.paths.current()).ok(),
            old_previous: std::fs::read_link(self.paths.previous()).ok(),
            switched: false,
            rewrote: false,
        })
    }

    /// The sample record is read again after the drain, when writes are refused, so the
    /// hash that the new daemon must match is the hash of the data that was stopped.
    fn reread_sample(&self, snap: &Snapshot) -> Result<Option<Sample>, String> {
        let Some(s) = &snap.sample else {
            return Ok(None);
        };
        let record_hash =
            daemon::record_hash(&self.socket(), &s.schema, &s.hash, s.range.as_deref())?;
        Ok(Some(Sample {
            record_hash,
            ..s.clone()
        }))
    }
}
