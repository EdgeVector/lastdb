use super::*;

/// Redacted V2 job observation. A receipt is not a proof of current coverage.
/// Standalone coordinators remain separate and cannot be inferred from a missing local job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DurabilityHealth {
    pub observed: bool,
    pub automatic_enabled: bool,
    pub running: bool,
    pub phase: Option<crate::byte_cloud_backup::Phase>,
    pub accepted_backup_recorded: bool,
    /// Unix seconds of the last published automatic log. Absent when no log is accepted.
    pub last_accepted_log_unix: Option<u64>,
    /// Unix seconds of the next automatic snapshot. Absent when automatic backup is off.
    pub next_snapshot_unix: Option<u64>,
    /// Fixed blocked text. This never carries a key, a phrase, or an object name.
    pub blocked_reason: Option<String>,
    pub degraded: Option<bool>,
    pub reasons: Vec<String>,
}

impl Default for DurabilityHealth {
    fn default() -> Self {
        Self {
            observed: false,
            automatic_enabled: false,
            running: false,
            phase: None,
            accepted_backup_recorded: false,
            last_accepted_log_unix: None,
            next_snapshot_unix: None,
            blocked_reason: None,
            degraded: None,
            reasons: vec!["v2_status_unavailable".into()],
        }
    }
}

impl From<crate::byte_cloud_backup::ByteBackupStatus> for DurabilityHealth {
    fn from(status: crate::byte_cloud_backup::ByteBackupStatus) -> Self {
        let automatic = status.automatic_backup.as_ref();
        let last_accepted_log_unix = automatic.and_then(|state| state.last_log_at);
        let next_snapshot_unix = status
            .automatic_backup_enabled
            .then(|| automatic.map(|state| state.next_snapshot_at))
            .flatten();
        let blocked_reason = blocked_label(&status);
        Self {
            observed: true,
            automatic_enabled: status.automatic_backup_enabled,
            running: status.running,
            phase: status.phase,
            accepted_backup_recorded: status.last_accepted_backup.is_some(),
            last_accepted_log_unix,
            next_snapshot_unix,
            // The log time is the acceptance time. It is not a claim that the
            // current local head is fully backed up.
            degraded: blocked_reason.is_some().then_some(true),
            reasons: vec![if blocked_reason.is_some() {
                "v2_job_blocked".into()
            } else if last_accepted_log_unix.is_some() {
                "v2_log_time_recorded".into()
            } else {
                "v2_log_not_recorded".into()
            }],
            blocked_reason,
        }
    }
}

impl DurabilityHealth {
    pub fn status_line(&self) -> String {
        if !self.observed {
            return "V2 backup: status unavailable".to_string();
        }
        let automatic = if self.automatic_enabled { "on" } else { "off" };
        let log = self
            .last_accepted_log_unix
            .map_or_else(|| "none".to_string(), unix_utc);
        let next = if self.automatic_enabled {
            match self.next_snapshot_unix {
                Some(0) => "due now".to_string(),
                Some(secs) => unix_utc(secs),
                None => "none".to_string(),
            }
        } else {
            "none".to_string()
        };
        let mut line = format!(
            "V2 backup: automatic {automatic}; last accepted log {log}; next snapshot {next}"
        );
        if let Some(reason) = &self.blocked_reason {
            line.push_str("; blocked: ");
            line.push_str(reason);
        }
        line
    }
}

fn unix_utc(secs: u64) -> String {
    let Ok(secs) = i64::try_from(secs) else {
        return "none".to_string();
    };
    chrono::DateTime::from_timestamp(secs, 0).map_or_else(
        || "none".to_string(),
        |time| time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    )
}

fn blocked_label(status: &crate::byte_cloud_backup::ByteBackupStatus) -> Option<String> {
    if let Some(reason) = status
        .blocked_reason
        .as_ref()
        .filter(|reason| !reason.is_empty())
    {
        return Some(reason.clone());
    }
    status
        .automatic_backup
        .as_ref()
        .and_then(|state| state.paused)
        .map(|pause| pause_label(pause.reason).to_string())
}

fn pause_label(reason: crate::backup_runtime::PauseReason) -> &'static str {
    use crate::backup_runtime::PauseReason::*;
    match reason {
        Temporary => "temporary",
        Disabled => "automatic backup is off",
        Busy => "a backup job is active",
        Missing => "saved backup is missing",
        Identity => "account or home differs",
        Invalid => "saved backup state differs",
        Disk => "disk operation failed",
        Source => "source needs attention",
        Unsupported => "backup format is unsupported",
        ResourceLimit => "resource limit",
        Quota => "cloud quota reached",
        Credentials => "credentials refused",
        Authority => "cloud authority changed",
        Refused => "cloud request refused",
        Closing => "backup closure is requested",
        Shutdown => "daemon shutdown",
        WorkerPanic => "backup worker needs attention",
    }
}
