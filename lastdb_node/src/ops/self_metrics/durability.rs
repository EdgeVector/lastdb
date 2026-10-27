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
            degraded: None,
            reasons: vec!["v2_status_unavailable".into()],
        }
    }
}

impl From<crate::byte_cloud_backup::ByteBackupStatus> for DurabilityHealth {
    fn from(status: crate::byte_cloud_backup::ByteBackupStatus) -> Self {
        let blocked = status.blocked_reason.is_some();
        Self {
            observed: true,
            automatic_enabled: false,
            running: status.running,
            phase: status.phase,
            accepted_backup_recorded: status.last_accepted_backup.is_some(),
            // No capture time or current-source equivalence is available here.
            // Never turn an accepted historical receipt into a fresh-coverage claim.
            degraded: blocked.then_some(true),
            reasons: vec![if blocked {
                "v2_job_blocked".into()
            } else {
                "v2_freshness_unmeasured".into()
            }],
        }
    }
}

impl DurabilityHealth {
    pub(super) fn status_line(&self) -> String {
        let state = if !self.observed {
            "status unavailable; use cloud backup-v2 status"
        } else if self.degraded == Some(true) {
            "job blocked; use cloud backup-v2 status"
        } else if self.running {
            "job active; use cloud backup-v2 status"
        } else if self.accepted_backup_recorded {
            "accepted backup recorded; current coverage is not measured"
        } else {
            "no receipt in this node; check any external coordinator separately"
        };
        format!("Manual V2 backup: {state}")
    }
}
