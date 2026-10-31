use super::*;

/// Redacted V2 job observation. A receipt is not a proof of current coverage.
/// Standalone coordinators remain separate and cannot be inferred from a missing local job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DurabilityHealth {
    pub observed: bool,
    pub automatic_enabled: bool,
    pub running: bool,
    pub phase: Option<crate::backup_runtime::Phase>,
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
        format!("V2 backup: {state}")
    }
}
