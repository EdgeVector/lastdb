use super::*;
use fold_db::storage::disk_pack::mutation_log::Frontiers;

/// An upload overdue by more than this makes a backup with waiting local writes stale.
/// It matches the 300 s recovery point bound of the safe upgrade.
const STALE_UPLOAD_SECS: u64 = 300;

/// Redacted V2 job observation.
///
/// `local_frontiers` and `backed_up_frontiers` come from one status read at `checked_at`,
/// so coverage is a claim about that read only. A later write waits for the next upload.
/// The backed-up position is the automatic worker's; a manual backup does not move it, so
/// coverage can under-report after a manual run but never over-report.
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
    pub checked_at: Option<u64>,
    pub local_frontiers: Option<Frontiers>,
    pub backed_up_frontiers: Option<Frontiers>,
    pub covered: Option<bool>,
    pub snapshot_required: bool,
    pub last_upload_at: Option<u64>,
    pub next_upload_at: Option<u64>,
    pub last_snapshot_at: Option<u64>,
    pub next_snapshot_at: Option<u64>,
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
            checked_at: None,
            local_frontiers: None,
            backed_up_frontiers: None,
            covered: None,
            snapshot_required: false,
            last_upload_at: None,
            next_upload_at: None,
            last_snapshot_at: None,
            next_snapshot_at: None,
        }
    }
}

/// What one status read can say about local writes in the backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coverage {
    Current,
    Behind,
    Stale,
    SnapshotRequired,
    Mismatch,
}

impl Coverage {
    fn reason(self) -> &'static str {
        match self {
            Self::Current => "v2_current",
            Self::Behind => "v2_behind",
            Self::Stale => "v2_stale",
            Self::SnapshotRequired => "v2_snapshot_required",
            Self::Mismatch => "v2_position_mismatch",
        }
    }
}

impl DurabilityHealth {
    fn coverage(&self) -> Option<Coverage> {
        let (local, backed) = (self.local_frontiers?, self.backed_up_frontiers?);
        if self.snapshot_required {
            return Some(Coverage::SnapshotRequired);
        }
        if local == backed {
            return Some(Coverage::Current);
        }
        if backed.main > local.main || backed.cas > local.cas {
            return Some(Coverage::Mismatch);
        }
        if self.upload_overdue_secs().unwrap_or(0) > STALE_UPLOAD_SECS {
            Some(Coverage::Stale)
        } else {
            Some(Coverage::Behind)
        }
    }

    fn upload_overdue_secs(&self) -> Option<u64> {
        Some(self.checked_at?.saturating_sub(self.next_upload_at?))
    }

    fn secs_since(&self, at: Option<u64>) -> Option<u64> {
        Some(self.checked_at?.saturating_sub(at?))
    }

    fn secs_until(&self, at: Option<u64>) -> Option<u64> {
        Some(at?.saturating_sub(self.checked_at?))
    }

    /// Fill `covered` and `reasons` from the fields of one status read.
    pub(crate) fn with_coverage(mut self) -> Self {
        let coverage = self.coverage();
        self.covered = coverage.map(|c| c == Coverage::Current);
        let reason = if self.degraded == Some(true) {
            "v2_job_blocked"
        } else {
            coverage.map_or("v2_freshness_unmeasured", Coverage::reason)
        };
        self.reasons = vec![reason.into()];
        self
    }

    fn coverage_text(&self, coverage: Coverage) -> String {
        let log = |f: Frontiers| format!("{}/{}", f.main, f.cas);
        let positions = match (self.local_frontiers, self.backed_up_frontiers) {
            (Some(local), Some(backed)) => {
                format!("local log {}, backed up {}", log(local), log(backed))
            }
            _ => "log positions unknown".to_string(),
        };
        let last_upload = self
            .secs_since(self.last_upload_at)
            .map(|s| format!("last upload {} ago", format_age_secs(s)));
        let mut parts = match coverage {
            Coverage::Current => vec![format!(
                "current; every local write is backed up (log {})",
                self.local_frontiers.map_or_else(String::new, log)
            )],
            Coverage::Behind => vec![format!("behind; {positions}")],
            Coverage::Stale => vec![format!("STALE; {positions}")],
            Coverage::SnapshotRequired => {
                vec![format!("a new snapshot is required; {positions}")]
            }
            Coverage::Mismatch => vec![format!("backup is ahead of the local log; {positions}")],
        };
        parts.extend(last_upload);
        match coverage {
            Coverage::Current => parts.extend(
                self.secs_until(self.next_snapshot_at)
                    .map(|s| format!("next snapshot in {}", format_age_secs(s))),
            ),
            Coverage::Behind => parts.extend(self.secs_until(self.next_upload_at).map(|s| {
                if s == 0 {
                    "next upload due now".to_string()
                } else {
                    format!("next upload in {}", format_age_secs(s))
                }
            })),
            Coverage::Stale | Coverage::SnapshotRequired | Coverage::Mismatch => {
                if let Some(s) = self.upload_overdue_secs().filter(|s| *s > 0) {
                    parts.push(format!("upload overdue {}", format_age_secs(s)));
                }
                parts.push("use cloud backup-v2 status".to_string());
            }
        }
        parts.join("; ")
    }

    pub(super) fn status_line(&self) -> String {
        let state = if !self.observed {
            "status unavailable; use cloud backup-v2 status".to_string()
        } else if self.degraded == Some(true) {
            "job blocked; use cloud backup-v2 status".to_string()
        } else if let Some(coverage) = self.coverage() {
            self.coverage_text(coverage)
        } else if self.running {
            "job active; use cloud backup-v2 status".to_string()
        } else if self.accepted_backup_recorded {
            "accepted backup recorded; local log position not reported".to_string()
        } else {
            "no receipt in this node; check any external coordinator separately".to_string()
        };
        format!("V2 backup: {state}")
    }
}
