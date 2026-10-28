//! The local copy of one accepted cloud recovery selection, saved before source release.
use super::{capture::Capture, work::LogContext, BackupError, Result};
use cloud_backup_protocol::v2::AcceptedBackup;
use fold_db::storage::disk_pack::mutation_log::{Frontiers, LogAnchor, LogPosition};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryPoint {
    pub accepted: AcceptedBackup,
    pub base: AcceptedBackup,
    pub anchor: Option<LogAnchor>,
    pub through: Option<LogPosition>,
    pub frontiers: Frontiers,
}
impl RecoveryPoint {
    pub fn validate(&self) -> Result<()> {
        self.accepted.validate()?;
        self.base.validate()?;
        if self.base.job().stream != self.accepted.job().stream
            || self.base.ordinal.get() > self.accepted.ordinal.get()
            || self.anchor.is_some() != self.through.is_some()
        {
            return Err(BackupError::Identity);
        }
        if let (Some(anchor), Some(through)) = (&self.anchor, &self.through) {
            let start = anchor.position()?;
            if through.epoch != start.epoch
                || through.frontiers != self.frontiers
                || (self.accepted == self.base && *through != start)
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
    pub fn log_context(&self) -> Result<LogContext> {
        self.validate()?;
        Ok(LogContext {
            base: self.base.clone(),
            previous: self.accepted.clone(),
            anchor: self.anchor.clone().ok_or(BackupError::Source)?,
            from: self.through.clone().ok_or(BackupError::Source)?,
        })
    }
}
impl Capture {
    pub fn recovery_point(&self, accepted: &AcceptedBackup) -> Result<RecoveryPoint> {
        let point = match self {
            Self::Snapshot(capture) => {
                let profile = capture.run_profile()?;
                let anchor = capture.mutation_log_anchor()?;
                let through = anchor.as_ref().map(LogAnchor::position).transpose()?;
                RecoveryPoint {
                    accepted: accepted.clone(),
                    base: accepted.clone(),
                    anchor,
                    through,
                    frontiers: Frontiers {
                        main: profile.main.frontier.get(),
                        cas: profile.cas.frontier.get(),
                    },
                }
            }
            Self::Log(capture) => capture.recovery_point(accepted),
        };
        point.validate()?;
        Ok(point)
    }
}
