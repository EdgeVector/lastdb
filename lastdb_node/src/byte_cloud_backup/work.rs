//! A saved request keeps its recovery base and exact log predecessor across every retry.
use super::{BackupError, Result};
use cloud_backup_protocol::v2::{AcceptedBackup, Begin};
use fold_db::storage::disk_pack::mutation_log::{Frontiers, LogAnchor, LogPosition};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LogContext {
    pub base: AcceptedBackup,
    pub previous: AcceptedBackup,
    pub anchor: LogAnchor,
    pub from: LogPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Work {
    Snapshot { minimum: Option<Frontiers> },
    MutationLog { context: Box<LogContext> },
}
impl Default for Work {
    fn default() -> Self {
        Self::Snapshot { minimum: None }
    }
}
impl Work {
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Snapshot { minimum: None })
    }
    pub fn label(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::MutationLog { .. } => "mutation_log",
        }
    }
    pub fn validate(&self, begin: Option<&Begin>) -> Result<()> {
        if let Self::MutationLog { context } = self {
            context.base.validate()?;
            context.previous.validate()?;
            context.anchor.position()?;
            if context.base.job().stream != context.previous.job().stream
                || context.from.epoch != context.anchor.epoch
            {
                return Err(BackupError::Identity);
            }
            if let Some(begin) = begin {
                if context.previous.job().stream != begin.job.stream
                    || begin.predecessor.as_ref() != Some(&context.previous.head())
                {
                    return Err(BackupError::Authority);
                }
            }
        }
        Ok(())
    }
}
