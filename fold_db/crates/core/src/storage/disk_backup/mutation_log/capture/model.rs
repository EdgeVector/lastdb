use super::*;
use cloud_backup_protocol::v2::{jobs::JobResult, Begin, Commit, Destination};
use cloud_backup_protocol::RelativePath;
use serde::{Deserialize, Serialize};

pub(super) const MAX_BATCH_ENTRIES: u64 = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenLogFile {
    pub entry_sequence: u64,
    pub role: CommitRole,
    pub run_id: Id,
    pub path: RelativePath,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenLogCapture {
    pub version: u16,
    pub begin: Begin,
    pub anchor: LogAnchor,
    pub from: LogPosition,
    pub through: LogPosition,
    pub entries: Vec<LogEntry>,
    pub files: Vec<FrozenLogFile>,
    pub native_bytes: u64,
    pub ready: bool,
    pub commit: Option<Commit>,
    pub terminal: Option<JobResult>,
    pub local_cancelled: bool,
    pub released: bool,
}

impl FrozenLogCapture {
    pub fn validate(&self) -> StorageResult<()> {
        self.begin
            .validate()
            .map_err(|_| invalid("mutation capture begin"))?;
        self.anchor.position()?;
        if self.version != 1
            || self.from.epoch != self.anchor.epoch
            || self.through.epoch != self.anchor.epoch
            || self.entries.is_empty()
            || self.entries.len() as u64 > MAX_BATCH_ENTRIES
            || self.begin.required_destinations != [Destination::R2, Destination::B2]
            || !self.begin.descriptor.as_value().is_null()
        {
            return Err(invalid("mutation capture context or bounds"));
        }
        let mut position = self.from.clone();
        let mut files = 0usize;
        let mut bytes = 0u64;
        for entry in &self.entries {
            position = entry.position_after(&position)?;
            for part in &entry.parts {
                let file = self
                    .files
                    .get(files)
                    .ok_or_else(|| invalid("mutation capture file is absent"))?;
                if file.entry_sequence != entry.sequence
                    || file.role != part.role
                    || file.run_id != part.run_id
                    || file.bytes != part.bytes
                    || file.path.as_str()
                        != log_path(&self.anchor.epoch, entry.sequence, part.role, &part.run_id)
                    || self.files[..files]
                        .iter()
                        .any(|old| old.run_id == file.run_id)
                {
                    return Err(invalid("mutation capture exact file identity"));
                }
                bytes = bytes
                    .checked_add(file.bytes)
                    .ok_or_else(|| invalid("mutation capture byte count"))?;
                files += 1;
            }
        }
        if position != self.through || files != self.files.len() || bytes != self.native_bytes {
            return Err(invalid("mutation capture exact totals"));
        }
        if let Some(commit) = &self.commit {
            commit
                .validate_for(&self.begin)
                .map_err(|_| invalid("mutation capture commit"))?;
        }
        if let Some(result) = &self.terminal {
            self.validate_terminal(result)?;
        }
        if self.local_cancelled && self.terminal.is_some() {
            return Err(invalid("mutation capture local and cloud terminal differ"));
        }
        if self.released && self.terminal.is_none() && !self.local_cancelled {
            return Err(invalid("mutation capture release authority"));
        }
        Ok(())
    }

    pub(super) fn validate_terminal(&self, result: &JobResult) -> StorageResult<()> {
        use cloud_backup_protocol::v2::jobs::JobStatus;
        result
            .validate_for_begin(&self.begin)
            .map_err(|_| invalid("mutation capture terminal job"))?;
        match &result.status {
            JobStatus::Published { backup } => {
                if self.commit.as_ref()
                    != Some(
                        &backup
                            .commit_input()
                            .map_err(|_| invalid("mutation capture accepted input"))?,
                    )
                {
                    return Err(invalid("mutation capture exact accepted input"));
                }
            }
            JobStatus::Closed { .. } => {}
            JobStatus::Active => {
                return Err(invalid("active mutation capture has no terminal authority"))
            }
        }
        Ok(())
    }
}

pub fn log_path(epoch: &Id, sequence: u64, role: CommitRole, run: &Id) -> String {
    format!(
        "mutation-log/{}/{sequence:020}/{}/{}.ldrun",
        epoch.as_str(),
        role.directory(),
        run.as_str()
    )
}
