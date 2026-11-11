use super::*;
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    Begin, Commit, Destination,
};
use cloud_backup_protocol::RelativePath;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;

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

pub struct MutationLogReader {
    inner: Arc<Inner>,
    file: File,
    remaining: u64,
}

impl MutationLogCapture {
    pub fn reader(
        &self,
        index: usize,
        offset: u64,
        bytes: u64,
    ) -> StorageResult<MutationLogReader> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if !state.ready
            || state.local_cancelled
            || state.terminal.is_some()
            || state.released
            || self.inner.readers.load(Ordering::Acquire)
                >= lastdb_model::storage_policy::BACKUP_RESOURCES.concurrent_streams
        {
            return Err(invalid("mutation capture reader admission"));
        }
        let selected = state
            .files
            .get(index)
            .ok_or_else(|| invalid("mutation capture file number"))?;
        if bytes == 0
            || offset
                .checked_add(bytes)
                .is_none_or(|end| end > selected.bytes)
        {
            return Err(invalid("mutation capture reader extent"));
        }
        let mut file = input(
            &self
                .inner
                .directory
                .join(format!("{}.ldrun", selected.run_id.as_str())),
            selected.bytes,
        )?;
        file.seek(SeekFrom::Start(offset))?;
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(MutationLogReader {
            inner: Arc::clone(&self.inner),
            file,
            remaining: bytes,
        })
    }

    pub fn seal_commit(&self, commit: &Commit) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        commit
            .validate_for(&state.begin)
            .map_err(|_| invalid("mutation capture sealed commit input"))?;
        if !state.ready
            || state.local_cancelled
            || state.terminal.is_some()
            || state.released
            || state.commit.as_ref().is_some_and(|old| old != commit)
        {
            return Err(invalid("mutation capture commit phase or identity"));
        }
        self.write_capture_state(&mut state, |saved| saved.commit = Some(commit.clone()))
    }

    pub fn terminal(&self, result: &JobResult) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if self.inner.readers.load(Ordering::Acquire) != 0 {
            return Err(invalid("mutation capture still has readers"));
        }
        if state.local_cancelled {
            return Err(invalid(
                "locally cancelled mutation capture has no cloud terminal",
            ));
        }
        state.validate_terminal(result)?;
        if state.terminal.as_ref().is_some_and(|old| old != result) {
            return Err(invalid("mutation capture terminal authority changed"));
        }
        if state.released {
            return Ok(());
        }
        if state.terminal.is_none() {
            self.write_capture_state(&mut state, |saved| {
                saved.terminal = Some(result.clone());
            })?;
        }
        if matches!(result.status, JobStatus::Published { .. }) {
            self.inner
                .runtime
                .mutation_log
                .acknowledge(&state.through)?;
        }
        self.release_capture_files(&mut state)
    }

    /// Only the coordinator's durable never-submitted branch can use this path.
    pub fn cancel_unsubmitted(&self) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if self.inner.readers.load(Ordering::Acquire) != 0
            || state.terminal.is_some()
            || state.commit.is_some()
        {
            return Err(invalid("mutation capture cannot be locally cancelled"));
        }
        if state.released {
            return Ok(());
        }
        self.write_capture_state(&mut state, |saved| saved.local_cancelled = true)?;
        self.release_capture_files(&mut state)
    }

    fn write_capture_state(
        &self,
        state: &mut FrozenLogCapture,
        change: impl FnOnce(&mut FrozenLogCapture),
    ) -> StorageResult<()> {
        let mut saved = state.clone();
        change(&mut saved);
        store::write(&self.inner.directory.join("capture.json"), &saved)?;
        *state = saved;
        Ok(())
    }

    fn release_capture_files(&self, state: &mut FrozenLogCapture) -> StorageResult<()> {
        for file in &state.files {
            store::remove_part(
                &self
                    .inner
                    .directory
                    .join(format!("{}.ldrun", file.run_id.as_str())),
                file.bytes,
            )?;
        }
        File::open(&self.inner.directory)?.sync_all()?;
        self.write_capture_state(state, |saved| saved.released = true)
    }
}

impl Read for MutationLogReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let maximum = self
            .remaining
            .min(buffer.len() as u64)
            .min(lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
            as usize;
        let count = self.file.read(&mut buffer[..maximum])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frozen mutation log file ends early",
            ));
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}

impl Drop for MutationLogReader {
    fn drop(&mut self) {
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
