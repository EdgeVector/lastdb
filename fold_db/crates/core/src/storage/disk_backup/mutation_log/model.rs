//! Native log identities, ordered batch facts, and active commit recording.
use super::*;
use cloud_backup_protocol::{bytes_sha256, v2::hash::canonical_bytes, Digest, Id};
use laststore::disk_run::home::RunCommit;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frontiers {
    pub main: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitRole {
    Main,
}
impl CommitRole {
    pub fn directory(self) -> &'static str {
        match self {
            Self::Main => "main",
        }
    }
}

/// The log metadata version: main-home parts only.
pub(super) const LOG_VERSION: u16 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogPart {
    pub role: CommitRole,
    pub run_id: Id,
    pub bytes: u64,
    pub frontier: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogAnchor {
    pub version: u16,
    pub epoch: Id,
    pub main_home: Id,
    pub frontiers: Frontiers,
}
impl LogAnchor {
    pub fn position(&self) -> StorageResult<LogPosition> {
        if self.version != LOG_VERSION {
            return Err(invalid("mutation log anchor version"));
        }
        Ok(LogPosition {
            epoch: self.epoch.clone(),
            sequence: 0,
            sha256: hash(self)?,
            frontiers: self.frontiers,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogPosition {
    pub epoch: Id,
    pub sequence: u64,
    pub sha256: Digest,
    pub frontiers: Frontiers,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogEntry {
    pub version: u16,
    pub epoch: Id,
    pub sequence: u64,
    pub previous_sha256: Digest,
    pub before: Frontiers,
    pub after: Frontiers,
    pub parts: Vec<LogPart>,
}
impl LogEntry {
    pub fn position_after(&self, previous: &LogPosition) -> StorageResult<LogPosition> {
        if self.version != LOG_VERSION
            || self.epoch != previous.epoch
            || previous.sequence.checked_add(1) != Some(self.sequence)
            || self.previous_sha256 != previous.sha256
            || self.before != previous.frontiers
            || self.parts.len() != 1
        {
            return Err(invalid("mutation log entry sequence or context"));
        }
        let mut frontiers = self.before;
        let mut role = None;
        for part in &self.parts {
            if part.bytes == 0 || role.is_some_and(|old| old >= part.role) {
                return Err(invalid("mutation log part order or length"));
            }
            let frontier = &mut frontiers.main;
            *frontier = frontier
                .checked_add(1)
                .ok_or_else(|| invalid("mutation log frontier overflow"))?;
            if *frontier != part.frontier {
                return Err(invalid("mutation log part frontier"));
            }
            role = Some(part.role);
        }
        if frontiers != self.after {
            return Err(invalid("mutation log complete batch frontiers"));
        }
        Ok(LogPosition {
            epoch: self.epoch.clone(),
            sequence: self.sequence,
            sha256: hash(self)?,
            frontiers,
        })
    }
    pub fn bytes(&self) -> StorageResult<u64> {
        self.parts.iter().try_fold(0u64, |sum, part| {
            sum.checked_add(part.bytes)
                .ok_or_else(|| invalid("mutation log retained size overflow"))
        })
    }
}

pub(super) fn hash<T: Serialize>(value: &T) -> StorageResult<Digest> {
    Ok(bytes_sha256(
        &canonical_bytes(value).map_err(|_| invalid("mutation log canonical metadata"))?,
    ))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EpochState {
    pub anchor: LogAnchor,
    pub head: LogPosition,
    pub acknowledged: u64,
    pub released: LogPosition,
    pub retained_bytes: u64,
    pub metadata_bytes: u64,
    pub gap: Option<String>,
}
impl EpochState {
    pub fn validate(&self) -> StorageResult<()> {
        self.anchor.position()?;
        if self.head.epoch != self.anchor.epoch
            || self.released.epoch != self.anchor.epoch
            || self.acknowledged > self.head.sequence
            || self.released.sequence > self.acknowledged
            || self.head.sequence == 0 && self.head != self.anchor.position()?
        {
            return Err(invalid("mutation log saved epoch"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Index {
    pub version: u16,
    pub enabled: bool,
    pub main_home: Id,
    pub epochs: Vec<Id>,
    pub active: Option<Id>,
    pub cleanup: Vec<Id>,
}

pub(in crate::storage) struct Recording<'a> {
    log: &'a MutationLog,
    _owner: MutexGuard<'a, Cache>,
    state: EpochState,
    entry: LogEntry,
    directory: PathBuf,
    earlier_bytes: u64,
    failed: bool,
}

impl MutationLog {
    pub fn begin(&self, v2: &V2Home) -> Option<Recording<'_>> {
        match Recording::new(self, v2) {
            Ok(recording) => recording,
            Err(error) => {
                tracing::warn!(target: "fold_db::mutation_log", error = %error,
                    "native mutation log needs a snapshot; local writes keep their native durability");
                None
            }
        }
    }
}

impl<'a> Recording<'a> {
    fn new(log: &'a MutationLog, v2: &V2Home) -> StorageResult<Option<Self>> {
        let owner = log.lock()?;
        let Some(index) = &owner.index else {
            return Ok(None);
        };
        if !index.enabled {
            return Ok(None);
        }
        let Some(epoch) = &index.active else {
            return Ok(None);
        };
        let mut state = log.state(epoch)?;
        if state.gap.is_some() {
            return Ok(None);
        }
        if state.head.frontiers != frontiers(v2) {
            state.gap = Some("native frontier exceeds the complete mutation log".to_owned());
            store::write(&store::epoch(&log.root, epoch).join("state.json"), &state)?;
            return Ok(None);
        }
        let sequence = state
            .head
            .sequence
            .checked_add(1)
            .filter(|n| *n <= store::MAX_ENTRIES)
            .ok_or_else(|| invalid("mutation log entry resource bound"))?;
        let directory = store::entry(&log.root, epoch, sequence)?;
        store::create(&directory)?;
        let entry = LogEntry {
            version: LOG_VERSION,
            epoch: epoch.clone(),
            sequence,
            previous_sha256: state.head.sha256.clone(),
            before: state.head.frontiers,
            after: state.head.frontiers,
            parts: Vec::new(),
        };
        store::write(&directory.join("pending.json"), &entry)?;
        let earlier_bytes = log.charge(index)?;
        Ok(Some(Self {
            log,
            _owner: owner,
            state,
            entry,
            directory,
            earlier_bytes,
            failed: false,
        }))
    }

    pub fn retain(&mut self, role: CommitRole, commit: &RunCommit) {
        if self.failed {
            return;
        }
        if let Err(error) = self.retain_inner(role, commit) {
            self.failed = true;
            self.state.gap = Some(
                "native mutation log byte retention failed; a snapshot is required".to_owned(),
            );
            tracing::warn!(target: "fold_db::mutation_log", error = %error,
                "native mutation log retained its incomplete entry for recovery");
        }
    }

    fn retain_inner(&mut self, role: CommitRole, commit: &RunCommit) -> StorageResult<()> {
        if self
            .entry
            .parts
            .last()
            .is_some_and(|part| part.role >= role)
        {
            return Err(invalid("mutation log native commit order"));
        }
        let frontier = &mut self.entry.after.main;
        if frontier.checked_add(1) != Some(commit.frontier) {
            return Err(invalid("mutation log native commit frontier"));
        }
        *frontier = commit.frontier;
        let part = LogPart {
            role,
            run_id: Id::try_from(commit.run_id.to_string())
                .map_err(|_| invalid("mutation log native file UUID"))?,
            bytes: commit.run_bytes,
            frontier: commit.frontier,
        };
        let quote = self
            .earlier_bytes
            .checked_add(self.entry.bytes()?)
            .and_then(|n| n.checked_add(part.bytes))
            .and_then(|n| n.checked_add(4 * store::MAX_JSON))
            .ok_or_else(|| invalid("mutation log backlog size overflow"))?;
        if quote > lastdb_model::storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid(
                "mutation log backlog resource bound; snapshot required",
            ));
        }
        let parent = self.directory.join(role.directory());
        store::create(&parent)?;
        self.entry.parts.push(part);
        store::write(&self.directory.join("pending.json"), &self.entry)?;
        commit
            .retain_file(&parent.join(format!("{}.ldrun", commit.run_id)))
            .map_err(crate::storage::disk_pack::map)
    }

    pub fn finish(mut self, v2: &V2Home) {
        if let Err(error) = self.finish_inner(v2) {
            self.state.gap =
                Some("native mutation log commit marker failed; a snapshot is required".to_owned());
            let _ = store::write(
                &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
                &self.state,
            );
            tracing::warn!(target: "fold_db::mutation_log", error = %error,
                "native mutation log needs recovery; no cloud frontier advanced");
        }
    }

    fn finish_inner(&mut self, v2: &V2Home) -> StorageResult<()> {
        if self.failed {
            return store::write(
                &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
                &self.state,
            );
        }
        if self.entry.after != frontiers(v2) {
            return Err(invalid("mutation log complete native batch"));
        }
        let position = self.entry.position_after(&self.state.head)?;
        store::write(&self.directory.join("entry.json"), &self.entry)?;
        let bytes = self
            .state
            .retained_bytes
            .checked_add(self.entry.bytes()?)
            .ok_or_else(|| invalid("mutation log retained size overflow"))?;
        let mut next = self.state.clone();
        next.head = position;
        next.retained_bytes = bytes;
        let metadata = canonical_bytes(&self.entry)
            .map_err(|_| invalid("mutation log canonical entry"))?
            .len() as u64;
        next.metadata_bytes = next
            .metadata_bytes
            .checked_add(2 * metadata)
            .ok_or_else(|| invalid("mutation log metadata size overflow"))?;
        store::write(
            &store::epoch(&self.log.root, &self.entry.epoch).join("state.json"),
            &next,
        )?;
        self.state = next;
        Ok(())
    }
}
