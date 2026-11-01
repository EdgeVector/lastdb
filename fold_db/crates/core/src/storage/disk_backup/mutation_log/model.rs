//! Native log identities and complete ordered batch facts. No network authority.
use super::{invalid, StorageResult};
use cloud_backup_protocol::{bytes_sha256, v2::hash::canonical_bytes, Digest, Id};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frontiers {
    pub main: u64,
    pub cas: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitRole {
    CasAdd,
    Main,
    CasDelete,
}
impl CommitRole {
    pub fn directory(self) -> &'static str {
        match self {
            Self::CasAdd => "cas-add",
            Self::Main => "main",
            Self::CasDelete => "cas-delete",
        }
    }
    pub fn is_cas(self) -> bool {
        self != Self::Main
    }
}

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
    pub cas_home: Id,
    pub frontiers: Frontiers,
}
impl LogAnchor {
    pub fn position(&self) -> StorageResult<LogPosition> {
        if self.version != 1 {
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
        if self.version != 1
            || self.epoch != previous.epoch
            || previous.sequence.checked_add(1) != Some(self.sequence)
            || self.previous_sha256 != previous.sha256
            || self.before != previous.frontiers
            || self.parts.is_empty()
            || self.parts.len() > 3
        {
            return Err(invalid("mutation log entry sequence or context"));
        }
        let mut frontiers = self.before;
        let mut role = None;
        for part in &self.parts {
            if part.bytes == 0 || role.is_some_and(|old| old >= part.role) {
                return Err(invalid("mutation log part order or length"));
            }
            let frontier = if part.role.is_cas() {
                &mut frontiers.cas
            } else {
                &mut frontiers.main
            };
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
    pub cas_home: Id,
    pub epochs: Vec<Id>,
    pub active: Option<Id>,
    pub cleanup: Vec<Id>,
}
