//! Client-only identity and path contract for unchanged disk V2 files.
use super::{invalid, protocol, StorageResult};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, Id, NativeRoot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBackupHome {
    pub home_id: Id,
    pub generation: DecimalU64,
    pub frontier: DecimalU64,
    pub descriptor_sha256: Digest,
    pub current_sha256: Digest,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBackupProfile {
    pub version: u16,
    pub main: RunBackupHome,
    /// The CAS home of a shared database. A personal snapshot carries no CAS
    /// home: its record blobs are B2 objects at client-derived paths, and a
    /// restore installs an empty CAS home.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cas: Option<RunBackupHome>,
}
impl RunBackupProfile {
    /// The node marker of the profile. Only the current home format backs up
    /// and restores.
    pub(crate) fn node_marker(&self) -> StorageResult<&'static [u8]> {
        use crate::storage::format::{
            RECORD_NODE_BYTES, RECORD_PROFILE, SHARED_NODE_BYTES, SHARED_RECORD_PROFILE,
        };
        match self.version {
            RECORD_PROFILE => Ok(RECORD_NODE_BYTES),
            SHARED_RECORD_PROFILE => Ok(SHARED_NODE_BYTES),
            _ => Err(invalid("run backup logical profile version")),
        }
    }

    /// Only a shared database snapshot carries its CAS home.
    pub(crate) fn carries_cas(version: u16) -> bool {
        version == crate::storage::format::SHARED_RECORD_PROFILE
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.node_marker()?;
        if Self::carries_cas(self.version) != self.cas.is_some()
            || self
                .cas
                .as_ref()
                .is_some_and(|cas| cas.home_id == self.main.home_id)
        {
            return Err(invalid("run backup profile identity"));
        }
        for home in std::iter::once(&self.main).chain(self.cas.as_ref()) {
            if home.generation.get() == 0
                || home.frontier.get() > home.generation.get()
                || home.files < 2
                || home.bytes < 64
            {
                return Err(invalid("run backup home bounds"));
            }
        }
        Ok(())
    }
    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| invalid("run profile encoding"))?;
        let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(&value).map_err(protocol)?;
        Ok(NativeRoot {
            home_id: self.main.home_id.clone(),
            home_digest: bytes_sha256(&bytes),
            current_sha256: self.main.current_sha256.clone(),
            generation: self.main.generation,
            frontier: self.main.frontier,
        })
    }
    pub fn validate_root(&self, root: &NativeRoot) -> StorageResult<()> {
        if self.native_root()? != *root {
            return Err(invalid("run backup native root differs"));
        }
        Ok(())
    }
}

/// Native V2 paths are a fixed client-owned allowlist, never cloud metadata.
pub fn run_backup_destination(path: &str) -> StorageResult<cloud_backup_protocol::v2::Destination> {
    use crate::storage::disk_pack::{CAS_DIR, NODE_MARKER, RUN_DIR};
    use cloud_backup_protocol::v2::Destination;
    use laststore::disk_run::home::{CURRENT, RUNS, RUN_HOME_MARKER};
    if path == NODE_MARKER {
        return Ok(Destination::R2);
    }
    let (home, tail) = path
        .split_once('/')
        .ok_or_else(|| invalid("run backup path"))?;
    if home != RUN_DIR && home != CAS_DIR {
        return Err(invalid("run backup home path"));
    }
    if tail == CURRENT || tail == RUN_HOME_MARKER {
        return Ok(Destination::R2);
    }
    let id = tail
        .strip_prefix(RUNS)
        .and_then(|s| s.strip_prefix('/'))
        .and_then(|s| s.strip_suffix(".ldrun"))
        .ok_or_else(|| invalid("run backup file path"))?;
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| invalid("run backup file identity"))?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(invalid("run backup canonical file identity"));
    }
    Ok(if home == CAS_DIR {
        Destination::B2
    } else {
        Destination::R2
    })
}
