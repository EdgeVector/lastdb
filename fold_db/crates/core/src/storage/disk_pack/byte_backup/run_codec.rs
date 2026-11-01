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
    pub cas: RunBackupHome,
}
impl RunBackupProfile {
    /// The node marker of the profile. Only format 7 (profile 5) backs up and
    /// restores: this release opens no older home.
    pub(crate) fn node_marker(&self) -> StorageResult<&'static [u8]> {
        match self.version {
            super::super::run_admission::RECORD_PROFILE => {
                Ok(super::super::run_admission::RECORD_NODE_BYTES)
            }
            2..=4 => Err(invalid(
                "run backup profile is format 4 to 6; this release restores only format 7",
            )),
            _ => Err(invalid("run backup logical profile version")),
        }
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.node_marker()?;
        if self.main.home_id == self.cas.home_id {
            return Err(invalid("run backup profile identity"));
        }
        for home in [&self.main, &self.cas] {
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
            required_features: DecimalU64::new(super::super::physical::V2_FEATURES),
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
    use cloud_backup_protocol::v2::Destination;
    if path == "laststore-pack-node-v1" {
        return Ok(Destination::R2);
    }
    let (home, tail) = path
        .split_once('/')
        .ok_or_else(|| invalid("run backup path"))?;
    if !matches!(home, "disk-run" | "disk-cas") {
        return Err(invalid("run backup home path"));
    }
    if matches!(tail, "CURRENT" | "laststore-run-home-v2") {
        return Ok(Destination::R2);
    }
    let id = tail
        .strip_prefix("runs/")
        .and_then(|s| s.strip_suffix(".ldrun"))
        .ok_or_else(|| invalid("run backup file path"))?;
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| invalid("run backup file identity"))?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(invalid("run backup canonical file identity"));
    }
    Ok(if home == "disk-cas" {
        Destination::B2
    } else {
        Destination::R2
    })
}
