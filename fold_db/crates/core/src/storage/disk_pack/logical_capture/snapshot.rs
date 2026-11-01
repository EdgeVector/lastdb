//! An encrypted client root carries the signed positions of its exact native cut.

use super::*;
use crate::storage::disk_pack::byte_backup::RunBackupProfile;
use cloud_backup_protocol::{DecimalU64, NativeRoot};
use ed25519_dalek::Signature;
use laststore::disk_run::home::RunRead;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod positions;
mod read;
mod wire;

const MAX_DEVICES: usize = 33;
const MAX_MAP_BYTES: usize = 32 * 1024;
const MAX_IMPORTED_RECEIPTS: u64 = 65_536;
pub(crate) const SNAPSHOT_INCLUSION_WORK_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotInclusion {
    Disabled,
    Enabled { map: SnapshotInclusionMap },
}

impl<'de> Deserialize<'de> for SnapshotInclusion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        wire::deserialize(deserializer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDevicePosition {
    pub origin: String,
    pub anchor_sha256: String,
    pub sequence: DecimalU64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<SnapshotIdentityPosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInclusionMap {
    pub version: u8,
    pub producer: String,
    pub snapshot: NativeRoot,
    #[serde(deserialize_with = "positions::deserialize")]
    pub positions: Vec<SnapshotDevicePosition>,
    pub sha256: String,
    pub signature: String,
}

impl SnapshotInclusion {
    pub fn validate_for(&self, expected: &NativeRoot) -> StorageResult<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Enabled { map } => map.validate_for(expected),
        }
    }
}

impl SnapshotInclusionMap {
    pub fn validate_for(&self, expected: &NativeRoot) -> StorageResult<()> {
        bounded::serialized_size_with_limit(self, MAX_MAP_BYTES)?;
        if !matches!(self.version, 1 | 2) || self.snapshot != *expected {
            return Err(invalid("snapshot inclusion native identity differs"));
        }
        self.validate_positions()?;
        let signer = verify::origin_key(&self.producer)?;
        let signature = Signature::from_bytes(&unhex::<64>(&self.signature)?);
        let expected = unhex::<32>(&self.sha256)?;
        let bytes = self.signing_bytes()?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != expected {
            return Err(invalid("snapshot inclusion hash differs"));
        }
        signer
            .verify_strict(&bytes, &signature)
            .map_err(|_| invalid("snapshot inclusion signature differs"))
    }

    fn validate_positions(&self) -> StorageResult<()> {
        if self.positions.is_empty()
            || self.positions.len() > MAX_DEVICES
            || self
                .positions
                .windows(2)
                .any(|pair| pair[0].origin >= pair[1].origin)
        {
            return Err(invalid("snapshot inclusion device order or count differs"));
        }
        let mut producer = 0usize;
        let mut imported = 0u64;
        for position in &self.positions {
            verify::origin_key(&position.origin)?;
            unhex::<32>(&position.anchor_sha256)?;
            unhex::<32>(&position.sha256)?;
            if position.sequence.get() == 0 && position.sha256 != position.anchor_sha256 {
                return Err(invalid(
                    "snapshot inclusion initial device position differs",
                ));
            }
            let base = match (self.version, &position.base) {
                (1, None) => 0,
                (2, Some(base)) => {
                    base.validate_for(position)?;
                    base.sequence.get()
                }
                _ => return Err(invalid("snapshot inclusion retained base version differs")),
            };
            if position.origin == self.producer {
                producer += 1;
            } else {
                imported = imported
                    .checked_add(position.sequence.get() - base)
                    .filter(|count| *count <= MAX_IMPORTED_RECEIPTS)
                    .ok_or_else(|| invalid("snapshot inclusion imported receipt cap differs"))?;
            }
        }
        if producer != 1 {
            return Err(invalid("snapshot inclusion producer position is absent"));
        }
        Ok(())
    }

    fn signing_bytes(&self) -> StorageResult<Vec<u8>> {
        encode(&(
            if self.version == 1 {
                "lastdb-snapshot-inclusion-map-v1"
            } else {
                "lastdb-snapshot-inclusion-map-v2"
            },
            self.version,
            &self.producer,
            &self.snapshot,
            &self.positions,
        ))
    }
}

impl PackRuntime {
    pub(crate) fn snapshot_inclusion(
        &self,
        main: &mut RunRead,
        profile: &RunBackupProfile,
    ) -> StorageResult<SnapshotInclusion> {
        // A backup of an old home still signs with the device file on disk.
        // The live capture stream no longer stays on the runtime.
        let loaded = identity::Identity::load(&self.physical)?;
        signed_inclusion(loaded.as_ref(), main, profile)
    }
}

pub(super) fn signed_inclusion(
    identity: Option<&identity::Identity>,
    main: &mut RunRead,
    profile: &RunBackupProfile,
) -> StorageResult<SnapshotInclusion> {
    let Some(facts) = read::positions(main, profile)? else {
        return Ok(SnapshotInclusion::Disabled);
    };
    let identity = identity.ok_or_else(|| invalid("snapshot inclusion signer is unavailable"))?;
    if identity.origin() != facts.producer {
        return Err(invalid(
            "snapshot inclusion signer differs from the captured producer",
        ));
    }
    let mut map = SnapshotInclusionMap {
        version: facts.version,
        producer: facts.producer,
        snapshot: profile.native_root()?,
        positions: facts.positions,
        sha256: String::new(),
        signature: String::new(),
    };
    map.validate_positions()?;
    bounded::serialized_size_with_limit(&map, MAX_MAP_BYTES)?;
    let bytes = map.signing_bytes()?;
    map.sha256 = hex(&Sha256::digest(&bytes));
    map.signature = identity.sign(&bytes);
    map.validate_for(&profile.native_root()?)?;
    Ok(SnapshotInclusion::Enabled { map })
}

/// Compare the authenticated certificate with the actual restored native cut.
/// This needs no local device signer and grants no device trust or adoption.
pub(crate) fn verify_snapshot_inclusion(
    main: &mut RunRead,
    profile: &RunBackupProfile,
    inclusion: &SnapshotInclusion,
) -> StorageResult<()> {
    inclusion.validate_for(&profile.native_root()?)?;
    match (inclusion, read::positions(main, profile)?) {
        (SnapshotInclusion::Disabled, None) => Ok(()),
        (SnapshotInclusion::Enabled { map }, Some(facts))
            if map.version == facts.version
                && map.producer == facts.producer
                && map.positions == facts.positions =>
        {
            Ok(())
        }
        _ => Err(invalid(
            "snapshot inclusion differs from the native device positions",
        )),
    }
}
