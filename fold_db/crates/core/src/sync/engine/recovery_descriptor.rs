//! Account-root recovery descriptor for a LastStore cloud backup.
//!
//! The database root is derived from a random store UUID. A lost home also
//! loses that UUID and its layout file, so the account keeps one encrypted
//! descriptor per published backup cut outside the database root. The object
//! name supplies only opaque hashes. The phrase-derived key authenticates the
//! contents before a restore opens a destination store.

use crate::crypto::{decrypt_envelope_v2, encrypt_envelope_v2, KeyId};
use crate::hex::sha256_hex;
use crate::storage::laststore::cloud_db_hash_for_store_uuid;
use crate::sync::auth::ops::BackupLatestPointer;
use laststore::{
    HashAlgo, HashGroupKey, LastStoreOptions, LayoutDescriptor, LayoutMode, PackagingMode,
};
use serde::{Deserialize, Serialize};

const DESCRIPTOR_VERSION: u32 = 1;
const NAME_PREFIX: &str = "lastdb-recovery-v1-";
const NAME_SUFFIX: &str = ".enc";
const KEY_ID: KeyId = KeyId::from_bytes(*b"RCV1");

/// A descriptor whose bytes are encrypted under the account's portable key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDescriptorV1 {
    pub version: u32,
    pub mode: String,
    pub store_uuid: String,
    pub db_hash: String,
    pub layout: RecoveryLayoutV1,
    pub manifest_sha256: String,
    pub counter: u64,
    pub epoch: u64,
}

/// Every placement field in `laststore-layout-v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryLayoutV1 {
    pub layout_mode: String,
    pub shard_bits: u8,
    pub hash_group_bits: u8,
    pub hash_algo: String,
    pub hash_group_key: String,
    pub hash_group_partition_fanout: u32,
    pub layout_epoch: u32,
    pub packaging: String,
}

impl RecoveryLayoutV1 {
    fn from_laststore(layout: LayoutDescriptor) -> Self {
        Self {
            layout_mode: match layout.layout_mode {
                LayoutMode::SegmentLog => "segment_log",
                LayoutMode::HashGroup => "hash_group",
            }
            .into(),
            shard_bits: layout.shard_bits,
            hash_group_bits: layout.hash_group_bits,
            hash_algo: match layout.hash_algo {
                HashAlgo::Fnv1a64 => "fnv1a64",
            }
            .into(),
            hash_group_key: match layout.hash_group_key {
                HashGroupKey::FullKey => "full_key",
                HashGroupKey::PartitionPrefix => "partition_prefix",
            }
            .into(),
            hash_group_partition_fanout: layout.hash_group_partition_fanout,
            layout_epoch: layout.layout_epoch,
            packaging: match layout.packaging {
                PackagingMode::Plain => "plain",
                PackagingMode::FrameAead => "frame_aead",
            }
            .into(),
        }
    }

    fn to_laststore(&self) -> Result<LayoutDescriptor, String> {
        let layout_mode = match self.layout_mode.as_str() {
            "segment_log" => LayoutMode::SegmentLog,
            "hash_group" => LayoutMode::HashGroup,
            _ => return Err("invalid recovery layout mode".into()),
        };
        let hash_algo = match self.hash_algo.as_str() {
            "fnv1a64" => HashAlgo::Fnv1a64,
            _ => return Err("invalid recovery layout hash algorithm".into()),
        };
        let hash_group_key = match self.hash_group_key.as_str() {
            "full_key" => HashGroupKey::FullKey,
            "partition_prefix" => HashGroupKey::PartitionPrefix,
            _ => return Err("invalid recovery layout hash group key".into()),
        };
        let packaging = match self.packaging.as_str() {
            "plain" => PackagingMode::Plain,
            "frame_aead" => PackagingMode::FrameAead,
            _ => return Err("invalid recovery layout packaging".into()),
        };
        if self.shard_bits > 12 || self.hash_group_bits > 16 {
            return Err("invalid recovery layout bit count".into());
        }
        if layout_mode == LayoutMode::HashGroup
            && (self.hash_group_bits == 0
                || self.hash_group_partition_fanout == 0
                || !self.hash_group_partition_fanout.is_power_of_two()
                || self.hash_group_partition_fanout > (1u32 << self.hash_group_bits))
        {
            return Err("invalid recovery layout group fanout".into());
        }
        Ok(LayoutDescriptor {
            layout_mode,
            shard_bits: self.shard_bits,
            hash_group_bits: self.hash_group_bits,
            hash_algo,
            hash_group_key,
            hash_group_partition_fanout: self.hash_group_partition_fanout,
            layout_epoch: self.layout_epoch,
            packaging,
        })
    }
}

impl RecoveryDescriptorV1 {
    pub fn new(
        store_uuid: &str,
        db_hash: &str,
        layout: LayoutDescriptor,
        manifest_sha256: &str,
        counter: u64,
        epoch: u64,
    ) -> Result<Self, String> {
        Self::new_with_mode(
            "s0_only",
            store_uuid,
            db_hash,
            layout,
            manifest_sha256,
            counter,
            epoch,
        )
    }

    /// Describe one normal `backup/latest` cut, including its exact layout.
    pub fn new_normal(
        store_uuid: &str,
        db_hash: &str,
        layout: LayoutDescriptor,
        manifest_sha256: &str,
        counter: u64,
        epoch: u64,
    ) -> Result<Self, String> {
        Self::new_with_mode(
            "replay_tail",
            store_uuid,
            db_hash,
            layout,
            manifest_sha256,
            counter,
            epoch,
        )
    }

    fn new_with_mode(
        mode: &str,
        store_uuid: &str,
        db_hash: &str,
        layout: LayoutDescriptor,
        manifest_sha256: &str,
        counter: u64,
        epoch: u64,
    ) -> Result<Self, String> {
        let descriptor = Self {
            version: DESCRIPTOR_VERSION,
            mode: mode.into(),
            store_uuid: store_uuid.to_string(),
            db_hash: db_hash.to_string(),
            layout: RecoveryLayoutV1::from_laststore(layout),
            manifest_sha256: manifest_sha256.to_string(),
            counter,
            epoch,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != DESCRIPTOR_VERSION
            || (self.mode != "s0_only" && self.mode != "replay_tail")
            || self.store_uuid.is_empty()
            || self.store_uuid.len() > 128
            || self.store_uuid.contains('/')
            || self.store_uuid.contains("..")
            || self.store_uuid.bytes().any(|byte| byte.is_ascii_control())
            || !crate::hex::is_lower_hex_sha256(&self.db_hash)
            || self.db_hash != cloud_db_hash_for_store_uuid(&self.store_uuid)
            || !crate::hex::is_lower_hex_sha256(&self.manifest_sha256)
            || self.counter == 0
        {
            return Err("invalid recovery descriptor identity".into());
        }
        self.layout.to_laststore()?;
        Ok(())
    }

    fn aad(&self) -> Vec<u8> {
        format!(
            "lastdb-recovery-v1:{}:{}",
            self.db_hash, self.manifest_sha256
        )
        .into_bytes()
    }

    /// Seal a descriptor and name it by its encrypted bytes. Store the object
    /// before the immutable rescue S0 commit for this manifest.
    pub fn seal(&self, e2e_key: &[u8; 32]) -> Result<(String, Vec<u8>), String> {
        self.validate()?;
        let plaintext = serde_json::to_vec(self)
            .map_err(|error| format!("encode recovery descriptor: {error}"))?;
        let ciphertext = encrypt_envelope_v2(e2e_key, KEY_ID, &plaintext, &self.aad())
            .map_err(|error| format!("encrypt recovery descriptor: {error}"))?;
        let ciphertext_sha = sha256_hex(&ciphertext);
        let name = format!(
            "{NAME_PREFIX}{}-{}-{ciphertext_sha}{NAME_SUFFIX}",
            self.db_hash, self.manifest_sha256
        );
        Ok((name, ciphertext))
    }

    /// Authenticate the object name and payload before using any layout field.
    pub fn open(name: &str, ciphertext: &[u8], e2e_key: &[u8; 32]) -> Result<Self, String> {
        let (db_hash, manifest_sha256, ciphertext_sha) = parse_name(name)?;
        if sha256_hex(ciphertext) != ciphertext_sha {
            return Err("recovery descriptor ciphertext hash mismatch".into());
        }
        let aad = format!("lastdb-recovery-v1:{db_hash}:{manifest_sha256}");
        let plaintext = decrypt_envelope_v2(e2e_key, ciphertext, aad.as_bytes())
            .map_err(|error| format!("decrypt recovery descriptor: {error}"))?;
        let descriptor: Self = serde_json::from_slice(&plaintext)
            .map_err(|error| format!("decode recovery descriptor: {error}"))?;
        descriptor.validate()?;
        if descriptor.db_hash != db_hash || descriptor.manifest_sha256 != manifest_sha256 {
            return Err("recovery descriptor name does not match payload".into());
        }
        Ok(descriptor)
    }

    /// Match the authenticated cloud pointer before restoring any S0 chunk.
    pub fn validate_latest(&self, latest: &BackupLatestPointer) -> Result<(), String> {
        self.validate()?;
        if latest.store_uuid != self.store_uuid
            || latest.manifest_sha256 != self.manifest_sha256
            || latest.counter != self.counter
            || latest.epoch != self.epoch
        {
            return Err("recovery descriptor does not match the latest cloud backup".into());
        }
        Ok(())
    }

    /// Return exact placement options for a fresh destination.
    pub fn to_options(
        &self,
        e2e_key: &[u8; 32],
        frame_aead_opt_in: bool,
    ) -> Result<LastStoreOptions, String> {
        self.validate()?;
        let layout = self.layout.to_laststore()?;
        let mut opts = match layout.layout_mode {
            LayoutMode::SegmentLog => LastStoreOptions::segment_log(),
            LayoutMode::HashGroup => LastStoreOptions::hash_group(),
        };
        opts.shard_bits = layout.shard_bits;
        opts.hash_group_bits = layout.hash_group_bits;
        opts.hash_algo = layout.hash_algo;
        opts.hash_group_key = layout.hash_group_key;
        opts.hash_group_partition_fanout = layout.hash_group_partition_fanout;
        opts.layout_epoch = layout.layout_epoch;
        opts.packaging = layout.packaging;
        match layout.packaging {
            PackagingMode::Plain if frame_aead_opt_in => {
                return Err("plain recovery layout conflicts with frame AEAD opt-in".into());
            }
            PackagingMode::Plain => opts.data_key = None,
            PackagingMode::FrameAead if !frame_aead_opt_in => {
                return Err("frame AEAD recovery layout requires opt-in".into());
            }
            PackagingMode::FrameAead => {
                opts.data_key = Some(*e2e_key);
                opts.hash_group_key_sidecar = false;
            }
        }
        Ok(opts)
    }

    /// Parse the public name without downloading any bytes.
    pub fn identity_from_name(name: &str) -> Result<(&str, &str), String> {
        let (db_hash, manifest_sha256, _) = parse_name(name)?;
        Ok((db_hash, manifest_sha256))
    }
}

fn parse_name(name: &str) -> Result<(&str, &str, &str), String> {
    let stem = name
        .strip_prefix(NAME_PREFIX)
        .and_then(|stem| stem.strip_suffix(NAME_SUFFIX))
        .ok_or_else(|| "invalid recovery descriptor object name".to_string())?;
    let parts = stem.split('-').collect::<Vec<_>>();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| !crate::hex::is_lower_hex_sha256(part))
    {
        return Err("invalid recovery descriptor object name".into());
    }
    Ok((parts[0], parts[1], parts[2]))
}
