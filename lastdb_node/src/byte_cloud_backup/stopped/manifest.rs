use super::*;
use cloud_backup_protocol::v2::Begin;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::{ByteBackupObjectManifest, RunBackupProfile};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub version: u16,
    pub begin: Begin,
    pub run_profile: RunBackupProfile,
    pub native_files: u64,
    pub native_bytes: u64,
    pub objects: u64,
    pub descriptors_sha256: Digest,
}

pub(in crate::byte_cloud_backup) fn retain_manifest(
    capture: &Capture,
    store: &Store,
) -> Result<()> {
    let Capture::Snapshot(snapshot) = capture else {
        return Err(BackupError::Invalid);
    };
    let status = snapshot.status()?;
    let mut hash = Sha256::new();
    hash.update(b"lastdb:stopped-byte-source-manifest:v1\0");
    for index in 0..snapshot.object_count()? {
        let object = snapshot.object_manifest(index)?;
        let raw = serde_json::to_vec(&object).map_err(|_| BackupError::Invalid)?;
        hash.update(index.to_be_bytes());
        hash.update((raw.len() as u64).to_be_bytes());
        hash.update(&raw);
        same(store, &object_name(index), &object)?;
    }
    same(
        store,
        "stopped-source.json",
        &Manifest {
            version: 1,
            begin: snapshot.begin()?,
            run_profile: snapshot.run_profile()?,
            native_files: status.native_files,
            native_bytes: status.native_bytes,
            objects: status.objects,
            descriptors_sha256: Digest::try_from(fold_db::hex::hex_lower(hash.finalize()))?,
        },
    )
}

pub(super) fn object_name(index: u64) -> String {
    format!("stopped-object-{index:08}.json")
}

pub(super) fn same<T: Serialize>(store: &Store, name: &str, value: &T) -> Result<()> {
    let encoded = serde_json::to_value(value).map_err(|_| BackupError::Invalid)?;
    if let Some(previous) = store.read::<serde_json::Value>(name)? {
        if previous != encoded {
            return Err(BackupError::Identity);
        }
        return Ok(());
    }
    store.write(name, value)
}

pub(super) fn object(store: &Store, index: u64) -> Result<ByteBackupObjectManifest> {
    store.read(&object_name(index))?.ok_or(BackupError::Missing)
}
