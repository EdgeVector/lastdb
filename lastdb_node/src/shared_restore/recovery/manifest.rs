//! Encrypted shared checkpoint inventory, bound to the public commitment.

use super::{stream_id, MAX_MANIFEST_BYTES};
use crate::byte_cloud_restore::{RestoreError, Result};
use cloud_backup_protocol::shared::{RecoveryView, SealedObject, SnapshotManifest};
use cloud_backup_protocol::{Id, RelativePath};
use fold_db::crypto::{decrypt_envelope, encrypt_envelope};
use fold_db::portable_mutation::{MutationEntry, StreamId, StreamProgress};
use fold_db::portable_replay::{encode_log_range, LogRangeId, VerifiedLogRange};
use fold_db::storage::disk_pack::{
    byte_backup::{run_backup_destination, RunBackupProfile},
    HOME_FORMAT,
};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;
use zeroize::Zeroizing;

use crate::shared_restore::RestoreCut;

const MANIFEST_ROLE: &[u8] = b"snapshot-manifest";
const LOG_ROLE: &[u8] = b"mutation-log";
const ENVELOPE_SALT: &[u8] = b"lastdb:shared-cloud:v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotFile {
    /// Relative to the recoverable native data root.
    pub path: RelativePath,
    pub object: SealedObject,
}

/// Private plaintext of the checkpoint manifest. The public cloud protocol
/// commits to the sealed bytes and exact object inventory.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotContents {
    pub version: u8,
    pub database_id: Id,
    pub snapshot_id: Id,
    pub disk_format: u32,
    pub native_profile: RunBackupProfile,
    pub progress: BTreeMap<StreamId, StreamProgress>,
    pub delete_reclaimed_through_ms: Option<u64>,
    pub files: Vec<SnapshotFile>,
}

impl SnapshotContents {
    pub fn seal(&self, content_key: &[u8; 32]) -> Result<Vec<u8>> {
        self.validate()?;
        let plaintext = Zeroizing::new(
            serde_json::to_vec(self)
                .map_err(|_| RestoreError::Invalid("snapshot manifest JSON"))?,
        );
        let sealed = seal(content_key, &self.database_id, MANIFEST_ROLE, &plaintext)?;
        if sealed.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(RestoreError::Limit("shared snapshot manifest"));
        }
        Ok(sealed)
    }

    pub(super) fn open(
        sealed: &[u8],
        manifest: &SnapshotManifest,
        content_key: &[u8; 32],
    ) -> Result<Self> {
        let plaintext = open(content_key, &manifest.database_id, MANIFEST_ROLE, sealed)?;
        let contents: Self = serde_json::from_slice(&plaintext)
            .map_err(|_| RestoreError::Invalid("snapshot manifest JSON"))?;
        let canonical = Zeroizing::new(
            serde_json::to_vec(&contents)
                .map_err(|_| RestoreError::Invalid("snapshot manifest JSON"))?,
        );
        if *canonical != *plaintext {
            return Err(RestoreError::Invalid("noncanonical snapshot manifest"));
        }
        contents.validate()?;
        if contents.database_id != manifest.database_id
            || contents.snapshot_id != manifest.snapshot_id
            || contents.disk_format != manifest.disk_format
        {
            return Err(RestoreError::Invalid("snapshot manifest scope"));
        }
        let mut objects: Vec<_> = contents
            .files
            .iter()
            .map(|file| file.object.clone())
            .collect();
        objects.sort_unstable_by(|left, right| left.version_id.cmp(&right.version_id));
        manifest.validate_inventory(&objects)?;
        contents.validate_covers(manifest)?;
        Ok(contents)
    }

    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.disk_format != HOME_FORMAT {
            return Err(RestoreError::Invalid("shared snapshot disk format"));
        }
        self.native_profile
            .validate()
            .map_err(RestoreError::native_proof)?;
        if self.files.is_empty()
            || self.files.len() > cloud_backup_protocol::limits::NATIVE_FILES_MAX
            || self.progress.len() > cloud_backup_protocol::shared::MAX_WRITERS
        {
            return Err(RestoreError::Limit("shared snapshot inventory"));
        }
        if self
            .files
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        {
            return Err(RestoreError::Invalid("ordered snapshot paths"));
        }
        for file in &self.files {
            file.object.validate()?;
            run_backup_destination(file.path.as_str()).map_err(RestoreError::native_proof)?;
        }
        for cursor in self.progress.values() {
            cursor
                .validate()
                .map_err(|_| RestoreError::Invalid("checkpoint cursor"))?;
            if cursor.uploaded_through.is_some() {
                return Err(RestoreError::Invalid("device-local checkpoint progress"));
            }
        }
        Ok(())
    }

    fn validate_covers(&self, manifest: &SnapshotManifest) -> Result<()> {
        let covered = self
            .progress
            .values()
            .filter(|cursor| cursor.applied_through != 0)
            .count();
        if covered != manifest.covers.len() {
            return Err(RestoreError::Invalid("checkpoint cover count"));
        }
        for cover in &manifest.covers {
            let source = stream_id(&cover.source_stream)?;
            if self
                .progress
                .get(&source)
                .is_none_or(|cursor| cursor.applied_through != cover.through.sequence.get())
            {
                return Err(RestoreError::Invalid("checkpoint cover position"));
            }
        }
        Ok(())
    }

    pub(super) fn cut(&self, view: &RecoveryView) -> Result<RestoreCut> {
        let captured_heads = view
            .streams
            .iter()
            .map(|cut| {
                let sequence = cut.head.as_ref().map_or(0, |head| head.sequence.get());
                Ok((stream_id(&cut.source_stream)?, sequence))
            })
            .collect::<Result<_>>()?;
        Ok(RestoreCut {
            captured_heads,
            progress: self.progress.clone(),
            delete_reclaimed_through_ms: self.delete_reclaimed_through_ms,
        })
    }
}

/// Seal one canonical mutation range with its full source identity and positions.
pub fn seal_log(
    content_key: &[u8; 32],
    database: &Id,
    id: &LogRangeId,
    entries: &[MutationEntry],
) -> Result<Vec<u8>> {
    let plaintext = Zeroizing::new(
        encode_log_range(id, entries).map_err(|_| RestoreError::Invalid("shared mutation log"))?,
    );
    seal(content_key, database, LOG_ROLE, &plaintext)
}

pub(super) fn open_log(
    content_key: &[u8; 32],
    database: &Id,
    id: LogRangeId,
    sealed: &[u8],
) -> Result<VerifiedLogRange> {
    let plaintext = open(content_key, database, LOG_ROLE, sealed)?;
    VerifiedLogRange::decode(id, &plaintext)
        .map_err(|_| RestoreError::Invalid("shared mutation log"))
}

fn seal(content_key: &[u8; 32], database: &Id, role: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    let key = envelope_key(content_key, database, role)?;
    encrypt_envelope(&key, plaintext).map_err(|_| RestoreError::Invalid("shared object encryption"))
}

fn open(
    content_key: &[u8; 32],
    database: &Id,
    role: &[u8],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let key = envelope_key(content_key, database, role)?;
    decrypt_envelope(&key, sealed)
        .map(Zeroizing::new)
        .map_err(|_| RestoreError::Invalid("shared object authentication"))
}

fn envelope_key(content_key: &[u8; 32], database: &Id, role: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0; 32]);
    // The canonical database ID has exactly 36 ASCII bytes.
    let info = [database.as_str().as_bytes(), role].concat();
    Hkdf::<Sha256>::new(Some(ENVELOPE_SALT), content_key)
        .expand(&info, &mut *key)
        .map_err(|_| RestoreError::Invalid("shared object encryption context"))?;
    Ok(key)
}
