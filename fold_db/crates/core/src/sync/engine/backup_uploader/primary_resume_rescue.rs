//! Accept only the committed S0 rescue chunks at a fresh local root cut.

use super::*;
use crate::sync::auth::ops::RescueS0Pointer;
use crate::sync::auth::S3ObjectInfo;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MAX_FRESH_CLOUD_CHUNKS: usize = 100_000;

/// This proof can only come from an empty complete list or an exact S0 check.
pub(super) struct FreshCloudProof {
    allowed_shas: BTreeSet<String>,
}

impl FreshCloudProof {
    fn new(allowed_shas: BTreeSet<String>) -> Self {
        Self { allowed_shas }
    }

    pub(super) fn matches(&self, presence: &CloudChunkPresence) -> bool {
        presence.listing_complete && self.allowed_shas == presence.present_shas
    }
}

fn require_exact_rescue_chunks(
    pointer: &RescueS0Pointer,
    manifest: &BackupManifest,
    listed: &[S3ObjectInfo],
) -> SyncResult<CloudChunkPresence> {
    pointer.validate()?;
    if manifest.version != 1
        || manifest
            .atom_chunks
            .iter()
            .chain(&manifest.mutable_chunks)
            .any(|chunk| chunk.pack.is_some())
    {
        return Err(SyncError::Storage(
            "S0 rescue supports direct file manifests only".into(),
        ));
    }
    if manifest.store_uuid != pointer.store_uuid
        || manifest.epoch != pointer.epoch
        || manifest.counter != pointer.counter
        || cloud_db_hash_for_store_uuid(&manifest.store_uuid) != pointer.db_hash
        || !manifest.b2_cas_blob_refs.is_empty()
        || !manifest.named_holes.is_empty()
    {
        return Err(SyncError::Storage(
            "S0 rescue manifest identity or scope mismatch".into(),
        ));
    }
    validate_manifest_chain(None, manifest).map_err(|error| {
        SyncError::Storage(format!("S0 rescue is not an independent root: {error}"))
    })?;

    let mut expected = BTreeMap::new();
    for chunk in manifest.mutable_chunks.iter().chain(&manifest.atom_chunks) {
        if !crate::hex::is_lower_hex_sha256(&chunk.sha256)
            || chunk.bytes == 0
            || chunk.instance.is_some()
        {
            return Err(SyncError::Storage(
                "S0 rescue has an invalid chunk reference".into(),
            ));
        }
        if expected
            .get(&chunk.sha256)
            .is_some_and(|bytes| *bytes != chunk.bytes)
        {
            return Err(SyncError::Storage(
                "S0 rescue repeats a chunk hash with another size".into(),
            ));
        }
        expected.insert(chunk.sha256.clone(), chunk.bytes);
    }
    let mut found = BTreeMap::new();
    for object in listed {
        let Some(sha) = super::super::backup_keys::v1_chunk_sha(&object.key) else {
            return Err(SyncError::Storage(
                "fresh cloud backup found a non-S0 chunk key".into(),
            ));
        };
        if !crate::hex::is_lower_hex_sha256(sha)
            || found.insert(sha.to_string(), object.size).is_some()
        {
            return Err(SyncError::Storage(
                "fresh cloud backup found an invalid or repeated chunk key".into(),
            ));
        }
    }
    if found != expected {
        return Err(SyncError::Storage(
            "fresh cloud backup chunks differ from committed S0".into(),
        ));
    }
    Ok(CloudChunkPresence::from_complete_listing(found.into_keys()))
}

impl SyncEngine {
    pub(super) async fn verified_fresh_cloud_presence(
        &self,
    ) -> SyncResult<(CloudChunkPresence, FreshCloudProof)> {
        let source = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage("fresh cloud backup requires a LastStore source".into())
        })?;
        let db_hash = source
            .cloud_db_hash()
            .map_err(|error| SyncError::Storage(format!("read local cloud identity: {error}")))?
            .ok_or_else(|| {
                SyncError::Storage("fresh cloud backup lacks a local cloud identity".into())
            })?;
        let rescues: Vec<_> = self
            .auth
            .rescue_s0_list()
            .await?
            .into_iter()
            .filter(|pointer| pointer.db_hash == db_hash)
            .collect();
        let listed = self
            .auth
            .list_db_objects_at_most(&db_hash, "backup/chunks/", MAX_FRESH_CLOUD_CHUNKS)
            .await?;
        if rescues.is_empty() && listed.is_empty() {
            let empty = CloudChunkPresence::from_complete_listing(BTreeSet::new());
            return Ok((empty, FreshCloudProof::new(BTreeSet::new())));
        }
        let [pointer] = rescues.as_slice() else {
            return Err(SyncError::Storage(
                "fresh cloud backup requires one committed S0 pointer".into(),
            ));
        };
        if self.auth.rescue_s0_get(&pointer.manifest_sha256).await? != *pointer {
            return Err(SyncError::Storage(
                "S0 rescue pointer changed during preflight".into(),
            ));
        }
        // This normal upload presign checks the hold, committed marker, and
        // pointer before its HEAD. No PUT URL is used by this read-only proof.
        if !self
            .auth
            .require_backup_manifest_present(&pointer.manifest_sha256)
            .await?
        {
            return Err(SyncError::Storage("committed S0 manifest is absent".into()));
        }
        let url = self
            .auth
            .presign_backup_manifest_download(&pointer.manifest_sha256)
            .await?;
        let bytes = self
            .s3
            .download_limited(&url, Some(16 * 1024 * 1024))
            .await?
            .ok_or_else(|| SyncError::Storage("S0 rescue manifest is absent".into()))?;
        if format!("{:x}", Sha256::digest(&bytes)) != pointer.manifest_sha256 {
            return Err(SyncError::Crypto("S0 rescue manifest hash mismatch".into()));
        }
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| SyncError::Storage(format!("decode S0 rescue manifest: {error}")))?;
        if manifest_sha256_hex(&manifest)
            .map_err(|error| SyncError::Storage(format!("hash S0 rescue manifest: {error}")))?
            != pointer.manifest_sha256
        {
            return Err(SyncError::Storage(
                "S0 rescue manifest is not canonical".into(),
            ));
        }
        let presence = require_exact_rescue_chunks(pointer, &manifest, &listed)?;
        let proof = FreshCloudProof::new(presence.present_shas.clone());
        Ok((presence, proof))
    }
}
