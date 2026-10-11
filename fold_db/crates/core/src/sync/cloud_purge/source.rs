//! A saved cut owns independent source bytes; cloud carry-forward is forbidden.

use super::files;
use super::protocol::{CloudAccount, CloudPurgeObject, PreparedCloudSource};
use crate::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, validate_manifest_chain,
    BackupChunkUploadCandidate, BackupManifest,
};
use crate::sync::error::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const SOURCE_FILE: &str = "source.json";
const MAX_SOURCE_RECEIPT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedSourceFile {
    pub object: CloudPurgeObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedSource {
    pub format_version: u32,
    #[serde(default)]
    pub local_only: bool,
    pub account: CloudAccount,
    pub prepared: PreparedCloudSource,
    pub manifest: BackupManifest,
    pub files: Vec<RetainedSourceFile>,
    #[serde(skip)]
    pub directory: PathBuf,
}

/// Exact private source cut before account control creation.
pub struct CloudSourceCut<'a> {
    pub local_only: bool,
    pub source_id: String,
    pub manifest: BackupManifest,
    pub manifest_ciphertext: &'a [u8],
    pub layout_bytes: &'a [u8],
    pub candidates: Vec<BackupChunkUploadCandidate>,
    pub writer_frontier: BTreeMap<String, u64>,
    pub org_catalog_ciphertext: Option<&'a [u8]>,
    pub shares: Vec<super::RetainedShareSnapshot>,
}

impl RetainedSource {
    pub fn create(
        directory: &Path,
        account: CloudAccount,
        cut: CloudSourceCut<'_>,
    ) -> SyncResult<Self> {
        let CloudSourceCut {
            local_only,
            source_id,
            manifest,
            manifest_ciphertext,
            layout_bytes,
            candidates,
            writer_frontier,
            org_catalog_ciphertext,
            shares,
        } = cut;
        if directory.join(SOURCE_FILE).exists() {
            return Err(SyncError::Storage(
                "retained source already exists; reopen its exact cut".into(),
            ));
        }
        validate_source_cut(&manifest, local_only)?;
        files::owner_directory(directory)?;
        files::owner_directory(&directory.join("objects"))?;
        let total = source_capacity(
            &candidates,
            manifest_ciphertext,
            layout_bytes,
            org_catalog_ciphertext,
            &shares,
        );
        files::require_capacity(directory, total)?;
        let root = cloud_db_hash_for_store_uuid(&manifest.store_uuid);
        let manifest_sha256 = manifest_sha256_hex(&manifest)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        let layout_sha256 = crate::hex::sha256_hex(layout_bytes);
        let mut source = Self {
            format_version: 1,
            local_only,
            account,
            prepared: PreparedCloudSource {
                source_id,
                manifest_sha256,
                layout_sha256,
                writer_frontier,
                objects: Vec::new(),
                roots: Vec::new(),
            },
            manifest,
            files: Vec::new(),
            directory: directory.to_path_buf(),
        };
        source.copy_chunks(candidates, &root)?;
        source.add_bytes(
            &root,
            format!("backup/manifests/{}.enc", source.prepared.manifest_sha256),
            "manifest",
            manifest_ciphertext,
        )?;
        source.add_bytes(
            &root,
            format!("backup/layouts/{}.enc", source.prepared.layout_sha256),
            "layout",
            layout_bytes,
        )?;
        if let Some(metadata) = org_catalog_ciphertext {
            source.add_bytes(
                &root,
                format!("backup/metadata/{}.enc", crate::hex::sha256_hex(metadata)),
                "org_catalog",
                metadata,
            )?;
        }
        source.add_source_roots(&root, shares)?;
        source.prepared.objects = source
            .files
            .iter()
            .map(|file| file.object.clone())
            .collect();
        source.verify()?;
        files::atomic_json(&directory.join(SOURCE_FILE), &source)?;
        Ok(source)
    }

    fn copy_chunks(
        &mut self,
        candidates: Vec<BackupChunkUploadCandidate>,
        root: &str,
    ) -> SyncResult<()> {
        let expected: BTreeSet<_> = self
            .manifest
            .atom_chunks
            .iter()
            .chain(&self.manifest.mutable_chunks)
            .map(|chunk| {
                (
                    chunk.collection.clone(),
                    chunk.shard,
                    chunk.group_id,
                    chunk.chunk_uuid.clone(),
                    chunk.sha256.clone(),
                    chunk.bytes,
                )
            })
            .collect();
        let mut found = BTreeSet::new();
        let mut object_keys = BTreeSet::new();
        for candidate in candidates {
            let chunk = candidate.chunk;
            let identity = (
                chunk.collection.clone(),
                chunk.shard,
                chunk.group_id,
                chunk.chunk_uuid.clone(),
                chunk.sha256.clone(),
                chunk.bytes,
            );
            if !expected.contains(&identity) {
                continue;
            }
            let destination = files::retained_path(&self.directory, &chunk.sha256)?;
            files::copy_prefix(&candidate.path, &destination, chunk.bytes, &chunk.sha256)?;
            found.insert(identity);
            if object_keys.insert(chunk.sha256.clone()) {
                self.files.push(RetainedSourceFile {
                    object: CloudPurgeObject {
                        logical_root: root.into(),
                        relative_key: format!("backup/chunks/{}", chunk.sha256),
                        sha256: chunk.sha256,
                        bytes: chunk.bytes,
                        role: "chunk".into(),
                    },
                });
            }
        }
        if found != expected {
            return Err(SyncError::Storage(
                "source manifest contains files absent from its local candidate set".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn add_bytes(
        &mut self,
        root: &str,
        relative_key: String,
        role: &str,
        bytes: &[u8],
    ) -> SyncResult<()> {
        let sha256 = crate::hex::sha256_hex(bytes);
        let object = CloudPurgeObject {
            logical_root: root.into(),
            relative_key,
            sha256,
            bytes: bytes.len() as u64,
            role: role.into(),
        };
        let path = files::retained_path(&self.directory, &object.sha256)?;
        if path.exists() {
            files::verify_file(&path, object.bytes, &object.sha256)?;
        } else {
            files::atomic_bytes(&path, bytes)?;
        }
        self.files.push(RetainedSourceFile { object });
        Ok(())
    }

    pub fn reopen(directory: &Path) -> SyncResult<Self> {
        let path = directory.join(SOURCE_FILE);
        if std::fs::metadata(&path)?.len() > MAX_SOURCE_RECEIPT_BYTES {
            return Err(SyncError::Storage(
                "retained source receipt exceeds its byte limit".into(),
            ));
        }
        let raw = std::fs::read(path)?;
        let mut source: Self = serde_json::from_slice(&raw)
            .map_err(|error| SyncError::Storage(format!("decode retained source: {error}")))?;
        source.directory = directory.to_path_buf();
        source.verify()?;
        Ok(source)
    }

    pub fn verify(&self) -> SyncResult<()> {
        if self.format_version != 1
            || self.prepared.source_id.is_empty()
            || self.files.is_empty()
            || self.prepared.objects
                != self
                    .files
                    .iter()
                    .map(|file| file.object.clone())
                    .collect::<Vec<_>>()
        {
            return Err(SyncError::Storage("invalid retained source receipt".into()));
        }
        let digest = manifest_sha256_hex(&self.manifest)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        if digest != self.prepared.manifest_sha256
            || self.manifest.previous_manifest_sha256.is_some()
            || !self.manifest.b2_cas_blob_refs.is_empty()
            || !self.manifest.named_holes.is_empty()
        {
            return Err(SyncError::Storage(
                "retained source manifest identity differs".into(),
            ));
        }
        validate_manifest_chain(None, &self.manifest)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        let root = cloud_db_hash_for_store_uuid(&self.manifest.store_uuid);
        for chunk in self
            .manifest
            .atom_chunks
            .iter()
            .chain(&self.manifest.mutable_chunks)
        {
            if !self.prepared.objects.iter().any(|object| {
                object.role == "chunk"
                    && object.logical_root == root
                    && object.sha256 == chunk.sha256
                    && object.bytes == chunk.bytes
            }) {
                return Err(SyncError::Storage(
                    "retained source chunk inventory is incomplete".into(),
                ));
            }
        }
        if self
            .prepared
            .objects
            .iter()
            .filter(|object| object.role == "manifest")
            .count()
            != 1
            || self
                .prepared
                .objects
                .iter()
                .filter(|object| {
                    object.role == "layout" && object.sha256 == self.prepared.layout_sha256
                })
                .count()
                != 1
        {
            return Err(SyncError::Storage(
                "retained source recovery inventory is incomplete".into(),
            ));
        }
        if self.account.kind == super::protocol::CloudAccountKind::Org
            && self
                .prepared
                .objects
                .iter()
                .filter(|object| object.role == "org_catalog")
                .count()
                != 1
        {
            return Err(SyncError::Storage(
                "org source lacks its encrypted catalog inventory".into(),
            ));
        }
        self.verify_source_roots()?;
        let mut identities = BTreeSet::new();
        for file in &self.files {
            file.object.validate().map_err(SyncError::Storage)?;
            if !identities.insert((&file.object.logical_root, &file.object.relative_key)) {
                return Err(SyncError::Storage(
                    "duplicate retained source object identity".into(),
                ));
            }
            files::verify_file(
                &self.file_path(&file.object)?,
                file.object.bytes,
                &file.object.sha256,
            )?;
        }
        Ok(())
    }

    pub fn file_path(&self, object: &CloudPurgeObject) -> SyncResult<PathBuf> {
        if !self.prepared.objects.contains(object) {
            return Err(SyncError::Storage(
                "upload object is not part of the retained source".into(),
            ));
        }
        files::retained_path(&self.directory, &object.sha256)
    }
}

fn source_capacity(
    candidates: &[BackupChunkUploadCandidate],
    manifest: &[u8],
    layout: &[u8],
    catalog: Option<&[u8]>,
    shares: &[super::RetainedShareSnapshot],
) -> u64 {
    let metadata = shares.iter().fold(
        (manifest.len() as u64)
            .saturating_add(layout.len() as u64)
            .saturating_add(catalog.map_or(0, |bytes| bytes.len() as u64)),
        |sum, share| sum.saturating_add(share.ciphertext_bytes),
    );
    candidates.iter().fold(metadata, |sum, candidate| {
        sum.saturating_add(candidate.chunk.bytes)
    })
}

fn validate_source_cut(manifest: &BackupManifest, local_only: bool) -> SyncResult<()> {
    validate_manifest_chain(None, manifest)
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    if !local_only
        && manifest
            .atom_chunks
            .iter()
            .chain(&manifest.mutable_chunks)
            .any(|chunk| chunk.bytes > 5_000_000_000_000)
    {
        return Err(SyncError::Storage(
            "source chunk exceeds the provider object limit before cloud hold".into(),
        ));
    }
    if manifest.previous_manifest_sha256.is_some()
        || !manifest.b2_cas_blob_refs.is_empty()
        || !manifest.named_holes.is_empty()
    {
        return Err(SyncError::Storage(
            "replacement source depends on old cloud bytes or named holes".into(),
        ));
    }
    Ok(())
}
