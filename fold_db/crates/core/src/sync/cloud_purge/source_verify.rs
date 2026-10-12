//! Retained source verification: each check covers one part of the receipt.

use super::files;
use super::RetainedSource;
use crate::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, validate_manifest_chain,
};
use crate::sync::error::{SyncError, SyncResult};
use std::collections::BTreeSet;

impl RetainedSource {
    /// The receipt has a known format and lists exactly the files it carries.
    pub(super) fn verify_receipt_shape(&self) -> SyncResult<()> {
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
        Ok(())
    }

    /// The manifest hashes to the prepared digest and starts a fresh chain.
    pub(super) fn verify_manifest_identity(&self) -> SyncResult<()> {
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
        Ok(())
    }

    /// Every manifest chunk appears in the prepared object inventory.
    pub(super) fn verify_chunk_inventory(&self) -> SyncResult<()> {
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
        Ok(())
    }

    /// The manifest, layout and (for orgs) catalog objects each appear once.
    pub(super) fn verify_recovery_inventory(&self) -> SyncResult<()> {
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
        Ok(())
    }

    /// Object identities are unique and each file matches its recorded size and digest.
    pub(super) fn verify_file_identities(&self) -> SyncResult<()> {
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
}
