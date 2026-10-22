//! Authenticated restore instructions. Native files retain their stored bytes.

use crate::limits::*;
use crate::transfer::ObjectReceipt;
use crate::{
    bytes_sha256, canonical_bytes, BackupJob, DecimalU64, Digest, Id, ObjectKind, ProtocolError,
    ReferenceRoot, RelativePath, Text, Version,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const NATIVE_SUPPORTED_FEATURES_V1: u64 = 0xff;
pub const NATIVE_REQUIRED_FEATURES_V1: u64 = 0xc7;

/// Service-visible summary. Exact CURRENT and home bytes remain authoritative
/// for directory roots, codecs, logical markers and all native format details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoot {
    pub home_id: Id,
    pub home_digest: Digest,
    pub current_sha256: Digest,
    pub generation: DecimalU64,
    pub frontier: DecimalU64,
    pub journal_frontier: DecimalU64,
    pub required_features: DecimalU64,
}

impl NativeRoot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let features = self.required_features.get();
        if self.generation.get() == 0
            || self.frontier != self.journal_frontier
            || self.frontier.get() == u64::MAX
            || features & !NATIVE_SUPPORTED_FEATURES_V1 != 0
            || features & NATIVE_REQUIRED_FEATURES_V1 != NATIVE_REQUIRED_FEATURES_V1
            || self.home_digest.as_str().bytes().all(|byte| byte == b'0')
        {
            return Err(ProtocolError::invalid("native_root"));
        }
        Ok(())
    }
}

/// The complete inventory must also verify bounds against the selected receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}

impl ObjectExtent {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.length.get() == 0
            || self
                .offset
                .get()
                .checked_add(self.length.get())
                .is_none_or(|end| end > CLOUD_OBJECT_MAX_BYTES)
        {
            return Err(ProtocolError::invalid("object_extent"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFileEntry {
    pub path: RelativePath,
    pub native_sha256: Digest,
    pub native_bytes: DecimalU64,
    pub extent: ObjectExtent,
}

impl NativeFileEntry {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.extent.validate()?;
        if self.native_bytes.get() == 0
            || self.native_bytes.get() > NATIVE_PART_MAX_BYTES
            || self.native_bytes != self.extent.length
        {
            return Err(ProtocolError::invalid("native_file"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDependencyKind {
    /// `/$lastdb_file`: blob_ref, dek and cipher_suite use the existing main access format.
    Main,
    /// `/$lastdb_file/thumbnail` with kind `image`; keys use its `access` object.
    Thumbnail,
    /// `/$lastdb_file/thumbnail` with kind `video-poster`; keys use its `access` object.
    Poster,
}

/// All extents select backup-owned objects. Authorized source access remains an
/// export requirement. Existing encrypted pointer data retains its per-file key.
/// Extents concatenate to the complete stored source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalFileDependency {
    pub kind: FileDependencyKind,
    pub logical_pointer_sha256: Digest,
    pub stored_source_sha256: Digest,
    pub stored_source_bytes: DecimalU64,
    pub encryption_suite: Text<128>,
    pub extents: Vec<ObjectExtent>,
}

/// Hash the exact decrypted whole file-pointer value, including its outer
/// `$lastdb_file` wrapper and every optional member, with RFC 8785 canonical JSON.
/// The caller validates the existing pointer format before this pure hash step.
/// Atom identifiers, atom metadata and encryption envelopes are not inputs.
pub fn logical_pointer_sha256(pointer: &serde_json::Value) -> Result<Digest, ProtocolError> {
    Ok(bytes_sha256(&canonical_bytes(pointer)?))
}

impl ExternalFileDependency {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.stored_source_bytes.get() == 0
            || self.stored_source_bytes.get() > BACKUP_STORED_BYTES_MAX
            || self.extents.is_empty()
            || self.extents.len() > MANIFEST_PAGE_ENTRIES_MAX
            || !matches!(
                self.encryption_suite.as_str(),
                "lastdb-file-blob-convergent-v1:aes-256-gcm"
                    | "lastdb-file-blob-dek-v1:aes-256-gcm"
            )
        {
            return Err(ProtocolError::invalid("file_dependency"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            extent.validate()?;
            bytes = bytes
                .checked_add(extent.length.get())
                .ok_or_else(|| ProtocolError::invalid("file_dependency.extent_bytes"))?;
        }
        if bytes != self.stored_source_bytes.get() {
            return Err(ProtocolError::invalid("file_dependency.extent_bytes"));
        }
        Ok(())
    }
}

/// Envelope v1 has no AAD. This context belongs inside authenticated plaintext.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPageContext {
    pub version: Version,
    pub job: BackupJob,
    pub current_sha256: Digest,
    pub page_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPage {
    pub context: ManifestPageContext,
    pub native_files: Vec<NativeFileEntry>,
    pub external_dependencies: Vec<ExternalFileDependency>,
}

impl ManifestPage {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.context.job.validate()?;
        let count = self
            .native_files
            .len()
            .checked_add(self.external_dependencies.len());
        if self.context.page_index as usize >= MANIFEST_PAGES_MAX
            || count.is_none_or(|count| count == 0 || count > MANIFEST_PAGE_ENTRIES_MAX)
        {
            return Err(ProtocolError::invalid("manifest_page.entries"));
        }
        let mut paths = BTreeSet::new();
        for entry in &self.native_files {
            entry.validate()?;
            if !paths.insert(&entry.path) {
                return Err(ProtocolError::invalid("manifest_page.duplicate_path"));
            }
        }
        let mut pointers = BTreeSet::new();
        for entry in &self.external_dependencies {
            entry.validate()?;
            if !pointers.insert((&entry.logical_pointer_sha256, entry.kind)) {
                return Err(ProtocolError::invalid("manifest_page.duplicate_dependency"));
            }
        }
        if canonical_bytes(self)?.len() as u64 > MANIFEST_PAGE_PLAINTEXT_MAX_BYTES {
            return Err(ProtocolError::invalid("manifest_page.bytes"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPageReference {
    pub page_index: u32,
    pub receipt_id: Digest,
}

/// The public reference root covers content and encrypted pages, but excludes
/// this encrypted index and its own public metadata to avoid a circular hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBackupManifest {
    pub version: Version,
    pub job: BackupJob,
    pub native_root: NativeRoot,
    pub native_file_count: u32,
    pub external_dependency_count: DecimalU64,
    pub native_bytes: DecimalU64,
    pub pages: Vec<ManifestPageReference>,
    pub object_references: ReferenceRoot,
    pub recovery_commitment: Digest,
}

impl NativeBackupManifest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.native_root.validate()?;
        self.object_references.validate()?;
        let files = self.native_file_count as u64;
        let entries = files.checked_add(self.external_dependency_count.get());
        if self.job.stream.source_home != self.native_root.home_id
            || files == 0
            || files > NATIVE_FILES_MAX as u64
            || self.native_bytes.get() < files
            || self.native_bytes.get() > files * NATIVE_PART_MAX_BYTES
            || self.pages.is_empty()
            || self.pages.len() > MANIFEST_PAGES_MAX
            || entries.is_none_or(|count| {
                count < self.pages.len() as u64
                    || count > (self.pages.len() * MANIFEST_PAGE_ENTRIES_MAX) as u64
            })
            || self.object_references.object_count.get() < self.pages.len() as u64
        {
            return Err(ProtocolError::invalid("native_manifest"));
        }
        let mut receipts = BTreeSet::new();
        for (index, page) in self.pages.iter().enumerate() {
            if page.page_index as usize != index || !receipts.insert(&page.receipt_id) {
                return Err(ProtocolError::invalid("native_manifest.pages"));
            }
        }
        if canonical_bytes(self)?.len() as u64 > MANIFEST_PAGE_PLAINTEXT_MAX_BYTES {
            return Err(ProtocolError::invalid("native_manifest.bytes"));
        }
        Ok(())
    }

    /// The importer first verifies and decrypts the selected index bytes.
    /// This check then binds its plaintext to that exact accepted backup.
    pub fn validate_for_accepted(
        &self,
        accepted: &crate::publication::AcceptedBackup,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        accepted.validate()?;
        if self.job != accepted.job
            || self.native_root != accepted.native_root
            || self.object_references != accepted.reference_root
            || self.recovery_commitment != accepted.recovery.commitment()?
        {
            return Err(ProtocolError::invalid("native_manifest.accepted_backup"));
        }
        Ok(())
    }

    pub fn validate_page_receipt(
        &self,
        page_index: u32,
        receipt: &ObjectReceipt,
    ) -> Result<(), ProtocolError> {
        receipt.validate()?;
        let page = self
            .pages
            .get(page_index as usize)
            .ok_or_else(|| ProtocolError::invalid("native_manifest.page_receipt.index"))?;
        if page.page_index != page_index
            || page.receipt_id != receipt.receipt_id
            || receipt.scope != self.job.stream.scope
            || receipt.object.kind != ObjectKind::ManifestPage
        {
            return Err(ProtocolError::invalid("native_manifest.page_receipt"));
        }
        Ok(())
    }

    /// The importer also verifies cross-page uniqueness, all receipt extents and
    /// the exact native inventory. A page alone cannot certify complete closure.
    pub fn validate_page(&self, page: &ManifestPage) -> Result<(), ProtocolError> {
        page.validate()?;
        if page.context.job != self.job
            || page.context.current_sha256 != self.native_root.current_sha256
            || page.context.page_index as usize >= self.pages.len()
        {
            return Err(ProtocolError::invalid("manifest_page.context"));
        }
        for file in &page.native_files {
            if (file.path.as_str() == "CURRENT"
                && (file.native_bytes.get() != 4096
                    || file.native_sha256 != self.native_root.current_sha256))
                || (file.path.as_str() == "JOURNAL_TAIL" && file.native_bytes.get() != 4096)
                || (file.path.as_str() == "laststore-pack-home-v1"
                    && (file.native_bytes.get() != 4096
                        || file.native_sha256 != self.native_root.home_digest))
            {
                return Err(ProtocolError::invalid("manifest_page.native_boundary"));
            }
        }
        Ok(())
    }
}
