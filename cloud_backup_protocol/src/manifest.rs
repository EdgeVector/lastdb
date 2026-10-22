//! Authenticated manifest plaintext. Native files retain their stored bytes.

use crate::limits::*;
use crate::transfer::ObjectReceipt;
use crate::{
    canonical_bytes, BackupStream, CloudScope, DecimalU64, Digest, Id, ObjectEncoding, ObjectKind,
    ProtocolError, ReferenceRoot, RelativePath, Text, Version,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// The current native reader supports exactly these CURRENT feature bits.
pub const NATIVE_SUPPORTED_FEATURES_V1: u64 = 0xff;
pub const NATIVE_REQUIRED_FEATURES_V1: u64 = 0xc7;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDirectoryRoot {
    pub object: Id,
    pub page: DecimalU64,
    pub height: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalKeyFormat {
    pub key: Text<32>,
    pub value: Text<1>,
}

/// Mirrors exact CURRENT and home-descriptor bytes; the importer verifies both.
/// This version contains a full root, never an incremental journal suffix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoot {
    pub home_id: Id,
    pub home_digest: Digest,
    pub current_sha256: Digest,
    pub generation: DecimalU64,
    pub previous_generation: DecimalU64,
    pub frontier: DecimalU64,
    pub journal_frontier: DecimalU64,
    pub directory: NativeDirectoryRoot,
    pub journal_stream: Id,
    pub published_ms: DecimalU64,
    pub required_features: DecimalU64,
    pub placement_codec: u16,
    pub key_codec: u16,
    pub layout_epoch: u32,
    pub hash_fanout: u16,
    pub logical_key_format: LogicalKeyFormat,
}

impl NativeRoot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let features = self.required_features.get();
        if self.generation.get() == 0
            || self.previous_generation.get() >= self.generation.get()
            || self.frontier != self.journal_frontier
            || self.frontier.get() == u64::MAX
            || !(1..=8).contains(&self.directory.height)
            || self.directory.page.get() / 512 > u64::from(u32::MAX)
            || features & !NATIVE_SUPPORTED_FEATURES_V1 != 0
            || features & NATIVE_REQUIRED_FEATURES_V1 != NATIVE_REQUIRED_FEATURES_V1
            || self.home_digest.as_str().bytes().all(|byte| byte == b'0')
            || self.placement_codec != 0x0101
            || self.key_codec != 0x0101
            || !self.hash_fanout.is_power_of_two()
            || self.hash_fanout > 16
            || self.logical_key_format.key.as_str() != "layout:v3:keys"
            || self.logical_key_format.value.as_str() != "4"
        {
            return Err(ProtocolError::invalid("native_root"));
        }
        Ok(())
    }
}

/// Receipt identities select verified objects, never credentials or signed URLs.
/// The complete reference inventory must also verify bounds against that object.
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
    pub creation_generation: DecimalU64,
    pub extent: ObjectExtent,
}

impl NativeFileEntry {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.extent.validate()?;
        if self.native_bytes.get() == 0
            || self.native_bytes.get() > NATIVE_PART_MAX_BYTES
            || self.native_bytes != self.extent.length
            || self.creation_generation.get() == 0
        {
            return Err(ProtocolError::invalid("native_file"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDependencyKind {
    Main,
    Thumbnail,
    Poster,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileDependencySource {
    /// The service must validate the exact enforceable hold before publication.
    Retained {
        receipt: Box<ObjectReceipt>,
        retention_hold_id: Id,
    },
    /// Concatenation in this order reproduces the complete stored source bytes.
    BackupExtents { extents: Vec<ObjectExtent> },
}

/// No raw DEK appears here. The key reference commits to access metadata in the
/// existing encrypted pointer; restore uses that pointer's existing key format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalFileDependency {
    pub kind: FileDependencyKind,
    pub logical_pointer_sha256: Digest,
    pub stored_source_sha256: Digest,
    pub stored_source_bytes: DecimalU64,
    pub owner_scope: CloudScope,
    pub encryption_suite: Text<128>,
    pub encryption_key_reference: Digest,
    pub source: FileDependencySource,
}

impl ExternalFileDependency {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.owner_scope.validate()?;
        if self.stored_source_bytes.get() == 0
            || self.stored_source_bytes.get() > BACKUP_STORED_BYTES_MAX
            || !matches!(
                self.encryption_suite.as_str(),
                "lastdb-file-blob-convergent-v1:aes-256-gcm"
                    | "lastdb-file-blob-dek-v1:aes-256-gcm"
            )
        {
            return Err(ProtocolError::invalid("file_dependency.bytes"));
        }
        match &self.source {
            FileDependencySource::Retained { receipt, .. } => {
                receipt.validate()?;
                if receipt.scope != self.owner_scope
                    || receipt.object.sha256 != self.stored_source_sha256
                    || receipt.object.bytes != self.stored_source_bytes
                    || receipt.object.kind != ObjectKind::BlobPart
                    || receipt.object.encoding != ObjectEncoding::BlobStoredV1
                {
                    return Err(ProtocolError::invalid("file_dependency.retained"));
                }
            }
            FileDependencySource::BackupExtents { extents } => {
                if extents.is_empty() || extents.len() > MANIFEST_PAGE_ENTRIES_MAX {
                    return Err(ProtocolError::invalid("file_dependency.extents"));
                }
                let mut bytes = 0u64;
                for extent in extents {
                    extent.validate()?;
                    bytes = bytes
                        .checked_add(extent.length.get())
                        .ok_or_else(|| ProtocolError::invalid("file_dependency.extent_bytes"))?;
                }
                if bytes != self.stored_source_bytes.get() {
                    return Err(ProtocolError::invalid("file_dependency.extent_bytes"));
                }
            }
        }
        Ok(())
    }
}

/// Envelope v1 has no AAD. This context belongs inside authenticated plaintext.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPageContext {
    pub version: Version,
    pub stream: BackupStream,
    pub commit_id: Id,
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
        self.context.stream.validate()?;
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
    pub encrypted_page: ObjectReceipt,
}

/// Encrypted index plaintext. Its object reference root covers native files,
/// blobs, metadata and encrypted pages, but excludes this index to avoid a cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBackupManifest {
    pub version: Version,
    pub stream: BackupStream,
    pub commit_id: Id,
    pub native_root: NativeRoot,
    pub native_file_count: u32,
    pub external_dependency_count: DecimalU64,
    pub native_bytes: DecimalU64,
    pub stored_bytes: DecimalU64,
    pub pages: Vec<ManifestPageReference>,
    pub object_references: ReferenceRoot,
    pub recovery_commitment: Digest,
}

impl NativeBackupManifest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        self.native_root.validate()?;
        self.object_references.validate()?;
        let files = self.native_file_count as u64;
        let entries = files.checked_add(self.external_dependency_count.get());
        if self.stream.source_home != self.native_root.home_id
            || files == 0
            || files > NATIVE_FILES_MAX as u64
            || self.native_bytes.get() < files
            || self.native_bytes.get() > files * NATIVE_PART_MAX_BYTES
            || self.stored_bytes != self.object_references.stored_bytes
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
            page.encrypted_page.validate()?;
            if page.page_index as usize != index
                || page.encrypted_page.scope != self.stream.scope
                || page.encrypted_page.object.kind != ObjectKind::ManifestPage
                || page.encrypted_page.object.encoding != ObjectEncoding::EnvelopeV1
                || !receipts.insert(&page.encrypted_page.receipt_id)
            {
                return Err(ProtocolError::invalid("native_manifest.pages"));
            }
        }
        if canonical_bytes(self)?.len() as u64 > MANIFEST_PAGE_PLAINTEXT_MAX_BYTES {
            return Err(ProtocolError::invalid("native_manifest.bytes"));
        }
        Ok(())
    }

    /// Cross-page uniqueness, complete receipts and exact native inventory remain
    /// mandatory importer checks; one page alone cannot certify that closure.
    pub fn validate_page(&self, page: &ManifestPage) -> Result<(), ProtocolError> {
        page.validate()?;
        if page.context.stream != self.stream
            || page.context.commit_id != self.commit_id
            || page.context.current_sha256 != self.native_root.current_sha256
            || page.context.page_index as usize >= self.pages.len()
        {
            return Err(ProtocolError::invalid("manifest_page.context"));
        }
        for file in &page.native_files {
            if file.creation_generation.get() > self.native_root.generation.get()
                || (file.path.as_str() == "CURRENT"
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
        for dependency in &page.external_dependencies {
            if matches!(&dependency.source, FileDependencySource::Retained { .. })
                && dependency.owner_scope != self.stream.scope
            {
                return Err(ProtocolError::invalid("manifest_page.retained_scope"));
            }
        }
        Ok(())
    }
}
