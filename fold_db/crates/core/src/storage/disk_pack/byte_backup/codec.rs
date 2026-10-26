//! Mini-owned encrypted metadata. The byte service never interprets these values.
//!
//! A backup of a disk format v2 home has version 3 metadata: a run profile in
//! the root and run file paths in the pages. Versions 1 and 2 described a
//! format v1 home, and this program refuses them.
use super::{invalid, protocol, StorageResult};
use cloud_backup_protocol::v2::Begin;
use cloud_backup_protocol::{
    DecimalU64, Digest, ExternalFileDependency, NativeRoot, RecoveryDescriptor, RelativePath,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const CODEC_PLAINTEXT_MAX_BYTES: u64 = 256 * 1024;
pub const CODEC_ENVELOPE_MAX_BYTES: u64 = CODEC_PLAINTEXT_MAX_BYTES + 29;
/// The metadata version of a run backup.
pub const CLIENT_VERSION: u16 = 3;
pub const MAX_CLIENT_PAGES: usize = 1024;
pub const MAX_PAGE_FILES: usize = 128;
pub const MAX_FILE_EXTENTS: usize = 16384;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientNativeFile {
    pub path: RelativePath,
    pub native_sha256: Digest,
    pub native_bytes: DecimalU64,
    pub extents: Vec<ClientExtent>,
}
impl ClientNativeFile {
    pub fn validate(&self) -> StorageResult<()> {
        if self.native_bytes.get() == 0
            || self.extents.is_empty()
            || self.extents.len() > MAX_FILE_EXTENTS
        {
            return Err(invalid("byte native file extent count"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            if extent.length.get() == 0
                || extent
                    .offset
                    .get()
                    .checked_add(extent.length.get())
                    .is_none()
            {
                return Err(invalid("byte native extent bounds"));
            }
            bytes = bytes
                .checked_add(extent.length.get())
                .ok_or_else(|| invalid("byte native extent overflow"))?;
        }
        if bytes != self.native_bytes.get() {
            return Err(invalid("byte native file exact length"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPageReference {
    pub page_index: u32,
    pub receipt_id: Digest,
    pub sha256: Digest,
    pub bytes: DecimalU64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPage {
    pub version: u16,
    pub page_index: u32,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub native_files: Vec<ClientNativeFile>,
    pub external_dependencies: Vec<ExternalFileDependency>,
}
impl ClientPage {
    pub fn validate_for(&self, begin: &Begin) -> StorageResult<()> {
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        if self.page_index as usize >= MAX_CLIENT_PAGES
            || self.native_files.is_empty()
            || self.native_files.len() > MAX_PAGE_FILES
            || !self.external_dependencies.is_empty()
        {
            return Err(invalid("byte page unsupported entries or bounds"));
        }
        let mut previous = None;
        for file in &self.native_files {
            file.validate()?;
            super::run_backup_destination(file.path.as_str())?;
            if previous.is_some_and(|path| path >= file.path.as_str()) {
                return Err(invalid("byte page path order"));
            }
            previous = Some(file.path.as_str());
        }
        encode(self).map(|_| ())
    }
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let value: Self = decode(bytes, key)?;
        value.validate_for(begin)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientRoot {
    pub version: u16,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub native_root: NativeRoot,
    pub recovery: RecoveryDescriptor,
    pub native_file_count: u64,
    pub native_bytes: u64,
    pub external_dependency_count: u64,
    pub pages: Vec<ClientPageReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<super::RunBackupProfile>,
}
impl ClientRoot {
    pub fn validate_for(&self, begin: &Begin) -> StorageResult<()> {
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        let profile = self
            .run
            .as_ref()
            .ok_or_else(|| invalid("run root profile missing"))?;
        profile.validate_root(&self.native_root)?;
        self.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(protocol)?;
        if self.native_file_count == 0
            || self.native_bytes == 0
            || self.pages.is_empty()
            || self.pages.len() > MAX_CLIENT_PAGES
            || self.native_file_count > (self.pages.len() * MAX_PAGE_FILES) as u64
            || self.external_dependency_count != 0
        {
            return Err(invalid("byte root identity or bounds"));
        }
        let files = profile
            .main
            .files
            .checked_add(profile.cas.files)
            .and_then(|n| n.checked_add(1));
        let bytes = profile
            .main
            .bytes
            .checked_add(profile.cas.bytes)
            .and_then(|n| n.checked_add(super::super::run_node::RUN_NODE_BYTES.len() as u64));
        if files != Some(self.native_file_count) || bytes != Some(self.native_bytes) {
            return Err(invalid("run root exact native totals"));
        }
        for (index, page) in self.pages.iter().enumerate() {
            if page.page_index as usize != index
                || page.bytes.get() <= 29
                || page.bytes.get() > CODEC_ENVELOPE_MAX_BYTES
            {
                return Err(invalid("byte root page bounds"));
            }
        }
        encode(self).map(|_| ())
    }
    pub fn validate_page(&self, page: &ClientPage) -> StorageResult<()> {
        if page.begin_sha256 != self.begin_sha256
            || page.version != self.version
            || page.descriptor_sha256 != self.descriptor_sha256
            || self.pages.get(page.page_index as usize).is_none()
        {
            return Err(invalid("byte root page context"));
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let value: Self = decode(bytes, key)?;
        value.validate_for(begin)?;
        Ok(value)
    }
}
fn context(version: u16, digest: &Digest, descriptor: &Digest, begin: &Begin) -> StorageResult<()> {
    begin.validate().map_err(protocol)?;
    if version != CLIENT_VERSION
        || !begin.descriptor.as_value().is_null()
        || *digest != begin.input_sha256().map_err(protocol)?
        || *descriptor != begin.descriptor.commitment().map_err(protocol)?
    {
        return Err(invalid("byte metadata frozen context"));
    }
    Ok(())
}
pub(super) fn encode(value: &impl Serialize) -> StorageResult<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("byte metadata encoding"))?;
    if bytes.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(invalid("byte metadata bound"));
    }
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(bytes: &[u8], key: &[u8; 32]) -> StorageResult<T> {
    if bytes.len() as u64 > CODEC_ENVELOPE_MAX_BYTES {
        return Err(invalid("byte metadata envelope bound"));
    }
    let plain = zeroize::Zeroizing::new(
        crate::crypto::decrypt_envelope(key, bytes)
            .map_err(|_| invalid("byte metadata encryption"))?,
    );
    if plain.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(invalid("byte metadata bound"));
    }
    serde_json::from_slice(&plain).map_err(|_| invalid("byte metadata format"))
}
