//! Client-owned encrypted metadata and disk V2 file path contract.

use super::{invalid, protocol, StorageResult};
use crate::storage::cloud_backup_plan::ClientNativeFile;
use cloud_backup_protocol::v2::Begin;
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, Id, NativeRoot, RecoveryDescriptor};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const CODEC_PLAINTEXT_MAX_BYTES: u64 = 256 * 1024;
pub const CODEC_ENVELOPE_MAX_BYTES: u64 = CODEC_PLAINTEXT_MAX_BYTES + 29;
/// New snapshot metadata; independent of the cloud protocol and disk versions.
pub const CLIENT_VERSION: u16 = 8;
pub const MAX_CLIENT_PAGES: usize = 1024;
pub const MAX_PAGE_FILES: usize = 128;

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
}

impl ClientPage {
    pub fn validate_for(&self, begin: &Begin) -> StorageResult<()> {
        validate_metadata_context(
            self.version,
            CLIENT_VERSION,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        if self.page_index as usize >= MAX_CLIENT_PAGES
            || self.native_files.is_empty()
            || self.native_files.len() > MAX_PAGE_FILES
        {
            return Err(invalid("byte page bounds"));
        }
        let mut previous = None;
        for file in &self.native_files {
            file.validate()?;
            run_backup_destination(file.path.as_str())?;
            if previous.is_some_and(|path| path >= file.path.as_str()) {
                return Err(invalid("byte page path order"));
            }
            previous = Some(file.path.as_str());
        }
        encode_metadata(self).map(|_| ())
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let value: Self = decode_metadata(bytes, key)?;
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
    pub pages: Vec<ClientPageReference>,
    pub run: RunBackupProfile,
}

impl ClientRoot {
    pub fn validate_for(&self, begin: &Begin) -> StorageResult<()> {
        if !begin.recovery_dependencies.is_empty() {
            return Err(invalid(
                "a full snapshot cannot declare recovery dependencies",
            ));
        }
        validate_metadata_context(
            self.version,
            CLIENT_VERSION,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        let profile = &self.run;
        profile.validate_root(&self.native_root)?;
        self.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(protocol)?;
        if self.native_file_count == 0
            || self.native_bytes == 0
            || self.pages.is_empty()
            || self.pages.len() > MAX_CLIENT_PAGES
            || self.native_file_count > (self.pages.len() * MAX_PAGE_FILES) as u64
        {
            return Err(invalid("byte root identity or bounds"));
        }
        let files = profile.main.files.checked_add(1);
        let bytes = profile
            .main
            .bytes
            .checked_add(profile.node_marker()?.len() as u64);
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
        encode_metadata(self).map(|_| ())
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
        let value: Self = decode_metadata(bytes, key)?;
        value.validate_for(begin)?;
        Ok(value)
    }

    pub fn version_of(bytes: &[u8], key: &[u8; 32]) -> StorageResult<u16> {
        #[derive(Deserialize)]
        struct Version {
            version: u16,
        }
        decode_metadata::<Version>(bytes, key).map(|value| value.version)
    }
}

pub fn validate_metadata_context(
    version: u16,
    expected_version: u16,
    digest: &Digest,
    descriptor: &Digest,
    begin: &Begin,
) -> StorageResult<()> {
    begin.validate().map_err(protocol)?;
    if version != expected_version
        || !begin.descriptor.as_value().is_null()
        || *digest != begin.input_sha256().map_err(protocol)?
        || *descriptor != begin.descriptor.commitment().map_err(protocol)?
    {
        return Err(invalid("byte metadata frozen context"));
    }
    Ok(())
}

pub fn encode_metadata(value: &impl Serialize) -> StorageResult<zeroize::Zeroizing<Vec<u8>>> {
    let bytes = zeroize::Zeroizing::new(
        serde_json::to_vec(value).map_err(|_| invalid("byte metadata encoding"))?,
    );
    if bytes.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(invalid("byte metadata bound"));
    }
    Ok(bytes)
}

pub fn decode_metadata<T: DeserializeOwned>(bytes: &[u8], key: &[u8; 32]) -> StorageResult<T> {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBackupHome {
    pub home_id: Id,
    pub generation: DecimalU64,
    pub frontier: DecimalU64,
    pub descriptor_sha256: Digest,
    pub current_sha256: Digest,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBackupProfile {
    pub version: u16,
    pub main: RunBackupHome,
}

impl RunBackupProfile {
    pub(crate) fn node_marker(&self) -> StorageResult<&'static [u8]> {
        use crate::storage::format::{RECORD_NODE_BYTES, RECORD_PROFILE};
        match self.version {
            RECORD_PROFILE => Ok(RECORD_NODE_BYTES),
            _ => Err(invalid("run backup logical profile version")),
        }
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.node_marker()?;
        if self.main.generation.get() == 0
            || self.main.frontier.get() > self.main.generation.get()
            || self.main.files < 2
            || self.main.bytes < 64
        {
            return Err(invalid("run backup home bounds"));
        }
        Ok(())
    }

    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| invalid("run profile encoding"))?;
        let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(&value).map_err(protocol)?;
        Ok(NativeRoot {
            home_id: self.main.home_id.clone(),
            home_digest: bytes_sha256(&bytes),
            current_sha256: self.main.current_sha256.clone(),
            generation: self.main.generation,
            frontier: self.main.frontier,
        })
    }

    pub fn validate_root(&self, root: &NativeRoot) -> StorageResult<()> {
        if self.native_root()? != *root {
            return Err(invalid("run backup native root differs"));
        }
        Ok(())
    }
}

/// Native V2 paths are a fixed client-owned allowlist, never cloud metadata.
pub fn run_backup_destination(path: &str) -> StorageResult<cloud_backup_protocol::v2::Destination> {
    use crate::storage::disk_pack::{NODE_MARKER, RUN_DIR};
    use cloud_backup_protocol::v2::Destination;
    use laststore::disk_run::home::{CURRENT, RUNS, RUN_HOME_MARKER};
    if path == NODE_MARKER {
        return Ok(Destination::R2);
    }
    let (home, tail) = path
        .split_once('/')
        .ok_or_else(|| invalid("run backup path"))?;
    if home != RUN_DIR {
        return Err(invalid("run backup home path"));
    }
    if tail == CURRENT || tail == RUN_HOME_MARKER {
        return Ok(Destination::R2);
    }
    let id = tail
        .strip_prefix(RUNS)
        .and_then(|s| s.strip_prefix('/'))
        .and_then(|s| s.strip_suffix(".ldrun"))
        .ok_or_else(|| invalid("run backup file path"))?;
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| invalid("run backup file identity"))?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(invalid("run backup canonical file identity"));
    }
    Ok(Destination::R2)
}
