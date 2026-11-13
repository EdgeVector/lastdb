//! Client-owned encrypted metadata and disk V2 file path contract for recipient staging.
use crate::storage::cloud_backup_plan::ClientNativeFile;
use crate::storage::disk_pack::{NODE_MARKER, RUN_DIR};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{RECORD_NODE_BYTES, RECORD_PROFILE};
use cloud_backup_protocol::v2::Begin;
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, Id, NativeRoot, RecoveryDescriptor};
use laststore::disk_run::home::{CURRENT, RUNS, RUN_HOME_MARKER};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use zeroize::Zeroizing;

pub const CODEC_PLAINTEXT_MAX_BYTES: u64 = 256 * 1024;
pub const CODEC_ENVELOPE_MAX_BYTES: u64 = 256 * 1024 + 29;
pub const CLIENT_VERSION: u16 = 8;
pub const MAX_CLIENT_PAGES: usize = 1024;
pub const MAX_PAGE_FILES: usize = 128;

fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPageReference {
    pub page_index: u32,
    pub receipt_id: Digest,
    pub sha256: Digest,
    pub bytes: DecimalU64,
}

pub fn validate_encoded_page(
    (version, expected): (u16, u16),
    page_index: u32,
    (digest, descriptor): (&Digest, &Digest),
    files: &[ClientNativeFile],
    begin: &Begin,
) -> StorageResult<()> {
    validate_metadata_context(version, expected, digest, descriptor, begin)?;
    if page_index as usize >= MAX_CLIENT_PAGES || !(1..=MAX_PAGE_FILES).contains(&files.len()) {
        return Err(invalid("byte page bounds"));
    }
    let mut prev = None;
    for file in files {
        file.validate().map_err(invalid)?;
        if prev
            .replace(file.path.as_str())
            .is_some_and(|p| p >= file.path.as_str())
        {
            return Err(invalid("byte page path order"));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPage {
    pub version: u16,
    pub page_index: u32,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub native_files: Vec<ClientNativeFile>,
}

impl ClientPage {
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let page: Self = decode_metadata(bytes, key)?;
        validate_encoded_page(
            (page.version, CLIENT_VERSION),
            page.page_index,
            (&page.begin_sha256, &page.descriptor_sha256),
            &page.native_files,
            begin,
        )?;
        for file in &page.native_files {
            validate_run_backup_path(file.path.as_str())?;
        }
        Ok(page)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let root: Self = decode_metadata(bytes, key)?;
        if !begin.recovery_dependencies.is_empty() {
            return Err(invalid(
                "a full snapshot cannot declare recovery dependencies",
            ));
        }
        validate_metadata_context(
            root.version,
            CLIENT_VERSION,
            &root.begin_sha256,
            &root.descriptor_sha256,
            begin,
        )?;
        root.run.validate_root(&root.native_root)?;
        root.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(|_| invalid("byte protocol validation"))?;
        let marker_len = root.run.node_marker()?.len() as u64;
        if root.native_file_count == 0
            || root.native_bytes == 0
            || !valid_page_refs(&root.pages)
            || root.native_file_count > (root.pages.len() * MAX_PAGE_FILES) as u64
            || root.run.main.files.checked_add(1) != Some(root.native_file_count)
            || root.run.main.bytes.checked_add(marker_len) != Some(root.native_bytes)
        {
            return Err(invalid("byte root identity or exact totals"));
        }
        Ok(root)
    }

    pub fn validate_page(&self, page: &ClientPage, index: u32) -> StorageResult<()> {
        ((
            page.version,
            page.page_index,
            &page.begin_sha256,
            &page.descriptor_sha256,
        ) == (
            self.version,
            index,
            &self.begin_sha256,
            &self.descriptor_sha256,
        ) && (index as usize) < self.pages.len())
        .then_some(())
        .ok_or_else(|| invalid("byte root page context"))
    }
}

pub fn validate_metadata_context(
    version: u16,
    expected_version: u16,
    digest: &Digest,
    descriptor: &Digest,
    begin: &Begin,
) -> StorageResult<()> {
    let proto = |_| invalid("byte protocol validation");
    begin.validate().map_err(proto)?;
    let in_sha = begin.input_sha256().map_err(proto)?;
    let desc_sha = begin.descriptor.commitment().map_err(proto)?;
    (version == expected_version
        && begin.descriptor.as_value().is_null()
        && *digest == in_sha
        && *descriptor == desc_sha)
        .then_some(())
        .ok_or_else(|| invalid("byte metadata frozen context"))
}

pub fn decode_metadata<T: DeserializeOwned>(bytes: &[u8], key: &[u8; 32]) -> StorageResult<T> {
    if bytes.len() as u64 > CODEC_ENVELOPE_MAX_BYTES {
        return Err(invalid("byte metadata envelope bound"));
    }
    let plain = Zeroizing::new(
        crate::crypto::decrypt_envelope(key, bytes)
            .map_err(|_| invalid("byte metadata encryption"))?,
    );
    if plain.len() as u64 > CODEC_PLAINTEXT_MAX_BYTES {
        return Err(invalid("byte metadata bound"));
    }
    serde_json::from_slice(&plain).map_err(|_| invalid("byte metadata format"))
}

pub fn valid_page_refs(pages: &[ClientPageReference]) -> bool {
    (1..=MAX_CLIENT_PAGES).contains(&pages.len())
        && pages.iter().enumerate().all(|(i, p)| {
            p.page_index as usize == i && (30..=CODEC_ENVELOPE_MAX_BYTES).contains(&p.bytes.get())
        })
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
        (self.version == RECORD_PROFILE)
            .then_some(RECORD_NODE_BYTES)
            .ok_or_else(|| invalid("run backup logical profile version"))
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.node_marker()?;
        (self.main.generation.get() > 0
            && self.main.frontier.get() <= self.main.generation.get()
            && self.main.files >= 2
            && self.main.bytes >= 64)
            .then_some(())
            .ok_or_else(|| invalid("run backup home bounds"))
    }

    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.validate()?;
        let val = serde_json::to_value(self).map_err(|_| invalid("run profile encoding"))?;
        let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(&val)
            .map_err(|_| invalid("byte protocol validation"))?;
        Ok(NativeRoot {
            home_id: self.main.home_id.clone(),
            home_digest: bytes_sha256(&bytes),
            current_sha256: self.main.current_sha256.clone(),
            generation: self.main.generation,
            frontier: self.main.frontier,
        })
    }

    pub fn validate_root(&self, root: &NativeRoot) -> StorageResult<()> {
        (self.native_root()? == *root)
            .then_some(())
            .ok_or_else(|| invalid("run backup native root differs"))
    }
}

fn validate_run_backup_path(path: &str) -> StorageResult<()> {
    if path == NODE_MARKER {
        return Ok(());
    }
    let (home, tail) = path
        .split_once('/')
        .ok_or_else(|| invalid("run backup path"))?;
    if home != RUN_DIR {
        return Err(invalid("run backup home path"));
    }
    if tail == CURRENT || tail == RUN_HOME_MARKER {
        return Ok(());
    }
    let id = tail
        .strip_prefix(RUNS)
        .and_then(|s| s.strip_prefix('/'))
        .and_then(|s| s.strip_suffix(".ldrun"))
        .ok_or_else(|| invalid("run backup file path"))?;
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| invalid("run backup file identity"))?;
    (!parsed.is_nil() && parsed.to_string() == id)
        .then_some(())
        .ok_or_else(|| invalid("run backup canonical file identity"))
}
