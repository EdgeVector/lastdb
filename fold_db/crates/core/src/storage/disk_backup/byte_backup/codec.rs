//! Client-owned encrypted metadata and disk V2 file path contract.

use super::{invalid, protocol, StorageResult};
use crate::storage::cloud_backup_plan::{expected_receipt, ClientNativeFile};
use crate::storage::disk_pack::{NODE_MARKER, RUN_DIR};
use crate::storage::format::{RECORD_NODE_BYTES, RECORD_PROFILE};
use cloud_backup_protocol::v2::{transfer::TransferInput, Begin, Destination};
use cloud_backup_protocol::{
    bytes_sha256, DecimalU64, Digest, Id, NativeRoot, ProtocolError, RecoveryDescriptor,
};
use laststore::disk_run::home::{CURRENT, RUNS, RUN_HOME_MARKER};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const CODEC_PLAINTEXT_MAX_BYTES: u64 = 256 * 1024;
pub const CODEC_ENVELOPE_MAX_BYTES: u64 = CODEC_PLAINTEXT_MAX_BYTES + 29;
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

impl ClientPageReference {
    pub fn for_object(page_index: u32, input: &TransferInput) -> Result<Self, ProtocolError> {
        Ok(Self {
            page_index,
            receipt_id: expected_receipt(input)?.receipt_id,
            sha256: input.object.sha256.clone(),
            bytes: input.object.bytes,
        })
    }
}

pub fn validate_page_files(page_index: u32, files: &[ClientNativeFile]) -> StorageResult<()> {
    if page_index as usize >= MAX_CLIENT_PAGES || !(1..=MAX_PAGE_FILES).contains(&files.len()) {
        return Err(invalid("byte page bounds"));
    }
    let mut prev = None;
    for file in files {
        file.validate()?;
        if prev
            .replace(file.path.as_str())
            .is_some_and(|p| p >= file.path.as_str())
        {
            return Err(invalid("byte page path order"));
        }
    }
    Ok(())
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
    pub fn new(
        page_index: u32,
        begin: &Begin,
        native_files: Vec<ClientNativeFile>,
    ) -> StorageResult<Self> {
        let page = Self {
            version: CLIENT_VERSION,
            page_index,
            begin_sha256: begin.input_sha256().map_err(protocol)?,
            descriptor_sha256: begin.descriptor.commitment().map_err(protocol)?,
            native_files,
        };
        page.validate_for(begin).map(|()| page)
    }

    pub fn validate_for(&self, begin: &Begin) -> StorageResult<()> {
        validate_metadata_context(
            self.version,
            CLIENT_VERSION,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        validate_page_files(self.page_index, &self.native_files)?;
        for file in &self.native_files {
            run_backup_destination(file.path.as_str())?;
        }
        encode_metadata(self).map(|_| ())
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let v: Self = decode_metadata(bytes, key)?;
        v.validate_for(begin).map(|()| v)
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
        self.run.validate_root(&self.native_root)?;
        self.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(protocol)?;
        if self.native_file_count == 0
            || self.native_bytes == 0
            || !valid_page_refs(&self.pages)
            || self.native_file_count > (self.pages.len() * MAX_PAGE_FILES) as u64
        {
            return Err(invalid("byte root identity or bounds"));
        }
        let marker_len = self.run.node_marker()?.len() as u64;
        if self.run.main.files.checked_add(1) != Some(self.native_file_count)
            || self.run.main.bytes.checked_add(marker_len) != Some(self.native_bytes)
        {
            return Err(invalid("run root exact native totals"));
        }
        encode_metadata(self).map(|_| ())
    }

    pub fn validate_page(&self, page: &ClientPage) -> StorageResult<()> {
        (page.begin_sha256 == self.begin_sha256
            && page.version == self.version
            && page.descriptor_sha256 == self.descriptor_sha256
            && self.pages.get(page.page_index as usize).is_some())
        .then_some(())
        .ok_or_else(|| invalid("byte root page context"))
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> StorageResult<Self> {
        let v: Self = decode_metadata(bytes, key)?;
        v.validate_for(begin).map(|()| v)
    }

    pub fn version_of(bytes: &[u8], key: &[u8; 32]) -> StorageResult<u16> {
        #[derive(Deserialize)]
        struct V {
            version: u16,
        }
        decode_metadata::<V>(bytes, key).map(|v| v.version)
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
    (version == expected_version
        && begin.descriptor.as_value().is_null()
        && *digest == begin.input_sha256().map_err(protocol)?
        && *descriptor == begin.descriptor.commitment().map_err(protocol)?)
    .then_some(())
    .ok_or_else(|| invalid("byte metadata frozen context"))
}

pub fn encode_metadata(value: &impl Serialize) -> StorageResult<zeroize::Zeroizing<Vec<u8>>> {
    let bytes = zeroize::Zeroizing::new(
        serde_json::to_vec(value).map_err(|_| invalid("byte metadata encoding"))?,
    );
    (bytes.len() as u64 <= CODEC_PLAINTEXT_MAX_BYTES)
        .then_some(bytes)
        .ok_or_else(|| invalid("byte metadata bound"))
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

pub fn valid_page_refs(pages: &[ClientPageReference]) -> bool {
    (1..=MAX_CLIENT_PAGES).contains(&pages.len())
        && pages.iter().enumerate().all(|(i, p)| {
            p.page_index as usize == i && (30..=CODEC_ENVELOPE_MAX_BYTES).contains(&p.bytes.get())
        })
}

pub fn seal_envelope(
    key: &[u8; 32],
    plain: &[u8],
    existing: Option<Vec<u8>>,
) -> Result<(cloud_backup_protocol::v2::Object, Vec<u8>, bool), &'static str> {
    let (bytes, fresh) = match existing {
        Some(b) => {
            let dec = zeroize::Zeroizing::new(
                crate::crypto::decrypt_envelope(key, &b)
                    .map_err(|_| "byte saved metadata encryption")?,
            );
            if dec.as_slice() != plain {
                return Err("byte frozen metadata differs");
            }
            (b, false)
        }
        None => (
            crate::crypto::encrypt_envelope(key, plain).map_err(|_| "byte metadata encryption")?,
            true,
        ),
    };
    let obj = cloud_backup_protocol::v2::Object {
        sha256: bytes_sha256(&bytes),
        bytes: DecimalU64::new(bytes.len() as u64),
        destination: Destination::R2,
    };
    Ok((obj, bytes, fresh))
}

pub struct SegmentReader<S, R, F> {
    remaining: std::collections::VecDeque<S>,
    active: Option<(R, u64)>,
    open: F,
}

impl<S, R, F> SegmentReader<S, R, F> {
    pub fn new(segments: Vec<S>, open: F) -> Self {
        Self {
            remaining: segments.into(),
            active: None,
            open,
        }
    }
}

impl<S, R, F> std::io::Read for SegmentReader<S, R, F>
where
    R: std::io::Read,
    F: FnMut(S) -> std::io::Result<(R, u64)>,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while !buf.is_empty() {
            if let Some((r, left)) = &mut self.active {
                if *left == 0 {
                    self.active = None;
                    continue;
                }
                let cap = (*left).min(buf.len() as u64) as usize;
                let n = r.read(&mut buf[..cap])?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "byte native source truncated",
                    ));
                }
                *left -= n as u64;
                return Ok(n);
            }
            let Some(seg) = self.remaining.pop_front() else {
                return Ok(0);
            };
            self.active = Some((self.open)(seg)?);
        }
        Ok(0)
    }
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
        let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(&val).map_err(protocol)?;
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

/// Native V2 paths are a fixed client-owned allowlist, never cloud metadata.
pub fn run_backup_destination(path: &str) -> StorageResult<Destination> {
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
    (!parsed.is_nil() && parsed.to_string() == id)
        .then_some(Destination::R2)
        .ok_or_else(|| invalid("run backup canonical file identity"))
}
