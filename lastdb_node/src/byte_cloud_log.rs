//! Client-only mutation-log manifests. The cloud never decodes these values.
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Destination};
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::byte_backup::{
    decode_metadata, encode_metadata, valid_page_refs, validate_metadata_context,
    validate_page_files, ClientPageReference, MAX_CLIENT_PAGES,
};
use fold_db::storage::disk_pack::mutation_log::{
    log_path, FrozenLogCapture, LogAnchor, LogEntry, LogPosition,
};
use serde::{Deserialize, Serialize};

/// Version 5 logs main-home runs only; record blobs upload by path.
pub const VERSION: u16 = 5;
pub const MAX_BATCH_ENTRIES: usize = 128;

#[derive(Debug)]
pub struct LogCodecError(&'static str);
impl std::fmt::Display for LogCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for LogCodecError {}
pub type Result<T> = std::result::Result<T, LogCodecError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogKind {
    NativeMutationLog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationLogPage {
    pub version: u16,
    pub page_index: u32,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub files: Vec<ClientNativeFile>,
}

impl MutationLogPage {
    pub fn new(page_index: u32, begin: &Begin, files: Vec<ClientNativeFile>) -> Result<Self> {
        let (begin_sha256, descriptor_sha256) = begin_digests(begin)?;
        let page = Self {
            version: VERSION,
            page_index,
            begin_sha256,
            descriptor_sha256,
            files,
        };
        page.validate_for(begin).map(|()| page)
    }

    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        validate_page_files(self.page_index, &self.files)
            .map_err(|_| LogCodecError("mutation log page files"))?;
        check_encoded(self)
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        decode_checked(bytes, key, |v: &Self| v.validate_for(begin))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationLogRoot {
    pub version: u16,
    pub kind: LogKind,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub recovery: RecoveryDescriptor,
    pub base: AcceptedBackup,
    pub previous: AcceptedBackup,
    pub anchor: LogAnchor,
    pub from: LogPosition,
    pub through: LogPosition,
    pub entries: Vec<LogEntry>,
    pub native_file_count: u64,
    pub native_bytes: u64,
    pub pages: Vec<ClientPageReference>,
}

impl MutationLogRoot {
    pub fn from_frozen(
        frozen: &FrozenLogCapture,
        recovery: RecoveryDescriptor,
        base: AcceptedBackup,
        previous: AcceptedBackup,
        pages: Vec<ClientPageReference>,
    ) -> Result<Self> {
        let (begin_sha256, descriptor_sha256) = begin_digests(&frozen.begin)?;
        let root = Self {
            version: VERSION,
            kind: LogKind::NativeMutationLog,
            begin_sha256,
            descriptor_sha256,
            recovery,
            base,
            previous,
            anchor: frozen.anchor.clone(),
            from: frozen.from.clone(),
            through: frozen.through.clone(),
            entries: frozen.entries.clone(),
            native_file_count: frozen.files.len() as u64,
            native_bytes: frozen.native_bytes,
            pages,
        };
        root.validate_for(&frozen.begin).map(|()| root)
    }

    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        let mut expected = vec![self.base.head()];
        if self.previous != self.base {
            expected.push(self.previous.head());
        }
        if begin.recovery_dependencies != expected {
            return Err(LogCodecError("mutation log declared recovery dependencies"));
        }
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        self.base
            .validate()
            .map_err(|_| LogCodecError("mutation log base selection"))?;
        self.previous
            .validate()
            .map_err(|_| LogCodecError("mutation log previous selection"))?;
        self.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(|_| LogCodecError("mutation log recovery identity"))?;
        if self.base.job().stream != begin.job.stream
            || self.previous.job().stream != begin.job.stream
            || begin.predecessor.as_ref() != Some(&self.previous.head())
            || self.base.ordinal.get() > self.previous.ordinal.get()
            || !(1..=MAX_BATCH_ENTRIES).contains(&self.entries.len())
            || self.from.epoch != self.anchor.epoch
            || self.through.epoch != self.anchor.epoch
            || !(1..=MAX_CLIENT_PAGES).contains(&self.pages.len())
        {
            return Err(LogCodecError(
                "mutation log root selection or resource bound",
            ));
        }
        let anchor = self
            .anchor
            .position()
            .map_err(|_| LogCodecError("mutation log anchor"))?;
        if (self.from.sequence == 0 || self.previous == self.base) && self.from != anchor {
            return Err(LogCodecError("mutation log snapshot boundary"));
        }
        let (mut pos, mut files, mut bytes) = (self.from.clone(), 0u64, 0u64);
        for entry in &self.entries {
            pos = entry
                .position_after(&pos)
                .map_err(|_| LogCodecError("mutation log sequence or hash"))?;
            files = files
                .checked_add(entry.parts.len() as u64)
                .ok_or(LogCodecError("mutation log file count"))?;
            let eb = entry
                .bytes()
                .map_err(|_| LogCodecError("mutation log entry size"))?;
            bytes = bytes
                .checked_add(eb)
                .ok_or(LogCodecError("mutation log byte count"))?;
        }
        if pos != self.through
            || files != self.native_file_count
            || bytes != self.native_bytes
            || !valid_page_refs(&self.pages)
        {
            return Err(LogCodecError("mutation log complete range totals"));
        }
        check_encoded(self)
    }

    pub fn validate_page(&self, page: &MutationLogPage) -> Result<()> {
        if (page.version, &page.begin_sha256, &page.descriptor_sha256)
            != (self.version, &self.begin_sha256, &self.descriptor_sha256)
            || self.pages.get(page.page_index as usize).is_none()
        {
            return Err(LogCodecError("mutation log page context"));
        }
        page.files
            .iter()
            .try_for_each(|f| self.validate_file(f).map(|_| ()))
    }

    pub fn validate_file(&self, file: &ClientNativeFile) -> Result<Destination> {
        file.validate()
            .map_err(|_| LogCodecError("mutation log exact file extents"))?;
        for entry in &self.entries {
            for part in &entry.parts {
                if file.path.as_str()
                    == log_path(&self.anchor.epoch, entry.sequence, part.role, &part.run_id)
                {
                    return (file.native_bytes.get() == part.bytes)
                        .then_some(Destination::R2)
                        .ok_or(LogCodecError("mutation log original file length"));
                }
            }
        }
        Err(LogCodecError("mutation log unexpected native path"))
    }

    pub fn validate_previous(&self, prev: &Self) -> Result<()> {
        (self.base == prev.base && self.anchor == prev.anchor && self.from == prev.through)
            .then_some(())
            .ok_or(LogCodecError("mutation log predecessor prefix differs"))
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        decode_checked(bytes, key, |v: &Self| v.validate_for(begin))
    }
}

fn decode_checked<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    key: &[u8; 32],
    check: impl FnOnce(&T) -> Result<()>,
) -> Result<T> {
    let v: T = decode_metadata(bytes, key).map_err(|_| LogCodecError("mutation log metadata"))?;
    check(&v).map(|()| v)
}

fn begin_digests(begin: &Begin) -> Result<(Digest, Digest)> {
    let a = begin
        .input_sha256()
        .map_err(|_| LogCodecError("mutation log begin"))?;
    let b = begin
        .descriptor
        .commitment()
        .map_err(|_| LogCodecError("mutation log descriptor"))?;
    Ok((a, b))
}

fn check_encoded(value: &impl Serialize) -> Result<()> {
    encode_metadata(value)
        .map(|_| ())
        .map_err(|_| LogCodecError("mutation log metadata size"))
}

fn context(version: u16, digest: &Digest, descriptor: &Digest, begin: &Begin) -> Result<()> {
    validate_metadata_context(version, VERSION, digest, descriptor, begin)
        .map_err(|_| LogCodecError("mutation log metadata frozen context"))?;
    (begin.required_destinations == [Destination::R2, Destination::B2])
        .then_some(())
        .ok_or(LogCodecError("mutation log metadata frozen context"))
}
