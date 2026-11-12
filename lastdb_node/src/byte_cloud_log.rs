//! Client-only mutation-log manifests. The cloud never decodes these values.
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Destination, Head};
use cloud_backup_protocol::{Digest, RecoveryDescriptor};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::byte_backup::{
    begin_digests, decode_metadata, encode_metadata, valid_page_refs, validate_encoded_page,
    validate_metadata_context, ClientPageReference, MAX_CLIENT_PAGES,
};
use fold_db::storage::disk_pack::mutation_log::{
    log_path, FrozenLogCapture, LogAnchor, LogEntry, LogPosition,
};
use serde::{Deserialize, Serialize};

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

pub fn dependency_heads(base: &AcceptedBackup, previous: &AcceptedBackup) -> Vec<Head> {
    if previous == base {
        vec![base.head()]
    } else {
        vec![base.head(), previous.head()]
    }
}

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
        let (begin_sha256, descriptor_sha256) =
            begin_digests(begin).map_err(|_| LogCodecError("mutation log begin"))?;
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
        let ctx = (&self.begin_sha256, &self.descriptor_sha256);
        validate_encoded_page(
            self,
            (self.version, VERSION),
            self.page_index,
            ctx,
            &self.files,
            begin,
        )
        .map_err(|_| LogCodecError("mutation log page"))?;
        require_destinations(begin)
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        decode_metadata::<Self>(bytes, key)
            .map_err(|_| LogCodecError("mutation log metadata"))
            .and_then(|v| v.validate_for(begin).map(|()| v))
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
        let (begin_sha256, descriptor_sha256) =
            begin_digests(&frozen.begin).map_err(|_| LogCodecError("mutation log begin"))?;
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
        let (b_sha, d_sha) = (&self.begin_sha256, &self.descriptor_sha256);
        validate_metadata_context(self.version, VERSION, b_sha, d_sha, begin)
            .map_err(|_| LogCodecError("mutation log metadata frozen context"))?;
        require_destinations(begin)?;
        self.base
            .validate()
            .and_then(|()| self.previous.validate())
            .and_then(|()| self.recovery.validate_for(&begin.job.stream.scope))
            .map_err(|_| LogCodecError("mutation log selection"))?;
        let anchor = self
            .anchor
            .position()
            .map_err(|_| LogCodecError("anchor"))?;
        if begin.recovery_dependencies != dependency_heads(&self.base, &self.previous)
            || self.base.job().stream != begin.job.stream
            || self.previous.job().stream != begin.job.stream
            || begin.predecessor.as_ref() != Some(&self.previous.head())
            || self.base.ordinal.get() > self.previous.ordinal.get()
            || !(1..=MAX_BATCH_ENTRIES).contains(&self.entries.len())
            || self.from.epoch != self.anchor.epoch
            || self.through.epoch != self.anchor.epoch
            || !(1..=MAX_CLIENT_PAGES).contains(&self.pages.len())
            || ((self.from.sequence == 0 || self.previous == self.base) && self.from != anchor)
        {
            return Err(LogCodecError("mutation log root selection or boundary"));
        }
        let (mut pos, mut files, mut bytes) = (self.from.clone(), 0u64, 0u64);
        for e in &self.entries {
            pos = e.position_after(&pos).map_err(|_| LogCodecError("seq"))?;
            files = files
                .checked_add(e.parts.len() as u64)
                .ok_or(LogCodecError("files"))?;
            let sz = e.bytes().map_err(|_| LogCodecError("entry size"))?;
            bytes = bytes.checked_add(sz).ok_or(LogCodecError("bytes"))?;
        }
        if (pos, files, bytes)
            != (
                self.through.clone(),
                self.native_file_count,
                self.native_bytes,
            )
            || !valid_page_refs(&self.pages)
        {
            return Err(LogCodecError("mutation log complete range totals"));
        }
        encode_metadata(self)
            .map(|_| ())
            .map_err(|_| LogCodecError("mutation log metadata size"))
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
        file.validate().map_err(|_| LogCodecError("file extents"))?;
        let epoch = &self.anchor.epoch;
        let found = self
            .entries
            .iter()
            .flat_map(|e| e.parts.iter().map(move |p| (e.sequence, p)))
            .find(|(seq, p)| file.path.as_str() == log_path(epoch, *seq, p.role, &p.run_id));
        match found {
            Some((_, p)) if file.native_bytes.get() == p.bytes => Ok(Destination::R2),
            Some(_) => Err(LogCodecError("mutation log original file length")),
            None => Err(LogCodecError("mutation log unexpected native path")),
        }
    }

    pub fn validate_previous(&self, prev: &Self) -> Result<()> {
        (self.base == prev.base && self.anchor == prev.anchor && self.from == prev.through)
            .then_some(())
            .ok_or(LogCodecError("mutation log predecessor prefix differs"))
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        decode_metadata::<Self>(bytes, key)
            .map_err(|_| LogCodecError("mutation log metadata"))
            .and_then(|v| v.validate_for(begin).map(|()| v))
    }
}

fn require_destinations(begin: &Begin) -> Result<()> {
    (begin.required_destinations == [Destination::R2, Destination::B2])
        .then_some(())
        .ok_or(LogCodecError("mutation log metadata frozen context"))
}
