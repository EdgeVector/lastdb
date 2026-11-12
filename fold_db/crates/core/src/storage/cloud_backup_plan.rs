//! The one upload planner for native LastDB cloud backups.

use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{blobs::BlobPath, Destination, Job, Object, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, ProtocolError, RelativePath};
use lastdb_model::storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::Read;

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
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.native_bytes.get() == 0 || !(1..=MAX_FILE_EXTENTS).contains(&self.extents.len()) {
            return Err(PlanError::Invalid("byte native file extent count"));
        }
        let bytes = self
            .extents
            .iter()
            .try_fold(0u64, |acc, ext| {
                let len = ext.length.get();
                (len > 0 && ext.offset.get().checked_add(len).is_some())
                    .then(|| acc.checked_add(len))
                    .flatten()
            })
            .ok_or(PlanError::Invalid("byte native extent bounds"))?;
        (bytes == self.native_bytes.get())
            .then_some(())
            .ok_or(PlanError::Invalid("byte native file exact length"))
    }
}

#[derive(Debug)]
pub enum PlanError {
    Invalid(&'static str),
    SourceLength,
    ExtentBound,
    Protocol(ProtocolError),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid(reason) => reason,
            Self::SourceLength => "byte native source length changed",
            Self::ExtentBound => "byte native file extent metadata bound",
            Self::Protocol(_) => "byte protocol validation",
        })
    }
}
impl std::error::Error for PlanError {}
impl From<ProtocolError> for PlanError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

pub fn object_receipt(scope: &CloudScope, object: &Object) -> Result<ObjectReceipt, ProtocolError> {
    let mut receipt = ObjectReceipt {
        version: Version::V2,
        receipt_id: object.sha256.clone(),
        scope: scope.clone(),
        object: object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256()?;
    receipt.validate().map(|()| receipt)
}

pub fn expected_receipt(input: &TransferInput) -> Result<ObjectReceipt, ProtocolError> {
    input.validate()?;
    object_receipt(&input.job.stream.scope, &input.object)
}

pub fn new_transfer_input(
    job: Job,
    object: Object,
    blob: Option<BlobPath>,
) -> Result<TransferInput, ProtocolError> {
    let request_id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
    let input = TransferInput {
        version: Version::V2,
        request_id,
        job,
        object,
        blob,
    };
    input.validate().map(|()| input)
}

#[derive(Clone, Debug)]
pub struct PlanFile {
    pub id: usize,
    pub path: RelativePath,
    pub bytes: u64,
    pub destination: Destination,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSegment {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug)]
pub struct PlannedObject {
    pub object: Object,
    pub segments: Vec<PlanSegment>,
    pub native_files: Vec<ClientNativeFile>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanTotals {
    pub objects: u64,
    pub packs: u64,
    pub multi_file_packs: u64,
}

struct PlanCtx<'a> {
    scope: &'a CloudScope,
    buf: Vec<u8>,
    totals: PlanTotals,
}

impl PlanCtx<'_> {
    fn seal(
        &self,
        hash: Sha256,
        bytes: u64,
        destination: Destination,
    ) -> Result<(Object, Digest), PlanError> {
        let object = Object {
            sha256: digest(hash)?,
            bytes: DecimalU64::new(bytes),
            destination,
        };
        let receipt_id = object_receipt(self.scope, &object)?.receipt_id;
        Ok((object, receipt_id))
    }

    fn next_index(&mut self, pack: bool, multi: bool) -> u64 {
        let idx = self.totals.objects;
        self.totals.objects += 1;
        self.totals.packs += u64::from(pack);
        self.totals.multi_file_packs += u64::from(pack && multi);
        idx
    }

    fn span<R: Read, E: From<PlanError> + From<std::io::Error>>(
        &mut self,
        file: &PlanFile,
        mut reader: R,
        save: &mut impl FnMut(u64, PlannedObject) -> Result<(), E>,
    ) -> Result<(), E> {
        let (mut file_hash, mut extents, mut base) = (Sha256::new(), Vec::new(), 0u64);
        while base < file.bytes {
            let len = OBJECT_CHUNK_BYTES.min(file.bytes - base);
            let mut obj_hash = Sha256::new();
            if !read_exact(
                &mut reader,
                &mut self.buf,
                len,
                &mut file_hash,
                &mut obj_hash,
            )? {
                return Err(PlanError::SourceLength.into());
            }
            let (object, receipt_id) = self.seal(obj_hash, len, file.destination)?;
            extents.push(ClientExtent {
                receipt_id,
                offset: DecimalU64::new(0),
                length: DecimalU64::new(len),
            });
            if extents.len() > MAX_FILE_EXTENTS {
                return Err(PlanError::ExtentBound.into());
            }
            let native_files = if base + len == file.bytes {
                if !is_eof(&mut reader)? {
                    return Err(PlanError::SourceLength.into());
                }
                vec![checked_file(
                    file,
                    digest(std::mem::take(&mut file_hash))?,
                    std::mem::take(&mut extents),
                )?]
            } else {
                Vec::new()
            };
            let segments = vec![PlanSegment {
                file: file.id,
                offset: base,
                bytes: len,
            }];
            let idx = self.next_index(false, false);
            save(
                idx,
                PlannedObject {
                    object,
                    segments,
                    native_files,
                },
            )?;
            base += len;
        }
        Ok(())
    }
}

/// Stream `files` in order into packed small-file objects and chunked spans.
pub fn plan_files<R, E>(
    files: &[PlanFile],
    scope: &CloudScope,
    mut open: impl FnMut(&PlanFile) -> Result<R, E>,
    mut save: impl FnMut(u64, PlannedObject) -> Result<(), E>,
) -> Result<PlanTotals, E>
where
    R: Read,
    E: From<PlanError> + From<std::io::Error>,
{
    let mut ctx = PlanCtx {
        scope,
        buf: vec![0u8; 65536],
        totals: PlanTotals::default(),
    };
    let mut cursor = 0;
    while cursor < files.len() {
        let first = &files[cursor];
        if first.bytes > OBJECT_CHUNK_BYTES {
            ctx.span(first, open(first)?, &mut save)?;
            cursor += 1;
            continue;
        }
        let end = pack_end(files, cursor);
        let pack = first.bytes < SMALL_NATIVE_FILE_BYTES;
        let (mut obj_hash, mut obj_bytes, mut members) =
            (Sha256::new(), 0u64, Vec::with_capacity(end - cursor));
        for file in &files[cursor..end] {
            let (mut reader, mut file_hash) = (open(file)?, Sha256::new());
            if !read_exact(
                &mut reader,
                &mut ctx.buf,
                file.bytes,
                &mut file_hash,
                &mut obj_hash,
            )? || !is_eof(&mut reader)?
            {
                return Err(PlanError::SourceLength.into());
            }
            members.push((file, digest(file_hash)?, obj_bytes));
            obj_bytes += file.bytes;
        }
        let (object, receipt_id) = ctx.seal(obj_hash, obj_bytes, first.destination)?;
        let cap = members.len();
        let (mut segments, mut native_files) = (Vec::with_capacity(cap), Vec::with_capacity(cap));
        for (file, sha, offset) in members {
            segments.push(PlanSegment {
                file: file.id,
                offset: 0,
                bytes: file.bytes,
            });
            let ext = ClientExtent {
                receipt_id: receipt_id.clone(),
                offset: DecimalU64::new(offset),
                length: DecimalU64::new(file.bytes),
            };
            native_files.push(checked_file(file, sha, vec![ext])?);
        }
        let idx = ctx.next_index(pack, segments.len() > 1);
        save(
            idx,
            PlannedObject {
                object,
                segments,
                native_files,
            },
        )?;
        cursor = end;
    }
    Ok(ctx.totals)
}

pub fn sum_segments(
    segments: &[PlanSegment],
    mut file_limit: impl FnMut(usize) -> Option<u64>,
) -> Option<u64> {
    (segments.len() <= MAX_PACK_MEMBERS).then_some(())?;
    segments.iter().try_fold(0u64, |acc, s| {
        (s.bytes > 0 && s.offset.checked_add(s.bytes)? <= file_limit(s.file)?)
            .then(|| acc.checked_add(s.bytes))?
    })
}

fn is_eof(reader: &mut impl Read) -> std::io::Result<bool> {
    Ok(reader.read(&mut [0u8; 1])? == 0)
}

fn read_exact(
    reader: &mut impl Read,
    buf: &mut [u8],
    mut rem: u64,
    file_hash: &mut Sha256,
    obj_hash: &mut Sha256,
) -> std::io::Result<bool> {
    while rem > 0 {
        let cap = rem.min(buf.len() as u64) as usize;
        let n = reader.read(&mut buf[..cap])?;
        if n == 0 {
            return Ok(false);
        }
        file_hash.update(&buf[..n]);
        obj_hash.update(&buf[..n]);
        rem -= n as u64;
    }
    Ok(true)
}

fn checked_file(
    file: &PlanFile,
    native_sha256: Digest,
    extents: Vec<ClientExtent>,
) -> Result<ClientNativeFile, PlanError> {
    let rec = ClientNativeFile {
        path: file.path.clone(),
        native_sha256,
        native_bytes: DecimalU64::new(file.bytes),
        extents,
    };
    rec.validate().map(|()| rec)
}

fn pack_end(files: &[PlanFile], start: usize) -> usize {
    let first = &files[start];
    let mut end = start + 1;
    if first.bytes < SMALL_NATIVE_FILE_BYTES {
        let mut bytes = first.bytes;
        while let Some(f) = files.get(end) {
            if f.bytes >= SMALL_NATIVE_FILE_BYTES
                || f.destination != first.destination
                || end - start >= MAX_PACK_MEMBERS
                || bytes
                    .checked_add(f.bytes)
                    .is_none_or(|n| n > METADATA_PACK_BYTES)
            {
                break;
            }
            bytes += f.bytes;
            end += 1;
        }
    }
    end
}

fn digest(hash: Sha256) -> Result<Digest, PlanError> {
    Digest::try_from(lastdb_model::hex::hex_lower(hash.finalize())).map_err(PlanError::Protocol)
}
