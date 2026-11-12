//! Upload planner and streaming segment reader for native cloud backups.
use cloud_backup_protocol::v2::{
    blobs::BlobPath,
    transfer::{ObjectReceipt, TransferInput},
    Destination, Job, Object, Version,
};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, ProtocolError, RelativePath};
use lastdb_model::storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::{self, Read};

const MAX_FILE_EXTENTS: usize = 16384;
pub const MAX_PAYLOAD_ENVELOPE_BYTES: u64 = 256 * 1024 + 29;

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
        let sum = self.extents.iter().try_fold(0u64, |acc, e| {
            let n = e.length.get();
            (n > 0 && e.offset.get().checked_add(n).is_some())
                .then(|| acc.checked_add(n))
                .flatten()
        });
        match sum {
            Some(b) if b == self.native_bytes.get() => Ok(()),
            Some(_) => Err(PlanError::Invalid("byte native file exact length")),
            None => Err(PlanError::Invalid("byte native extent bounds")),
        }
    }
}

#[derive(Debug)]
pub enum PlanError {
    Invalid(&'static str),
    Conflict,
    SourceLength,
    ExtentBound,
    Protocol(ProtocolError),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid(r) => r,
            Self::Conflict => "byte frozen object plan or receipt differs",
            Self::SourceLength => "byte native source length changed",
            Self::ExtentBound => "byte native file extent metadata bound",
            Self::Protocol(_) => "byte protocol validation",
        })
    }
}
impl std::error::Error for PlanError {}
impl From<ProtocolError> for PlanError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

fn object_receipt(scope: &CloudScope, object: &Object) -> Result<ObjectReceipt, ProtocolError> {
    let mut r = ObjectReceipt {
        version: Version::V2,
        receipt_id: object.sha256.clone(),
        scope: scope.clone(),
        object: object.clone(),
    };
    r.receipt_id = r.identity_sha256()?;
    r.validate().map(|()| r)
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
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
        job,
        object,
        blob,
    };
    input.validate().map(|()| input)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedRecord {
    pub input: TransferInput,
    pub segments: Vec<PlanSegment>,
    #[serde(alias = "ciphertext")]
    pub payload: Option<String>,
    pub receipt: Option<ObjectReceipt>,
}

impl PlannedRecord {
    pub fn expected_receipt_id(&self) -> Result<Digest, ProtocolError> {
        expected_receipt(&self.input).map(|r| r.receipt_id)
    }

    pub fn validate(
        &self,
        job: &Job,
        expected_payload: &str,
        mut file_limit: impl FnMut(usize) -> Option<u64>,
    ) -> Result<(), PlanError> {
        self.input.validate()?;
        let expected = self.input.object.bytes.get();
        let bad_payload = self
            .payload
            .as_deref()
            .is_some_and(|n| n != expected_payload || expected > MAX_PAYLOAD_ENVELOPE_BYTES);
        if &self.input.job != job
            || self.segments.is_empty() != self.payload.is_some()
            || bad_payload
            || self.segments.len() > MAX_PACK_MEMBERS
        {
            return Err(PlanError::Invalid("byte source object identity"));
        }
        let total = self
            .segments
            .iter()
            .try_fold(0u64, |acc, s| {
                (s.bytes > 0 && s.offset.checked_add(s.bytes)? <= file_limit(s.file)?)
                    .then(|| acc.checked_add(s.bytes))?
            })
            .ok_or(PlanError::Invalid("byte source segment bounds"))?;
        let bad_receipt = self
            .receipt
            .as_ref()
            .map(|r| expected_receipt(&self.input).map(|e| *r != e))
            .transpose()?
            .unwrap_or(false);
        ((self.segments.is_empty() || total == expected) && !bad_receipt)
            .then_some(())
            .ok_or(PlanError::Invalid("byte source object or receipt mismatch"))
    }

    pub fn upsert(
        prior: Option<Self>,
        job: &Job,
        (object, segments, payload): (Object, Vec<PlanSegment>, Option<String>),
        expected_payload: &str,
        mut file_limit: impl FnMut(usize) -> Option<u64>,
    ) -> Result<Self, PlanError> {
        let rec = match prior {
            Some(p) => {
                p.validate(job, expected_payload, &mut file_limit)?;
                if p.input.object != object || p.segments != segments || p.payload != payload {
                    return Err(PlanError::Conflict);
                }
                p
            }
            None => Self {
                input: new_transfer_input(job.clone(), object, None)?,
                segments,
                payload,
                receipt: None,
            },
        };
        rec.validate(job, expected_payload, file_limit)
            .map(|()| rec)
    }

    pub fn confirm(&mut self, receipt: &ObjectReceipt) -> Result<(), PlanError> {
        if expected_receipt(&self.input)? != *receipt
            || self.receipt.as_ref().is_some_and(|p| p != receipt)
        {
            return Err(PlanError::Conflict);
        }
        self.receipt = Some(receipt.clone());
        Ok(())
    }
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

pub use crate::storage::disk_backup::byte_backup::open_planned_reader;

pub fn plan_files<R: Read, E: From<PlanError> + From<io::Error>>(
    files: &[PlanFile],
    scope: &CloudScope,
    mut open: impl FnMut(&PlanFile) -> Result<R, E>,
    mut save: impl FnMut(u64, Object, Vec<PlanSegment>, Vec<ClientNativeFile>) -> Result<(), E>,
) -> Result<(u64, u64, u64), E> {
    let (mut buf, mut objects, mut packs, mut multi, mut cur) =
        (vec![0u8; 65536], 0u64, 0u64, 0u64, 0);
    while cur < files.len() {
        let first = &files[cur];
        if first.bytes > OBJECT_CHUNK_BYTES {
            let (mut rd, mut fh, mut exts, mut base) =
                (open(first)?, Sha256::new(), Vec::new(), 0u64);
            while base < first.bytes {
                let (len, mut oh) = (OBJECT_CHUNK_BYTES.min(first.bytes - base), Sha256::new());
                read_exact::<E>(&mut rd, &mut buf, len, &mut fh, &mut oh)?;
                let (obj, rid) = seal_object(scope, oh, len, first.destination)?;
                exts.push(extent(rid, 0, len));
                if exts.len() > MAX_FILE_EXTENTS {
                    return Err(PlanError::ExtentBound.into());
                }
                let nf = if base + len == first.bytes {
                    require_eof::<E>(&mut rd)?;
                    vec![checked_file(
                        first,
                        std::mem::take(&mut fh),
                        std::mem::take(&mut exts),
                    )?]
                } else {
                    Vec::new()
                };
                let seg = PlanSegment {
                    file: first.id,
                    offset: base,
                    bytes: len,
                };
                save(objects, obj, vec![seg], nf)?;
                (objects, base) = (objects + 1, base + len);
            }
            cur += 1;
            continue;
        }
        let end = pack_end(files, cur);
        let (mut oh, mut obj_bytes, mut members) = (Sha256::new(), 0u64, Vec::new());
        for f in &files[cur..end] {
            let (mut rd, mut fh) = (open(f)?, Sha256::new());
            read_exact::<E>(&mut rd, &mut buf, f.bytes, &mut fh, &mut oh)?;
            require_eof::<E>(&mut rd)?;
            members.push((f, fh, obj_bytes));
            obj_bytes += f.bytes;
        }
        let (obj, rid) = seal_object(scope, oh, obj_bytes, first.destination)?;
        let segs: Vec<_> = members
            .iter()
            .map(|(f, _, _)| PlanSegment {
                file: f.id,
                offset: 0,
                bytes: f.bytes,
            })
            .collect();
        let nfiles = members
            .into_iter()
            .map(|(f, fh, off)| checked_file(f, fh, vec![extent(rid.clone(), off, f.bytes)]))
            .collect::<Result<Vec<_>, _>>()?;
        let pack = first.bytes < SMALL_NATIVE_FILE_BYTES;
        (packs, multi) = (
            packs + u64::from(pack),
            multi + u64::from(pack && segs.len() > 1),
        );
        save(objects, obj, segs, nfiles)?;
        (objects, cur) = (objects + 1, end);
    }
    Ok((objects, packs, multi))
}

fn extent(receipt_id: Digest, offset: u64, length: u64) -> ClientExtent {
    ClientExtent {
        receipt_id,
        offset: DecimalU64::new(offset),
        length: DecimalU64::new(length),
    }
}

fn require_eof<E: From<PlanError> + From<io::Error>>(rd: &mut impl Read) -> Result<(), E> {
    (rd.read(&mut [0u8; 1])? == 0)
        .then_some(())
        .ok_or_else(|| PlanError::SourceLength.into())
}

fn seal_object(
    scope: &CloudScope,
    hash: Sha256,
    bytes: u64,
    destination: Destination,
) -> Result<(Object, Digest), PlanError> {
    let sha256 = Digest::try_from(lastdb_model::hex::hex_lower(hash.finalize()))?;
    let obj = Object {
        sha256,
        bytes: DecimalU64::new(bytes),
        destination,
    };
    Ok((obj.clone(), object_receipt(scope, &obj)?.receipt_id))
}

fn read_exact<E: From<PlanError> + From<io::Error>>(
    rd: &mut impl Read,
    buf: &mut [u8],
    mut rem: u64,
    fh: &mut Sha256,
    oh: &mut Sha256,
) -> Result<(), E> {
    while rem > 0 {
        let max = rem.min(buf.len() as u64) as usize;
        let n = rd.read(&mut buf[..max])?;
        if n == 0 {
            return Err(PlanError::SourceLength.into());
        }
        fh.update(&buf[..n]);
        oh.update(&buf[..n]);
        rem -= n as u64;
    }
    Ok(())
}

fn checked_file(
    file: &PlanFile,
    hash: Sha256,
    extents: Vec<ClientExtent>,
) -> Result<ClientNativeFile, PlanError> {
    let rec = ClientNativeFile {
        path: file.path.clone(),
        native_sha256: Digest::try_from(lastdb_model::hex::hex_lower(hash.finalize()))?,
        native_bytes: DecimalU64::new(file.bytes),
        extents,
    };
    rec.validate().map(|()| rec)
}

fn pack_end(files: &[PlanFile], start: usize) -> usize {
    let first = &files[start];
    let (mut end, mut bytes) = (start + 1, first.bytes);
    if first.bytes < SMALL_NATIVE_FILE_BYTES {
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
            (bytes, end) = (bytes + f.bytes, end + 1);
        }
    }
    end
}
