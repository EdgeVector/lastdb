//! The one upload planner for native LastDB cloud backups.

use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{Object, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, ProtocolError, RelativePath};
use lastdb_model::storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::Read;

pub const MAX_FILE_EXTENTS: usize = 16384;
const READ_BYTES: usize = 65536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}

/// One native file and the object ranges that hold its bytes, in order.
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
        if self.native_bytes.get() == 0
            || self.extents.is_empty()
            || self.extents.len() > MAX_FILE_EXTENTS
        {
            return Err(PlanError::Invalid("byte native file extent count"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            let len = extent.length.get();
            if len == 0 || extent.offset.get().checked_add(len).is_none() {
                return Err(PlanError::Invalid("byte native extent bounds"));
            }
            bytes = bytes
                .checked_add(len)
                .ok_or(PlanError::Invalid("byte native extent overflow"))?;
        }
        if bytes != self.native_bytes.get() {
            return Err(PlanError::Invalid("byte native file exact length"));
        }
        Ok(())
    }
}

/// A refusal from the planner. Each caller maps it to its own error.
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

/// The receipt the service gives for `object` in `scope`.
pub fn object_receipt(scope: &CloudScope, object: &Object) -> Result<ObjectReceipt, ProtocolError> {
    let mut receipt = ObjectReceipt {
        version: Version::V2,
        receipt_id: object.sha256.clone(),
        scope: scope.clone(),
        object: object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

/// The receipt that confirms one transfer input.
pub fn expected_receipt(input: &TransferInput) -> Result<ObjectReceipt, ProtocolError> {
    input.validate()?;
    object_receipt(&input.job.stream.scope, &input.object)
}

/// One frozen native file, in plan order.
#[derive(Clone, Debug)]
pub struct PlanFile {
    pub id: usize,
    pub path: RelativePath,
    pub bytes: u64,
    pub destination: cloud_backup_protocol::v2::Destination,
}

/// One byte range of one frozen file inside an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSegment {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
}

/// One transfer object, before the caller gives it a request.
#[derive(Clone, Debug)]
pub struct PlannedObject {
    pub object: Object,
    pub segments: Vec<PlanSegment>,
    pub pack: bool,
    pub native_files: Vec<ClientNativeFile>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanTotals {
    pub objects: u64,
    pub packs: u64,
    pub multi_file_packs: u64,
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
    let mut totals = PlanTotals::default();
    let mut buffer = vec![0u8; READ_BYTES];
    let mut cursor = 0;
    while cursor < files.len() {
        if files[cursor].bytes > OBJECT_CHUNK_BYTES {
            plan_span(
                &files[cursor],
                scope,
                &mut buffer,
                &mut totals,
                &mut open,
                &mut save,
            )?;
            cursor += 1;
        } else {
            let end = pack_end(files, cursor);
            plan_pack(
                &files[cursor..end],
                scope,
                &mut buffer,
                &mut totals,
                &mut open,
                &mut save,
            )?;
            cursor = end;
        }
    }
    Ok(totals)
}

fn plan_pack<R, E>(
    group: &[PlanFile],
    scope: &CloudScope,
    buffer: &mut [u8],
    totals: &mut PlanTotals,
    open: &mut impl FnMut(&PlanFile) -> Result<R, E>,
    save: &mut impl FnMut(u64, PlannedObject) -> Result<(), E>,
) -> Result<(), E>
where
    R: Read,
    E: From<PlanError> + From<std::io::Error>,
{
    let first = &group[0];
    let pack = first.bytes < SMALL_NATIVE_FILE_BYTES;
    let mut object_hash = Sha256::new();
    let mut object_bytes = 0u64;
    let mut members = Vec::with_capacity(group.len());
    for file in group {
        let mut reader = open(file)?;
        let mut file_hash = Sha256::new();
        if !hash_exact(
            &mut reader,
            file.bytes,
            buffer,
            &mut file_hash,
            &mut object_hash,
        )? || !at_eof(&mut reader)?
        {
            return Err(PlanError::SourceLength.into());
        }
        members.push((file, digest(file_hash)?, object_bytes));
        object_bytes += file.bytes;
    }
    let (object, receipt_id) = seal_object(scope, object_hash, object_bytes, first.destination)?;
    let mut segments = Vec::with_capacity(members.len());
    let mut native_files = Vec::with_capacity(members.len());
    for (file, native_sha256, offset) in members {
        segments.push(PlanSegment {
            file: file.id,
            offset: 0,
            bytes: file.bytes,
        });
        let record = ClientNativeFile {
            path: file.path.clone(),
            native_sha256,
            native_bytes: DecimalU64::new(file.bytes),
            extents: vec![ClientExtent {
                receipt_id: receipt_id.clone(),
                offset: DecimalU64::new(offset),
                length: DecimalU64::new(file.bytes),
            }],
        };
        record.validate()?;
        native_files.push(record);
    }
    let index = totals.objects;
    totals.packs += u64::from(pack);
    totals.multi_file_packs += u64::from(pack && segments.len() > 1);
    totals.objects += 1;
    save(
        index,
        PlannedObject {
            object,
            segments,
            pack,
            native_files,
        },
    )
}

fn plan_span<R, E>(
    file: &PlanFile,
    scope: &CloudScope,
    buffer: &mut [u8],
    totals: &mut PlanTotals,
    open: &mut impl FnMut(&PlanFile) -> Result<R, E>,
    save: &mut impl FnMut(u64, PlannedObject) -> Result<(), E>,
) -> Result<(), E>
where
    R: Read,
    E: From<PlanError> + From<std::io::Error>,
{
    let mut reader = open(file)?;
    let mut file_hash = Sha256::new();
    let mut extents = Vec::new();
    let mut base = 0u64;
    while base < file.bytes {
        let length = OBJECT_CHUNK_BYTES.min(file.bytes - base);
        let mut object_hash = Sha256::new();
        if !hash_exact(
            &mut reader,
            length,
            buffer,
            &mut file_hash,
            &mut object_hash,
        )? {
            return Err(PlanError::SourceLength.into());
        }
        let (object, receipt_id) = seal_object(scope, object_hash, length, file.destination)?;
        extents.push(ClientExtent {
            receipt_id,
            offset: DecimalU64::new(0),
            length: DecimalU64::new(length),
        });
        if extents.len() > MAX_FILE_EXTENTS {
            return Err(PlanError::ExtentBound.into());
        }
        let native_files = if base + length == file.bytes {
            if !at_eof(&mut reader)? {
                return Err(PlanError::SourceLength.into());
            }
            let record = ClientNativeFile {
                path: file.path.clone(),
                native_sha256: digest(std::mem::take(&mut file_hash))?,
                native_bytes: DecimalU64::new(file.bytes),
                extents: std::mem::take(&mut extents),
            };
            record.validate()?;
            vec![record]
        } else {
            Vec::new()
        };
        let index = totals.objects;
        totals.objects += 1;
        save(
            index,
            PlannedObject {
                object,
                segments: vec![PlanSegment {
                    file: file.id,
                    offset: base,
                    bytes: length,
                }],
                pack: false,
                native_files,
            },
        )?;
        base += length;
    }
    Ok(())
}

fn seal_object(
    scope: &CloudScope,
    hash: Sha256,
    bytes: u64,
    destination: cloud_backup_protocol::v2::Destination,
) -> Result<(Object, Digest), PlanError> {
    let object = Object {
        sha256: digest(hash)?,
        bytes: DecimalU64::new(bytes),
        destination,
    };
    let receipt_id = object_receipt(scope, &object)?.receipt_id;
    Ok((object, receipt_id))
}

fn pack_end(files: &[PlanFile], start: usize) -> usize {
    let first = &files[start];
    let mut end = start + 1;
    if first.bytes < SMALL_NATIVE_FILE_BYTES {
        let mut bytes = first.bytes;
        while let Some(file) = files.get(end) {
            if file.bytes >= SMALL_NATIVE_FILE_BYTES
                || file.destination != first.destination
                || end - start >= MAX_PACK_MEMBERS
                || bytes
                    .checked_add(file.bytes)
                    .is_none_or(|n| n > METADATA_PACK_BYTES)
            {
                break;
            }
            bytes += file.bytes;
            end += 1;
        }
    }
    end
}

fn hash_exact(
    reader: &mut impl Read,
    mut remaining: u64,
    buffer: &mut [u8],
    file_hash: &mut Sha256,
    object_hash: &mut Sha256,
) -> std::io::Result<bool> {
    while remaining > 0 {
        let limit = buffer.len().min(remaining as usize);
        let count = reader.read(&mut buffer[..limit])?;
        if count == 0 {
            return Ok(false);
        }
        file_hash.update(&buffer[..count]);
        object_hash.update(&buffer[..count]);
        remaining -= count as u64;
    }
    Ok(true)
}

fn at_eof(reader: &mut impl Read) -> std::io::Result<bool> {
    Ok(reader.read(&mut [0u8; 1])? == 0)
}

fn digest(hash: Sha256) -> Result<Digest, PlanError> {
    Digest::try_from(lastdb_model::hex::hex_lower(hash.finalize())).map_err(PlanError::Protocol)
}
