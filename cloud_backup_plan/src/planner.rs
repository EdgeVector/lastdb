//! Raw native file concatenation. File length never decides an allocation:
//! every read uses one fixed buffer.
use super::{object_receipt, ClientExtent, ClientNativeFile, PlanError, MAX_FILE_EXTENTS};
use cloud_backup_protocol::v2::{Destination, Object};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, RelativePath};
use lastdb_storage_policy::backup_transport::{
    MAX_PACK_MEMBERS, METADATA_PACK_BYTES, OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES,
};
use sha2::{Digest as _, Sha256};
use std::io::Read;

/// One frozen native file, in plan order.
#[derive(Clone, Debug)]
pub struct PlanFile {
    /// The caller's file id. Reads, segments and file records name this id.
    pub id: usize,
    pub path: RelativePath,
    pub bytes: u64,
    pub destination: Option<Destination>,
}
/// One byte range of one frozen file inside an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanSegment {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
}
/// One transfer object, before the caller gives it a request.
#[derive(Clone, Debug)]
pub struct PlannedObject {
    pub object: Object,
    /// The ranges of the object bytes, in order.
    pub segments: Vec<PlanSegment>,
    /// A small-file pack: its first file is under the small-file size.
    pub pack: bool,
    /// Each file whose last byte is in this object, with all its extents.
    pub native_files: Vec<(usize, ClientNativeFile)>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanTotals {
    pub objects: u64,
    pub packs: u64,
    pub multi_file_packs: u64,
}

/// Plan `files` in the given order. `open` reads one whole frozen file, and
/// `save` keeps each object at its index, from 0 up. The same frozen files
/// give the same objects in the same order, so a resumed plan can compare.
pub fn plan<R, E>(
    files: &[PlanFile],
    scope: &CloudScope,
    mut open: impl FnMut(&PlanFile) -> Result<R, E>,
    mut save: impl FnMut(u64, PlannedObject) -> Result<(), E>,
) -> Result<PlanTotals, E>
where
    R: Read,
    E: From<PlanError>,
{
    let mut totals = PlanTotals::default();
    let mut next = 0;
    while let Some(first) = files.get(next) {
        if first.bytes > OBJECT_CHUNK_BYTES {
            large(first, scope, &mut open, &mut save, &mut totals.objects)?;
            next += 1;
            continue;
        }
        let mut end = next + 1;
        if first.bytes < SMALL_NATIVE_FILE_BYTES {
            let mut bytes = first.bytes;
            while let Some(file) = files.get(end) {
                if file.bytes >= SMALL_NATIVE_FILE_BYTES
                    || file.destination != first.destination
                    || end - next >= MAX_PACK_MEMBERS
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
        let planned = group(&files[next..end], scope, &mut open)?;
        totals.packs += u64::from(planned.pack);
        totals.multi_file_packs += u64::from(planned.pack && planned.segments.len() > 1);
        save(totals.objects, planned)?;
        totals.objects += 1;
        next = end;
    }
    Ok(totals)
}

fn group<R, E>(
    members: &[PlanFile],
    scope: &CloudScope,
    open: &mut impl FnMut(&PlanFile) -> Result<R, E>,
) -> Result<PlannedObject, E>
where
    R: Read,
    E: From<PlanError>,
{
    let mut hash = Sha256::new();
    let mut segments = Vec::with_capacity(members.len());
    let mut hashed = Vec::with_capacity(members.len());
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    for file in members {
        let mut reader = open(file)?;
        let mut file_hash = Sha256::new();
        let mut length = 0u64;
        loop {
            let count = reader.read(&mut buffer).map_err(PlanError::Io)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
            file_hash.update(&buffer[..count]);
            length += count as u64;
        }
        if length != file.bytes {
            return Err(PlanError::SourceLength.into());
        }
        segments.push(PlanSegment {
            file: file.id,
            offset: 0,
            bytes: length,
        });
        hashed.push((file, digest(file_hash)?, bytes));
        bytes += length;
    }
    let object = Object {
        sha256: digest(hash)?,
        bytes: DecimalU64::new(bytes),
        destination: members[0].destination,
    };
    let receipt_id = object_receipt(scope, &object)
        .map_err(PlanError::Protocol)?
        .receipt_id;
    let mut native_files = Vec::with_capacity(hashed.len());
    for (file, native_sha256, offset) in hashed {
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
        native_files.push((file.id, record));
    }
    Ok(PlannedObject {
        object,
        segments,
        pack: members[0].bytes < SMALL_NATIVE_FILE_BYTES,
        native_files,
    })
}

/// One object per chunk. The file record rides on the last chunk.
fn large<R, E>(
    file: &PlanFile,
    scope: &CloudScope,
    open: &mut impl FnMut(&PlanFile) -> Result<R, E>,
    save: &mut impl FnMut(u64, PlannedObject) -> Result<(), E>,
    index: &mut u64,
) -> Result<(), E>
where
    R: Read,
    E: From<PlanError>,
{
    let mut reader = open(file)?;
    let mut file_hash = Sha256::new();
    let mut offset = 0u64;
    let mut extents = Vec::new();
    let mut buffer = [0u8; 65536];
    while offset < file.bytes {
        let length = OBJECT_CHUNK_BYTES.min(file.bytes - offset);
        let mut hash = Sha256::new();
        let mut remaining = length;
        while remaining > 0 {
            let count = buffer.len().min(remaining as usize);
            reader
                .read_exact(&mut buffer[..count])
                .map_err(PlanError::Io)?;
            hash.update(&buffer[..count]);
            file_hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        let object = Object {
            sha256: digest(hash)?,
            bytes: DecimalU64::new(length),
            destination: file.destination,
        };
        extents.push(ClientExtent {
            receipt_id: object_receipt(scope, &object)
                .map_err(PlanError::Protocol)?
                .receipt_id,
            offset: DecimalU64::new(0),
            length: DecimalU64::new(length),
        });
        if extents.len() > MAX_FILE_EXTENTS {
            return Err(PlanError::ExtentBound.into());
        }
        let native_files = if offset + length == file.bytes {
            let record = ClientNativeFile {
                path: file.path.clone(),
                native_sha256: digest(file_hash.clone())?,
                native_bytes: DecimalU64::new(file.bytes),
                extents: std::mem::take(&mut extents),
            };
            record.validate()?;
            vec![(file.id, record)]
        } else {
            Vec::new()
        };
        save(
            *index,
            PlannedObject {
                object,
                segments: vec![PlanSegment {
                    file: file.id,
                    offset,
                    bytes: length,
                }],
                pack: false,
                native_files,
            },
        )?;
        *index += 1;
        offset += length;
    }
    if reader.read(&mut buffer[..1]).map_err(PlanError::Io)? != 0 {
        return Err(PlanError::SourceLength.into());
    }
    Ok(())
}

fn digest(hash: Sha256) -> Result<Digest, PlanError> {
    let hex: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Digest::try_from(hex)?)
}
