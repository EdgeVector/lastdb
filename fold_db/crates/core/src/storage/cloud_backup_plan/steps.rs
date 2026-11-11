//! Pack and chunk-span state machines for [`super::PlanState`].

use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest};
use lastdb_model::storage_policy::backup_transport::{OBJECT_CHUNK_BYTES, SMALL_NATIVE_FILE_BYTES};
use sha2::{Digest as _, Sha256};

use super::{
    object_receipt, ClientExtent, ClientNativeFile, Phase, PlanError, PlanFile, PlanSegment,
    PlanState, PlannedObject, Range, MAX_FILE_EXTENTS, READ_BYTES,
};

#[derive(Debug)]
pub(super) struct Pack {
    start: usize,
    end: usize,
    pub(super) file: usize,
    pub(super) at: u64,
    object_at: u64,
    object_hash: Sha256,
    file_hash: Sha256,
    members: Vec<Member>,
}

#[derive(Debug)]
struct Member {
    file: usize,
    sha256: Digest,
    offset: u64,
}

#[derive(Debug)]
pub(super) struct Span {
    file: usize,
    base: u64,
    filled: u64,
    length: u64,
    object_hash: Sha256,
    file_hash: Sha256,
    extents: Vec<ClientExtent>,
}

impl Pack {
    pub(super) fn open(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            file: start,
            at: 0,
            object_at: 0,
            object_hash: Sha256::new(),
            file_hash: Sha256::new(),
            members: Vec::new(),
        }
    }

    pub(super) fn range(&self, files: &[PlanFile]) -> Range {
        let file = &files[self.file];
        if self.at == file.bytes {
            probe(file)
        } else {
            Range {
                file: file.id,
                offset: self.at,
                bytes: READ_BYTES.min(file.bytes - self.at),
                exact: false,
            }
        }
    }
}

impl Span {
    pub(super) fn open(file: usize, bytes: u64) -> Self {
        Self {
            file,
            base: 0,
            filled: 0,
            length: OBJECT_CHUNK_BYTES.min(bytes),
            object_hash: Sha256::new(),
            file_hash: Sha256::new(),
            extents: Vec::new(),
        }
    }

    pub(super) fn range(&self, files: &[PlanFile]) -> Range {
        Range {
            file: files[self.file].id,
            offset: self.base + self.filled,
            bytes: READ_BYTES.min(self.length - self.filled),
            exact: true,
        }
    }
}

pub(super) fn probe(file: &PlanFile) -> Range {
    Range {
        file: file.id,
        offset: file.bytes,
        bytes: 1,
        exact: false,
    }
}

pub(super) fn absorb_pack(state: &mut PlanState, bytes: &[u8]) -> Result<(), PlanError> {
    let pack = pack_mut(state)?;
    pack.object_hash.update(bytes);
    pack.file_hash.update(bytes);
    pack.at += bytes.len() as u64;
    Ok(())
}

pub(super) fn finish_pack_member(state: &mut PlanState) -> Result<(), PlanError> {
    let file = match &state.phase {
        Phase::Pack(pack) => pack.file,
        _ => return Err(PlanError::Invalid("byte plan range")),
    };
    let bytes = state.files[file].bytes;
    let done = {
        let pack = pack_mut(state)?;
        let sha256 = digest(std::mem::replace(&mut pack.file_hash, Sha256::new()))?;
        pack.members.push(Member {
            file,
            sha256,
            offset: pack.object_at,
        });
        pack.object_at += bytes;
        pack.file += 1;
        pack.at = 0;
        pack.file == pack.end
    };
    if done {
        finish_pack(state)?;
    }
    Ok(())
}

fn finish_pack(state: &mut PlanState) -> Result<(), PlanError> {
    let Phase::Pack(pack) = std::mem::replace(&mut state.phase, Phase::Finished) else {
        return Err(PlanError::Invalid("byte plan range"));
    };
    let destination = state.files[pack.start].destination;
    let small = state.files[pack.start].bytes < SMALL_NATIVE_FILE_BYTES;
    let object = Object {
        sha256: digest(pack.object_hash)?,
        bytes: DecimalU64::new(pack.object_at),
        destination,
    };
    let receipt_id = receipt(&state.scope, &object)?;
    let mut segments = Vec::with_capacity(pack.members.len());
    let mut native_files = Vec::with_capacity(pack.members.len());
    for member in pack.members {
        let file = &state.files[member.file];
        segments.push(PlanSegment {
            file: file.id,
            offset: 0,
            bytes: file.bytes,
        });
        let record = ClientNativeFile {
            path: file.path.clone(),
            native_sha256: member.sha256,
            native_bytes: DecimalU64::new(file.bytes),
            extents: vec![ClientExtent {
                receipt_id: receipt_id.clone(),
                offset: DecimalU64::new(member.offset),
                length: DecimalU64::new(file.bytes),
            }],
        };
        record.validate()?;
        native_files.push((file.id, record));
    }
    let end = pack.end;
    state.stage(
        PlannedObject {
            object,
            segments,
            pack: small,
            native_files,
        },
        Phase::Choose(end),
    );
    Ok(())
}

pub(super) fn absorb_span(state: &mut PlanState, bytes: &[u8]) -> Result<(), PlanError> {
    let done = {
        let span = span_mut(state)?;
        span.object_hash.update(bytes);
        span.file_hash.update(bytes);
        span.filled += bytes.len() as u64;
        span.filled == span.length
    };
    if done {
        finish_span(state)?;
    }
    Ok(())
}

fn finish_span(state: &mut PlanState) -> Result<(), PlanError> {
    let Phase::Span(span) = std::mem::replace(&mut state.phase, Phase::Finished) else {
        return Err(PlanError::Invalid("byte plan range"));
    };
    let file = &state.files[span.file];
    let file_id = file.id;
    let file_bytes = file.bytes;
    let path = file.path.clone();
    let destination = file.destination;
    let base = span.base;
    let length = span.length;
    let object = Object {
        sha256: digest(span.object_hash)?,
        bytes: DecimalU64::new(length),
        destination,
    };
    let mut extents = span.extents;
    extents.push(extent(receipt(&state.scope, &object)?, length));
    bounded(&extents)?;
    let last = base + length == file_bytes;
    let (native_files, phase) = if last {
        let record = ClientNativeFile {
            path,
            native_sha256: digest(span.file_hash)?,
            native_bytes: DecimalU64::new(file_bytes),
            extents,
        };
        record.validate()?;
        (vec![(file_id, record)], Phase::Tail(span.file))
    } else {
        let next = base + length;
        let phase = Phase::Span(Box::new(Span {
            file: span.file,
            base: next,
            filled: 0,
            length: OBJECT_CHUNK_BYTES.min(file_bytes - next),
            object_hash: Sha256::new(),
            file_hash: span.file_hash,
            extents,
        }));
        (Vec::new(), phase)
    };
    state.stage(
        PlannedObject {
            object,
            segments: vec![PlanSegment {
                file: file_id,
                offset: base,
                bytes: length,
            }],
            pack: false,
            native_files,
        },
        phase,
    );
    Ok(())
}

fn pack_mut(state: &mut PlanState) -> Result<&mut Pack, PlanError> {
    match &mut state.phase {
        Phase::Pack(pack) => Ok(pack),
        _ => Err(PlanError::Invalid("byte plan range")),
    }
}

fn span_mut(state: &mut PlanState) -> Result<&mut Span, PlanError> {
    match &mut state.phase {
        Phase::Span(span) => Ok(span),
        _ => Err(PlanError::Invalid("byte plan range")),
    }
}

fn digest(hash: Sha256) -> Result<Digest, PlanError> {
    let hex: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Digest::try_from(hex)?)
}

fn receipt(scope: &CloudScope, object: &Object) -> Result<Digest, PlanError> {
    Ok(object_receipt(scope, object)
        .map_err(PlanError::Protocol)?
        .receipt_id)
}

fn extent(receipt_id: Digest, length: u64) -> ClientExtent {
    ClientExtent {
        receipt_id,
        offset: DecimalU64::new(0),
        length: DecimalU64::new(length),
    }
}

fn bounded(extents: &[ClientExtent]) -> Result<(), PlanError> {
    if extents.len() > MAX_FILE_EXTENTS {
        Err(PlanError::ExtentBound)
    } else {
        Ok(())
    }
}
