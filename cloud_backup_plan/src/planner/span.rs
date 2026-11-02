use super::{
    bounded, digest, extent, receipt, Phase, PlanError, PlanSegment, PlanState, PlannedObject,
};
use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::DecimalU64;
use lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES;
use sha2::Digest as _;

pub(super) fn absorb(state: &mut PlanState, bytes: &[u8]) -> Result<(), PlanError> {
    let done = {
        let span = span(state)?;
        span.object_hash.update(bytes);
        span.file_hash.update(bytes);
        span.filled += bytes.len() as u64;
        span.filled == span.length
    };
    if done {
        finish(state)?;
    }
    Ok(())
}

fn finish(state: &mut PlanState) -> Result<(), PlanError> {
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
        let record = crate::ClientNativeFile {
            path,
            native_sha256: digest(span.file_hash)?,
            native_bytes: DecimalU64::new(file_bytes),
            extents,
        };
        record.validate()?;
        (vec![(file_id, record)], Phase::Tail(span.file))
    } else {
        let next = base + length;
        let phase = Phase::Span(Box::new(super::Span {
            file: span.file,
            base: next,
            filled: 0,
            length: OBJECT_CHUNK_BYTES.min(file_bytes - next),
            object_hash: sha2::Sha256::new(),
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

fn span(state: &mut PlanState) -> Result<&mut super::Span, PlanError> {
    match &mut state.phase {
        Phase::Span(span) => Ok(span),
        _ => Err(PlanError::Invalid("byte plan range")),
    }
}
