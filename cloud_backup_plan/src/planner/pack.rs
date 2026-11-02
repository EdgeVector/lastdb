use super::{
    digest, receipt, Member, Pack, Phase, PlanError, PlanSegment, PlanState, PlannedObject,
};
use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::DecimalU64;
use sha2::{Digest as _, Sha256};

pub(super) fn absorb(state: &mut PlanState, bytes: &[u8]) -> Result<(), PlanError> {
    let pack = pack(state)?;
    pack.object_hash.update(bytes);
    pack.file_hash.update(bytes);
    pack.at += bytes.len() as u64;
    Ok(())
}

pub(super) fn finish_member(state: &mut PlanState) -> Result<(), PlanError> {
    let file = match &state.phase {
        Phase::Pack(pack) => pack.file,
        _ => return Err(PlanError::Invalid("byte plan range")),
    };
    let bytes = state.files[file].bytes;
    let done = {
        let pack = pack(state)?;
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
        finish(state)?;
    }
    Ok(())
}

fn finish(state: &mut PlanState) -> Result<(), PlanError> {
    let Phase::Pack(pack) = std::mem::replace(&mut state.phase, Phase::Finished) else {
        return Err(PlanError::Invalid("byte plan range"));
    };
    let destination = state.files[pack.start].destination;
    let small = state.files[pack.start].bytes
        < lastdb_storage_policy::backup_transport::SMALL_NATIVE_FILE_BYTES;
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
        let record = crate::ClientNativeFile {
            path: file.path.clone(),
            native_sha256: member.sha256,
            native_bytes: DecimalU64::new(file.bytes),
            extents: vec![crate::ClientExtent {
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

fn pack(state: &mut PlanState) -> Result<&mut Pack, PlanError> {
    match &mut state.phase {
        Phase::Pack(pack) => Ok(pack),
        _ => Err(PlanError::Invalid("byte plan range")),
    }
}
