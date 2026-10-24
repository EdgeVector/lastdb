//! Raw native file concatenation and client metadata planning. No network authority.
use super::{
    invalid,
    model::{ObjectRecord, Segment, State},
    protocol, ClientExtent, ClientNativeFile, Inner, StorageResult,
};
use cloud_backup_protocol::v2::{
    transfer::{ObjectReceipt, TransferInput},
    Object, Version,
};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, Id, RelativePath};
use laststore::disk_pack::backup::export::SnapshotFile;
use sha2::{Digest as _, Sha256};
use std::io::Read;

mod large;
mod metadata;
/// Client chunk policy. This is not a V2 wire or native-file length limit.
pub(super) const OBJECT_BYTES: u64 = lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES;
const SMALL_FILE_BYTES: u64 = lastdb_storage_policy::backup_transport::SMALL_NATIVE_FILE_BYTES;
const PACK_BYTES: u64 = lastdb_storage_policy::backup_transport::METADATA_PACK_BYTES;

pub(super) fn build(inner: &Inner, state: &mut State, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    state.native_files = source.files().len() as u64;
    state.native_bytes = source.bytes();
    state.metadata_pack_objects = 0;
    state.multi_file_pack_objects = 0;
    inner.store.write("job.json", state)?;
    let mut index = 0;
    let mut files = source.files().iter().peekable();
    while let Some(first) = files.next() {
        if first.bytes > OBJECT_BYTES {
            large::plan(inner, state, &mut index, first)?;
            continue;
        }
        let mut members = vec![first];
        let mut bytes = first.bytes;
        if first.bytes < SMALL_FILE_BYTES {
            while let Some(next) = files.peek() {
                if next.bytes >= SMALL_FILE_BYTES
                    || members.len() >= lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
                    || bytes + next.bytes > PACK_BYTES
                {
                    break;
                }
                bytes += next.bytes;
                members.push(
                    files
                        .next()
                        .ok_or_else(|| invalid("byte native member missing"))?,
                );
            }
        }
        let object = group(inner, state, index, &members)?;
        state.metadata_pack_objects += u64::from(object.metadata_pack);
        state.multi_file_pack_objects += u64::from(object.metadata_pack && members.len() > 1);
        save(inner, state, index, object)?;
        index += 1;
    }
    metadata::finish(inner, state, key, index)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}
fn group(
    inner: &Inner,
    state: &State,
    index: u64,
    members: &[&SnapshotFile],
) -> StorageResult<ObjectRecord> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    let mut hash = Sha256::new();
    let mut native_files = Vec::new();
    let mut segments = Vec::new();
    let mut bytes = 0;
    let mut buffer = [0u8; 65536];
    for file in members {
        let mut reader = inner.home.reader(source, &file.path)?;
        let mut file_hash = Sha256::new();
        let mut length = 0u64;
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
            file_hash.update(&buffer[..count]);
            length += count as u64;
        }
        if length != file.bytes {
            return Err(invalid("byte native source length changed"));
        }
        let path = RelativePath::try_from(
            file.path
                .to_str()
                .ok_or_else(|| invalid("byte native UTF8 path"))?
                .to_owned(),
        )
        .map_err(protocol)?;
        segments.push(Segment {
            path: path.clone(),
            offset: 0,
            bytes: length,
        });
        native_files.push(ClientNativeFile {
            path,
            native_sha256: hex(file_hash.finalize().into())?,
            native_bytes: DecimalU64::new(length),
            extents: vec![ClientExtent {
                receipt_id: bytes_sha256(b"unfrozen"),
                offset: DecimalU64::new(bytes),
                length: DecimalU64::new(length),
            }],
        });
        bytes += length;
    }
    let input = input(
        inner,
        state,
        index,
        Object {
            sha256: hex(hash.finalize().into())?,
            bytes: DecimalU64::new(bytes),
        },
    )?;
    let receipt = expected_receipt(&input)?;
    for file in &mut native_files {
        file.extents[0].receipt_id = receipt.receipt_id.clone();
    }
    Ok(ObjectRecord {
        input,
        segments,
        native_files,
        metadata_pack: members[0].bytes < SMALL_FILE_BYTES,
        ciphertext: None,
        receipt: None,
    })
}
fn input(inner: &Inner, state: &State, index: u64, object: Object) -> StorageResult<TransferInput> {
    if index >= super::model::MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    if inner.store.exists(&super::model::object_name(index))? {
        let prior: ObjectRecord = inner.store.read(&super::model::object_name(index))?;
        prior.validate(&state.begin)?;
        if prior.input.object != object {
            return Err(invalid("byte frozen object bytes differ"));
        }
        return Ok(prior.input);
    }
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
        job: state.begin.job.clone(),
        object,
    };
    input.validate().map_err(protocol)?;
    Ok(input)
}
fn save(
    inner: &Inner,
    state: &mut State,
    index: u64,
    mut object: ObjectRecord,
) -> StorageResult<()> {
    let name = super::model::object_name(index);
    object.validate(&state.begin)?;
    if inner.store.exists(&name)? {
        let prior: ObjectRecord = inner.store.read(&name)?;
        prior.validate(&state.begin)?;
        if prior.input != object.input
            || prior.segments != object.segments
            || prior.native_files != object.native_files
            || prior.metadata_pack != object.metadata_pack
            || prior.ciphertext != object.ciphertext
        {
            return Err(invalid("byte frozen object plan differs"));
        }
        object.receipt = prior.receipt;
    }
    inner.store.write(&name, &object)?;
    state.objects = state.objects.max(index + 1);
    inner.store.write("job.json", state)
}
pub(super) fn expected_receipt(input: &TransferInput) -> StorageResult<ObjectReceipt> {
    let mut receipt = ObjectReceipt {
        version: Version::V2,
        receipt_id: input.object.sha256.clone(),
        scope: input.job.stream.scope.clone(),
        object: input.object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256().map_err(protocol)?;
    receipt.validate().map_err(protocol)?;
    Ok(receipt)
}
fn hex(value: [u8; 32]) -> StorageResult<Digest> {
    Digest::try_from(crate::hex::hex_lower(value)).map_err(protocol)
}
