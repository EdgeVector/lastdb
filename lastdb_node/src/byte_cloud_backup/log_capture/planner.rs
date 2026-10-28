use super::*;
use cloud_backup_protocol::v2::{transfer::TransferInput, Object, Version};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::storage::disk_pack::byte_backup::{ClientExtent, ClientNativeFile, MAX_FILE_EXTENTS};
use model::{destination, expected, ObjectRecord, Segment};
use sha2::{Digest as _, Sha256};

const CHUNK: u64 = lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES;
const SMALL: u64 = lastdb_storage_policy::backup_transport::SMALL_NATIVE_FILE_BYTES;
const PACK: u64 = lastdb_storage_policy::backup_transport::METADATA_PACK_BYTES;

pub(super) fn build(
    capture: &mut LogCapture,
    recovery: &RecoveryDescriptor,
    key: &[u8; 32],
) -> Result<()> {
    let mut order: Vec<_> = (0..capture.frozen.files.len()).collect();
    order.sort_by(|a, b| {
        capture.frozen.files[*a]
            .path
            .as_str()
            .cmp(capture.frozen.files[*b].path.as_str())
    });
    let mut next = 0u64;
    let mut packs = 0;
    let mut multi = 0;
    let mut files = order.into_iter().peekable();
    while let Some(first) = files.next() {
        if capture.frozen.files[first].bytes > CHUNK {
            large(capture, &mut next, first)?;
            continue;
        }
        let mut members = vec![first];
        let first_file = &capture.frozen.files[first];
        let mut bytes = first_file.bytes;
        if bytes < SMALL {
            while let Some(index) = files.peek() {
                let file = &capture.frozen.files[*index];
                if file.bytes >= SMALL
                    || destination(file) != destination(first_file)
                    || members.len() >= lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
                    || bytes.checked_add(file.bytes).is_none_or(|n| n > PACK)
                {
                    break;
                }
                bytes += file.bytes;
                members.push(files.next().ok_or(BackupError::Invalid)?);
            }
            packs += 1;
            multi += u64::from(members.len() > 1);
        }
        group(capture, next, &members)?;
        next += 1;
    }
    capture.plan.packs = packs;
    capture.plan.multi_file_packs = multi;
    metadata::finish(capture, recovery, key, next)?;
    capture.plan.ready = true;
    capture.store.write("log-plan.json", &capture.plan)
}

fn group(capture: &mut LogCapture, index: u64, members: &[usize]) -> Result<()> {
    let mut hash = Sha256::new();
    let mut records = Vec::new();
    let mut segments = Vec::new();
    let mut bytes = 0;
    let mut buffer = [0u8; 65536];
    for file_index in members {
        let file = &capture.frozen.files[*file_index];
        let mut reader = capture.core.reader(*file_index, 0, file.bytes)?;
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
            return Err(BackupError::Identity);
        }
        segments.push(Segment {
            file: *file_index,
            offset: 0,
            bytes: length,
        });
        records.push((
            *file_index,
            ClientNativeFile {
                path: file.path.clone(),
                native_sha256: digest(file_hash)?,
                native_bytes: DecimalU64::new(length),
                extents: vec![ClientExtent {
                    receipt_id: bytes_sha256(b"pending"),
                    offset: DecimalU64::new(bytes),
                    length: DecimalU64::new(length),
                }],
            },
        ));
        bytes += length;
    }
    let input = input(
        capture,
        index,
        Object {
            sha256: digest(hash)?,
            bytes: DecimalU64::new(bytes),
            destination: Some(destination(&capture.frozen.files[members[0]])),
        },
    )?;
    let receipt = expected(&input)?;
    for (file_index, mut file) in records {
        file.extents[0].receipt_id = receipt.receipt_id.clone();
        file.validate()?;
        capture.store.write(&model::file_name(file_index), &file)?;
    }
    save(
        capture,
        index,
        ObjectRecord {
            input,
            segments,
            payload: None,
            receipt: None,
        },
    )
}

fn large(capture: &mut LogCapture, index: &mut u64, file_index: usize) -> Result<()> {
    let file = capture.frozen.files[file_index].clone();
    let mut reader = capture.core.reader(file_index, 0, file.bytes)?;
    let mut file_hash = Sha256::new();
    let mut offset = 0;
    let mut extents = Vec::new();
    let mut buffer = [0u8; 65536];
    while offset < file.bytes {
        let length = CHUNK.min(file.bytes - offset);
        let mut hash = Sha256::new();
        let mut remaining = length;
        while remaining > 0 {
            let count = buffer.len().min(remaining as usize);
            reader.read_exact(&mut buffer[..count])?;
            hash.update(&buffer[..count]);
            file_hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        let input = input(
            capture,
            *index,
            Object {
                sha256: digest(hash)?,
                bytes: DecimalU64::new(length),
                destination: Some(destination(&file)),
            },
        )?;
        extents.push(ClientExtent {
            receipt_id: expected(&input)?.receipt_id,
            offset: DecimalU64::new(0),
            length: DecimalU64::new(length),
        });
        if extents.len() > MAX_FILE_EXTENTS {
            return Err(BackupError::Limit);
        }
        save(
            capture,
            *index,
            ObjectRecord {
                input,
                segments: vec![Segment {
                    file: file_index,
                    offset,
                    bytes: length,
                }],
                payload: None,
                receipt: None,
            },
        )?;
        *index += 1;
        offset += length;
    }
    if reader.read(&mut buffer[..1])? != 0 {
        return Err(BackupError::Identity);
    }
    let record = ClientNativeFile {
        path: file.path,
        native_sha256: digest(file_hash)?,
        native_bytes: DecimalU64::new(file.bytes),
        extents,
    };
    record.validate()?;
    capture.store.write(&model::file_name(file_index), &record)
}

pub(super) fn input(capture: &LogCapture, index: u64, object: Object) -> Result<TransferInput> {
    if index >= model::MAX_OBJECTS {
        return Err(BackupError::Limit);
    }
    if let Some(prior) = capture
        .store
        .read::<ObjectRecord>(&model::object_name(index))?
    {
        prior.validate(&capture.frozen, index)?;
        if prior.input.object != object {
            return Err(BackupError::Identity);
        }
        return Ok(prior.input);
    }
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
        job: capture.frozen.begin.job.clone(),
        object,
    };
    input.validate()?;
    Ok(input)
}
pub(super) fn save(capture: &mut LogCapture, index: u64, mut record: ObjectRecord) -> Result<()> {
    record.validate(&capture.frozen, index)?;
    if let Some(prior) = capture
        .store
        .read::<ObjectRecord>(&model::object_name(index))?
    {
        prior.validate(&capture.frozen, index)?;
        record.receipt = prior.receipt.clone();
        if prior != record {
            return Err(BackupError::Identity);
        }
    }
    capture.store.write(&model::object_name(index), &record)?;
    capture.plan.objects = capture.plan.objects.max(index + 1);
    capture.budget(capture.store.bytes())?;
    capture.store.write("log-plan.json", &capture.plan)
}
fn digest(hash: Sha256) -> Result<Digest> {
    Ok(Digest::try_from(fold_db::hex::hex_lower(hash.finalize()))?)
}
