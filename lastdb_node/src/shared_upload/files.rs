use super::{Result, SharedObjectUploader, UploadError};
use cloud_backup_protocol::shared::{
    FileDependency, LogBatch, SealedObject, WriterStream, MAX_BATCH_FILE_BYTES,
    MAX_SINGLE_BATCH_FILE_BYTES,
};
use cloud_backup_protocol::{bytes_sha256, input_digest, DecimalU64, Id};
use fold_db::fold_db_core::MutationManager;
use fold_db::portable_mutation::{MutationEntry, MutationOperation, Position};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet};

const FILE_TRANSFER_SLOTS: usize = 2;

pub(super) fn bounded_prefix(entries: &[MutationEntry]) -> Result<&[MutationEntry]> {
    let mut versions = BTreeSet::new();
    let mut bytes = 0u64;
    for (index, entry) in entries.iter().enumerate() {
        let MutationOperation::ReplaceRecord { data, .. } = &entry.operation else {
            continue;
        };
        let Some(file) = &data.file else {
            continue;
        };
        if !versions.insert(&file.version) {
            continue;
        }
        // The marker carries plaintext length. Reserve the bounded envelope
        // overhead before reading or allocating any retained file bytes.
        let sealed = file
            .size_bytes
            .checked_add(MAX_SINGLE_BATCH_FILE_BYTES - MAX_BATCH_FILE_BYTES)
            .filter(|size| *size <= MAX_SINGLE_BATCH_FILE_BYTES)
            .ok_or(UploadError::Invalid("portable file bytes"))?;
        let next = bytes + sealed;
        if versions.len() > 1 && next > MAX_BATCH_FILE_BYTES {
            return Ok(&entries[..index]);
        }
        bytes = next;
    }
    Ok(entries)
}

fn file_entries(entries: &[MutationEntry]) -> BTreeMap<&Position, &MutationEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            entry
                .operation
                .file_version()
                .map(|version| (version, entry))
        })
        .collect()
}

pub(super) async fn dependencies(
    native: &MutationManager,
    writer: &WriterStream,
    entries: &[MutationEntry],
) -> Result<Vec<FileDependency>> {
    let files = file_entries(entries);
    let mut requests = Vec::with_capacity(files.len());
    for (position, entry) in files {
        requests.push(dependency(native, writer, position, entry));
    }
    stream::iter(requests)
        .buffered(FILE_TRANSFER_SLOTS)
        .try_collect()
        .await
}

async fn dependency(
    native: &MutationManager,
    writer: &WriterStream,
    position: &Position,
    entry: &MutationEntry,
) -> Result<FileDependency> {
    let sealed = native.portable_outgoing_file(entry).await?;
    let source_stream = Id::try_from(position.source_stream.as_str().to_owned())?;
    let sequence = DecimalU64::new(position.sequence);
    let sha256 = bytes_sha256(&sealed);
    // A later replacement or another device can retain this logical file
    // version. The same exact bytes resolve to the same transport identity.
    let identity = input_digest(
        "lastdb:shared-log:file-object:v1",
        &(&writer.database_id, &source_stream, sequence, &sha256),
    )?;
    let digest = fold_db::hex::hex_decode_array::<32>(identity.as_str())
        .ok_or(UploadError::Invalid("file object digest"))?;
    let mut uuid: [u8; 16] = std::array::from_fn(|index| digest[index]);
    uuid[6] = (uuid[6] & 0x0f) | 0x80;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    let object = SealedObject {
        version_id: Id::try_from(uuid::Uuid::from_bytes(uuid).to_string())?,
        bytes: DecimalU64::new(sealed.len() as u64),
        sha256,
    };
    Ok(FileDependency {
        source_stream,
        sequence,
        object,
    })
}

pub(super) async fn upload(
    native: &MutationManager,
    uploader: &impl SharedObjectUploader,
    batch: &LogBatch,
    entries: &[MutationEntry],
) -> Result<()> {
    let files = file_entries(entries);
    if files.len() != batch.file_versions.len() {
        return Err(UploadError::Invalid("frozen file versions"));
    }
    let mut requests = Vec::with_capacity(files.len());
    for ((position, entry), expected) in files.into_iter().zip(&batch.file_versions) {
        requests.push(upload_file(
            native,
            uploader,
            &batch.stream,
            position,
            entry,
            expected,
        ));
    }
    stream::iter(requests)
        .buffer_unordered(FILE_TRANSFER_SLOTS)
        .try_for_each(|()| async { Ok(()) })
        .await
}

async fn upload_file(
    native: &MutationManager,
    uploader: &impl SharedObjectUploader,
    writer: &WriterStream,
    position: &Position,
    entry: &MutationEntry,
    expected: &FileDependency,
) -> Result<()> {
    if position.source_stream.as_str() != expected.source_stream.as_str()
        || position.sequence != expected.sequence.get()
    {
        return Err(UploadError::Invalid("frozen file position"));
    }
    let sealed = native.portable_outgoing_file(entry).await?;
    if sealed.len() as u64 != expected.object.bytes.get()
        || bytes_sha256(&sealed) != expected.object.sha256
    {
        return Err(UploadError::Invalid("frozen file bytes"));
    }
    uploader.put_file(writer, expected, sealed).await
}
