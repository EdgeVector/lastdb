use super::{Result, SharedObjectUploader, UploadError};
use cloud_backup_protocol::shared::{FileDependency, LogBatch, SealedObject, WriterStream};
use cloud_backup_protocol::{bytes_sha256, input_digest, DecimalU64, Id};
use fold_db::fold_db_core::MutationManager;
use fold_db::portable_mutation::{MutationEntry, MutationOperation, Position};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::BTreeMap;

const FILE_TRANSFER_SLOTS: usize = 2;

fn file_entries(entries: &[MutationEntry]) -> BTreeMap<&Position, &MutationEntry> {
    entries
        .iter()
        .filter_map(|entry| match &entry.operation {
            MutationOperation::ReplaceRecord { data, .. } => {
                data.file.as_ref().map(|file| (&file.version, entry))
            }
            MutationOperation::DeleteRecord { .. } => None,
        })
        .collect()
}

pub(super) async fn dependencies(
    native: &MutationManager,
    writer: &WriterStream,
    entries: &[MutationEntry],
) -> Result<Vec<FileDependency>> {
    stream::iter(
        file_entries(entries)
            .into_iter()
            .map(|(position, entry)| async move {
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
            }),
    )
    .buffered(FILE_TRANSFER_SLOTS)
    .try_collect()
    .await
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
    let requests = files.into_iter().zip(&batch.file_versions);
    stream::iter(requests.map(|((position, entry), expected)| async move {
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
        uploader.put_file(&batch.stream, expected, sealed).await
    }))
    .buffer_unordered(FILE_TRANSFER_SLOTS)
    .try_for_each(|()| async { Ok(()) })
    .await
}
