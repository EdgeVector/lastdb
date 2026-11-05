use super::{proof_expires, PeerWorker, RestoreError, Result};
use cloud_backup_protocol::shared::peer::PeerFileRequest;
use cloud_backup_protocol::shared::{
    WriterStream, MAX_BATCH_FILE_BYTES, MAX_SINGLE_BATCH_FILE_BYTES,
};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::portable_mutation::{MutationEntry, MutationOperation, Position, StreamId};
use std::collections::BTreeMap;
use tokio::io::{AsyncRead, AsyncReadExt};

impl PeerWorker<'_> {
    pub(super) async fn files(
        &self,
        reader: &WriterStream,
        entries: &[MutationEntry],
        needed: Vec<usize>,
    ) -> Result<BTreeMap<Position, Vec<u8>>> {
        let mut files = BTreeMap::new();
        let mut remaining = MAX_SINGLE_BATCH_FILE_BYTES;
        for index in needed {
            let entry = entries
                .get(index)
                .ok_or(RestoreError::Invalid("peer replay index"))?;
            let MutationOperation::ReplaceRecord { data, .. } = &entry.operation else {
                continue;
            };
            let Some(marker) = &data.file else { continue };
            if files.contains_key(&marker.version) {
                continue;
            }
            let maximum = marker
                .size_bytes
                .checked_add(MAX_SINGLE_BATCH_FILE_BYTES - MAX_BATCH_FILE_BYTES)
                .filter(|size| *size <= MAX_SINGLE_BATCH_FILE_BYTES)
                .ok_or(RestoreError::Limit("peer native file input"))?
                .min(remaining);
            let request = PeerFileRequest {
                reader: reader.clone(),
                source_stream: id(&entry.stamp.position.source_stream)?,
                sequence: DecimalU64::new(entry.stamp.position.sequence),
                file_stream: id(&marker.version.source_stream)?,
                file_sequence: DecimalU64::new(marker.version.sequence),
            };
            let object = self
                .client
                .file(&request, maximum, proof_expires()?, |bytes| {
                    Ok(self.identity.device_signer.sign(bytes).to_bytes())
                })
                .await?;
            let sealed = read_exact(object, maximum).await?;
            remaining -= sealed.len() as u64;
            files.insert(marker.version.clone(), sealed);
        }
        Ok(files)
    }
}

pub(super) async fn read_exact(mut input: impl AsyncRead + Unpin, maximum: u64) -> Result<Vec<u8>> {
    let capacity =
        usize::try_from(maximum).map_err(|_| RestoreError::Limit("peer object bytes"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| RestoreError::Limit("peer object bytes"))?;
    bytes.resize(capacity, 0);
    let mut filled = 0;
    while filled < capacity {
        let count = input
            .read(&mut bytes[filled..])
            .await
            .map_err(RestoreError::from)?;
        if count == 0 {
            bytes.truncate(filled);
            return Ok(bytes);
        }
        filled += count;
    }
    if input.read(&mut [0]).await.map_err(RestoreError::from)? != 0 {
        return Err(RestoreError::Limit("peer object bytes").into());
    }
    Ok(bytes)
}

fn id(stream: &StreamId) -> Result<Id> {
    Id::try_from(stream.as_str().to_owned())
        .map_err(RestoreError::from)
        .map_err(Into::into)
}
