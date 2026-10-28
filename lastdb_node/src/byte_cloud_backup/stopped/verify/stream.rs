use super::*;
use cloud_backup_protocol::Digest;
use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;

pub(super) async fn object(
    client: &ByteBackupClient,
    grant: &ReadGrant,
    receipt: &Digest,
    spans: &BTreeMap<Digest, Vec<plan::Span>>,
    files: &mut [plan::FileHash],
) -> Result<()> {
    let mut input = client.open(grant).await?;
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut offset = 0u64;
    let mut hash = Sha256::new();
    while offset < grant.bytes.get() {
        let maximum = (grant.bytes.get() - offset).min(buffer.len() as u64) as usize;
        let n = input.read(&mut buffer[..maximum]).await?;
        if n == 0 {
            return Err(BackupError::Source);
        }
        let end = offset + n as u64;
        hash.update(&buffer[..n]);
        if let Some(parts) = spans.get(receipt) {
            for part in parts {
                let first = offset.max(part.object_offset);
                let last = end.min(part.object_offset + part.bytes);
                if first < last {
                    let file = &mut files[part.file];
                    if file.done != part.file_offset + first - part.object_offset {
                        return Err(BackupError::Identity);
                    }
                    file.hash
                        .update(&buffer[(first - offset) as usize..(last - offset) as usize]);
                    file.done += last - first;
                }
            }
        }
        offset = end;
    }
    if input.read(&mut buffer[..1]).await? != 0
        || fold_db::hex::hex_lower(hash.finalize()) != grant.sha256.as_str()
    {
        return Err(BackupError::Source);
    }
    Ok(())
}

pub(super) async fn metadata(client: &ByteBackupClient, grant: &ReadGrant) -> Result<()> {
    grant.validate()?;
    object(client, grant, &grant.sha256, &BTreeMap::new(), &mut []).await
}
