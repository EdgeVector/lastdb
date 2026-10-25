//! One bounded reader per client stream; no native body is spooled or decoded.
use super::{download, inventory::Inventory, record_bridge, record_plan::StreamPlan};
use super::{source_error, ByteBackupClient, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::access::{ReadGrant, ReadSelector};
use fold_db::storage::disk_pack::byte_backup::ClientStreamChunk;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt;

pub(super) async fn feed(
    session: &RestoreSession,
    client: &ByteBackupClient,
    inventory: &Inventory,
    plan: &StreamPlan,
    sender: record_bridge::Sender,
    key: &[u8; 32],
) -> Result<()> {
    for chunk in &plan.chunks {
        let grant = download::one(
            session,
            client,
            ReadSelector::Receipt {
                receipt_id: chunk.receipt_id.clone(),
            },
        )
        .await?;
        grant.validate_for_receipt(inventory.receipt(&chunk.receipt_id)?)?;
        if chunk.stream.encrypted() {
            encrypted(client, &grant, chunk, &sender, key).await?;
        } else {
            raw(client, &grant, &sender).await?;
        }
    }
    drop(sender);
    Ok(())
}

async fn raw(
    client: &ByteBackupClient,
    grant: &ReadGrant,
    sender: &record_bridge::Sender,
) -> Result<()> {
    let mut stream = client.open(grant).await.map_err(source_error)?;
    let mut buffer = vec![0; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut left = grant.bytes.get();
    let mut digest = Sha256::new();
    while left != 0 {
        let maximum = left.min(buffer.len() as u64) as usize;
        let count = stream.read(&mut buffer[..maximum]).await?;
        if count == 0 {
            return Err(RestoreError::Invalid("short record stream object"));
        }
        digest.update(&buffer[..count]);
        record_bridge::send(sender, buffer[..count].to_vec()).await?;
        left -= count as u64;
    }
    if stream.read(&mut buffer[..1]).await? != 0
        || hex::encode(digest.finalize()) != grant.sha256.as_str()
    {
        return Err(RestoreError::Invalid("record stream object digest or EOF"));
    }
    Ok(())
}

async fn encrypted(
    client: &ByteBackupClient,
    grant: &ReadGrant,
    chunk: &ClientStreamChunk,
    sender: &record_bridge::Sender,
    key: &[u8; 32],
) -> Result<()> {
    let maximum = lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes
        + cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES;
    if grant.bytes.get() > maximum {
        return Err(RestoreError::Limit("encrypted record stream chunk"));
    }
    let mut stream = client.open(grant).await.map_err(source_error)?;
    let mut bytes = vec![0; grant.bytes.get() as usize];
    stream.read_exact(&mut bytes).await?;
    if stream.read(&mut [0; 1]).await? != 0
        || cloud_backup_protocol::bytes_sha256(&bytes) != grant.sha256
    {
        return Err(RestoreError::Invalid(
            "encrypted record chunk digest or EOF",
        ));
    }
    let plaintext = zeroize::Zeroizing::new(
        fold_db::crypto::decrypt_envelope(key, &bytes)
            .map_err(|_| RestoreError::Invalid("record stream metadata encryption"))?,
    );
    drop(bytes);
    if plaintext.len() as u64 != chunk.plaintext_bytes.get() {
        return Err(RestoreError::Invalid("record stream plaintext length"));
    }
    for part in plaintext.chunks(cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize) {
        record_bridge::send(sender, part.to_vec()).await?;
    }
    Ok(())
}
