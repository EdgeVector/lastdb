//! Concrete chunk closure without cloud-visible native keys or file metadata.
use super::{
    download, inventory::Inventory, ByteBackupClient, RestoreError, RestoreSession, Result,
};
use cloud_backup_protocol::v2::{access::ReadSelector, Destination};
use fold_db::storage::disk_pack::byte_backup::{
    ClientPage, ClientRoot, ClientStreamChunk, ClientStreamSummary, CODEC_ENVELOPE_MAX_BYTES,
};

pub(super) struct StreamPlan {
    pub summary: ClientStreamSummary,
    pub chunks: Vec<ClientStreamChunk>,
    bytes: u64,
}

pub(super) async fn collect(
    session: &RestoreSession,
    client: &ByteBackupClient,
    root: &ClientRoot,
    inventory: &mut Inventory,
    key: &[u8; 32],
) -> Result<Vec<StreamPlan>> {
    let profile = root
        .records
        .as_ref()
        .ok_or(RestoreError::Invalid("record profile"))?;
    reserve_import_memory(inventory, profile.header.required_features())?;
    let mut plans: Vec<_> = profile
        .streams
        .iter()
        .map(|summary| StreamPlan {
            summary: summary.clone(),
            chunks: Vec::new(),
            bytes: 0,
        })
        .collect();
    for reference in &root.pages {
        let receipt = inventory.receipt(&reference.receipt_id)?;
        if receipt.object.destination != Some(Destination::R2)
            || receipt.object.sha256 != reference.sha256
            || receipt.object.bytes != reference.bytes
        {
            return Err(RestoreError::Invalid("record page receipt"));
        }
        let grant = download::one(
            session,
            client,
            ReadSelector::Receipt {
                receipt_id: reference.receipt_id.clone(),
            },
        )
        .await?;
        grant.validate_for_receipt(receipt)?;
        inventory.use_receipt(&reference.receipt_id)?;
        let path = download::fetch(session, client, &grant, true).await?;
        let page = ClientPage::decode(
            &crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
            key,
            &session.accepted().begin,
        )
        .map_err(|_| RestoreError::Invalid("record client page"))?;
        root.validate_page(&page)
            .map_err(|_| RestoreError::Invalid("record page context"))?;
        if page.page_index != reference.page_index
            || !page.native_files.is_empty()
            || !page.external_dependencies.is_empty()
        {
            return Err(RestoreError::Invalid("record page layout"));
        }
        for chunk in page.stored_chunks {
            include(&mut plans, inventory, chunk)?;
        }
    }
    for plan in &plans {
        if plan.chunks.len() as u64 != plan.summary.chunks || plan.bytes != plan.summary.bytes {
            return Err(RestoreError::Invalid("complete record stream chunk counts"));
        }
    }
    inventory.finish()?;
    Ok(plans)
}

fn include(
    plans: &mut [StreamPlan],
    inventory: &mut Inventory,
    chunk: ClientStreamChunk,
) -> Result<()> {
    let plan = plans
        .iter_mut()
        .find(|plan| plan.summary.stream == chunk.stream)
        .ok_or(RestoreError::Invalid("record stream kind"))?;
    let cap = if chunk.stream.encrypted() {
        lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes
    } else {
        lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES
    };
    let bytes = chunk.plaintext_bytes.get();
    let overhead = if chunk.stream.encrypted() {
        cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES
    } else {
        0
    };
    let receipt = inventory.receipt(&chunk.receipt_id)?;
    if chunk.ordinal != plan.chunks.len() as u64
        || bytes == 0
        || bytes > cap
        || receipt.object.bytes.get()
            != bytes
                .checked_add(overhead)
                .ok_or(RestoreError::Limit("record stream stored length"))?
        || receipt.object.destination != Some(chunk.stream.destination())
    {
        return Err(RestoreError::Invalid("record stream chunk identity"));
    }
    plan.bytes = plan
        .bytes
        .checked_add(bytes)
        .ok_or(RestoreError::Limit("record stream bytes"))?;
    if plan.bytes > plan.summary.bytes || chunk.ordinal >= plan.summary.chunks {
        return Err(RestoreError::Invalid("record stream exceeds summary"));
    }
    inventory.use_receipt(&chunk.receipt_id)?;
    // Covers bounded Vec capacity growth and the owned digest string.
    inventory.charge(
        4 * std::mem::size_of::<ClientStreamChunk>() as u64
            + 2 * chunk.receipt_id.as_str().len() as u64,
    )?;
    plan.chunks.push(chunk);
    Ok(())
}

fn reserve_import_memory(inventory: &mut Inventory, features: u64) -> Result<()> {
    let policy = lastdb_storage_policy::BACKUP_RESOURCES;
    let key = if features & laststore::disk_pack::FEATURE_EXTENDED_KEYS != 0 {
        policy.decoded_values_bytes
    } else {
        2 * policy.stream_buffer_bytes
    };
    // Sixteen native buffers plus bounded channels, crypto and HTTP read buffers.
    let transient = key
        .checked_add(30 * policy.stream_buffer_bytes)
        .ok_or(RestoreError::Limit("record import memory"))?;
    let available = policy
        .working_memory_bytes
        .checked_sub(transient)
        .ok_or(RestoreError::Limit("record import memory"))?;
    let current =
        policy.decoded_values_bytes - lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES;
    inventory.charge(current.saturating_sub(available))
}
