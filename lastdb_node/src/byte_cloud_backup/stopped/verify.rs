//! Independent accepted-member GETs; hash every complete native file in memory.
use super::*;
use cloud_backup_protocol::v2::access::{
    ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadSelector,
};
use cloud_backup_protocol::v2::transfer::ObjectReceipt;
use cloud_backup_protocol::v2::Version;
use fold_db::sync::auth::byte_backup::ByteBackupClient;
use sha2::{Digest as _, Sha256};

mod inventory;
mod plan;
mod stream;

pub(super) async fn remote(
    client: &ByteBackupClient,
    accepted: &AcceptedBackup,
    manifest: &manifest::Manifest,
    store: &Store,
) -> Result<u64> {
    let mut plan = plan::build(manifest, store)?;
    inventory::check(accepted, store, &plan.objects)?;
    for group in plan.objects.chunks(4) {
        let selectors = group
            .iter()
            .map(|receipt| ReadSelector::Receipt {
                receipt_id: receipt.receipt_id.clone(),
            })
            .collect();
        let grants = grants(client, accepted, selectors).await?;
        for (receipt, grant) in group.iter().zip(grants) {
            grant.validate_for_receipt(receipt)?;
            stream::object(
                client,
                &grant,
                &receipt.receipt_id,
                &plan.spans,
                &mut plan.files,
            )
            .await?;
        }
    }
    for file in plan.files {
        if file.done != file.bytes
            || fold_db::hex::hex_lower(file.hash.finalize()) != file.expected.as_str()
        {
            return Err(BackupError::Source);
        }
    }
    let metadata = inventory::remote(client, accepted, store).await?;
    (plan.objects.len() as u64)
        .checked_add(metadata)
        .ok_or(BackupError::Limit)
}

async fn grants(
    client: &ByteBackupClient,
    accepted: &AcceptedBackup,
    selectors: Vec<ReadSelector>,
) -> Result<Vec<ReadGrant>> {
    let request = ReadGrantRequest {
        version: Version::V2,
        job: accepted.job().clone(),
        objects: selectors,
    };
    request.validate()?;
    let response = client.read_grants(&request, accepted).await?;
    response.validate_for(&request, accepted)?;
    response
        .items
        .into_iter()
        .map(|item| match item {
            ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            ReadGrantItemResult::Error { .. } => Err(BackupError::Source),
        })
        .collect()
}
