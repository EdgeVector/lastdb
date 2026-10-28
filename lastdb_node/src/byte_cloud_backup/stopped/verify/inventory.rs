use super::*;
use cloud_backup_protocol::v2::inventory::{InventoryIndex, InventoryPage, InventoryVerifier};
use cloud_backup_protocol::v2::{hash::canonical_bytes, Destination};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn check(
    accepted: &AcceptedBackup,
    store: &Store,
    objects: &[ObjectReceipt],
) -> Result<()> {
    let index: InventoryIndex = store
        .read("inventory-index.json")?
        .ok_or(BackupError::Missing)?;
    if index.root()? != accepted.inventory_root
        || index.scope != accepted.job().stream.scope
        || objects.len() as u64 != accepted.inventory_root.object_count.get()
    {
        return Err(BackupError::Identity);
    }
    let expected: BTreeMap<_, _> = objects
        .iter()
        .map(|object| (object.receipt_id.clone(), object))
        .collect();
    let mut seen = BTreeSet::new();
    let mut proof = InventoryVerifier::new(&index, accepted.root_receipt_id.clone())?;
    for descriptor in &index.pages {
        let page: InventoryPage = store
            .read(&format!("inventory-page-{:04}.json", descriptor.page_index))?
            .ok_or(BackupError::Missing)?;
        proof.push(&page)?;
        for receipt in &page.objects {
            if !seen.insert(receipt.receipt_id.clone())
                || expected.get(&receipt.receipt_id).copied() != Some(receipt)
            {
                return Err(BackupError::Identity);
            }
        }
    }
    proof.finish()?;
    if seen.len() != objects.len() {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(super) async fn remote(
    client: &ByteBackupClient,
    accepted: &AcceptedBackup,
    store: &Store,
) -> Result<u64> {
    let index: InventoryIndex = store
        .read("inventory-index.json")?
        .ok_or(BackupError::Missing)?;
    let raw = canonical_bytes(&index)?;
    let mut expected = vec![(
        ReadSelector::InventoryIndex {
            sha256: accepted.inventory_root.sha256.clone(),
        },
        accepted.inventory_root.sha256.clone(),
        raw.len() as u64,
    )];
    expected.extend(index.pages.iter().map(|page| {
        (
            ReadSelector::InventoryPage {
                sha256: page.sha256.clone(),
            },
            page.sha256.clone(),
            page.bytes.get(),
        )
    }));
    for group in expected.chunks(4) {
        let grants = super::grants(
            client,
            accepted,
            group.iter().map(|item| item.0.clone()).collect(),
        )
        .await?;
        for ((_, hash, bytes), grant) in group.iter().zip(grants) {
            if &grant.sha256 != hash
                || grant.bytes.get() != *bytes
                || grant.destination != Some(Destination::R2)
            {
                return Err(BackupError::Identity);
            }
            stream::metadata(client, &grant).await?;
        }
    }
    Ok(expected.len() as u64)
}
