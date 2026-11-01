use super::capture::Capture;
use super::{model::Saved, retry, store::Store, transfer, wait, BackupError, Phase, Result};
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::access::{
    InventoryMetadata, InventoryPutRequest, InventoryPutResult,
};
use cloud_backup_protocol::v2::inventory::{InventoryIndex, InventoryPage, InventoryVerifier};
use cloud_backup_protocol::v2::publication::{CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::v2::{AcceptedBackup, Commit, Version};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;

pub(super) async fn publish(
    capture: &Capture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<AcceptedBackup> {
    let (index, pages) = inventory(capture, saved)?;
    let root_receipt_id = capture.root_receipt_id()?;
    let mut verifier = InventoryVerifier::new(&index, root_receipt_id.clone())?;
    for page in &pages {
        verifier.push(page)?;
    }
    let proof = verifier.finish()?;
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?.clone();
    let commit = Commit {
        version: Version::V2,
        job: begin.job.clone(),
        begin_sha256: begin.input_sha256()?,
        root_receipt_id,
        inventory_root: index.root()?,
    };
    commit.validate_inventory(&proof)?;
    if saved.commit.as_ref().is_some_and(|old| old != &commit) {
        return Err(BackupError::Identity);
    }
    saved.commit = Some(commit.clone());
    capture.seal_commit(&commit)?;
    saved.phase = Phase::Inventory;
    budget(capture, store)?;
    store.save(saved)?;
    for page in pages {
        check_work(store, saved, pause)?;
        budget(capture, store)?;
        store.write(
            &format!("inventory-page-{:04}.json", page.page_index),
            &page,
        )?;
        budget(capture, store)?;
        put(client, &begin.job, InventoryMetadata::Page { page }, pause).await?;
    }
    check_work(store, saved, pause)?;
    budget(capture, store)?;
    store.write("inventory-index.json", &index)?;
    budget(capture, store)?;
    put(
        client,
        &begin.job,
        InventoryMetadata::Index { index },
        pause,
    )
    .await?;
    saved.phase = Phase::Committing;
    store.save(saved)?;
    budget(capture, store)?;
    let response = retry::control("commit_prepare", pause, || async {
        Ok(client.commit(&begin, &commit, &proof).await?)
    })
    .await?;
    accepted(client, store, saved, response, pause).await
}

fn budget(capture: &Capture, store: &Store) -> Result<()> {
    let quote = store
        .bytes()
        .checked_add(lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES)
        .ok_or(BackupError::Limit)?;
    capture.set_coordinator_spool_bytes(quote)?;
    Ok(())
}

fn inventory(capture: &Capture, saved: &Saved) -> Result<(InventoryIndex, Vec<InventoryPage>)> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let mut receipts = BTreeMap::new();
    for index in 0..capture.object_count()? {
        let object = capture.object(index)?;
        let receipt = object.receipt.ok_or(BackupError::Invalid)?;
        receipt.validate()?;
        if receipt.scope != begin.job.stream.scope || receipt.object != object.input.object {
            return Err(BackupError::Identity);
        }
        let quote = (receipts.len() as u64 + 1)
            .checked_mul(lastdb_storage_policy::backup_decode::RECEIPT_MEMORY_BYTES)
            .ok_or(BackupError::Limit)?;
        if quote > lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes / 2 {
            return Err(BackupError::Limit);
        }
        if receipts
            .insert(receipt.receipt_id.clone(), receipt.clone())
            .is_some_and(|old| old != receipt)
        {
            return Err(BackupError::Identity);
        }
    }
    let mut pages = Vec::new();
    let mut values = receipts.into_values().peekable();
    while values.peek().is_some() {
        let page = InventoryPage {
            version: Version::V2,
            scope: begin.job.stream.scope.clone(),
            page_index: u32::try_from(pages.len()).map_err(|_| BackupError::Limit)?,
            objects: values
                .by_ref()
                .take(cloud_backup_protocol::v2::limits::MAX_PAGE_ENTRIES)
                .collect(),
        };
        page.validate()?;
        pages.push(page);
    }
    let index = InventoryIndex {
        version: Version::V2,
        scope: begin.job.stream.scope.clone(),
        pages: pages
            .iter()
            .map(InventoryPage::descriptor)
            .collect::<std::result::Result<_, _>>()?,
    };
    index.validate()?;
    Ok((index, pages))
}

async fn put(
    client: &ByteBackupClient,
    job: &cloud_backup_protocol::v2::Job,
    metadata: InventoryMetadata,
    pause: &AtomicBool,
) -> Result<()> {
    let request = InventoryPutRequest {
        version: Version::V2,
        job: job.clone(),
        metadata,
    };
    retry::control("inventory_put", pause, || async {
        match client.put_inventory(&request).await? {
            InventoryPutResult::Ok { .. } => Ok(()),
            InventoryPutResult::Error { error } => Err(transfer::cloud_error(error)),
        }
    })
    .await
}

async fn accepted(
    client: &ByteBackupClient,
    store: &Store,
    saved: &Saved,
    response: CommitResponse,
    pause: &AtomicBool,
) -> Result<AcceptedBackup> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let commit = saved.commit.as_ref().ok_or(BackupError::Invalid)?;
    let request = CommitStatusRequest {
        version: Version::V2,
        job: begin.job.clone(),
    };
    response.validate_for(begin, commit)?;
    match response {
        CommitResponse::Accepted { backup } => return Ok(*backup),
        CommitResponse::Rejected { .. } => return Err(BackupError::Source),
        CommitResponse::Pending { .. } => {}
    }
    let context = wait::Context::new(store, saved, pause);
    wait::poll(&context, "commit_status", || async {
        let response = client.commit_status(&request).await?;
        response.validate_for(begin, commit)?;
        match response {
            CommitResponse::Accepted { backup } => Ok(Some(*backup)),
            CommitResponse::Rejected { .. } => Err(BackupError::Source),
            CommitResponse::Pending { .. } => Ok(None),
        }
    })
    .await
}

pub(super) fn check_work(store: &Store, saved: &Saved, pause: &AtomicBool) -> Result<()> {
    transfer::check_pause(pause)?;
    if let Some(id) = store.read::<cloud_backup_protocol::Id>("close.json")? {
        if id != saved.request_id {
            return Err(BackupError::Identity);
        }
        return Err(BackupError::Closing);
    }
    Ok(())
}
