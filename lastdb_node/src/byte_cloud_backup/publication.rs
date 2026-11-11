use super::model::{Saved, StreamRecord};
use super::work::{self, Capture};
use super::{source::Source, store::Store, transfer, BackupError, Phase, Result};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::access::{
    InventoryMetadata, InventoryPutRequest, InventoryPutResult,
};
use cloud_backup_protocol::v2::inventory::{InventoryIndex, InventoryPage, InventoryVerifier};
use cloud_backup_protocol::v2::jobs::{CloseReason, JobResult, JobStatus};
use cloud_backup_protocol::v2::publication::{CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::v2::{AcceptedBackup, Commit, Stream, Version};
use cloud_backup_protocol::{CloudScope, Id};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;

const CI_ACCOUNT: &str = "2a81c95623f2445854a794b5b48471d6";

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
    let response = work::control("commit_prepare", pause, || async {
        Ok(client.commit(&begin, &commit, &proof).await?)
    })
    .await?;
    accepted(client, store, saved, response, pause).await
}

fn budget(capture: &Capture, store: &Store) -> Result<()> {
    let quote = store
        .bytes()
        .checked_add(fold_db::storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES)
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
            .checked_mul(fold_db::storage_policy::backup_decode::RECEIPT_MEMORY_BYTES)
            .ok_or(BackupError::Limit)?;
        if quote > fold_db::storage_policy::BACKUP_RESOURCES.decoded_values_bytes / 2 {
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
    work::control("inventory_put", pause, || async {
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
    let context = work::Context::new(store, saved, pause);
    work::poll(&context, "commit_status", || async {
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
    if let Some(id) = store.read::<Id>("close.json")? {
        if id != saved.request_id {
            return Err(BackupError::Identity);
        }
        return Err(BackupError::Closing);
    }
    Ok(())
}

fn expected_stream(source: &Source) -> Result<Option<Id>> {
    if let Some(id) = &source.publication_stream {
        return Ok(Some(id.clone()));
    }
    let value = match std::env::var("LASTDB_CLOUD_CI_STREAM_ID") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err(BackupError::Identity),
    };
    if source.account.as_str() != CI_ACCOUNT {
        return Err(BackupError::Identity);
    }
    Ok(Some(
        Id::try_from(value).map_err(|_| BackupError::Identity)?,
    ))
}

pub(super) fn validate_saved_stream(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let Some(expected) = expected_stream(source)? else {
        return Ok(());
    };
    let record: StreamRecord = store
        .read_base("stream.json")?
        .ok_or(BackupError::Identity)?;
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    if record.source_home != source.source_home
        || record.stream != begin.job.stream
        || record.stream.stream_id != expected
        || record.stream.scope.account_id != source.account
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(super) fn select_stream(source: &Source, store: &Store, scope: CloudScope) -> Result<Stream> {
    let expected = expected_stream(source)?;
    let previous: Option<StreamRecord> = store.read_base("stream.json")?;
    let stream = match previous {
        Some(record)
            if record.source_home == source.source_home && record.stream.scope == scope =>
        {
            if expected
                .as_ref()
                .is_some_and(|id| *id != record.stream.stream_id)
            {
                return Err(BackupError::Identity);
            }
            record.stream
        }
        Some(_) if expected.is_some() => return Err(BackupError::Identity),
        _ => Stream {
            scope,
            stream_id: match expected {
                Some(id) => id,
                None => Id::try_from(uuid::Uuid::new_v4().to_string())?,
            },
        },
    };
    stream.validate()?;
    store.write_base(
        "stream.json",
        &StreamRecord {
            source_home: source.source_home.clone(),
            stream: stream.clone(),
        },
    )?;
    Ok(stream)
}

pub(super) fn same_scope(source: &Source, store: &Store, scope: &CloudScope) -> Result<bool> {
    let Some(record) = store.read_base::<StreamRecord>("stream.json")? else {
        return Ok(false);
    };
    record.stream.validate()?;
    Ok(record.source_home == source.source_home
        && record.stream.scope == *scope
        && expected_stream(source)?.is_none_or(|id| id == record.stream.stream_id))
}

pub(super) fn source_unavailable(result: &JobResult) -> bool {
    matches!(
        result.status,
        JobStatus::Closed {
            reason: CloseReason::SourceUnavailable,
        }
    )
}

pub(super) fn validate_terminal(saved: &Saved, result: &JobResult) -> Result<()> {
    saved.validate()?;
    if !saved.begin_submitted {
        return Err(BackupError::Invalid);
    }
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    result.validate_for_begin(begin)?;
    match &result.status {
        JobStatus::Published { backup } => {
            backup.validate_for(begin, saved.commit.as_ref().ok_or(BackupError::Invalid)?)?;
            if saved
                .accepted
                .as_ref()
                .is_some_and(|old| old != backup.as_ref())
            {
                return Err(BackupError::Identity);
            }
        }
        JobStatus::Closed { .. } => {
            if saved.accepted.is_some() || saved.phase == Phase::Published {
                return Err(BackupError::Identity);
            }
        }
        JobStatus::Active => return Err(BackupError::Invalid),
    }
    Ok(())
}
