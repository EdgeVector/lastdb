//! Cloud object transfer batches, inventory staging, and commit publication.
use super::{
    source::Saved,
    store::Store,
    work::{self, budget, check_pause, cloud_error, Capture},
    BackupError, Phase, Result,
};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::{
    access::{InventoryMetadata, InventoryPutRequest, InventoryPutResult},
    inventory::{InventoryIndex, InventoryPage, InventoryVerifier},
    limits::MAX_PAGE_ENTRIES,
    publication::{CommitResponse, CommitStatusRequest},
    transfer::*,
    AcceptedBackup, Commit, Job, Version,
};
use fold_db::storage_policy::{backup_decode, BACKUP_RESOURCES};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) async fn upload_batch(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    context: &work::Context<'_>,
    mut open_reader: impl FnMut(usize) -> Result<Box<dyn Read + Send>>,
    mut on_receipt: impl FnMut(usize, ObjectReceipt) -> Result<()>,
) -> Result<Option<BackupError>> {
    let (items, error) = prepare(client, inputs, context.pause).await?;
    if let Some(error) = error {
        for (pos, item) in items.into_iter().enumerate() {
            if let Ok(Some(receipt)) = verified(item) {
                on_receipt(pos, receipt)?;
            }
        }
        return Ok(Some(error));
    }
    let results = futures::future::join_all(inputs.iter().zip(items).enumerate().map(
        |(pos, (input, item))| {
            let reader = open_reader(pos);
            async move {
                let res = complete_one(client, input, item, reader?, context).await;
                if res.is_err() {
                    context.stopped.store(true, Ordering::Release);
                }
                res
            }
        },
    ))
    .await;
    let rank = |e: &BackupError| match e {
        BackupError::Waiting => 0,
        e if work::temporary(e) => 1,
        _ => 2,
    };
    let mut failure: Option<BackupError> = None;
    for (pos, (input, res)) in inputs.iter().zip(results).enumerate() {
        let err = match res {
            Ok(r) if r.object == input.object => {
                on_receipt(pos, r)?;
                continue;
            }
            Ok(_) => BackupError::Identity,
            Err(e) => e,
        };
        if failure.as_ref().is_none_or(|old| rank(&err) > rank(old)) {
            failure = Some(err);
        }
    }
    Ok(failure)
}

async fn prepare(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    pause: &AtomicBool,
) -> Result<(Vec<TransferItemResult>, Option<BackupError>)> {
    let req = TransferPrepareRequest {
        version: Version::V2,
        items: inputs.to_vec(),
    };
    let observed = RefCell::new(None::<Vec<TransferItemResult>>);
    let settled = |item: &TransferItemResult| {
        matches!(
            item,
            TransferItemResult::Ok { transfer }
                if matches!(transfer.state, TransferStatus::Verified { .. } | TransferStatus::Rejected { .. })
        )
    };
    let res = work::control("transfer_prepare", pause, || async {
        let mut items = client.prepare(&req).await?.items;
        let error = batch_error(&items);
        if let Some(prev) = observed.borrow().as_ref() {
            for (old, item) in prev.iter().zip(&mut items).filter(|(o, _)| settled(o)) {
                if settled(item) && item != old {
                    return Err(BackupError::Identity);
                }
                *item = old.clone();
            }
        }
        *observed.borrow_mut() = Some(items.clone());
        error.map_or(Ok(items), Err)
    })
    .await;
    match res {
        Ok(items) => Ok((items, None)),
        Err(e) => match observed.into_inner() {
            Some(items) => Ok((items, Some(e))),
            None => Err(e),
        },
    }
}

fn batch_error(items: &[TransferItemResult]) -> Option<BackupError> {
    let mut first = None;
    for item in items {
        let err = match item {
            TransferItemResult::Error { error, .. } => cloud_error(error.clone()),
            TransferItemResult::Ok { transfer }
                if matches!(transfer.state, TransferStatus::Rejected { .. }) =>
            {
                BackupError::Source
            }
            TransferItemResult::Ok { .. } => continue,
        };
        if !work::temporary(&err) {
            return Some(err);
        }
        first.get_or_insert(err);
    }
    first
}

async fn complete_one(
    client: &ByteBackupClient,
    input: &TransferInput,
    prepared: TransferItemResult,
    reader: Box<dyn Read + Send>,
    context: &work::Context<'_>,
) -> Result<ObjectReceipt> {
    check_pause(context.pause)?;
    if let Some(receipt) = verified(prepared)? {
        return Ok(receipt);
    }
    context.check()?;
    let resumed = work::control("transfer_resume", context.pause, || async {
        context.check()?;
        match client.resume(input).await? {
            TransferResumeResult::Error { error, .. } => Err(cloud_error(error)),
            res => Ok(res),
        }
    })
    .await?;
    match resumed {
        TransferResumeResult::Grant { response } => {
            context.check()?;
            let begin = context.saved.begin.as_ref().ok_or(BackupError::Invalid)?;
            if begin.job != input.job || response.grant.transfer_mode.is_some() {
                return Err(BackupError::Identity);
            }
            let _obs = client.upload(input, &response, reader).await;
        }
        TransferResumeResult::State { transfer } => {
            drop(reader);
            if let Some(receipt) = verified(TransferItemResult::Ok { transfer })? {
                return Ok(receipt);
            }
        }
        TransferResumeResult::Error { error, .. } => return Err(cloud_error(error)),
    }
    check_pause(context.pause)?;
    let completed = work::control("transfer_complete", context.pause, || async {
        match client.complete(input).await? {
            TransferItemResult::Error { error, .. } => Err(cloud_error(error)),
            item => Ok(item),
        }
    })
    .await?;
    if let Some(receipt) = verified(completed)? {
        return Ok(receipt);
    }
    let req = TransferStatusRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_ids: vec![input.request_id.clone()],
    };
    work::poll(context, "transfer_status", || async {
        let item = client
            .transfer_status(&req, std::slice::from_ref(input))
            .await?
            .items
            .into_iter()
            .next();
        verified(item.ok_or(BackupError::Invalid)?)
    })
    .await
}

fn verified(value: TransferItemResult) -> Result<Option<ObjectReceipt>> {
    match value {
        TransferItemResult::Ok { transfer } => match transfer.state {
            TransferStatus::Verified { receipt } => Ok(Some(receipt)),
            TransferStatus::Pending { .. } => Ok(None),
            TransferStatus::Rejected { .. } => Err(BackupError::Source),
        },
        TransferItemResult::Error { error, .. } => Err(cloud_error(error)),
    }
}

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
    let ctx = work::Context::new(store, saved, pause);
    for page in pages {
        put_inventory(
            capture,
            &ctx,
            client,
            &begin.job,
            InventoryMetadata::Page { page },
        )
        .await?;
    }
    put_inventory(
        capture,
        &ctx,
        client,
        &begin.job,
        InventoryMetadata::Index { index },
    )
    .await?;
    saved.phase = Phase::Committing;
    store.save(saved)?;
    budget(capture, store)?;
    let resp = work::control("commit_prepare", pause, || async {
        Ok(client.commit(&begin, &commit, &proof).await?)
    })
    .await?;
    let decode = |r: CommitResponse| -> Result<Option<AcceptedBackup>> {
        r.validate_for(&begin, &commit)?;
        match r {
            CommitResponse::Accepted { backup } => Ok(Some(*backup)),
            CommitResponse::Rejected { .. } => Err(BackupError::Source),
            CommitResponse::Pending { .. } => Ok(None),
        }
    };
    if let Some(backup) = decode(resp)? {
        return Ok(backup);
    }
    let req = CommitStatusRequest {
        version: Version::V2,
        job: begin.job.clone(),
    };
    let ctx = work::Context::new(store, saved, pause);
    work::poll(&ctx, "commit_status", || async {
        decode(client.commit_status(&req).await?)
    })
    .await
}

fn inventory(capture: &Capture, saved: &Saved) -> Result<(InventoryIndex, Vec<InventoryPage>)> {
    let scope = &saved
        .begin
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .job
        .stream
        .scope;
    let max_mem = BACKUP_RESOURCES.decoded_values_bytes / 2;
    let mut receipts = BTreeMap::new();
    for i in 0..capture.object_count()? {
        let obj = capture.object(i)?;
        let receipt = obj.receipt.ok_or(BackupError::Invalid)?;
        receipt.validate()?;
        if &receipt.scope != scope || receipt.object != obj.input.object {
            return Err(BackupError::Identity);
        }
        if (receipts.len() as u64 + 1)
            .checked_mul(backup_decode::RECEIPT_MEMORY_BYTES)
            .is_none_or(|q| q > max_mem)
        {
            return Err(BackupError::Limit);
        }
        if receipts
            .insert(receipt.receipt_id.clone(), receipt.clone())
            .is_some_and(|o| o != receipt)
        {
            return Err(BackupError::Identity);
        }
    }
    let values: Vec<_> = receipts.into_values().collect();
    let pages: Vec<_> = values
        .chunks(MAX_PAGE_ENTRIES)
        .enumerate()
        .map(|(idx, chunk)| {
            let page = InventoryPage {
                version: Version::V2,
                scope: scope.clone(),
                page_index: u32::try_from(idx).map_err(|_| BackupError::Limit)?,
                objects: chunk.to_vec(),
            };
            page.validate().map(|()| page).map_err(BackupError::from)
        })
        .collect::<Result<_>>()?;
    let index = InventoryIndex {
        version: Version::V2,
        scope: scope.clone(),
        pages: pages
            .iter()
            .map(InventoryPage::descriptor)
            .collect::<std::result::Result<_, _>>()?,
    };
    index
        .validate()
        .map(|()| (index, pages))
        .map_err(Into::into)
}

async fn put_inventory(
    capture: &Capture,
    ctx: &work::Context<'_>,
    client: &ByteBackupClient,
    job: &Job,
    metadata: InventoryMetadata,
) -> Result<()> {
    ctx.check()?;
    budget(capture, ctx.store)?;
    match &metadata {
        InventoryMetadata::Page { page } => {
            ctx.store
                .write(&format!("inventory-page-{:04}.json", page.page_index), page)?;
        }
        InventoryMetadata::Index { index } => ctx.store.write("inventory-index.json", index)?,
    }
    budget(capture, ctx.store)?;
    let req = InventoryPutRequest {
        version: Version::V2,
        job: job.clone(),
        metadata,
    };
    work::control("inventory_put", ctx.pause, || async {
        match client.put_inventory(&req).await? {
            InventoryPutResult::Ok { .. } => Ok(()),
            InventoryPutResult::Error { error } => Err(cloud_error(error)),
        }
    })
    .await
}
