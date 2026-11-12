use super::model::Saved;
use super::recovery::RecoveryPoint;
use super::work::{self, Capture, LogContext, Work};
use super::{
    connection,
    source::Source,
    store::{self, Store},
    transfer, worker, BackupError, Phase, Result,
};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::{
    access::{InventoryMetadata, InventoryPutRequest, InventoryPutResult},
    inventory::{InventoryIndex, InventoryPage, InventoryVerifier},
    jobs::JobResult,
    limits::MAX_PAGE_ENTRIES,
    publication::{CommitResponse, CommitStatusRequest},
    AcceptedBackup, Commit, Job, Stream, Version,
};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use fold_db::storage_policy::{backup_decode, BACKUP_RESOURCES};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::time::Instant;

const CAPABILITY_TTL: Duration = Duration::from_secs(15);

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
        let name = format!("inventory-page-{:04}.json", page.page_index);
        put_inventory(
            capture,
            &ctx,
            client,
            &begin.job,
            &name,
            InventoryMetadata::Page { page },
        )
        .await?;
    }
    let idx_meta = InventoryMetadata::Index { index };
    put_inventory(
        capture,
        &ctx,
        client,
        &begin.job,
        "inventory-index.json",
        idx_meta,
    )
    .await?;
    saved.phase = Phase::Committing;
    store.save(saved)?;
    budget(capture, store)?;
    let resp = work::control("commit_prepare", pause, || async {
        Ok(client.commit(&begin, &commit, &proof).await?)
    })
    .await?;
    accepted(client, store, saved, resp, pause).await
}

pub(super) fn budget(capture: &Capture, store: &Store) -> Result<()> {
    let quote = store
        .bytes()
        .checked_add(backup_decode::JSON_PAGE_RESERVE_BYTES);
    capture.set_coordinator_spool_bytes(quote.ok_or(BackupError::Limit)?)
}

fn inventory(capture: &Capture, saved: &Saved) -> Result<(InventoryIndex, Vec<InventoryPage>)> {
    let scope = &saved
        .begin
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .job
        .stream
        .scope;
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
            .is_none_or(|q| q > BACKUP_RESOURCES.decoded_values_bytes / 2)
        {
            return Err(BackupError::Limit);
        }
        if receipts
            .insert(receipt.receipt_id.clone(), receipt.clone())
            .is_some_and(|old| old != receipt)
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
    name: &str,
    metadata: InventoryMetadata,
) -> Result<()> {
    ctx.check()?;
    budget(capture, ctx.store)?;
    match &metadata {
        InventoryMetadata::Page { page } => ctx.store.write(name, page)?,
        InventoryMetadata::Index { index } => ctx.store.write(name, index)?,
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
    let decode = |resp: CommitResponse| -> Result<Option<AcceptedBackup>> {
        resp.validate_for(begin, commit)?;
        match resp {
            CommitResponse::Accepted { backup } => Ok(Some(*backup)),
            CommitResponse::Rejected { .. } => Err(BackupError::Source),
            CommitResponse::Pending { .. } => Ok(None),
        }
    };
    if let Some(backup) = decode(response)? {
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

pub(super) fn check_work(store: &Store, saved: &Saved, pause: &AtomicBool) -> Result<()> {
    transfer::check_pause(pause)?;
    match store.read::<Id>("close.json")? {
        Some(id) if id == saved.request_id => Err(BackupError::Closing),
        Some(_) => Err(BackupError::Identity),
        None => Ok(()),
    }
}

pub(super) async fn share_capability_limit(
    base: &Path,
    source: &Source,
    stream_id: &Id,
    cached: &mut Option<(Stream, String, Option<u8>, Instant)>,
    client: &ByteBackupClient,
) -> Result<Option<u8>> {
    let ep = client.canonical_endpoint();
    let stream = connection::publication_stream(base, source, stream_id, client).await?;
    if let Some((s, cached_ep, limit, exp)) = cached.as_ref() {
        if *s == stream && *cached_ep == ep && Instant::now() < *exp {
            return Ok(*limit);
        }
    }
    let limit = client.share_recovery_limit(&stream).await?;
    *cached = Some((stream, ep, limit, Instant::now() + CAPABILITY_TTL));
    Ok(limit)
}

pub(super) fn read_publication_point(
    base: &Path,
    stream_id: &Id,
    account: &AccountId,
) -> Result<Option<RecoveryPoint>> {
    let point: Option<RecoveryPoint> = store::read(&base.join("recovery.json"))?;
    if let Some(p) = &point {
        p.validate()?;
        let (stream, deps) = (
            &p.accepted.job().stream,
            &p.accepted.begin.recovery_dependencies,
        );
        let bad_chain = p.base != p.accepted
            && (p.anchor.is_none()
                || deps.first() != Some(&p.base.head())
                || deps.last() != p.accepted.begin.predecessor.as_ref());
        if stream.stream_id != *stream_id
            || stream.scope.account_id != *account
            || !p.base.begin.recovery_dependencies.is_empty()
            || bad_chain
        {
            return Err(BackupError::Identity);
        }
    }
    Ok(point)
}

pub(super) fn verify_publication_terminal(
    saved: &Saved,
    store: &Store,
    base: &Path,
    stream_id: &Id,
    account: &AccountId,
    minimum: Frontiers,
) -> Result<bool> {
    match saved.phase {
        Phase::Published => {
            let pt =
                read_publication_point(base, stream_id, account)?.ok_or(BackupError::Missing)?;
            if saved.accepted.as_ref() != Some(&pt.accepted) || pt.frontiers.main < minimum.main {
                return Err(BackupError::Identity);
            }
            Ok(true)
        }
        Phase::Closed if saved.local_action.is_some() => Ok(true),
        Phase::Closed => {
            let term: JobResult = store.read("terminal.json")?.ok_or(BackupError::Missing)?;
            worker::validate_terminal(saved, &term)?;
            if !worker::source_unavailable(&term) || store.read::<Id>("close.json")?.is_some() {
                return Err(BackupError::Authority);
            }
            Ok(false)
        }
        _ => Err(BackupError::Invalid),
    }
}

pub(super) fn replace_unmaterialized(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    limit: Option<u8>,
) -> Result<bool> {
    let Work::MutationLog { context } = &saved.work else {
        return Ok(false);
    };
    if saved.begin_submitted {
        return Ok(false);
    }
    let replacing = saved.local_action.is_some();
    if !replacing && limit.is_some() {
        let j = source.runtime.mutation_log_status()?;
        if !j.snapshot_required && j.anchor.as_ref() == Some(&context.anchor) {
            return Ok(false);
        }
    }
    if source
        .runtime
        .mutation_log_capture_materialized(&saved.request_id)?
    {
        return match (replacing, limit.is_some()) {
            (true, _) => Err(BackupError::Identity),
            (false, true) => Ok(false),
            (false, false) => Err(BackupError::AdmissionPending),
        };
    }
    source.replace_with_snapshot(store, saved).map(|()| true)
}

pub(super) fn next_share_work(
    base: &Path,
    source: &Source,
    stream_id: &Id,
    limit: Option<u8>,
) -> Result<Option<(Frontiers, Option<Box<LogContext>>)>> {
    let rt = &source.runtime;
    let frontier = rt.durable_main_frontier();
    let point = read_publication_point(base, stream_id, &source.account)?;
    if point.as_ref().is_some_and(|p| p.frontiers.main > frontier) {
        return Err(BackupError::Identity);
    }
    let (mut minimum, mut log) = (Frontiers { main: frontier }, None);
    let journal = rt.mutation_log_status()?;
    if !journal.enabled {
        rt.configure_mutation_log(true)?;
    }
    let anchored = point.as_ref().filter(|p| {
        journal.enabled
            && !journal.snapshot_required
            && p.anchor.is_some()
            && journal.anchor == p.anchor
    });
    if let (Some(limit), Some(p)) = (limit, anchored) {
        if p.frontiers.main == frontier {
            return Ok(None);
        }
        if p.accepted.ordinal.get() - p.base.ordinal.get() < u64::from(limit) {
            let ctx = p.log_context()?;
            if journal.head.is_some_and(|h| h.sequence > ctx.from.sequence) {
                (minimum.main, log) = (p.frontiers.main + 1, Some(Box::new(ctx)));
            }
        }
    } else if anchored.is_some_and(|p| p.base == p.accepted && p.frontiers.main == frontier) {
        return Ok(None);
    }
    Ok(Some((minimum, log)))
}
