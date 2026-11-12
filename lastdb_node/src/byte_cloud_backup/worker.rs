use super::work::{self, budget, check_pause, cloud_error, Capture};
use super::{
    connection,
    source::{Saved, Source},
    store::Store,
    transfer, BackupError, Phase, Result,
};
use crate::cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupClient};
use cloud_backup_protocol::v2::{
    jobs::{CloseReason, JobCloseRequest, JobResult, JobStatus},
    limits::MAX_BATCH_ITEMS,
    publication::{HeadReadRequest, HeadReadResult},
    AcceptedBackup, Begin, ClientDescriptor, Destination, Job, Version,
};
use cloud_backup_protocol::{ErrorCode, Id};
use std::sync::atomic::AtomicBool;

fn source_unavailable(result: &JobResult) -> bool {
    matches!(
        result.status,
        JobStatus::Closed {
            reason: CloseReason::SourceUnavailable
        }
    )
}

pub(super) fn unclosed_source_unavailable(store: &Store, saved: &Saved) -> Result<Option<bool>> {
    let Some(term) = store.read::<JobResult>("terminal.json")? else {
        return Ok(None);
    };
    validate_terminal(saved, &term)?;
    Ok(Some(
        source_unavailable(&term) && store.read::<Id>("close.json")?.is_none(),
    ))
}

pub(super) fn reconcilable(source: &Source, store: &Store) -> Result<Option<Saved>> {
    let Some(term) = store.read::<JobResult>("terminal.json")? else {
        return Ok(None);
    };
    let saved = store.state()?.ok_or(BackupError::Missing)?;
    source.validate(&saved)?;
    validate_terminal(&saved, &term).map(|()| Some(saved))
}

fn validate_terminal(saved: &Saved, result: &JobResult) -> Result<()> {
    saved.validate()?;
    if !saved.begin_submitted {
        return Err(BackupError::Invalid);
    }
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    result.validate_for_begin(begin)?;
    let ok = match &result.status {
        JobStatus::Published { backup } => {
            backup.validate_for(begin, saved.commit.as_ref().ok_or(BackupError::Invalid)?)?;
            !saved.accepted.as_ref().is_some_and(|old| old != &**backup)
        }
        JobStatus::Closed { .. } => saved.accepted.is_none() && saved.phase != Phase::Published,
        JobStatus::Active => return Err(BackupError::Invalid),
    };
    ok.then_some(()).ok_or(BackupError::Identity)
}

pub(super) async fn reconcile(source: &Source, store: &Store, saved: &mut Saved) -> Result<()> {
    source.validate(saved)?;
    let result: JobResult = store.read("terminal.json")?.ok_or(BackupError::Missing)?;
    validate_terminal(saved, &result)?;
    let capture = source.resume(saved, store).await?;
    finish(&capture, store, saved, &result)
}

pub(super) async fn run(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    source.validate(saved)?;
    if saved.begin.is_some() {
        connection::validate_saved_stream(source, store, saved)?;
    }
    if saved.local_action.is_some() {
        return source.finish_replacement(store, saved);
    }
    if store.read::<JobResult>("terminal.json")?.is_some() {
        return reconcile(source, store, saved).await;
    }
    check_pause(pause)?;
    if store.read::<Id>("close.json")?.is_some() {
        return close(source, store, saved, client, pause).await;
    }
    setup(source, store, saved, client, pause).await?;
    let capture = source.prepare(saved, store).await?;
    refresh(&capture, store, saved)?;
    source.validate(saved)?;
    let result = drive(source, &capture, store, saved, client, pause).await;
    source.validate(saved)?;
    match result {
        Ok(res) => finish(&capture, store, saved, &res),
        Err(BackupError::Closing) => {
            drop(capture);
            close(source, store, saved, client, pause).await
        }
        Err(e) => Err(e),
    }
}

async fn setup(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    if saved.begin.is_some() {
        return Ok(());
    }
    let req = connection::context_request(source.account.clone());
    let ctx = work::control("context_read", pause, || async {
        Ok(client.context(&req).await?)
    })
    .await?;
    if !ctx.new_work_allowed {
        return Err(BackupError::AdmissionPending);
    }
    let stream = connection::select_stream(source, store, ctx.scope)?;
    let head_req = HeadReadRequest {
        version: Version::V2,
        stream: stream.clone(),
    };
    let predecessor = work::control("head_read", pause, || async {
        match client.head(&head_req).await? {
            HeadReadResult::Ok { response: r } => {
                if r.active_commit_id
                    .as_ref()
                    .is_some_and(|id| id != &saved.request_id)
                {
                    return Err(BackupError::Busy);
                }
                Ok(r.accepted.as_ref().map(AcceptedBackup::head))
            }
            HeadReadResult::Error { error: e }
                if e.code == ErrorCode::NotFound && e.field.as_str() == "stream" =>
            {
                Ok(None)
            }
            HeadReadResult::Error { error } => Err(cloud_error(error)),
        }
    })
    .await?;
    let covered_logs = saved
        .work
        .covered_logs(source, &stream.stream_id, predecessor.as_ref());
    let begin = Begin {
        version: Version::V2,
        job: Job {
            stream,
            commit_id: saved.request_id.clone(),
        },
        descriptor: ClientDescriptor::empty(),
        required_destinations: vec![Destination::R2, Destination::B2],
        r2_multipart: None,
        predecessor,
        recovery_dependencies: saved.work.recovery_dependencies(),
        covered_logs,
    };
    begin.validate()?;
    saved.begin = Some(begin);
    saved.phase = Phase::Capturing;
    store.save(saved)?;
    source.runtime.flush_native().await?;
    Ok(())
}

async fn drive(
    source: &Source,
    capture: &Capture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<JobResult> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?.clone();
    work::check_work(store, saved, pause)?;
    saved.begin_submitted = true;
    store.save(saved)?;
    let call_begin = || {
        work::control("job_begin", pause, || async {
            Ok(client.begin(&begin).await?)
        })
    };
    let begun = call_begin().await?;
    match &begun.status {
        JobStatus::Published { .. } => return Ok(begun),
        JobStatus::Closed { .. } if source_unavailable(&begun) => return Ok(begun),
        JobStatus::Closed { .. } => return Err(BackupError::Authority),
        JobStatus::Active => {}
    }
    match upload_and_publish(source, capture, store, saved, client, pause).await {
        Ok(backup) => Ok(JobResult {
            status: JobStatus::Published {
                backup: Box::new(backup),
            },
            ..begun
        }),
        Err(BackupError::Cloud(BackupTransportError::Protocol(e)))
            if e.code == ErrorCode::JobClosed =>
        {
            let res = call_begin().await?;
            (source_unavailable(&res) || matches!(res.status, JobStatus::Published { .. }))
                .then_some(res)
                .ok_or_else(|| cloud_error(e))
        }
        Err(e) => Err(e),
    }
}

async fn upload_and_publish(
    source: &Source,
    capture: &Capture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<AcceptedBackup> {
    saved.phase = Phase::Uploading;
    store.save(saved)?;
    let parallelism = fold_db::storage_policy::BACKUP_RESOURCES
        .concurrent_streams
        .min(MAX_BATCH_ITEMS);
    let (total, mut next) = (capture.object_count()?, 0);
    while next < total {
        work::check_work(store, saved, pause)?;
        budget(capture, store)?;
        let mut batch = Vec::new();
        while next < total && batch.len() < parallelism {
            let obj = capture.object(next)?;
            if obj.receipt.is_none() {
                batch.push((next, obj.input));
            }
            next += 1;
        }
        if !batch.is_empty() {
            let inputs: Vec<_> = batch.iter().map(|(_, input)| input.clone()).collect();
            let ctx = work::Context::new(store, saved, pause);
            let failure = transfer::upload_batch(
                client,
                &inputs,
                &ctx,
                |pos| capture.reader(batch[pos].0),
                |pos, receipt| capture.confirm(batch[pos].0, &receipt),
            )
            .await?;
            refresh(capture, store, saved)?;
            if let Some(error) = failure {
                return Err(error);
            }
        }
    }
    source.validate(saved)?;
    transfer::publish(capture, store, saved, client, pause).await
}

fn refresh(capture: &Capture, store: &Store, saved: &mut Saved) -> Result<()> {
    saved.apply_status(&capture.status()?);
    store.save(saved)
}

fn finish(capture: &Capture, store: &Store, saved: &mut Saved, result: &JobResult) -> Result<()> {
    validate_terminal(saved, result)?;
    if capture.begin()? != *saved.begin.as_ref().ok_or(BackupError::Invalid)? {
        return Err(BackupError::Identity);
    }
    let published = match &result.status {
        JobStatus::Published { backup } => {
            if capture.root_receipt_id()? != backup.root_receipt_id {
                return Err(BackupError::Identity);
            }
            saved.accepted = Some((**backup).clone());
            Some(backup.as_ref())
        }
        JobStatus::Closed { .. } => None,
        JobStatus::Active => return Err(BackupError::Invalid),
    };
    store.write("terminal.json", result)?;
    if let Some(backup) = published {
        store.write_base("accepted.json", backup)?;
        let point = capture.recovery_point(backup)?;
        store.write("recovery.json", &point)?;
        store.write_base("recovery.json", &point)?;
    }
    capture.terminal(result)?;
    saved.phase = published.map_or(Phase::Closed, |_| Phase::Published);
    saved.blocked_reason = None;
    refresh(capture, store, saved)
}

async fn close(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    if saved.begin.is_none() || !saved.begin_submitted {
        if saved.begin.is_some() {
            source.cancel(saved).await?;
        }
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    let capture = source.resume(saved, store).await?;
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    if capture.begin()? != *begin {
        return Err(BackupError::Identity);
    }
    let req = JobCloseRequest {
        version: Version::V2,
        job: begin.job.clone(),
        begin_sha256: begin.input_sha256()?,
        reason: CloseReason::UserRequested,
    };
    let result = work::control("job_close", pause, || async {
        Ok(client.close(&req).await?)
    })
    .await?;
    finish(&capture, store, saved, &result)
}
