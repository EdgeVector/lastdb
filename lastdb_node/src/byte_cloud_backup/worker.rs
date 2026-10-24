use super::{
    model::{Saved, StreamRecord},
    publication,
    source::Source,
    store::Store,
    transfer, BackupError, Phase, Result,
};
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::jobs::{CloseReason, JobCloseRequest, JobResult, JobStatus};
use cloud_backup_protocol::v2::publication::{HeadReadRequest, HeadReadResult};
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Job, Stream, Version};
use cloud_backup_protocol::{ErrorCode, Id};
use fold_db::storage::disk_pack::byte_backup::{Bootstrap, ByteBackupCapture};
use fold_db::sync::auth::byte_backup::ByteBackupClient;
use std::sync::atomic::AtomicBool;

mod terminal;
pub(super) use terminal::{terminal_invocation, validate_terminal};

pub(super) async fn run(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    source.validate(saved)?;
    if store.read::<JobResult>("terminal.json")?.is_some() {
        return terminal::reconcile(source, store, saved).await;
    }
    transfer::check_pause(pause)?;
    if store.read::<Id>("close.json")?.is_some() {
        return close(source, store, saved, client).await;
    }
    setup(source, store, saved, client).await?;
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?.clone();
    let capture = source
        .runtime
        .prepare_byte_backup_capture(begin.clone(), &source.key)
        .await?;
    refresh(&capture, store, saved)?;
    let result = drive(&capture, store, saved, client, pause).await;
    match result {
        Ok(backup) => finish(
            &capture,
            store,
            saved,
            JobResult {
                version: Version::V2,
                job: begin.job.clone(),
                begin_sha256: begin.input_sha256()?,
                status: JobStatus::Published {
                    backup: Box::new(backup),
                },
            },
        ),
        Err(BackupError::Closing) => {
            drop(capture);
            close(source, store, saved, client).await
        }
        Err(error) => Err(error),
    }
}

async fn setup(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
) -> Result<()> {
    if saved.begin.is_some() {
        return Ok(());
    }
    crate::native_cloud_backup::require_v1_complete(
        &source.home,
        &source.account,
        &source.source_home,
    )?;
    let context = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: source.account.clone(),
        })
        .await?;
    if !context.new_work_allowed {
        return Err(BackupError::Authority);
    }
    let previous: Option<StreamRecord> = store.read_base("stream.json")?;
    let stream = match previous {
        Some(record)
            if record.source_home == source.source_home && record.stream.scope == context.scope =>
        {
            record.stream
        }
        _ => Stream {
            scope: context.scope,
            stream_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
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
    let predecessor = match client
        .head(&HeadReadRequest {
            version: Version::V2,
            stream: stream.clone(),
        })
        .await?
    {
        HeadReadResult::Ok { response } => {
            if response
                .active_commit_id
                .as_ref()
                .is_some_and(|id| id != &saved.request_id)
            {
                return Err(BackupError::Busy);
            }
            response.accepted.map(|a| a.head())
        }
        HeadReadResult::Error { error } if error.code == ErrorCode::NotFound => None,
        HeadReadResult::Error { error } => return Err(transfer::cloud_error(error)),
    };
    let begin = Begin {
        version: Version::V2,
        job: Job {
            stream,
            commit_id: saved.request_id.clone(),
        },
        descriptor: Bootstrap {
            recovery: source.recovery.clone(),
        }
        .descriptor()?,
        predecessor,
    };
    begin.validate()?;
    saved.begin = Some(begin);
    saved.phase = Phase::Capturing;
    store.save(saved)?;
    source.runtime.flush_native().await?;
    Ok(())
}

async fn drive(
    capture: &ByteBackupCapture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<AcceptedBackup> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?.clone();
    publication::check_work(store, saved, pause)?;
    saved.begin_submitted = true;
    store.save(saved)?;
    match client.begin(&begin).await?.status {
        JobStatus::Published { backup } => {
            if saved.commit.as_ref() != Some(&backup.commit_input()?) {
                return Err(BackupError::Identity);
            }
            return Ok(*backup);
        }
        JobStatus::Closed { .. } => return Err(BackupError::Authority),
        JobStatus::Active => {}
    }
    saved.phase = Phase::Uploading;
    store.save(saved)?;
    let mut next = 0;
    while next < capture.object_count()? {
        publication::check_work(store, saved, pause)?;
        capture.set_coordinator_spool_bytes(
            store.bytes() + lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES,
        )?;
        let mut batch = Vec::new();
        while next < capture.object_count()?
            && batch.len() < cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS
        {
            let object = capture.object(next)?;
            if object.receipt.is_none() {
                batch.push((next, object.input));
            }
            next += 1;
        }
        if batch.is_empty() {
            continue;
        }
        let inputs: Vec<_> = batch.iter().map(|(_, input)| input.clone()).collect();
        let prepared = transfer::prepare(client, &inputs).await?;
        let mut work = Vec::new();
        for ((index, input), prepared) in batch.iter().zip(prepared) {
            let reader = Box::new(capture.reader(*index)?);
            work.push(transfer::complete(client, input, prepared, reader, pause));
        }
        // Settle every upload future before releasing source/reader ownership.
        let results = futures::future::join_all(work).await;
        let mut failure = None;
        for ((index, _), result) in batch.iter().zip(results) {
            match result {
                Ok(receipt) => capture.confirm(*index, &receipt)?,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        refresh(capture, store, saved)?;
        if let Some(error) = failure {
            return Err(error);
        }
    }
    publication::publish(capture, store, saved, client, pause).await
}

fn refresh(capture: &ByteBackupCapture, store: &Store, saved: &mut Saved) -> Result<()> {
    let status = capture.status()?;
    saved.objects_verified = status.verified_objects;
    saved.metadata_pack_objects = status.metadata_pack_objects;
    saved.multi_file_pack_objects = status.multi_file_pack_objects;
    saved.native_files = status.native_files;
    saved.native_bytes = status.native_bytes;
    saved.retained_source_bytes = Some(status.retained_source_bytes);
    store.save(saved)
}

fn finish(
    capture: &ByteBackupCapture,
    store: &Store,
    saved: &mut Saved,
    result: JobResult,
) -> Result<()> {
    result.validate_for_begin(saved.begin.as_ref().ok_or(BackupError::Invalid)?)?;
    if let JobStatus::Published { backup } = &result.status {
        if saved.commit.as_ref() != Some(&backup.commit_input()?)
            || capture.root_receipt_id()? != backup.root_receipt_id
        {
            return Err(BackupError::Identity);
        }
        saved.accepted = Some((**backup).clone());
    }
    store.write("terminal.json", &result)?;
    if let JobStatus::Published { backup } = &result.status {
        store.write_base("accepted.json", backup)?;
    }
    capture.terminal(&result)?;
    saved.phase = match result.status {
        JobStatus::Published { .. } => Phase::Published,
        JobStatus::Closed { .. } => Phase::Closed,
        JobStatus::Active => return Err(BackupError::Invalid),
    };
    refresh(capture, store, saved)
}

async fn close(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
) -> Result<()> {
    if saved.begin.is_none() {
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    if !saved.begin_submitted {
        source
            .runtime
            .cancel_unsubmitted_byte_backup_capture(
                saved.begin.as_ref().ok_or(BackupError::Invalid)?,
                &source.key,
            )
            .await?;
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    let capture = source
        .runtime
        .resume_byte_backup_capture(&source.key)
        .await?;
    if capture.begin()? != *saved.begin.as_ref().ok_or(BackupError::Invalid)? {
        return Err(BackupError::Identity);
    }
    close_capture(&capture, store, saved, client).await
}

async fn close_capture(
    capture: &ByteBackupCapture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
) -> Result<()> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let result = client
        .close(&JobCloseRequest {
            version: Version::V2,
            job: begin.job.clone(),
            begin_sha256: begin.input_sha256()?,
            reason: CloseReason::UserRequested,
        })
        .await?;
    finish(capture, store, saved, result)
}
