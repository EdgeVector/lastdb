use super::capture::Capture;
use super::{
    model::Saved, publication, retry, source::Source, store::Store, transfer, BackupError, Phase,
    Result,
};
use cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupClient};
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::jobs::{CloseReason, JobCloseRequest, JobResult, JobStatus};
use cloud_backup_protocol::v2::publication::{HeadReadRequest, HeadReadResult};
use cloud_backup_protocol::v2::{
    AcceptedBackup, Begin, ClientDescriptor, Destination, Job, Version,
};
use cloud_backup_protocol::{ErrorCode, Id};
use std::sync::atomic::AtomicBool;

mod stream;
mod terminal;
pub(super) use terminal::{source_unavailable, terminal_invocation, validate_terminal};

pub(super) async fn run(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    source.validate(saved)?;
    if saved.begin.is_some() {
        stream::validate_saved(source, store, saved)?;
    }
    if saved.local_action.is_some() {
        source.cancel_replacement(saved)?;
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        saved.blocked_reason = None;
        return store.save(saved);
    }
    if store.read::<JobResult>("terminal.json")?.is_some() {
        return terminal::reconcile(source, store, saved).await;
    }
    transfer::check_pause(pause)?;
    if store.read::<Id>("close.json")?.is_some() {
        return close(source, store, saved, client, pause).await;
    }
    setup(source, store, saved, client, pause).await?;
    super::blobs::plan_releases(source, store, saved).await?;
    let capture = source.prepare(saved, store).await?;
    refresh(&capture, store, saved)?;
    if source.retain_source_manifest {
        super::stopped::retain_manifest(&capture, store)?;
    }
    source.validate(saved)?;
    let result = drive(source, &capture, store, saved, client, pause).await;
    source.validate(saved)?;
    match result {
        Ok(result) => {
            finish(&capture, store, saved, &result)?;
            super::blobs::accepted(source, store, saved).await
        }
        Err(BackupError::Closing) => {
            drop(capture);
            close(source, store, saved, client, pause).await
        }
        Err(error) => Err(error),
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
    let context_request = ContextReadRequest {
        version: Version::V2,
        account_id: source.account.clone(),
        required_destinations: vec![Destination::R2, Destination::B2],
    };
    let context = retry::control("context_read", pause, || async {
        Ok(client.context(&context_request).await?)
    })
    .await?;
    if !context.new_work_allowed {
        if saved.automatic_revision.is_some() && stream::same_scope(source, store, &context.scope)?
        {
            return Err(BackupError::AdmissionPending);
        }
        return Err(BackupError::Authority);
    }
    let stream = stream::select(source, store, context.scope)?;
    let head_request = HeadReadRequest {
        version: Version::V2,
        stream: stream.clone(),
    };
    let head = retry::control("head_read", pause, || async {
        match client.head(&head_request).await? {
            HeadReadResult::Error { error }
                if error.code != ErrorCode::NotFound || error.field.as_str() != "stream" =>
            {
                Err(transfer::cloud_error(error))
            }
            result => Ok(result),
        }
    })
    .await?;
    let predecessor = match head {
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
        HeadReadResult::Error { error }
            if error.code == ErrorCode::NotFound && error.field.as_str() == "stream" =>
        {
            None
        }
        HeadReadResult::Error { error } => return Err(transfer::cloud_error(error)),
    };
    let begin = Begin {
        version: Version::V2,
        job: Job {
            stream,
            commit_id: saved.request_id.clone(),
        },
        descriptor: ClientDescriptor::empty(),
        required_destinations: vec![Destination::R2, Destination::B2],
        r2_multipart: (source.publication_stream.is_none()
            && env_flag::var_truthy("LASTDB_CLOUD_CI_TERMINAL_R2_UPLOADS"))
        .then_some(true),
        predecessor,
        // Declared so the service can retire points that no kept point needs.
        // Every deployed V2 service package accepts this field.
        recovery_dependencies: saved.work.recovery_dependencies(),
    };
    begin.validate()?;
    saved.begin = Some(begin);
    saved.phase = Phase::Capturing;
    store.save(saved)?;
    source.flush().await?;
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
    publication::check_work(store, saved, pause)?;
    saved.begin_submitted = true;
    store.save(saved)?;
    let begun = begin_exact(client, &begin, pause).await?;
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
        Err(BackupError::Cloud(BackupTransportError::Protocol(error)))
            if error.code == ErrorCode::JobClosed =>
        {
            // Expiry can win during transfer or publication. Only the
            // digest-bound terminal result can release this local capture.
            let result = begin_exact(client, &begin, pause).await?;
            if source_unavailable(&result) || matches!(result.status, JobStatus::Published { .. }) {
                Ok(result)
            } else {
                Err(transfer::cloud_error(error))
            }
        }
        Err(error) => Err(error),
    }
}

async fn begin_exact(
    client: &ByteBackupClient,
    begin: &Begin,
    pause: &AtomicBool,
) -> Result<JobResult> {
    retry::control("job_begin", pause, || async {
        Ok(client.begin(begin).await?)
    })
    .await
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
    // Record blobs go to their own paths before the job's inventory objects.
    super::blobs::upload(source, store, saved, client, pause).await?;
    let parallelism = capture
        .recommended_parallelism()?
        .min(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS);
    let mut next = 0;
    while next < capture.object_count()? {
        publication::check_work(store, saved, pause)?;
        capture.set_coordinator_spool_bytes(
            store.bytes() + lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES,
        )?;
        let mut batch = Vec::new();
        while next < capture.object_count()? && batch.len() < parallelism {
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
        let prepared = transfer::prepare(client, &inputs, pause).await?;
        if let Some(error) = prepared.error {
            for ((index, _), item) in batch.iter().zip(prepared.items) {
                if let Ok(Some(receipt)) = transfer::verified(item) {
                    capture.confirm(*index, &receipt)?;
                }
            }
            refresh(capture, store, saved)?;
            return Err(error);
        }
        let context = super::wait::Context::new(store, saved, pause);
        let mut work = Vec::new();
        for ((index, input), prepared) in batch.iter().zip(prepared.items) {
            let reader = capture.reader(*index)?;
            work.push(transfer::complete(
                client, input, prepared, reader, &context,
            ));
        }
        // Settle every upload future before releasing source/reader ownership.
        let results = futures::future::join_all(work).await;
        let mut failure = None;
        for ((index, _), result) in batch.iter().zip(results) {
            match result {
                Ok(receipt) => capture.confirm(*index, &receipt)?,
                Err(error) => {
                    super::wait::record_error(&mut failure, error);
                }
            }
        }
        refresh(capture, store, saved)?;
        if let Some(error) = failure {
            return Err(error);
        }
    }
    super::blobs::release(store, saved, client, pause).await?;
    source.validate(saved)?;
    publication::publish(capture, store, saved, client, pause).await
}

fn refresh(capture: &Capture, store: &Store, saved: &mut Saved) -> Result<()> {
    let status = capture.status()?;
    saved.objects_verified = status.verified_objects;
    saved.metadata_pack_objects = status.metadata_pack_objects;
    saved.multi_file_pack_objects = status.multi_file_pack_objects;
    saved.native_files = status.native_files;
    saved.native_bytes = status.native_bytes;
    saved.retained_source_bytes = Some(status.retained_source_bytes);
    store.save(saved)
}

fn finish(capture: &Capture, store: &Store, saved: &mut Saved, result: &JobResult) -> Result<()> {
    validate_terminal(saved, result)?;
    if capture.begin()? != *saved.begin.as_ref().ok_or(BackupError::Invalid)? {
        return Err(BackupError::Identity);
    }
    if let JobStatus::Published { backup } = &result.status {
        if capture.root_receipt_id()? != backup.root_receipt_id {
            return Err(BackupError::Identity);
        }
        saved.accepted = Some((**backup).clone());
    }
    store.write("terminal.json", result)?;
    if let JobStatus::Published { backup } = &result.status {
        store.write_base("accepted.json", backup)?;
        let point = capture.recovery_point(backup)?;
        store.write("recovery.json", &point)?;
        store.write_base("recovery.json", &point)?;
    }
    capture.terminal(result)?;
    saved.phase = match result.status {
        JobStatus::Published { .. } => Phase::Published,
        JobStatus::Closed { .. } => Phase::Closed,
        JobStatus::Active => return Err(BackupError::Invalid),
    };
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
    if saved.begin.is_none() {
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    if !saved.begin_submitted {
        source.cancel(saved).await?;
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    let capture = source.resume(saved, store).await?;
    if capture.begin()? != *saved.begin.as_ref().ok_or(BackupError::Invalid)? {
        return Err(BackupError::Identity);
    }
    close_capture(&capture, store, saved, client, pause).await?;
    super::blobs::accepted(source, store, saved).await
}

async fn close_capture(
    capture: &Capture,
    store: &Store,
    saved: &mut Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let request = JobCloseRequest {
        version: Version::V2,
        job: begin.job.clone(),
        begin_sha256: begin.input_sha256()?,
        reason: CloseReason::UserRequested,
    };
    let result = retry::control("job_close", pause, || async {
        Ok(client.close(&request).await?)
    })
    .await?;
    finish(capture, store, saved, &result)
}
