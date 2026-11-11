//! Stream binding and durable terminal replay for backup workers.
use super::super::model::StreamRecord;
use super::*;
use crate::backup_runtime::InvocationOutcome;
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::CloudScope;
use std::fs::File;
use std::sync::Arc;

const CI_ACCOUNT: &str = "2a81c95623f2445854a794b5b48471d6";

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

pub(in crate::byte_cloud_backup) fn source_unavailable(result: &JobResult) -> bool {
    matches!(
        result.status,
        JobStatus::Closed {
            reason: CloseReason::SourceUnavailable,
        }
    )
}

pub(in crate::byte_cloud_backup) fn validate_terminal(
    saved: &Saved,
    result: &JobResult,
) -> Result<()> {
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

pub(in crate::byte_cloud_backup) async fn reconcile(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
) -> Result<()> {
    source.validate(saved)?;
    let result: JobResult = store.read("terminal.json")?.ok_or(BackupError::Missing)?;
    validate_terminal(saved, &result)?;
    let capture = source.resume(saved, store).await?;
    super::finish(&capture, store, saved, &result)?;
    if source.publication_stream.is_none() {
        crate::byte_cloud_backup::blobs::accepted(source, store, saved).await?;
    }
    Ok(())
}

pub(in crate::byte_cloud_backup) fn terminal_invocation(
    source: Source,
    store: Store,
    mut saved: Saved,
    permit: File,
    pause: Arc<AtomicBool>,
) -> InvocationOutcome {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transfer::check_pause(&pause)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        runtime.block_on(reconcile(&source, &store, &mut saved))
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let outcome = match result {
        Ok(()) => InvocationOutcome::Complete,
        Err(error) => {
            saved.blocked_reason = Some(error.to_string());
            match store.reopen().and_then(|fresh| {
                fresh.write("pause.json", &error.pause_state())?;
                fresh.save(&saved)
            }) {
                Ok(()) => InvocationOutcome::Paused(error.pause_state()),
                Err(checkpoint) => InvocationOutcome::Paused(checkpoint.pause_state()),
            }
        }
    };
    drop(store);
    drop(source);
    drop(permit);
    drop(pause);
    outcome
}
