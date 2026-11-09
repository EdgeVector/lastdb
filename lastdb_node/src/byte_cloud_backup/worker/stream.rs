//! Bind a native source to its ordinary, publication, or dedicated CI stream.
use super::super::{model::StreamRecord, source::Source, store::Store, BackupError, Result};
use super::Saved;
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::{CloudScope, Id};

const CI_ACCOUNT: &str = "2a81c95623f2445854a794b5b48471d6";

fn expected(source: &Source) -> Result<Option<Id>> {
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

pub(super) fn validate_saved(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let Some(expected) = expected(source)? else {
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

pub(super) fn select(source: &Source, store: &Store, scope: CloudScope) -> Result<Stream> {
    let expected = expected(source)?;
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

/// A closed admission can be temporary. Never treat a different cloud
/// generation, source home, or required CI stream as that same scope.
pub(super) fn same_scope(source: &Source, store: &Store, scope: &CloudScope) -> Result<bool> {
    let Some(record) = store.read_base::<StreamRecord>("stream.json")? else {
        return Ok(false);
    };
    record.stream.validate()?;
    Ok(record.source_home == source.source_home
        && record.stream.scope == *scope
        && expected(source)?.is_none_or(|id| id == record.stream.stream_id))
}
