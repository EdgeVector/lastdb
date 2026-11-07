//! Explicit shared writer upload. The caller owns provider admission and activation.
mod files;
mod service;
mod spool;
pub use service::ServiceObjects;

use crate::shared_restore::recovery::seal_log;
use cloud_backup_client::shared_log::{
    AppendAuthorization, AppendOutcome, BackupTransportError, SharedLogClient,
};
use cloud_backup_protocol::shared::{
    FileDependency, LogBatch, LogHead, SealedObject, WriterStream,
};
use cloud_backup_protocol::{bytes_sha256, AccountId, DecimalU64, ErrorCode, Id, ProtocolError};
use fold_db::fold_db_core::MutationManager;
use fold_db::portable_mutation::MutationEntry;
use fold_db::portable_replay::{LogRangeId, MAX_LOG_BYTES, MAX_LOG_ENTRIES};
use fold_db::schema::SchemaError;
use fold_db::security::Ed25519KeyPair;
use std::{
    io,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub type Result<T> = std::result::Result<T, UploadError>;

/// One bounded pass. A reconciliation head leaves the local batch available for retry.
#[derive(Debug)]
pub enum UploadOutcome {
    Idle,
    Accepted(LogHead),
    Reconcile(LogHead),
}

/// Registered device proof and the shared database's content key.
pub struct UploadIdentity<'a> {
    pub account: &'a AccountId,
    pub device: &'a Id,
    pub device_signer: &'a Ed25519KeyPair,
    pub content_key: &'a [u8; 32],
}

/// Create or reuse exact objects. Success means the service verified durable bytes.
#[async_trait::async_trait]
pub trait SharedObjectUploader: Sync {
    async fn put_file(
        &self,
        stream: &WriterStream,
        file: &FileDependency,
        sealed: Vec<u8>,
    ) -> Result<()>;

    async fn put_segment(&self, batch: &LogBatch, sealed: Vec<u8>) -> Result<()>;
}

/// Freeze the exact next batch, confirm its file dependencies and log segment,
/// and advance native upload progress only from an exact accepted receipt.
pub async fn upload_next(
    home_data: &Path,
    native: &MutationManager,
    identity: &UploadIdentity<'_>,
    client: &SharedLogClient,
    uploader: &impl SharedObjectUploader,
) -> Result<UploadOutcome> {
    let spool = spool::Spool::open(home_data)?;
    // The exclusive owner is held before reading the native upload frontier.
    let page = native.portable_outgoing().await?;
    let frozen = match spool.load()? {
        Some(frozen) => {
            let head = frozen.batch.head()?;
            if frozen.batch.stream != page.stream {
                return Err(UploadError::Invalid("frozen writer identity"));
            }
            if page.uploaded_head.as_ref() == Some(&head) {
                spool.release()?;
                return Ok(UploadOutcome::Accepted(head));
            }
            if frozen.batch.after != page.uploaded_head {
                return Err(UploadError::Invalid("frozen writer predecessor"));
            }
            // A lost response may have left this batch accepted. Its objects
            // may already be eligible for cleanup, so confirm before reupload.
            match append_exact(client, identity, &frozen.batch).await {
                Ok(outcome) => return settle(native, &spool, &frozen.batch, outcome).await,
                Err(UploadError::Transport(BackupTransportError::Protocol(error)))
                    if error.code == ErrorCode::MissingDependency => {}
                Err(error) => return Err(error),
            }
            frozen
        }
        None if page.entries.is_empty() => {
            native.reclaim_legacy_shared_retention().await?;
            return Ok(UploadOutcome::Idle);
        }
        None => {
            let frozen = prepare(
                native,
                &page.stream,
                page.uploaded_head.as_ref(),
                &page.entries,
                identity.content_key,
            )
            .await?;
            spool.save(&frozen)?;
            frozen
        }
    };
    let count = prefix_count(&frozen.batch)?;
    let entries = page
        .entries
        .get(..count)
        .ok_or(UploadError::Invalid("frozen mutation prefix"))?;
    files::upload(native, uploader, &frozen.batch, entries).await?;
    uploader.put_segment(&frozen.batch, frozen.sealed).await?;
    let outcome = append_exact(client, identity, &frozen.batch).await?;
    settle(native, &spool, &frozen.batch, outcome).await
}

async fn append_exact(
    client: &SharedLogClient,
    identity: &UploadIdentity<'_>,
    batch: &LogBatch,
) -> Result<AppendOutcome> {
    let proof = AppendAuthorization::sign(
        identity.account,
        identity.device,
        proof_expiry()?,
        batch,
        |message| Ok(identity.device_signer.sign(message).to_bytes()),
    )?;
    Ok(client.append(batch, proof).await?)
}

async fn settle(
    native: &MutationManager,
    spool: &spool::Spool,
    batch: &LogBatch,
    outcome: AppendOutcome,
) -> Result<UploadOutcome> {
    match outcome {
        AppendOutcome::Accepted(receipt) => {
            native.confirm_portable_upload(batch, &receipt).await?;
            spool.release()?;
            Ok(UploadOutcome::Accepted(receipt.head))
        }
        AppendOutcome::Reconcile(head) => Ok(UploadOutcome::Reconcile(head)),
    }
}

async fn prepare(
    native: &MutationManager,
    stream: &WriterStream,
    after: Option<&LogHead>,
    entries: &[MutationEntry],
    content_key: &[u8; 32],
) -> Result<spool::Frozen> {
    // Each admitted entry has a fixed upper bound, larger only when it carries
    // an inline file. Reserve frame/header bytes so every selected prefix fits
    // without a second encode or a sealing retry.
    let mut budget = MAX_LOG_BYTES - 64;
    let mut limit = 0;
    for entry in entries.iter().take(MAX_LOG_ENTRIES) {
        let Some(left) = budget.checked_sub(entry.max_encoded_bytes() + 4) else {
            break;
        };
        budget = left;
        limit += 1;
    }
    let entries = files::bounded_prefix(&entries[..limit])?;
    let first = after
        .map_or(0, |head| head.sequence.get())
        .checked_add(1)
        .ok_or(UploadError::Invalid("sequence overflow"))?;
    let last = entries
        .last()
        .ok_or(UploadError::Invalid("empty upload range"))?
        .stamp
        .position
        .sequence;
    let source = stream
        .source_stream
        .as_str()
        .parse()
        .map_err(|_| UploadError::Invalid("origin stream id"))?;
    let range =
        LogRangeId::new(source, first, last).map_err(|_| UploadError::Invalid("upload range"))?;
    let sealed = seal_log(content_key, &stream.database_id, &range, entries)
        .map_err(|_| UploadError::Invalid("seal mutation log"))?;
    let segment = SealedObject {
        version_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
        bytes: DecimalU64::new(sealed.len() as u64),
        sha256: bytes_sha256(&sealed),
    };
    let batch = LogBatch {
        stream: stream.clone(),
        after: after.cloned(),
        through: DecimalU64::new(last),
        segment,
        file_versions: files::dependencies(native, stream, entries).await?,
    };
    Ok(spool::Frozen { batch, sealed })
}

fn prefix_count(batch: &LogBatch) -> Result<usize> {
    let start = batch.after.as_ref().map_or(0, |head| head.sequence.get());
    let count = batch
        .through
        .get()
        .checked_sub(start)
        .filter(|count| (1..=MAX_LOG_ENTRIES as u64).contains(count))
        .ok_or(UploadError::Invalid("frozen range size"))?;
    Ok(count as usize)
}

fn proof_expiry() -> Result<DecimalU64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|age| u64::try_from(age.as_millis()).ok())
        .and_then(|now| now.checked_add(120_000))
        .map(DecimalU64::new)
        .ok_or(UploadError::Invalid("device proof clock"))
}

#[derive(Debug)]
pub enum UploadError {
    Invalid(&'static str),
    Io(io::Error),
    Protocol(ProtocolError),
    Transport(BackupTransportError),
    Native(SchemaError),
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(field) => write!(formatter, "invalid shared upload: {field}"),
            Self::Io(error) => write!(formatter, "shared upload state: {error}"),
            Self::Protocol(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Native(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for UploadError {}

impl From<io::Error> for UploadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for UploadError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<BackupTransportError> for UploadError {
    fn from(error: BackupTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<SchemaError> for UploadError {
    fn from(error: SchemaError) -> Self {
        Self::Native(error)
    }
}
