//! Backup work selection, capture dispatch, bounded retries, and pending-poll state.
use super::{
    log_capture::LogCapture, model::Saved, publication, recovery::RecoveryPoint, source::Source,
    store::Store, transfer, BackupError, Result,
};
use crate::cloud_backup_client::byte_backup::BackupTransportError;
use cloud_backup_protocol::v2::{
    jobs::JobResult,
    transfer::{ObjectReceipt, TransferInput, UploadGrant},
    AcceptedBackup, Begin, Commit, CoveredLog, Head,
};
use cloud_backup_protocol::{Digest, ErrorCode, Id, ProtocolError};
use fold_db::storage::disk_pack::byte_backup::{
    ByteBackupCapture, ByteBackupCaptureStatus, ByteBackupObject,
};
use fold_db::storage::disk_pack::mutation_log::{Frontiers, LogAnchor, LogPosition};
use futures::future::{ready, Either};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LogContext {
    pub base: AcceptedBackup,
    pub previous: AcceptedBackup,
    pub anchor: LogAnchor,
    pub from: LogPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Work {
    Snapshot { minimum: Option<Frontiers> },
    MutationLog { context: Box<LogContext> },
}
impl Default for Work {
    fn default() -> Self {
        Self::Snapshot { minimum: None }
    }
}
impl Work {
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Snapshot { minimum: None })
    }
    pub fn label(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::MutationLog { .. } => "mutation_log",
        }
    }
    /// The accepted points a restore of this work needs, in ordinal order.
    pub fn recovery_dependencies(&self) -> Vec<Head> {
        match self {
            Self::Snapshot { .. } => Vec::new(),
            Self::MutationLog { context } => {
                let mut heads = vec![context.base.head()];
                if context.previous != context.base {
                    heads.push(context.previous.head());
                }
                heads
            }
        }
    }
    pub fn covered_logs(
        &self,
        source: &Source,
        stream_id: &Id,
        predecessor: Option<&Head>,
    ) -> Vec<CoveredLog> {
        match (self, predecessor) {
            (Self::Snapshot { .. }, Some(head)) => vec![CoveredLog {
                user_id: source.account.clone(),
                device_id: source.source_home.clone(),
                stream_id: stream_id.clone(),
                through_commit_id: head.commit_id.clone(),
                through_ordinal: head.ordinal,
                through_sha256: head.commit_sha256.clone(),
            }],
            _ => Vec::new(),
        }
    }
    pub fn validate(&self, begin: Option<&Begin>) -> Result<()> {
        if let Self::MutationLog { context } = self {
            context.base.validate()?;
            context.previous.validate()?;
            context.anchor.position()?;
            if context.base.job().stream != context.previous.job().stream
                || context.from.epoch != context.anchor.epoch
            {
                return Err(BackupError::Identity);
            }
            if let Some(begin) = begin {
                if context.previous.job().stream != begin.job.stream
                    || begin.predecessor.as_ref() != Some(&context.previous.head())
                {
                    return Err(BackupError::Authority);
                }
            }
        }
        Ok(())
    }
}

pub(super) enum Capture {
    Snapshot(ByteBackupCapture),
    Log(Box<LogCapture>),
}
impl Capture {
    pub fn begin(&self) -> Result<Begin> {
        match self {
            Self::Snapshot(c) => Ok(c.begin()?),
            Self::Log(c) => c.begin(),
        }
    }
    pub fn object_count(&self) -> Result<u64> {
        match self {
            Self::Snapshot(c) => Ok(c.object_count()?),
            Self::Log(c) => c.object_count(),
        }
    }
    pub fn object(&self, index: u64) -> Result<ByteBackupObject> {
        match self {
            Self::Snapshot(c) => Ok(c.object(index)?),
            Self::Log(c) => c.object(index),
        }
    }
    pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>> {
        match self {
            Self::Snapshot(c) => Ok(Box::new(c.reader(index)?)),
            Self::Log(c) => c.reader(index),
        }
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.confirm(index, receipt)?),
            Self::Log(c) => c.confirm(index, receipt),
        }
    }
    pub fn root_receipt_id(&self) -> Result<Digest> {
        match self {
            Self::Snapshot(c) => Ok(c.root_receipt_id()?),
            Self::Log(c) => c.root_receipt_id(),
        }
    }
    pub fn status(&self) -> Result<ByteBackupCaptureStatus> {
        match self {
            Self::Snapshot(c) => Ok(c.status()?),
            Self::Log(c) => c.status(),
        }
    }
    pub fn recommended_parallelism(&self) -> Result<usize> {
        Ok(fold_db::storage_policy::BACKUP_RESOURCES.concurrent_streams)
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.set_coordinator_spool_bytes(bytes)?),
            Self::Log(c) => c.budget(bytes),
        }
    }
    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        match self {
            Self::Snapshot(c) => {
                if c.root_receipt_id()? != commit.root_receipt_id {
                    return Err(BackupError::Identity);
                }
                Ok(())
            }
            Self::Log(c) => c.seal_commit(commit),
        }
    }
    pub fn terminal(&self, result: &JobResult) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.terminal(result)?),
            Self::Log(c) => c.terminal(result),
        }
    }
    pub fn recovery_point(&self, accepted: &AcceptedBackup) -> Result<RecoveryPoint> {
        let point = match self {
            Self::Snapshot(capture) => {
                let profile = capture.run_profile()?;
                let anchor = capture.mutation_log_anchor()?;
                let through = anchor.as_ref().map(LogAnchor::position).transpose()?;
                RecoveryPoint {
                    accepted: accepted.clone(),
                    base: accepted.clone(),
                    anchor,
                    through,
                    frontiers: Frontiers {
                        main: profile.main.frontier.get(),
                    },
                }
            }
            Self::Log(capture) => capture.recovery_point(accepted),
        };
        point.validate()?;
        Ok(point)
    }
}

fn temporary_protocol(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::ProviderPending | ErrorCode::RetryLater | ErrorCode::OutcomeUnknown
    )
}

pub(super) fn temporary(error: &BackupError) -> bool {
    matches!(error, BackupError::Cloud(BackupTransportError::Protocol(error)) if temporary_protocol(error))
}

pub(super) async fn control<T, F, Fut>(
    operation: &'static str,
    pause: &AtomicBool,
    mut request: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    for attempt in 0..6 {
        transfer::check_pause(pause)?;
        if Instant::now() >= deadline {
            break;
        }
        let result = tokio::time::timeout_at(deadline, request()).await;
        transfer::check_pause(pause)?;
        match result {
            Ok(Err(error)) if temporary(&error) => last_error = Some(error),
            Ok(result) => return result,
            Err(_) => break,
        }
        if attempt < 5 {
            let ceiling = 100u64 << attempt;
            let delay = Duration::from_millis(1 + rand::random::<u64>() % ceiling);
            tokio::time::sleep_until(deadline.min(Instant::now() + delay)).await;
            transfer::check_pause(pause)?;
        }
    }
    tracing::warn!(
        operation,
        error_class = "temporary_control",
        "V2 control retry budget exhausted"
    );
    Err(last_error.unwrap_or(BackupError::Waiting))
}

pub(super) struct Context<'a> {
    store: &'a Store,
    saved: &'a Saved,
    pub pause: &'a AtomicBool,
    stopped: AtomicBool,
}

impl<'a> Context<'a> {
    pub fn new(store: &'a Store, saved: &'a Saved, pause: &'a AtomicBool) -> Self {
        Self {
            store,
            saved,
            pause,
            stopped: AtomicBool::new(false),
        }
    }

    pub fn check(&self) -> Result<()> {
        publication::check_work(self.store, self.saved, self.pause)?;
        if self.stopped.load(Ordering::Acquire) {
            Err(BackupError::Waiting)
        } else {
            Ok(())
        }
    }

    pub fn check_upload(&self, input: &TransferInput, grant: &UploadGrant) -> Result<()> {
        let begin = self.saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        if begin.job != input.job || grant.transfer_mode.is_some() {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    pub fn note_result<T>(&self, result: &Result<T>) {
        if result.is_err() {
            self.stopped.store(true, Ordering::Release);
        }
    }

    async fn delay(&self, duration: Duration) -> Result<()> {
        let deadline = Instant::now() + duration;
        loop {
            self.check()?;
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(());
            }
            tokio::time::sleep(left.min(Duration::from_millis(250))).await;
        }
    }
}

pub(super) async fn poll<T, F, Fut>(
    context: &Context<'_>,
    operation: &'static str,
    mut request: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let mut delay = Duration::from_secs(1);
    loop {
        context.check()?;
        let result = control(operation, context.pause, || match context.check() {
            Ok(()) => Either::Right(request()),
            Err(error) => Either::Left(ready(Err(error))),
        })
        .await;
        match result {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(error) if matches!(error, BackupError::Waiting) || temporary(&error) => {}
            Err(error) => return Err(error),
        }
        context.delay(delay).await?;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

/// A local sibling stop must not hide the original non-continuable refusal.
pub(super) fn record_error(previous: &mut Option<BackupError>, error: BackupError) {
    if previous
        .as_ref()
        .is_none_or(|old| priority(&error) > priority(old))
    {
        *previous = Some(error);
    }
}

fn priority(error: &BackupError) -> u8 {
    if matches!(error, BackupError::Waiting) {
        0
    } else if temporary(error) {
        1
    } else {
        2
    }
}
