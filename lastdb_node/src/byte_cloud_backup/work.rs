//! Backup work selection, capture dispatch, bounded retries, and pending-poll state.
use super::{
    log_capture::LogCapture,
    source::{Saved, Source},
    store::{self, Store},
    BackupError, Result,
};
use crate::byte_cloud_log::dependency_heads;
use crate::cloud_backup_client::byte_backup::BackupTransportError;
use cloud_backup_protocol::v2::{
    jobs::JobResult, transfer::ObjectReceipt, AcceptedBackup, Begin, Commit, CoveredLog, Head,
};
use cloud_backup_protocol::{AccountId, Digest, ErrorCode, Id, ProtocolError};
use fold_db::cloud_backup_plan::PlannedRecord;
use fold_db::storage::disk_pack::byte_backup::{ByteBackupCapture, ByteBackupCaptureStatus};
use fold_db::storage::disk_pack::mutation_log::{Frontiers, LogAnchor, LogPosition};
use fold_db::storage_policy::backup_decode;
use futures::future::{ready, Either};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryPoint {
    pub accepted: AcceptedBackup,
    pub base: AcceptedBackup,
    pub anchor: Option<LogAnchor>,
    pub through: Option<LogPosition>,
    pub frontiers: Frontiers,
}

impl RecoveryPoint {
    pub fn read_at(base: &Path, share: Option<(&Id, &AccountId)>) -> Result<Option<Self>> {
        let point: Option<Self> = store::read(&base.join("recovery.json"))?;
        if let Some(p) = &point {
            p.validate()?;
            if let Some((stream_id, account)) = share {
                let st = &p.accepted.job().stream;
                let deps = &p.accepted.begin.recovery_dependencies;
                let bad_chain = p.base != p.accepted
                    && (p.anchor.is_none()
                        || deps.first() != Some(&p.base.head())
                        || deps.last() != p.accepted.begin.predecessor.as_ref());
                if st.stream_id != *stream_id
                    || st.scope.account_id != *account
                    || !p.base.begin.recovery_dependencies.is_empty()
                    || bad_chain
                {
                    return Err(BackupError::Identity);
                }
            }
        }
        Ok(point)
    }

    pub fn validate(&self) -> Result<()> {
        self.accepted.validate()?;
        self.base.validate()?;
        if self.base.job().stream != self.accepted.job().stream
            || self.base.ordinal.get() > self.accepted.ordinal.get()
            || self.anchor.is_some() != self.through.is_some()
        {
            return Err(BackupError::Identity);
        }
        if let (Some(anchor), Some(through)) = (&self.anchor, &self.through) {
            let start = anchor.position()?;
            if through.epoch != start.epoch
                || through.frontiers != self.frontiers
                || (self.accepted == self.base && *through != start)
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }

    pub fn log_context(&self) -> Result<LogContext> {
        self.validate()?;
        Ok(LogContext {
            base: self.base.clone(),
            previous: self.accepted.clone(),
            anchor: self.anchor.clone().ok_or(BackupError::Source)?,
            from: self.through.clone().ok_or(BackupError::Source)?,
        })
    }
}

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

    pub fn recovery_dependencies(&self) -> Vec<Head> {
        match self {
            Self::Snapshot { .. } => Vec::new(),
            Self::MutationLog { context } => dependency_heads(&context.base, &context.previous),
        }
    }

    pub fn covered_logs(
        &self,
        source: &Source,
        stream_id: &Id,
        predecessor: Option<&Head>,
    ) -> Vec<CoveredLog> {
        match (self, predecessor) {
            (Self::Snapshot { .. }, Some(h)) => vec![CoveredLog {
                user_id: source.account.clone(),
                device_id: source.source_home.clone(),
                stream_id: stream_id.clone(),
                through_commit_id: h.commit_id.clone(),
                through_ordinal: h.ordinal,
                through_sha256: h.commit_sha256.clone(),
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
            if begin.is_some_and(|b| {
                context.previous.job().stream != b.job.stream
                    || b.predecessor.as_ref() != Some(&context.previous.head())
            }) {
                return Err(BackupError::Authority);
            }
        }
        Ok(())
    }
}

pub(super) enum Capture {
    Snapshot(ByteBackupCapture),
    Log(Box<LogCapture>),
}

macro_rules! delegate_capture {
    ($(pub fn $name:ident(&self $(, $arg:ident: $ty:ty)*) -> Result<$ret:ty>;)+) => {
        $(pub fn $name(&self $(, $arg: $ty)*) -> Result<$ret> {
            match self {
                Self::Snapshot(c) => Ok(c.$name($($arg),*)?),
                Self::Log(c) => c.$name($($arg),*),
            }
        })+
    };
}

impl Capture {
    delegate_capture! {
        pub fn begin(&self) -> Result<Begin>;
        pub fn object_count(&self) -> Result<u64>;
        pub fn object(&self, index: u64) -> Result<PlannedRecord>;
        pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>>;
        pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()>;
        pub fn root_receipt_id(&self) -> Result<Digest>;
        pub fn status(&self) -> Result<ByteBackupCaptureStatus>;
        pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> Result<()>;
        pub fn terminal(&self, result: &JobResult) -> Result<()>;
    }

    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        match self {
            Self::Snapshot(c) => (c.root_receipt_id()? == commit.root_receipt_id)
                .then_some(())
                .ok_or(BackupError::Identity),
            Self::Log(c) => c.seal_commit(commit),
        }
    }

    pub fn recovery_point(&self, accepted: &AcceptedBackup) -> Result<RecoveryPoint> {
        let point = match self {
            Self::Snapshot(c) => {
                let anchor = c.mutation_log_anchor()?;
                let through = anchor.as_ref().map(LogAnchor::position).transpose()?;
                let main = c.run_profile()?.main.frontier.get();
                RecoveryPoint {
                    accepted: accepted.clone(),
                    base: accepted.clone(),
                    anchor,
                    through,
                    frontiers: Frontiers { main },
                }
            }
            Self::Log(c) => c.recovery_point(accepted),
        };
        point.validate().map(|()| point)
    }
}

pub(super) fn check_pause(pause: &AtomicBool) -> Result<()> {
    (!pause.load(Ordering::Acquire))
        .then_some(())
        .ok_or(BackupError::Shutdown)
}

pub(super) fn cloud_error(error: ProtocolError) -> BackupError {
    BackupTransportError::Protocol(error).into()
}

pub(super) fn budget(capture: &Capture, store: &Store) -> Result<()> {
    let quote = store
        .bytes()
        .checked_add(backup_decode::JSON_PAGE_RESERVE_BYTES);
    capture.set_coordinator_spool_bytes(quote.ok_or(BackupError::Limit)?)
}

pub(super) fn temporary(error: &BackupError) -> bool {
    matches!(
        error,
        BackupError::Cloud(BackupTransportError::Protocol(e))
            if matches!(e.code, ErrorCode::ProviderPending | ErrorCode::RetryLater | ErrorCode::OutcomeUnknown)
    )
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
        check_pause(pause)?;
        if Instant::now() >= deadline {
            break;
        }
        let result = tokio::time::timeout_at(deadline, request()).await;
        check_pause(pause)?;
        match result {
            Ok(Err(e)) if temporary(&e) => last_error = Some(e),
            Ok(res) => return res,
            Err(_) => break,
        }
        if attempt < 5 {
            let delay = Duration::from_millis(1 + rand::random::<u64>() % (100u64 << attempt));
            tokio::time::sleep_until(deadline.min(Instant::now() + delay)).await;
            check_pause(pause)?;
        }
    }
    tracing::warn!(
        operation,
        error_class = "temporary_control",
        "V2 control retry budget exhausted"
    );
    Err(last_error.unwrap_or(BackupError::Waiting))
}

pub(super) fn check_work(store: &Store, saved: &Saved, pause: &AtomicBool) -> Result<()> {
    check_pause(pause)?;
    match store.read::<Id>("close.json")? {
        Some(id) if id == saved.request_id => Err(BackupError::Closing),
        Some(_) => Err(BackupError::Identity),
        None => Ok(()),
    }
}

pub(super) struct Context<'a> {
    pub store: &'a Store,
    pub saved: &'a Saved,
    pub pause: &'a AtomicBool,
    pub stopped: AtomicBool,
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
        check_work(self.store, self.saved, self.pause)?;
        (!self.stopped.load(Ordering::Acquire))
            .then_some(())
            .ok_or(BackupError::Waiting)
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
        let res = control(operation, context.pause, || match context.check() {
            Ok(()) => Either::Right(request()),
            Err(e) => Either::Left(ready(Err(e))),
        })
        .await;
        match res {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(e) if matches!(e, BackupError::Waiting) || temporary(&e) => {}
            Err(e) => return Err(e),
        }
        context.delay(delay).await?;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}
