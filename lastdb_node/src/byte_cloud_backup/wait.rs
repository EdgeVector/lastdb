//! Continue one admitted job through Pending without repeating its upload.
use super::{model::Saved, publication, retry, store::Store, BackupError, Result};
use futures::future::{ready, Either};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

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

    pub fn check_upload(
        &self,
        input: &cloud_backup_protocol::v2::transfer::TransferInput,
        grant: &cloud_backup_protocol::v2::transfer::UploadGrant,
    ) -> Result<()> {
        use cloud_backup_protocol::v2::{transfer::TransferMode, Destination};
        let begin = self.saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        if begin.job != input.job
            || (begin.r2_multipart == Some(true)
                && input.object.destination == Some(Destination::R2)
                && grant.transfer_mode != Some(TransferMode::MultipartPart))
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    pub fn note_result<T>(&self, result: &Result<T>) {
        // Temporary status failures stay inside poll. Any escaped transfer
        // error ends this batch after all started effects and readers settle.
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
        let result = retry::control(operation, context.pause, || match context.check() {
            Ok(()) => Either::Right(request()),
            Err(error) => Either::Left(ready(Err(error))),
        })
        .await;
        match result {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(error) if continuable(&error) => {}
            Err(error) => return Err(error),
        }
        context.delay(delay).await?;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

fn continuable(error: &BackupError) -> bool {
    matches!(error, BackupError::Waiting) || retry::temporary(error)
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
    } else if retry::temporary(error) {
        1
    } else {
        2
    }
}
