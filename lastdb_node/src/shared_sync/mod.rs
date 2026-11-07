//! Automatic two-way synchronization for a restored, enrolled shared home.

mod config;
mod snapshot;
mod worker;
pub use config::save_bound;

use crate::byte_cloud_backup::{BackupError, Result};
use cloud_backup_protocol::{shared::WriterStream, Id};
use serde::Serialize;
use std::sync::Mutex;
use tracing::Instrument;

#[derive(Default)]
pub struct Runtime {
    pub(crate) gate: tokio::sync::Mutex<()>,
    state: Mutex<Progress>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Progress {
    pub running: bool,
    pub last_sync_at: Option<u64>,
    pub last_upload_at: Option<u64>,
    pub next_sync_at: u64,
    pub uploaded_through: u64,
    pub applied_ranges: u64,
    pub needs_restore: Option<Id>,
    pub blocked_reason: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub enabled: bool,
    pub stream: WriterStream,
    pub device: Id,
    pub publisher: bool,
    pub interval_seconds: u64,
    pub last_snapshot_at: Option<u64>,
    pub last_snapshot_id: Option<Id>,
    pub next_snapshot_at: u64,
    pub progress: Progress,
}

impl Runtime {
    pub(crate) fn update(&self, change: impl FnOnce(&mut Progress)) {
        change(&mut self.state.lock().expect("shared sync progress"));
    }
}

pub(crate) fn status(host: &crate::Host) -> Result<Option<Status>> {
    let Some(config) = config::load(&host.home)? else {
        return Ok(None);
    };
    if config.account.as_str() != host.user_hash {
        return Err(BackupError::Identity);
    }
    Ok(Some(Status {
        enabled: config.enabled,
        stream: config.stream.clone(),
        device: config.device.clone(),
        publisher: config.publisher,
        interval_seconds: config.interval_seconds,
        last_snapshot_at: config.last_snapshot_at,
        last_snapshot_id: config.last_snapshot_id.clone(),
        next_snapshot_at: config.next_snapshot_at,
        progress: host
            .shared_sync
            .state
            .lock()
            .expect("shared sync progress")
            .clone(),
    }))
}

pub(crate) fn configure(host: &crate::Host, enabled: bool, log: u64, snapshot: u64) -> Result<()> {
    config::configure(host, enabled, log, snapshot)
}

pub fn spawn_automatic(
    host: std::sync::Arc<crate::Host>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(worker::run(host, shutdown).in_current_span())
}

impl Status {
    pub(crate) fn health(self) -> crate::self_metrics::DurabilityHealth {
        crate::self_metrics::DurabilityHealth {
            observed: true,
            automatic_enabled: self.enabled,
            running: self.progress.running,
            phase: self
                .progress
                .running
                .then_some(crate::backup_runtime::Phase::Uploading),
            accepted_backup_recorded: self.last_snapshot_id.is_some()
                || self.progress.last_upload_at.is_some(),
            degraded: Some(self.progress.blocked_reason.is_some()),
            reasons: self
                .progress
                .blocked_reason
                .into_iter()
                .map(str::to_owned)
                .collect(),
            checked_at: Some(fold_db::clock::unix_secs()),
            local_frontiers: None,
            backed_up_frontiers: None,
            covered: None,
            snapshot_required: self.progress.needs_restore.is_some(),
            last_upload_at: self.progress.last_upload_at,
            next_upload_at: Some(self.progress.next_sync_at),
            last_snapshot_at: self.last_snapshot_at,
            next_snapshot_at: self.publisher.then_some(self.next_snapshot_at),
        }
    }
}
