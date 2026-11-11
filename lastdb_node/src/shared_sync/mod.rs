//! Automatic two-way synchronization for a restored, enrolled shared home.
//!
//! The legacy worker is replaced in the client cutover PR. There is no flag.
//! The worker does not construct a mutation log.

mod config;
mod snapshot;
mod worker;
pub use crate::backup_runtime::{SharedSyncProgress as Progress, SharedSyncRuntime as Runtime};
pub use config::save_bound;

use crate::byte_cloud_backup::{BackupError, Result};
use cloud_backup_protocol::{shared::WriterStream, Id};
use serde::Serialize;
use tracing::Instrument;

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
        progress: host.shared_sync.snapshot(),
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

/// Resolve any saved publication before replacing the native data directories.
pub fn require_rejoin_ready(home: &std::path::Path) -> Result<()> {
    if config::load(home)?.is_some_and(|config| config.pending_snapshot.is_some()) {
        return Err(BackupError::Busy);
    }
    Ok(())
}
