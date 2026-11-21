//! Thread-safe cloud sync status snapshot exposed via `GET /api/status`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CloudSyncStatus {
    pub(crate) connected: bool,
    pub(crate) environment: Option<String>,
    pub(crate) captures_accepted: bool,
    pub(crate) payment_hold: bool,
    pub(crate) last_error: Option<String>,
    /// Last observed synchronization progress by owned personal database ID.
    pub(crate) databases: BTreeMap<String, DatabaseSyncStatus>,
}

/// Separate local upload and download progress for one owned database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DatabaseSyncStatus {
    pub(crate) upload: UploadSyncStatus,
    pub(crate) download: DownloadSyncStatus,
}

/// Positions in this device's durable upload queue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct UploadSyncStatus {
    pub(crate) spool_head: Option<u64>,
    pub(crate) spool_confirmed_through: Option<u64>,
    pub(crate) last_error: Option<String>,
}

/// Local applied tip for signed-history pull (stream writer_seq).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DownloadSyncStatus {
    pub(crate) applied_through: Option<u64>,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct CloudSyncTracker {
    state: Mutex<CloudSyncStatus>,
    wake_notify: Arc<Notify>,
}

impl CloudSyncTracker {
    pub(crate) fn from_boot(home: &Path, captures_accepted: bool) -> Self {
        let (connected, environment, last_error) = read_home_connection(home);
        Self {
            state: Mutex::new(CloudSyncStatus {
                connected,
                environment,
                captures_accepted,
                payment_hold: false,
                last_error,
                databases: BTreeMap::new(),
            }),
            wake_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn wake(&self) {
        self.wake_notify.notify_one();
    }

    pub(crate) fn record_error(&self, error: impl Into<String>) {
        let message = error.into();
        let mut guard = self.lock();
        if message == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
            guard.payment_hold = true;
        }
        guard.last_error = Some(message);
    }

    /// Drop the sticky top-level last_error after a successful sync turn.
    pub(crate) fn clear_error(&self) {
        self.lock().last_error = None;
    }

    pub(crate) fn snapshot(&self) -> CloudSyncStatus {
        let mut status = self.lock().clone();
        if status.last_error.is_none() {
            status.last_error = status.databases.values().find_map(|database| {
                database
                    .upload
                    .last_error
                    .as_ref()
                    .or(database.download.last_error.as_ref())
                    .cloned()
            });
        }
        status
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CloudSyncStatus> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn read_home_connection(home: &Path) -> (bool, Option<String>, Option<String>) {
    match crate::cloud::load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let env = saved
                .connection
                .as_ref()
                .map(|c| c.environment.clone())
                .or_else(|| saved.environment().map(|e| e.as_str().to_owned()));
            (true, env, None)
        }
        Ok(None) => (false, None, None),
        Err(err) => (false, None, Some(err)),
    }
}
