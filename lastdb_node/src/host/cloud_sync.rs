//! Thread-safe cloud sync status snapshot exposed via `GET /api/status`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{futures::Notified, Notify};

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

/// Position in the database's cloud delivery sequence.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DownloadSyncStatus {
    pub(crate) applied_through: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) blocked: Option<BlockedDelivery>,
}

/// A specific delivery refusing replay while the local cursor stays behind it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct BlockedDelivery {
    pub(crate) delivery_sequence: u64,
    pub(crate) reason: ReplayBlockReason,
}

/// The local rule preventing a delivered mutation from being acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayBlockReason {
    AttachmentConflict,
    SchemaConflict,
    MissingEarlierRecord,
}

impl ReplayBlockReason {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::AttachmentConflict => "concurrent attachment changes require reconciliation",
            Self::SchemaConflict => "a local schema change or write awaits ordered replay",
            Self::MissingEarlierRecord => "replay lacks the earlier accepted record",
        }
    }
}

/// One database's sync positions: upload head, upload confirmed, download applied.
pub(crate) type ProgressMark = (String, Option<u64>, Option<u64>, Option<u64>);

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

    pub(crate) fn notified(&self) -> Notified<'_> {
        self.wake_notify.notified()
    }

    /// The sync positions of every tracked database. Two equal values from
    /// consecutive turns mean the later turn uploaded and applied nothing.
    pub(crate) fn progress_marks(&self) -> Vec<ProgressMark> {
        self.lock()
            .databases
            .iter()
            .map(|(id, status)| {
                (
                    id.clone(),
                    status.upload.spool_head,
                    status.upload.spool_confirmed_through,
                    status.download.applied_through,
                )
            })
            .collect()
    }

    pub(crate) fn refresh(&self, home: &Path, captures_accepted: bool) {
        let (connected, environment, cred_err) = read_home_connection(home);
        let mut guard = self.lock();
        if guard.environment != environment {
            guard.databases.clear();
        }
        guard.connected = connected;
        guard.environment = environment;
        guard.captures_accepted = captures_accepted;
        if let Some(err) = cred_err {
            guard.last_error = Some(err);
        } else if !connected {
            guard.payment_hold = false;
            guard.last_error = None;
        }
    }

    pub(crate) fn record_payment_hold(&self) {
        let mut guard = self.lock();
        guard.payment_hold = true;
        guard.last_error = Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE.to_owned());
    }

    pub(crate) fn record_error(&self, error: impl Into<String>) {
        let message = error.into();
        let mut guard = self.lock();
        if message == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
            guard.payment_hold = true;
        }
        guard.last_error = Some(message);
    }

    pub(crate) fn record_healthy(&self) {
        let mut guard = self.lock();
        guard.payment_hold = false;
        guard.last_error = None;
    }

    pub(crate) fn retain_databases(&self, keep: impl Fn(&str) -> bool) {
        self.lock().databases.retain(|id, _| keep(id));
    }

    pub(crate) fn record_upload(
        &self,
        database_id: &str,
        head: u64,
        confirmed: u64,
        error: Option<&str>,
    ) {
        self.lock()
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .upload = UploadSyncStatus {
            spool_head: Some(head),
            spool_confirmed_through: Some(confirmed),
            last_error: error.map(str::to_owned),
        };
    }

    pub(crate) fn record_upload_error(&self, database_id: &str, error: &str) {
        self.lock()
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .upload
            .last_error = Some(error.to_owned());
    }

    pub(crate) fn record_download(
        &self,
        database_id: &str,
        through: u64,
        error: Option<String>,
        blocked: Option<BlockedDelivery>,
    ) {
        self.lock()
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .download = DownloadSyncStatus {
            applied_through: Some(through),
            last_error: error,
            blocked,
        };
    }

    pub(crate) fn clear_payment_hold(&self) {
        let mut guard = self.lock();
        if guard.payment_hold {
            guard.payment_hold = false;
            if guard.last_error.as_deref() == Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE) {
                guard.last_error = None;
            }
        }
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
