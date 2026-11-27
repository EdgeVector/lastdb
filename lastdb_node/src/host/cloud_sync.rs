//! Thread-safe cloud sync status snapshot exposed via `GET /api/status`.
//!
//! Upload, download, snapshot, cleanup, and turn-wide errors are tracked independently so a
//! successful pull never overwrites or clears an unresolved upload failure or blocked queue.

mod types;

pub(crate) use types::{
    BlockedUploadStatus, CloudSyncStatus, CloudUsage, DatabaseSyncStatus, DownloadSyncStatus,
    FeatureStatus, ReclaimStatus, RelayStatus, SnapshotState, SnapshotStatus, UploadSyncStatus,
};

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

struct TrackerState {
    connected: bool,
    environment: Option<String>,
    captures_accepted: bool,
    payment_hold: bool,
    boot_error: Option<String>,
    turn_error: Option<String>,
    personal_database_id: Option<String>,
    databases: BTreeMap<String, DatabaseSyncStatus>,
    snapshot: SnapshotStatus,
    reclaim: ReclaimStatus,
    relay: RelayStatus,
}

impl TrackerState {
    fn has_payment_error(&self) -> bool {
        self.boot_error.as_deref().is_some_and(is_payment_refusal)
            || self.turn_error.as_deref().is_some_and(is_payment_refusal)
            || self.databases.values().any(|db| {
                db.upload
                    .last_error
                    .as_deref()
                    .is_some_and(is_payment_refusal)
                    || db
                        .download
                        .last_error
                        .as_deref()
                        .is_some_and(is_payment_refusal)
            })
    }

    fn refresh_payment_hold(&mut self) {
        self.payment_hold = self.has_payment_error();
    }

    /// Derive the top-level connection error without letting a clean pull mask a blocked upload.
    fn effective_last_error(&self) -> Option<String> {
        if let Some(err) = &self.boot_error {
            return Some(err.clone());
        }
        if let Some(personal_id) = &self.personal_database_id {
            if let Some(err) = self
                .databases
                .get(personal_id)
                .and_then(|db| db.upload.last_error.as_ref())
            {
                return Some(err.clone());
            }
        }
        if let Some(err) = self
            .databases
            .values()
            .find_map(|db| db.upload.last_error.as_ref())
        {
            return Some(err.clone());
        }
        if let Some(err) = &self.turn_error {
            return Some(err.clone());
        }
        if let Some(personal_id) = &self.personal_database_id {
            if let Some(err) = self
                .databases
                .get(personal_id)
                .and_then(|db| db.download.last_error.as_ref())
            {
                return Some(err.clone());
            }
        }
        self.databases
            .values()
            .filter(|db| db.owned)
            .find_map(|db| db.download.last_error.as_ref())
            .cloned()
    }
}

pub(crate) struct CloudSyncTracker {
    state: Mutex<TrackerState>,
    wake_notify: Arc<Notify>,
}

impl CloudSyncTracker {
    pub(crate) fn from_boot(home: &Path, captures_accepted: bool) -> Self {
        let (connected, environment, boot_error) = read_home_connection(home);
        let personal_database_id = crate::host::owned::registered_databases(home)
            .into_iter()
            .find(|db| db.kind == "personal")
            .map(|db| db.id);
        let payment_hold = boot_error.as_deref().is_some_and(is_payment_refusal);
        Self {
            state: Mutex::new(TrackerState {
                connected,
                environment,
                captures_accepted,
                payment_hold,
                boot_error,
                turn_error: None,
                personal_database_id,
                databases: BTreeMap::new(),
                snapshot: SnapshotStatus::default(),
                reclaim: ReclaimStatus::default(),
                relay: RelayStatus::default(),
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
        guard.turn_error = Some(message);
        guard.refresh_payment_hold();
    }

    /// Clear only the turn-wide error slot; per-database upload and download errors stay active
    /// until their own upload or pull succeeds.
    pub(crate) fn clear_turn_error(&self) {
        let mut guard = self.lock();
        guard.turn_error = None;
        guard.refresh_payment_hold();
    }

    /// Record one database's upload position, error, and oldest blocked entry (none after a clean
    /// upload drain). Never modifies `download`.
    pub(crate) fn record_upload(
        &self,
        database_id: &str,
        head: u64,
        confirmed_through: u64,
        error: Option<String>,
        oldest_blocked: Option<BlockedUploadStatus>,
        owned: bool,
    ) {
        let mut guard = self.lock();
        let entry = guard.databases.entry(database_id.to_owned()).or_default();
        entry.owned |= owned;
        entry.upload.spool_head = Some(head);
        entry.upload.spool_confirmed_through = Some(confirmed_through);
        entry.upload.oldest_blocked = if error.is_some() {
            oldest_blocked
        } else {
            None
        };
        entry.upload.last_error = error;
        guard.refresh_payment_hold();
    }

    /// Record an upload failure discovered before the sender ran (such as `list_streams` failing
    /// while local writes wait in the spool) without overwriting an already-recorded upload error.
    pub(crate) fn record_upload_if_unreported(
        &self,
        database_id: &str,
        head: u64,
        confirmed_through: u64,
        error: String,
        oldest_blocked: BlockedUploadStatus,
        owned: bool,
    ) {
        let mut guard = self.lock();
        let entry = guard.databases.entry(database_id.to_owned()).or_default();
        entry.owned |= owned;
        entry.upload.spool_head = Some(head);
        entry.upload.spool_confirmed_through = Some(confirmed_through);
        if entry.upload.last_error.is_none() {
            entry.upload.last_error = Some(error);
            entry.upload.oldest_blocked = Some(oldest_blocked);
        }
        guard.refresh_payment_hold();
    }

    /// Refresh queue positions while a refused member's uploads wait in their hold window,
    /// preserving the active upload refusal and oldest blocked entry.
    pub(crate) fn refresh_upload_queue(&self, database_id: &str, head: u64, confirmed: u64) {
        let mut guard = self.lock();
        let entry = guard.databases.entry(database_id.to_owned()).or_default();
        entry.upload.spool_head = Some(head);
        entry.upload.spool_confirmed_through = Some(confirmed);
        if head <= confirmed {
            entry.upload.last_error = None;
            entry.upload.oldest_blocked = None;
            guard.refresh_payment_hold();
        }
    }

    /// Record the result of reading remote streams for one database. Never modifies `upload`.
    pub(crate) fn record_download(
        &self,
        database_id: &str,
        applied_through: Option<u64>,
        error: Option<String>,
        owned: bool,
    ) {
        let mut guard = self.lock();
        let entry = guard.databases.entry(database_id.to_owned()).or_default();
        entry.owned |= owned;
        if let Some(through) = applied_through {
            entry.download.applied_through = Some(through);
        }
        entry.download.last_error = error;
        guard.refresh_payment_hold();
    }

    /// The snapshot this node just published or installed.
    pub(crate) fn record_snapshot(&self, snapshot: SnapshotStatus) {
        self.lock().snapshot = snapshot;
    }

    /// Mark the snapshot as `state` (in progress or blocked), keeping the facts of the newest one.
    pub(crate) fn mark_snapshot(&self, state: SnapshotState, message: Option<String>) {
        let mut guard = self.lock();
        guard.snapshot.state = state;
        guard.snapshot.message = message;
    }

    /// What the relay held after the sweep that just finished, or that the node kept on disk.
    pub(crate) fn record_reclaim(&self, reclaim: ReclaimStatus) {
        self.lock().reclaim = reclaim;
    }

    /// Say why the sweep that is due waits, or that it waits no longer, keeping the facts of the
    /// last sweep that finished.
    pub(crate) fn mark_reclaim(&self, message: Option<String>) {
        self.lock().reclaim.message = message;
    }

    /// The contract revision the relay named on its last answer of a sync turn.
    pub(crate) fn record_relay_revision(&self, revision: u32) {
        let mut guard = self.lock();
        if guard.relay.revision != Some(revision) {
            guard.relay = RelayStatus::of(revision);
        }
    }

    /// Attach per-database snapshot and cleanup status once the personal database entry exists.
    pub(crate) fn attach_personal_maintenance(&self, status: &mut CloudSyncStatus) {
        let guard = self.lock();
        if let Some(personal_id) = &guard.personal_database_id {
            if let Some(db) = status.databases.get_mut(personal_id) {
                db.snapshot = Some(guard.snapshot.clone());
                db.cleanup = Some(guard.reclaim.clone());
            }
        }
    }

    pub(crate) fn snapshot(&self) -> CloudSyncStatus {
        let guard = self.lock();
        let mut databases = guard.databases.clone();
        if let Some(personal_id) = &guard.personal_database_id {
            if let Some(db) = databases.get_mut(personal_id) {
                db.snapshot = Some(guard.snapshot.clone());
                db.cleanup = Some(guard.reclaim.clone());
            }
        }
        CloudSyncStatus {
            connected: guard.connected,
            environment: guard.environment.clone(),
            captures_accepted: guard.captures_accepted,
            payment_hold: guard.payment_hold,
            last_error: guard.effective_last_error(),
            databases,
            snapshot: guard.snapshot.clone(),
            reclaim: guard.reclaim.clone(),
            relay: guard.relay.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn is_payment_refusal(error: &str) -> bool {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE
        || error.contains(crate::cloud::PAYMENT_REQUIRED_MESSAGE)
        || error.contains("payment.subscription")
    {
        return true;
    }
    if !error.contains("HTTP 403") && !error.contains("HTTP 402") {
        return false;
    }
    let body = error
        .split_once(": ")
        .map_or(error.as_bytes(), |(_, text)| text.as_bytes());
    crate::cloud::is_payment_subscription_error(403, body)
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
