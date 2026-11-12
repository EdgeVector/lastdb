//! One sync turn: pull, apply, then ack. Local capture is the native commit.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mutation_log_sync::{SyncError, SyncState};
use serde::Deserialize;
use tracing::Instrument;

use crate::host::Host;

use super::cloud::CloudLog;
use super::cutover::{self, Commit, Gate, ServiceView};
use super::disk;
use super::http;
use super::members;
use super::native;
use super::received;
use super::registry::{self, Database};
use super::run;
use super::session;

const GRANTS_LIMIT: usize = 65_536;

static CLOSED_WARNED: AtomicBool = AtomicBool::new(false);
static AUTHORITY_WARNED: AtomicBool = AtomicBool::new(false);

pub(super) enum Pause {
    Idle,
    Retry,
    Snapshot,
}

enum Prep {
    Pause(Pause),
    Ready(Box<run::Ready>),
}

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            while !shutdown.load(Ordering::Acquire) {
                let pause = one_turn(&host).await;
                sleep_interruptible(&shutdown, pause).await;
            }
        }
        .in_current_span(),
    )
}

async fn one_turn(host: &Arc<Host>) -> Pause {
    let databases = registry::databases(&host.home);
    let mut pause = if databases.is_empty() {
        Pause::Idle
    } else {
        Pause::Snapshot
    };
    for database in &databases {
        let next = if fresh_native(database) {
            native::turn(host, database).await
        } else {
            receipt_turn(host, database).await
        };
        pause = shorter(pause, next);
    }
    shorter(pause, received::turn(host).await)
}

fn fresh_native(database: &Database) -> bool {
    database.native && database.kind == "shareable" && database.spec.is_some()
}

async fn receipt_turn(host: &Arc<Host>, database: &Database) -> Pause {
    match prepare(host, database).await {
        Prep::Pause(pause) => pause,
        Prep::Ready(ready) => run::run_ready(host.as_ref(), *ready).await,
    }
}

async fn prepare(host: &Arc<Host>, database: &Database) -> Prep {
    if cutover::local_baseline_ready(host, &database.id) {
        note_ready(&host.home, database);
    }
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            cutover::observe(&database.id, ServiceView::Unavailable);
            return paused_prep(&database.id);
        }
    };
    let wanted = probe_revision(&host.home, &database.id, &database.kind);
    let log = match CloudLog::connect(host, &database.id, &device, wanted).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            cutover::observe(&database.id, ServiceView::Unavailable);
            return paused_prep(&database.id);
        }
    };
    let view = log.service_view(database).await;
    cutover::observe(&database.id, view);
    match cutover::gate_for(&database.id) {
        Gate::Legacy => Prep::Pause(Pause::Idle),
        Gate::Closed(_) => {
            warn_once(
                &CLOSED_WARNED,
                "service confirmation is unavailable and legacy resume stays closed",
            );
            Prep::Pause(Pause::Retry)
        }
        Gate::Native => Prep::Pause(open_native(host, database, log, &device).await),
        Gate::SignedOnly(commit) => open_signed(host, commit, log, &device).await,
    }
}

fn paused_prep(database_id: &str) -> Prep {
    Prep::Pause(paused_for(database_id))
}

pub(super) fn paused_for(database_id: &str) -> Pause {
    match cutover::gate_for(database_id) {
        Gate::Closed(_) => Pause::Retry,
        Gate::Legacy | Gate::SignedOnly(_) | Gate::Native => Pause::Idle,
    }
}

async fn open_signed(host: &Arc<Host>, commit: Commit, log: CloudLog, device: &str) -> Prep {
    if commit.database_kind != "personal" && !publisher_is_this_node(host, &commit.database_id) {
        warn_once(
            &AUTHORITY_WARNED,
            "shareable cutover publisher is not this node and sync stays closed",
        );
        return Prep::Pause(Pause::Idle);
    }
    if !cutover::local_baseline_ready(host, &commit.database_id) {
        return Prep::Pause(Pause::Snapshot);
    }
    finish_open(
        host,
        &commit.database_id,
        &commit.database_kind,
        log,
        device,
    )
    .await
}

pub(super) async fn open_native(
    host: &Arc<Host>,
    database: &Database,
    log: CloudLog,
    device: &str,
) -> Pause {
    if !native_publisher(host, &database.id) {
        warn_once(
            &AUTHORITY_WARNED,
            "named database authority is not this node and sync stays closed",
        );
        return Pause::Idle;
    }
    match finish_open(host, &database.id, &database.kind, log, device).await {
        Prep::Pause(pause) => {
            if matches!(pause, Pause::Retry) {
                cutover::observe_native(&database.id, cutover::NativeView::Unavailable);
            }
            paused_for(&database.id)
        }
        Prep::Ready(ready) => run::run_ready(host.as_ref(), *ready).await,
    }
}

async fn finish_open(
    host: &Arc<Host>,
    database_id: &str,
    kind: &str,
    mut log: CloudLog,
    device: &str,
) -> Prep {
    if kind != "personal" {
        let Some(revision) = members::revision(&log).await else {
            return Prep::Pause(Pause::Retry);
        };
        log.member_revision = revision;
    }
    let state = match run::opened_state(database_id, kind, &log, device, &host.home) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync state was refused");
            return Prep::Pause(Pause::Retry);
        }
    };
    match resume_state(&log, state).await {
        Ok(state) => Prep::Ready(Box::new(run::Ready {
            database_id: database_id.to_owned(),
            publisher: None,
            log,
            state,
        })),
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync resume failed");
            Prep::Pause(Pause::Retry)
        }
    }
}

async fn resume_state(log: &CloudLog, state: SyncState) -> Result<SyncState, SyncError> {
    log.resumed(state).await
}

/// Revision for the confirmation read. A missing or unreadable grants file does
/// not stop legacy resume. After a verified commit, that revision is the fallback.
pub(super) fn probe_revision(home: &Path, database_id: &str, kind: &str) -> u64 {
    if kind == "personal" {
        return 0;
    }
    let path = home.join("databases").join(database_id).join("grants.json");
    if let Ok(Some(bytes)) = disk::read_limited(&path, GRANTS_LIMIT) {
        if let Ok(grants) = serde_json::from_slice::<GrantsFile>(&bytes) {
            if grants.revision != 0 {
                return grants.revision;
            }
        }
    }
    cutover::receipt_revision(database_id).unwrap_or(0)
}

fn note_ready(home: &Path, database: &Database) {
    let Ok(environment) = http::environment_label(home) else {
        return;
    };
    if let Err(error) = cutover::publish_daemon_ready(home, environment, database) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            error = %error,
            "daemon-ready evidence was not written"
        );
    }
}

pub(super) fn shorter(current: Pause, next: Pause) -> Pause {
    if pause_rank(&next) < pause_rank(&current) {
        next
    } else {
        current
    }
}

fn pause_rank(pause: &Pause) -> u8 {
    match pause {
        Pause::Idle => 0,
        Pause::Retry => 1,
        Pause::Snapshot => 2,
    }
}

pub(super) fn warn_once(flag: &AtomicBool, message: &str) {
    if !flag.swap(true, Ordering::Relaxed) {
        tracing::warn!(target: "lastdbd::log_sync", "{message}");
    }
}

async fn sleep_interruptible(shutdown: &AtomicBool, pause: Pause) {
    let seconds = match pause {
        Pause::Idle => 1,
        Pause::Retry => 5,
        Pause::Snapshot => 30,
    };
    for _ in 0..seconds {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(super) fn native_publisher(host: &Host, database_id: &str) -> bool {
    match grants_authority(&host.home, database_id) {
        Some(publisher) => publisher == hex::encode(host.keypair.public_key_bytes()),
        None => true,
    }
}

fn publisher_is_this_node(host: &Host, database_id: &str) -> bool {
    let key = hex::encode(host.keypair.public_key_bytes());
    grants_authority(&host.home, database_id).is_some_and(|publisher| publisher == key)
}

fn grants_authority(home: &Path, database_id: &str) -> Option<String> {
    let path = home.join("databases").join(database_id).join("grants.json");
    let bytes = disk::read_limited(&path, GRANTS_LIMIT).ok().flatten()?;
    let grants = serde_json::from_slice::<GrantsFile>(&bytes).ok()?;
    let key = grants.authority_node_key?;
    (key.len() == 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    .then_some(key)
}

#[derive(Deserialize)]
struct GrantsFile {
    #[serde(default)]
    authority_node_key: Option<String>,
    revision: u64,
}
