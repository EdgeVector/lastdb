//! One sync turn: append the local outbox, pull, apply, then ack.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mutation_log::{DatabaseId, DatabaseKind, DeviceId, Namespace, PrincipalId};
use mutation_log_sync::{Outcome, SyncError, SyncEvent, SyncRole, SyncState};
use serde::Deserialize;

use crate::host::Host;

use super::apply::{self, PagePlan};
use super::cloud::CloudLog;
use super::cutover::{self, Commit, Gate, ServiceView};
use super::disk;
use super::http;
use super::outbox;
use super::registry::{self, Owner};
use super::session;

const GRANTS_LIMIT: usize = 65_536;

static CLOSED_WARNED: AtomicBool = AtomicBool::new(false);
static AUTHORITY_WARNED: AtomicBool = AtomicBool::new(false);

enum Pause {
    Idle,
    Retry,
    Snapshot,
}

struct Ready {
    commit: Commit,
    log: CloudLog,
    state: SyncState,
}

enum Prep {
    Pause(Pause),
    Ready(Ready),
}

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !shutdown.load(Ordering::Acquire) {
            let pause = one_turn(&host).await;
            sleep_interruptible(&shutdown, pause).await;
        }
    })
}

async fn one_turn(host: &Host) -> Pause {
    match prepare(host).await {
        Prep::Pause(pause) => pause,
        Prep::Ready(ready) => run_ready(host, ready).await,
    }
}

async fn prepare(host: &Host) -> Prep {
    let Some(owner) = registry::owner(&host.home) else {
        return Prep::Pause(Pause::Idle);
    };
    note_ready(&host.home);
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            cutover::observe(&owner.id, ServiceView::Unavailable);
            return paused(&host.home);
        }
    };
    let log = match CloudLog::connect(host, &owner.id, &device, probe_revision(&host.home, &owner))
        .await
    {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            cutover::observe(&owner.id, ServiceView::Unavailable);
            return paused(&host.home);
        }
    };
    let view = log.service_view(&owner).await;
    cutover::observe(&owner.id, view);
    match cutover::gate(&host.home) {
        Gate::Legacy => Prep::Pause(Pause::Idle),
        Gate::Closed(_) => {
            warn_once(
                &CLOSED_WARNED,
                "signed-only confirmation is unavailable and legacy resume stays closed",
            );
            Prep::Pause(Pause::Retry)
        }
        Gate::SignedOnly(commit) => open_signed(host, commit, log, &device).await,
    }
}

fn paused(home: &Path) -> Prep {
    match cutover::gate(home) {
        Gate::Closed(_) => Prep::Pause(Pause::Retry),
        _ => Prep::Pause(Pause::Idle),
    }
}

async fn open_signed(host: &Host, commit: Commit, mut log: CloudLog, device: &str) -> Prep {
    if commit.database_kind != "personal" && !publisher_is_this_node(host, &commit.database_id) {
        warn_once(
            &AUTHORITY_WARNED,
            "shareable cutover publisher is not this node and sync stays closed",
        );
        return Prep::Pause(Pause::Idle);
    }
    let wanted = if commit.database_kind == "personal" {
        0
    } else {
        commit.grant_revision
    };
    if log.member_revision() != wanted {
        log = match CloudLog::connect(host, &commit.database_id, device, wanted).await {
            Ok(log) => log,
            Err(error) => {
                tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
                cutover::observe(&commit.database_id, ServiceView::Unavailable);
                return Prep::Pause(Pause::Retry);
            }
        };
    }
    let state = match opened_state(&commit, &log, device, &host.home) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync state was refused");
            return Prep::Pause(Pause::Retry);
        }
    };
    match resume_state(&log, state).await {
        Ok(state) => Prep::Ready(Ready { commit, log, state }),
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync resume failed");
            Prep::Pause(Pause::Retry)
        }
    }
}

async fn run_ready(host: &Host, mut ready: Ready) -> Pause {
    if drain_outbox(
        &ready.log,
        &mut ready.state,
        &host.home,
        &ready.commit.database_id,
    )
    .await
    .is_err()
    {
        return Pause::Retry;
    }
    pull_and_ack(host, &mut ready).await
}

async fn drain_outbox(
    log: &CloudLog,
    state: &mut SyncState,
    home: &Path,
    database_id: &str,
) -> Result<(), ()> {
    let notes = outbox::pending(home, database_id).map_err(|_| {
        tracing::warn!(target: "lastdbd::log_sync", "cannot read the log outbox");
    })?;
    for (path, note) in notes {
        let namespace = Namespace::new(&note.namespace).map_err(|_| {
            tracing::warn!(target: "lastdbd::log_sync", "queued mutation namespace was refused");
        })?;
        let event = SyncEvent::Append {
            namespace,
            payload: note.payload,
            authored_at_ms: note.authored_at_ms,
            accepted_at_ms: note.accepted_at_ms,
        };
        match log.drive_once(state.clone(), event).await {
            Ok((next, Outcome::Appended { .. } | Outcome::Duplicate { .. })) => {
                *state = next;
                if let Err(error) = outbox::remove(&path) {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        error = %error,
                        "queued mutation was appended and the local note remains"
                    );
                }
            }
            Ok(_) => {
                tracing::warn!(target: "lastdbd::log_sync", "append returned an unexpected outcome");
                return Err(());
            }
            Err(error) => {
                tracing::warn!(target: "lastdbd::log_sync", error = %error, "append failed");
                return Err(());
            }
        }
    }
    Ok(())
}

async fn pull_and_ack(host: &Host, ready: &mut Ready) -> Pause {
    let event = SyncEvent::Pull {
        now_ms: fold_db::clock::unix_millis(),
    };
    let (next, outcome) = match ready.log.drive_once(ready.state.clone(), event).await {
        Ok(pulled) => pulled,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pull failed");
            return Pause::Retry;
        }
    };
    ready.state = next;
    let shareable = ready.commit.database_kind == "shareable";
    let plan = match apply::apply_page(
        host,
        &ready.log,
        &mut ready.state,
        &outcome,
        &ready.commit.database_id,
        shareable,
    )
    .await
    {
        Ok(plan) => plan,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pulled page was not applied");
            return Pause::Retry;
        }
    };
    match plan {
        PagePlan::SnapshotHeld => Pause::Snapshot,
        PagePlan::Ack(seq) | PagePlan::Hold(Some(seq)) => {
            finish_ack(host, &ready.log, &ready.commit.database_id, seq).await
        }
        PagePlan::Hold(None) => Pause::Retry,
    }
}

async fn finish_ack(host: &Host, log: &CloudLog, database_id: &str, seq: u64) -> Pause {
    if let Err(error) = log.ack_through(log.reader(), seq).await {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "ack failed");
        return Pause::Retry;
    }
    if let Err(error) = session::store_cursor(&host.home, database_id, seq) {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "delivery cursor was not stored");
        return Pause::Retry;
    }
    Pause::Idle
}

async fn resume_state(log: &CloudLog, state: SyncState) -> Result<SyncState, SyncError> {
    log.resumed(state).await
}

fn opened_state(
    commit: &Commit,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Result<SyncState, String> {
    let database_id =
        DatabaseId::new(&commit.database_id).map_err(|_| "database id was refused")?;
    let principal =
        PrincipalId::new(log.principal().as_str()).map_err(|_| "node key was refused")?;
    let device = DeviceId::new(device).map_err(|_| "device id was refused")?;
    let (kind, role) = if commit.database_kind == "personal" {
        (
            DatabaseKind::Personal,
            SyncRole::Owner { principal, device },
        )
    } else {
        (
            DatabaseKind::Shareable {
                publisher: principal.clone(),
            },
            SyncRole::Publisher { principal, device },
        )
    };
    let cursor = session::load_cursor(home, &commit.database_id)?;
    SyncState::open(database_id, kind, role)
        .map(|state| state.with_cursor(cursor))
        .map_err(|error| error.to_string())
}

/// Revision for the confirmation read. A missing or unreadable grants file does
/// not stop legacy resume. After a verified commit, that revision is the fallback.
fn probe_revision(home: &Path, owner: &Owner) -> u64 {
    if owner.kind == "personal" {
        return 0;
    }
    let path = home.join("databases").join(&owner.id).join("grants.json");
    if let Ok(Some(bytes)) = disk::read_limited(&path, GRANTS_LIMIT) {
        if let Ok(grants) = serde_json::from_slice::<GrantsFile>(&bytes) {
            if grants.revision != 0 {
                return grants.revision;
            }
        }
    }
    cutover::remembered(home)
        .filter(|commit| commit.database_id == owner.id)
        .map(|commit| commit.grant_revision)
        .unwrap_or(0)
}

fn note_ready(home: &Path) {
    let Ok(environment) = http::environment_label(home) else {
        return;
    };
    if let Err(error) = cutover::publish_daemon_ready(home, environment) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            error = %error,
            "daemon-ready evidence was not written"
        );
    }
}

fn warn_once(flag: &AtomicBool, message: &str) {
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
