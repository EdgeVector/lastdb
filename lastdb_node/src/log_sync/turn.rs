//! One sync turn: append the local outbox, pull, apply, then ack.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mutation_log::{DatabaseId, DatabaseKind, DeviceId, LogError, Namespace, PrincipalId};
use mutation_log_sync::{Outcome, SyncError, SyncEvent, SyncRole, SyncState};
use serde::Deserialize;

use crate::host::Host;

use super::apply::{self, PagePlan};
use super::cloud::CloudLog;
use super::cutover::{self, Gate, Record};
use super::disk;
use super::outbox;
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
    record: Record,
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
    match cutover::gate(&host.home) {
        Gate::Legacy => {
            note_ready(&host.home);
            Prep::Pause(Pause::Idle)
        }
        Gate::Closed => {
            warn_once(
                &CLOSED_WARNED,
                "cutover record is present and log sync will not start",
            );
            Prep::Pause(Pause::Idle)
        }
        Gate::SignedOnly(record) => open_signed(host, record).await,
    }
}

async fn open_signed(host: &Host, record: Record) -> Prep {
    note_ready(&host.home);
    let key = hex::encode(host.keypair.public_key_bytes());
    if record.authority_node_key != key {
        warn_once(
            &AUTHORITY_WARNED,
            "cutover authority does not match this node key",
        );
        return Prep::Pause(Pause::Idle);
    }
    let revision = match member_revision(&host.home, &record) {
        Ok(revision) => revision,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "member revision is unavailable");
            return Prep::Pause(Pause::Retry);
        }
    };
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            return Prep::Pause(Pause::Retry);
        }
    };
    let log = match CloudLog::connect(host, &record.database_id, &device, revision).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            return Prep::Pause(Pause::Retry);
        }
    };
    let state = match opened_state(&record, &log, &device, &host.home) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync state was refused");
            return Prep::Pause(Pause::Retry);
        }
    };
    match resume_state(&log, state, record.database_kind == "personal").await {
        Ok(state) => Prep::Ready(Ready { record, log, state }),
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
        &ready.record.database_id,
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
    let shareable = ready.record.database_kind == "shareable";
    let plan = match apply::apply_page(
        host,
        &ready.log,
        &mut ready.state,
        &outcome,
        &ready.record.database_id,
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
            finish_ack(host, &ready.log, &ready.record.database_id, seq).await
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

async fn resume_state(
    log: &CloudLog,
    state: SyncState,
    personal: bool,
) -> Result<SyncState, SyncError> {
    match log.resumed(state.clone()).await {
        Err(SyncError::Log(LogError::Unauthorized)) if personal => {
            log.grant_owner().await?;
            log.resumed(state).await
        }
        other => other,
    }
}

fn opened_state(
    record: &Record,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Result<SyncState, String> {
    let database_id =
        DatabaseId::new(&record.database_id).map_err(|_| "database id was refused")?;
    let principal =
        PrincipalId::new(log.principal().as_str()).map_err(|_| "node key was refused")?;
    let device = DeviceId::new(device).map_err(|_| "device id was refused")?;
    let (kind, role) = if record.database_kind == "personal" {
        (
            DatabaseKind::Personal,
            SyncRole::Owner { principal, device },
        )
    } else {
        let publisher = PrincipalId::new(&record.authority_node_key)
            .map_err(|_| "publisher key was refused")?;
        (
            DatabaseKind::Shareable { publisher },
            SyncRole::Publisher { principal, device },
        )
    };
    let cursor = session::load_cursor(home, &record.database_id)?;
    SyncState::open(database_id, kind, role)
        .map(|state| state.with_cursor(cursor))
        .map_err(|error| error.to_string())
}

fn member_revision(home: &Path, record: &Record) -> Result<u64, String> {
    if record.database_kind == "personal" {
        return Ok(0);
    }
    let path = home
        .join("databases")
        .join(&record.database_id)
        .join("grants.json");
    match disk::read_limited(&path, GRANTS_LIMIT) {
        Ok(None) => Ok(record.grant_revision),
        Ok(Some(bytes)) => {
            let grants: GrantsFile =
                serde_json::from_slice(&bytes).map_err(|_| "grants file is invalid")?;
            if grants.revision == 0 {
                Ok(record.grant_revision)
            } else {
                Ok(grants.revision)
            }
        }
        Err(_) => Err("cannot read grants file".into()),
    }
}

fn note_ready(home: &Path) {
    if let Err(error) = cutover::publish_daemon_ready(home) {
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

#[derive(Deserialize)]
struct GrantsFile {
    revision: u64,
}
