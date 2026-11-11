//! Append, pull, apply, and ack for one database that is already open.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use mutation_log::{DatabaseId, DatabaseKind, DeviceId, LogError, Namespace, PrincipalId};
use mutation_log_sync::{Outcome, SyncError, SyncEvent, SyncRole, SyncState};

use crate::host::Host;

use super::apply::{self, PagePlan};
use super::cloud::CloudLog;
use super::outbox;
use super::session;
use super::turn::{self, Pause};

static SNAPSHOT_WARNED: AtomicBool = AtomicBool::new(false);

pub(super) struct Ready {
    pub(super) database_id: String,
    pub(super) shareable: bool,
    pub(super) log: CloudLog,
    pub(super) state: SyncState,
}

pub(super) async fn run_ready(host: &Host, mut ready: Ready) -> Pause {
    if drain_outbox(&ready.log, &mut ready.state, &host.home, &ready.database_id)
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
            if matches!(
                error,
                SyncError::Log(LogError::CursorExpired { .. } | LogError::SnapshotRequired)
            ) {
                turn::warn_once(
                    &SNAPSHOT_WARNED,
                    "list cursor is below the snapshot floor; POST /api/cloud/backups/v2/mutation-log/delivery/snapshot is not installed by byte_cloud_restore::native::restore_at",
                );
            }
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pull failed");
            return Pause::Retry;
        }
    };
    ready.state = next;
    let plan = match apply::apply_page(
        host,
        &ready.log,
        &mut ready.state,
        &outcome,
        &ready.database_id,
        ready.shareable,
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
            finish_ack(host, &ready.log, &ready.database_id, seq).await
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

pub(super) fn opened_state(
    database_id: &str,
    database_kind: &str,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Result<SyncState, String> {
    let database_id = DatabaseId::new(database_id).map_err(|_| "database id was refused")?;
    let principal =
        PrincipalId::new(log.principal().as_str()).map_err(|_| "node key was refused")?;
    let device = DeviceId::new(device).map_err(|_| "device id was refused")?;
    let (kind, role) = if database_kind == "personal" {
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
    let cursor = session::load_cursor(home, database_id.as_str())?;
    SyncState::open(database_id, kind, role)
        .map(|state| state.with_cursor(cursor))
        .map_err(|error| error.to_string())
}
