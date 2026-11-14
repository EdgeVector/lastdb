//! Pull, apply, and save local download progress for an open database.
//!
//! An owned database publishes one accepted page first. A download
//! position advances only after the downloaded changes are durable.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use mutation_log::{DatabaseId, DatabaseKind, DeviceId, ListPage, LogError, PrincipalId};
use mutation_log_sync::{Outcome, SyncError, SyncEvent, SyncRole, SyncState};

use crate::host::Host;

use super::accepted;
use super::apply::{self, PagePlan};
use super::cloud::CloudLog;
use super::session;
use super::turn::{self, Pause};

static SNAPSHOT_WARNED: AtomicBool = AtomicBool::new(false);

pub(super) struct Ready {
    pub(super) database_id: String,
    /// Publisher of a received log. An owned database leaves this empty.
    pub(super) publisher: Option<String>,
    pub(super) log: CloudLog,
    pub(super) state: SyncState,
}

pub(super) async fn run_ready(host: &Host, mut ready: Ready) -> Pause {
    if ready.publisher.is_none() {
        if let Err(reason) = accepted::publish_owned(host, &ready.log).await {
            tracing::warn!(target: "lastdbd::log_sync", database_id = %ready.database_id,
                %reason, "owned pull awaits safe local capture");
            return Pause::Retry;
        }
    }
    pull_and_checkpoint(host, &mut ready).await
}

pub(super) async fn pull_and_checkpoint(host: &Host, ready: &mut Ready) -> Pause {
    if super::native_reset::supports(host, ready) {
        let after = ready.state.cursor();
        match ready.log.list_with_native_reset(after).await {
            Ok(Ok(page)) => {
                return apply_contiguous(host, ready, after, page).await;
            }
            Ok(Err(cut)) => {
                // The service keeps older entries during migration. Avoid a
                // database replacement while the missing edits are still there.
                match ready.log.list_retained(after).await {
                    Ok(page) if !page.entries.is_empty() && contiguous(after, &page) => {
                        return apply_contiguous(host, ready, after, page).await;
                    }
                    Ok(_) | Err(LogError::CursorExpired { .. } | LogError::SnapshotRequired) => {}
                    Err(error) => {
                        tracing::warn!(target: "lastdbd::log_sync", %error, "retained log read failed");
                        return Pause::Retry;
                    }
                }
                return match super::native_reset::prepare(host, &ready.log, after, &cut).await {
                    Ok(()) => Pause::Reopen,
                    Err(error) => {
                        tracing::warn!(target: "lastdbd::log_sync", %error, "native recovery awaits retry");
                        Pause::Retry
                    }
                };
            }
            Err(LogError::CursorExpired { .. }) => {
                // Existing signed archives are handled by the original pull.
            }
            Err(error) => {
                tracing::warn!(target: "lastdbd::log_sync", %error, "pull failed");
                return Pause::Retry;
            }
        }
    }
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
                    "snapshot restore stopped before this database could advance",
                );
            }
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pull failed");
            return Pause::Retry;
        }
    };
    ready.state = next;
    apply_and_checkpoint(host, ready, &outcome).await
}

fn contiguous(after: u64, page: &ListPage) -> bool {
    page.entries.iter().try_fold(after, |last, entry| {
        (last.checked_add(1) == Some(entry.delivery_seq)).then_some(entry.delivery_seq)
    }) == Some(page.next)
}

async fn apply_contiguous(host: &Host, ready: &Ready, after: u64, page: ListPage) -> Pause {
    if !contiguous(after, &page) {
        tracing::warn!(target: "lastdbd::log_sync", "personal delivery page has a gap");
        return Pause::Retry;
    }
    let outcome = Outcome::Pulled {
        cursor: page.next,
        entries: page.entries,
        reset: None,
    };
    apply_and_checkpoint(host, ready, &outcome).await
}

async fn apply_and_checkpoint(host: &Host, ready: &Ready, outcome: &Outcome) -> Pause {
    let plan = match apply::apply_page(host, &ready.log, outcome, ready.publisher.as_deref()).await
    {
        Ok(plan) => plan,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pulled page was not applied");
            return Pause::Retry;
        }
    };
    match plan {
        PagePlan::Ack(seq) | PagePlan::Hold(Some(seq)) => {
            checkpoint_download(host, &ready.database_id, seq)
        }
        PagePlan::Hold(None) => Pause::Retry,
    }
}

fn checkpoint_download(host: &Host, database_id: &str, seq: u64) -> Pause {
    if let Err(error) = session::store_cursor(&host.home, database_id, seq) {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "delivery cursor was not stored");
        return Pause::Retry;
    }
    if let Err(error) = session::mark_prefix(&host.home, database_id, seq) {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "durable prefix was not stored");
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
