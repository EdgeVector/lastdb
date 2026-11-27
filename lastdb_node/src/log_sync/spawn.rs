//! Daemon sync loop: idle without relay URL; otherwise upload the log and pull.

mod pull_turns;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::Instrument;

use crate::host::owned::RegisteredDatabase;
use crate::host::Host;

use super::history_store::HistoryMap;
use super::relay::RelayClient;
use super::upload::{shareable_uploads_pending, upload_log, upload_received_logs};
use super::wake;
use pull_turns::{load_received_histories, pull_owned_shareables, pull_received_shares};

const IDLE: Duration = Duration::from_secs(5);
const ACTIVE: Duration = Duration::from_secs(2);

struct TurnReport {
    turn_error: Option<String>,
    personal_ok: bool,
    received_upload_ok: bool,
}

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    // The status names the snapshot this node kept, before the first turn runs.
    if let Some(snapshot) = super::known_snapshot(&host.home) {
        host.cloud_sync.record_snapshot(snapshot);
    }
    if let Some(reclaim) = super::last_reclaim(&host.home) {
        host.cloud_sync.record_reclaim(reclaim);
    }
    tokio::spawn(
        async move {
            let histories = super::histories_for(&host);
            load_received_histories(&host, &histories);
            while !shutdown.load(Ordering::Acquire) {
                match crate::cloud::relay_base_url(&host.home) {
                    None => {
                        let _ = wake::consume_wake(&host.home);
                        tracing::trace!(
                            target: "lastdbd::log_sync",
                            "no relay URL configured; signed-history sync idle"
                        );
                        tokio::time::sleep(IDLE).await;
                    }
                    Some(base) => run_turn_and_pause(&host, &histories, &base).await,
                }
            }
            // A snapshot that is still uploading stops here, and the next start takes it again.
            super::personal::stop_snapshot_lane().await;
        }
        .in_current_span(),
    )
}

async fn run_turn_and_pause(host: &Arc<Host>, histories: &HistoryMap, base: &str) {
    let personal_was_pending = super::personal::pending(host);
    let pre_acked = host.db.db_ops().native_runtime().accepted_status().acked;
    let _ = wake::consume_wake(&host.home);
    let report = one_turn(host, histories, base).await;
    if let Some(err) = report.turn_error {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %err,
            "sync turn failed"
        );
        host.cloud_sync.record_error(err);
        // Keep the full ACTIVE backoff on errors so 4xx/429 cannot spin when
        // authors keep writing wake markers.
        tokio::time::sleep(ACTIVE).await;
        return;
    }
    // Clear only the turn-wide error slot; per-database upload and download
    // errors remain active until their own upload or pull succeeds.
    host.cloud_sync.clear_turn_error();
    let shareable_ready = shareable_uploads_pending(host, report.received_upload_ok);
    let post_accepted = host.db.db_ops().native_runtime().accepted_status();
    let personal_ready = report.personal_ok
        && super::personal::pending(host)
        && (!personal_was_pending
            || post_accepted.acked > pre_acked
            // A write can reach the spool after this turn observed its empty page. Its ack has
            // not advanced, but the next turn must drain it before a requested fresh snapshot.
            || post_accepted.head > post_accepted.acked);
    if shareable_ready || personal_ready {
        wake::wake_loop();
    } else if !report.personal_ok || !report.received_upload_ok {
        let _ = wake::consume_wake(&host.home);
    }
    wake::pause_unless_wake(&host.home, ACTIVE).await;
}

async fn one_turn(host: &Arc<Host>, histories: &HistoryMap, base: &str) -> TurnReport {
    let client = match RelayClient::for_host(host, base) {
        Ok(client) => client,
        Err(err) => {
            return TurnReport {
                turn_error: Some(err),
                personal_ok: false,
                received_upload_ok: false,
            };
        }
    };

    let owned: Vec<_> = crate::host::owned::registered_databases(&host.home)
        .into_iter()
        .filter(|database| syncable_owned(database.kind, &database.id))
        .collect();
    // Upload failures on owned shareables fail the turn (ACTIVE backoff). Soft
    // personal and received shares keep shareable grant tips on the clean wake
    // path (#1844, #1846) without letting failed queues spin without sleep.
    let mut turn_error = publish_declared_schemas(host, &owned);
    let _ = upload_owned_logs(host, &client, &owned).await;
    turn_error = pull_owned_shareables(host, histories, &client, &owned)
        .await
        .or(turn_error);
    // Write leases come after the pull, so a closing records what the member
    // streams hold now. A database with no lease length has none.
    turn_error = super::lease_publish::renew_owned(host, histories, &owned).or(turn_error);
    let personal_ok = if let Some(err) = super::personal::turn(host, &client).await {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %err,
            "personal database turn failed; shareable uploads keep the clean path"
        );
        false
    } else {
        true
    };
    // Phase d: decisions and leases the publisher signed during the pull go up
    // this turn, so members settle without waiting another loop. Any owned log
    // upload still failing in Phase b fails here too and triggers ACTIVE backoff.
    turn_error = upload_owned_logs(host, &client, &owned)
        .await
        .or(turn_error);
    // Phase e: proposals on received databases go up before their pull, so
    // publish.lastAttempt reflects a real attempt.
    let received_upload_ok = upload_received_logs(host, &client).await;
    // Phase f: per-share errors stay soft (see pull_received_shares).
    pull_received_shares(host, histories, &client).await;
    if let Some(revision) = client.announced_revision() {
        host.cloud_sync.record_relay_revision(revision);
    }
    TurnReport {
        turn_error,
        personal_ok,
        received_upload_ok,
    }
}

/// Phase a: sign every schema an owned shareable database declares. It runs
/// first so a receiver can read a schema that has no record yet.
fn publish_declared_schemas(host: &Host, owned: &[RegisteredDatabase]) -> Option<String> {
    let mut turn_error = None;
    for database in owned.iter().filter(|database| database.kind == "shareable") {
        if let Err(err) = super::mirror::publish_declared_schemas(host, &database.id) {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                %err,
                "declared schemas could not be published"
            );
            turn_error = Some(err);
        }
    }
    turn_error
}

/// Phases b and d: upload every owned shareable log. This runs before any pull
/// because a shareable whose birth is not on the relay yet gets log/read 403
/// field=grant; that must not keep the birth and grant entries from uploading.
/// The personal database has no log file: `personal` sends its spool. Each database drains every pending batch, so a
/// long run of mirrored data cannot leave grants behind it.
async fn upload_owned_logs(
    host: &Host,
    client: &RelayClient,
    owned: &[RegisteredDatabase],
) -> Option<String> {
    let mut turn_error = None;
    for database in owned.iter().filter(|database| database.kind != "personal") {
        if let Some(err) = upload_log(client, host, &database.id, false).await {
            turn_error = Some(err);
        }
    }
    turn_error
}

fn syncable_owned(kind: &str, id: &str) -> bool {
    if kind != "personal" && kind != "shareable" {
        return false;
    }
    if mutation_log::DatabaseId::from_hex(id).is_err() {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %id,
            "skip sync: database_id is not a 64-hex database id"
        );
        return false;
    }
    true
}
