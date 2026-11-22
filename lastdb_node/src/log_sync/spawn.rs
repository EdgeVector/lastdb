//! Daemon sync loop: idle without relay URL; otherwise upload the log and pull.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cloud_backup_protocol::Id;
use mutation_log::NodeKey;
use tracing::Instrument;

use crate::host::owned::RegisteredDatabase;
use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

use super::config;
use super::device;
use super::history_store::HistoryMap;
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;
use super::upload::{upload_log, upload_received_logs, uploads_pending};
use super::wake;

const IDLE: Duration = Duration::from_secs(5);
const ACTIVE: Duration = Duration::from_secs(2);

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let histories = super::histories_for(&host);
            while !shutdown.load(Ordering::Acquire) {
                match config::relay_base_url(&host.home) {
                    None => {
                        let _ = wake::consume_wake(&host.home);
                        tracing::trace!(
                            target: "lastdbd::log_sync",
                            "no relay URL configured; signed-history sync idle"
                        );
                        tokio::time::sleep(IDLE).await;
                    }
                    Some(base) => {
                        let failed = match one_turn(&host, &histories, &base).await {
                            Ok(()) => {
                                // Prior sticky last_error must not survive a
                                // clean turn (People grant_403_for_node reads
                                // the node-global field with no database_id).
                                host.cloud_sync.clear_error();
                                false
                            }
                            Err(err) => {
                                tracing::warn!(
                                    target: "lastdbd::log_sync",
                                    %err,
                                    "sync turn failed"
                                );
                                host.cloud_sync.record_error(err);
                                true
                            }
                        };
                        if failed {
                            // Keep the full ACTIVE backoff on errors so 4xx/429
                            // cannot spin when authors keep writing wake markers.
                            if uploads_pending(&host) {
                                wake::request_wake(&host.home);
                            }
                            let _ = wake::consume_wake(&host.home);
                            tokio::time::sleep(ACTIVE).await;
                        } else {
                            // Clean turn: a tip authored during the turn, or a
                            // wake written mid-pause, should run again without
                            // waiting out ACTIVE.
                            if uploads_pending(&host) {
                                wake::request_wake(&host.home);
                            }
                            let woken = wake::consume_wake(&host.home);
                            if !woken {
                                wake::pause_unless_wake(&host.home, ACTIVE).await;
                            }
                        }
                    }
                }
            }
        }
        .in_current_span(),
    )
}

async fn one_turn(host: &Host, histories: &HistoryMap, base: &str) -> Result<(), String> {
    let client = RelayClient::for_host(host, base)?;

    let owned: Vec<_> = crate::host::owned::registered_databases(&host.home)
        .into_iter()
        .filter(|database| syncable_owned(database.kind, &database.id))
        .collect();
    // No phase stops the turn. A later phase's error replaces an earlier one,
    // and the turn reports the last.
    let mut turn_error = publish_declared_schemas(host, &owned);
    turn_error = upload_owned_logs(host, &client, &owned)
        .await
        .or(turn_error);
    turn_error = pull_owned_shareables(host, histories, &client, &owned)
        .await
        .or(turn_error);
    // Phase c2: the other devices of this owner write the personal database.
    turn_error = super::personal::turn(host, histories, &client)
        .await
        .or(turn_error);
    // Phase d: decisions the publisher signed during the pull go up this turn,
    // so members settle their submission status without waiting another loop.
    turn_error = upload_owned_logs(host, &client, &owned)
        .await
        .or(turn_error);
    // Phase e: proposals on received databases go up before their pull, so
    // publish.lastAttempt reflects a real attempt.
    turn_error = upload_received_logs(host, &client).await.or(turn_error);
    // Phase f.
    if let Err(err) = pull_received_shares(host, histories, &client).await {
        turn_error = Some(err);
    }
    match turn_error {
        Some(err) => Err(err),
        None => Ok(()),
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

/// Phases b and d: upload every owned log. This runs before any pull because
/// a personal database, or a shareable whose birth is not on the relay yet,
/// gets log/read 403 field=grant; that must not keep the birth and grant
/// entries from uploading. Each database drains every pending batch, so a
/// long run of mirrored data cannot leave grants behind it.
async fn upload_owned_logs(
    host: &Host,
    client: &RelayClient,
    owned: &[RegisteredDatabase],
) -> Option<String> {
    let mut turn_error = None;
    for database in owned {
        if let Some(err) = upload_log(client, host, &database.id).await {
            turn_error = Some(err);
        }
    }
    turn_error
}

/// Phase c: pull the member streams of owned shareable databases that have a
/// birth, then decide the proposals they carry. This node is the publisher, so
/// a database with no birth has no chain on the relay and a pull would only
/// get log/read 403 has_publisher:false. The personal database is pulled by
/// `personal`, which treats the 403 of an owner with no birth on the relay as
/// an empty relay. A sibling personal database has no birth, and no pull. The
/// history is rebuilt from the log when
/// this process has not loaded it yet; a log that cannot be read is this
/// turn's error.
async fn pull_owned_shareables(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    owned: &[RegisteredDatabase],
) -> Option<String> {
    let mut turn_error = None;
    for database in owned {
        if database.kind == "personal" {
            continue;
        }
        let has_birth = match histories.with_mut(&database.id, |history| history.owner().is_some())
        {
            Ok(has_birth) => has_birth,
            Err(err) => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database.id,
                    %err,
                    "history could not be loaded from the log"
                );
                turn_error = Some(err);
                continue;
            }
        };
        if !has_birth {
            continue;
        }
        if let Err(err) = pull_owned(host, histories, client, &database.id).await {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                %err,
                "owned stream pull failed"
            );
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

async fn pull_owned(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    database_id: &str,
) -> Result<(), String> {
    let Some(store) = materialize::store_for(host, database_id) else {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            "skip pull: no FoldDB store is open for this database"
        );
        return Ok(());
    };
    let db = DatabaseCtx {
        database_id,
        store: &store,
    };
    let caller = NodeKey::from_bytes(host.keypair.public_key_bytes());
    let Some(publisher_hex) = histories
        .peek_with(database_id, |history| {
            history.owner().map(|owner| owner.to_hex())
        })
        .flatten()
    else {
        return Ok(());
    };
    let local_device = device::device_id(&host.home)?.to_hex();
    let pulled = super::pull::pull_share_writers(
        client,
        histories,
        &db,
        caller,
        &publisher_hex,
        [local_device],
    )
    .await;
    keep_received(host, histories, database_id);
    pulled?;
    super::incorporate::resolve_owned(host, histories, &store, database_id).await
}

/// Store what a pull accepted from other streams, so a restart rebuilds the
/// history without the relay. Entries stay in memory when the write fails, and
/// the next pull tries again.
pub(super) fn keep_received(host: &Host, histories: &HistoryMap, database_id: &str) {
    let stored = super::chain_author::identity_of(host)
        .map_err(|error| error.reply().message.to_owned())
        .and_then(|identity| {
            let own = mutation_log::VerifiedHistory::stream(identity.node_key, identity.device_id);
            histories.persist_received(database_id, &own)
        });
    if let Err(error) = stored {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            %error,
            "received entries could not be stored"
        );
    }
}

/// Pull publisher and write/submit member streams for signed-log received DBs.
async fn pull_received_shares(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
) -> Result<(), String> {
    let shares = registered_share::list(&host.home).map_err(|e| e.to_string())?;
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        if let Err(err) = pull_one_received(host, histories, client, &share).await {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %share.id.as_str(),
                %err,
                "received stream pull failed"
            );
            return Err(err);
        }
    }
    Ok(())
}

async fn pull_one_received(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    share: &RegisteredShare,
) -> Result<(), String> {
    let id = Id::try_from(share.id.as_str().to_owned()).map_err(|e| e.to_string())?;
    let store = registered_share::receiver_store(host, &id)
        .await
        .map_err(|e| e.to_string())?;
    // Ensure an in-memory history exists so AcceptEntry has a place to land.
    histories.with_mut(share.id.as_str(), |_| ())?;
    let db = DatabaseCtx {
        database_id: share.id.as_str(),
        store: &store,
    };
    let caller = NodeKey::from_bytes(host.keypair.public_key_bytes());
    let known = share.publisher_device_id.iter().cloned();
    let pulled = super::pull::pull_share_writers(
        client,
        histories,
        &db,
        caller,
        share.publisher.as_str(),
        known,
    )
    .await;
    keep_received(host, histories, share.id.as_str());
    pulled
}
