//! Daemon sync loop: idle without relay URL; otherwise upload outbox and pull.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use app_identity_crypto::SigningKey;
use cloud_backup_protocol::Id;
use mutation_log::{entry_hash, NodeKey, SignedEntry};
use tracing::Instrument;

use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

use super::config;
use super::device;
use super::history_store::{self, HistoryMap};
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;
use super::wake;

const IDLE: Duration = Duration::from_secs(5);
const ACTIVE: Duration = Duration::from_secs(2);

pub fn spawn(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
    _reopen: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let histories = super::histories_for(&host.home);
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
                        if failed && outbox_pending(&host, &histories) {
                            // Mark for another pass; keep ACTIVE delay so fast
                            // failures (refused / 4xx / 429) cannot spin.
                            wake::request_wake(&host.home);
                        }
                        let woken = wake::consume_wake(&host.home);
                        if failed || !woken {
                            tokio::time::sleep(ACTIVE).await;
                        }
                    }
                }
            }
        }
        .in_current_span(),
    )
}

async fn one_turn(host: &Host, histories: &HistoryMap, base: &str) -> Result<(), String> {
    let signing_key = SigningKey::from_bytes(&host.keypair.secret_key_bytes());
    let node_key = NodeKey::from_bytes(host.keypair.public_key_bytes());
    let node_key_hex = node_key.to_hex();
    let device_id = device::device_id(&host.home)?;
    let client = RelayClient::new(base, signing_key, node_key_hex.clone(), &device_id)?;

    // Upload every owned and received outbox before any pull. Personal (and any
    // shareable whose birth is not on the relay yet) gets log/read 403
    // field=grant; that must not abort the turn before shareable birth/grant
    // entries upload. Personal DBs are not pulled: empty-index grant-403 is
    // expected there and must not poison turn_error / sticky last_error.
    let mut turn_error: Option<String> = None;
    let owned: Vec<_> = crate::host::owned::registered_databases(&host.home)
        .into_iter()
        .filter(|database| syncable_owned(database.kind, &database.id))
        .collect();
    for database in &owned {
        match drain_outbox(&client, histories, &host.home, &database.id).await {
            Ok(()) => {}
            Err(err) => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database.id,
                    %err,
                    "outbox upload failed"
                );
                turn_error = Some(err);
            }
        }
    }
    if let Err(err) = drain_received_outboxes(host, histories, &client).await {
        turn_error = Some(err);
    }
    for database in &owned {
        if database.kind == "personal" {
            continue;
        }
        // This node is the publisher of its owned shareables. Skip pull until
        // local birth is accepted: an offline setup with only birth.json has
        // no chain on the relay, and log/read 403 has_publisher:false must not
        // poison the turn (People receiver-created *-offline databases).
        let has_birth = histories
            .peek_with(&database.id, |history| history.owner().is_some())
            .unwrap_or(false);
        if !has_birth {
            continue;
        }
        if let Err(err) = pull_owned(host, histories, &client, &database.id).await {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                %err,
                "owned stream pull failed"
            );
            turn_error = Some(err);
        }
    }
    if let Err(err) = pull_received_shares(host, histories, &client).await {
        turn_error = Some(err);
    }
    match turn_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
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

async fn drain_received_outboxes(
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
        if let Err(err) = drain_outbox(client, histories, &host.home, share.id.as_str()).await {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %share.id.as_str(),
                %err,
                "received outbox upload failed"
            );
            return Err(err);
        }
    }
    Ok(())
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
        home: &host.home,
        database_id,
        store: &store,
    };
    let seed = super::pull::writer_keys(histories, database_id);
    super::pull::pull_writers(client, histories, &db, seed).await
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
        home: &host.home,
        database_id: share.id.as_str(),
        store: &store,
    };
    let mut seed = super::pull::writer_keys(histories, share.id.as_str());
    if seed.is_empty() {
        seed.push(share.publisher.as_str().to_owned());
    }
    super::pull::pull_writers(client, histories, &db, seed).await
}

/// Upload until this database's outbox is empty (or the batch cap is hit).
async fn drain_outbox(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
) -> Result<(), String> {
    // Reload disk outbox into memory if history is empty.
    let disk_entries = history_store::load_outbox_entries(home, database_id)?;
    if !disk_entries.is_empty() {
        histories.with_mut(database_id, |history| {
            history_store::requeue_outbox(history, disk_entries)
        })??;
    }
    // Cap batches so a runaway outbox cannot block the turn forever.
    for _ in 0..64 {
        let batch = histories.with_mut(database_id, |history| {
            history
                .outbox()
                .batch(32)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })?;
        if batch.is_empty() {
            return Ok(());
        }
        upload_batch(client, histories, home, database_id, &batch).await?;
    }
    Err(format!(
        "outbox for {database_id} still pending after 64 upload batches"
    ))
}

async fn upload_batch(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
    batch: &[SignedEntry],
) -> Result<(), String> {
    let (stored, duplicate) = client.append(database_id, batch).await?;
    let _ = duplicate;
    let expected = batch.len() as u64;
    if stored < expected {
        return Err(format!(
            "relay log.append stored {stored} of {expected} outbox entries"
        ));
    }
    histories.with_mut(database_id, |history| {
        for entry in batch {
            if let Ok(hash) = entry_hash(entry) {
                history.outbox_mut().ack(&hash);
            }
        }
        history_store::persist_outbox(home, database_id, history.outbox())
    })??;
    Ok(())
}

/// True when any syncable database still has unacked outbox entries.
fn outbox_pending(host: &Host, histories: &HistoryMap) -> bool {
    for database in crate::host::owned::registered_databases(&host.home) {
        if !syncable_owned(database.kind, &database.id) {
            continue;
        }
        if outbox_pending_one(histories, &host.home, &database.id) {
            return true;
        }
    }
    let Ok(shares) = registered_share::list(&host.home) else {
        return false;
    };
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        if outbox_pending_one(histories, &host.home, share.id.as_str()) {
            return true;
        }
    }
    false
}

fn outbox_pending_one(histories: &HistoryMap, home: &std::path::Path, database_id: &str) -> bool {
    let memory = histories
        .peek_with(database_id, |history| !history.outbox().is_empty())
        .unwrap_or(false);
    if memory {
        return true;
    }
    matches!(
        history_store::load_outbox_entries(home, database_id),
        Ok(entries) if !entries.is_empty()
    )
}
