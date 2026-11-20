//! Daemon sync loop: idle without relay URL; otherwise upload outbox and pull.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use app_identity_crypto::SigningKey;
use cloud_backup_protocol::Id;
use mutation_log::{entry_hash, DeviceId, NodeKey, VerifiedHistory};
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
                            Ok(()) => false,
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

    let databases = crate::host::owned::registered_databases(&host.home);
    for database in databases {
        if database.kind != "personal" && database.kind != "shareable" {
            continue;
        }
        // Registry ids are Hex32 (db verifying key); refuse non-hex rows.
        if mutation_log::DatabaseId::from_hex(&database.id).is_err() {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                "skip sync: database_id is not a 64-hex database id"
            );
            continue;
        }
        let Some(store) = materialize::store_for(host, &database.id) else {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                "skip sync: no FoldDB store is open for this database"
            );
            continue;
        };
        upload_outbox(&client, histories, &host.home, &database.id).await?;
        // Pull own stream tip advances.
        let from = 1u64; // DESIGN: persist per-stream read cursor; Phase 1 re-reads from 1 (idempotent Accept).
        let db = DatabaseCtx {
            home: &host.home,
            database_id: &database.id,
            store: &store,
        };
        let _ = super::pull::pull_stream(
            &client,
            histories,
            &db,
            &node_key_hex,
            &device_id.to_string(),
            from,
        )
        .await?;
    }
    pull_received_shares(host, histories, &client).await?;
    Ok(())
}

/// Pull publisher streams for signed-log received databases.
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
        let Some(device) = share.publisher_device_id.as_deref() else {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %share.id.as_str(),
                "skip received sync: open link has no publisher device_id"
            );
            continue;
        };
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        pull_one_received(host, histories, client, &share, device).await?;
    }
    Ok(())
}

async fn pull_one_received(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    share: &RegisteredShare,
    publisher_device: &str,
) -> Result<(), String> {
    let id = Id::try_from(share.id.as_str().to_owned()).map_err(|e| e.to_string())?;
    let store = registered_share::receiver_store(host, &id)
        .await
        .map_err(|e| e.to_string())?;
    let publisher = NodeKey::from_hex(share.publisher.as_str())
        .map_err(|_| "received share publisher is not hex32")?;
    let device = DeviceId::from_hex(publisher_device)
        .map_err(|_| "received share device_id is not hex32")?;
    // Ensure an in-memory history exists so AcceptEntry has a place to land.
    let _ = histories.with_mut(share.id.as_str(), |_| ())?;
    let from = histories
        .peek_with(share.id.as_str(), |history| {
            let stream = VerifiedHistory::stream(publisher, device);
            history
                .stream_head(&stream)
                .map(|head| head.writer_seq.saturating_add(1))
                .unwrap_or(1)
        })
        .unwrap_or(1);
    let db = DatabaseCtx {
        home: &host.home,
        database_id: share.id.as_str(),
        store: &store,
    };
    let _ = super::pull::pull_stream(
        client,
        histories,
        &db,
        share.publisher.as_str(),
        publisher_device,
        from,
    )
    .await?;
    Ok(())
}

async fn upload_outbox(
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
    let (stored, duplicate) = client.append(database_id, &batch).await?;
    let _ = (stored, duplicate);
    histories.with_mut(database_id, |history| {
        for entry in &batch {
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
        if database.kind != "personal" && database.kind != "shareable" {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(&database.id).is_err() {
            continue;
        }
        let memory = histories
            .peek_with(&database.id, |history| !history.outbox().is_empty())
            .unwrap_or(false);
        if memory {
            return true;
        }
        match history_store::load_outbox_entries(&host.home, &database.id) {
            Ok(entries) if !entries.is_empty() => return true,
            _ => {}
        }
    }
    false
}
