//! Daemon sync loop: idle without relay URL; otherwise upload outbox and pull.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use app_identity_crypto::SigningKey;
use mutation_log::{entry_hash, NodeKey};
use tracing::Instrument;

use crate::host::Host;

use super::config;
use super::device;
use super::history_store::{self, HistoryMap};
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
            let histories = HistoryMap::new();
            while !shutdown.load(Ordering::Acquire) {
                wake::consume_wake(&host.home);
                match config::relay_base_url(&host.home) {
                    None => {
                        tracing::trace!(
                            target: "lastdbd::log_sync",
                            "no relay URL configured; signed-history sync idle"
                        );
                        tokio::time::sleep(IDLE).await;
                    }
                    Some(base) => {
                        if let Err(err) = one_turn(&host, &histories, &base).await {
                            tracing::warn!(target: "lastdbd::log_sync", %err, "sync turn failed");
                            host.cloud_sync.record_error(err);
                        }
                        tokio::time::sleep(ACTIVE).await;
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
    let client = RelayClient::new(
        base.to_owned(),
        signing_key,
        node_key_hex.clone(),
        device_id,
    );

    let databases = crate::host::owned::registered_databases(&host.home);
    for database in databases {
        // Sync every registered owned database; received shares pull separately.
        if database.kind != "personal" && database.kind != "shareable" {
            // DESIGN: received/shareable membership pull paths still TBD.
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
        upload_outbox(&client, histories, &host.home, &database.id).await?;
        // Pull own stream tip advances.
        let from = 1u64; // DESIGN: persist per-stream read cursor; Phase 1 re-reads from 1 (idempotent Accept).
        let _ = super::pull::pull_stream(
            &client,
            histories,
            &host.home,
            &database.id,
            &node_key_hex,
            &device_id.to_string(),
            from,
        )
        .await?;
    }
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
