//! Daemon sync loop: idle without relay URL; otherwise upload outbox and pull.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use app_identity_crypto::SigningKey;
use cloud_backup_protocol::Id;
use mutation_log::{DeviceId, NodeKey, VerifiedHistory};
use tracing::Instrument;

use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

use super::config;
use super::device;
use super::history_store::HistoryMap;
use super::materialize::{self, DatabaseCtx};
use super::outbox_upload::{outbox_pending, upload_outbox, upload_received_outboxes};
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

    // Upload every owned outbox before any pull. Personal (and any shareable
    // whose birth is not on the relay yet) gets log/read 403 field=grant; that
    // must not abort the turn before shareable birth/grant entries upload.
    // Personal DBs are not pulled: empty-index grant-403 is expected there and
    // must not poison turn_error / sticky cloud_sync.last_error.
    let mut turn_error: Option<String> = None;
    let databases = crate::host::owned::registered_databases(&host.home);
    let owned: Vec<_> = databases
        .into_iter()
        .filter(|database| {
            if database.kind != "personal" && database.kind != "shareable" {
                return false;
            }
            if mutation_log::DatabaseId::from_hex(&database.id).is_err() {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database.id,
                    "skip sync: database_id is not a 64-hex database id"
                );
                return false;
            }
            true
        })
        .collect();
    for database in &owned {
        // Drain every pending batch this turn. A single 32-entry prefix left
        // grants behind continuous mirrored data and readers hit empty indexes.
        if let Some(err) = upload_outbox(&client, histories, &host.home, &database.id).await {
            turn_error = Some(err);
        }
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
        if let Err(err) = pull_owned(
            host,
            histories,
            &client,
            &database.id,
            &node_key_hex,
            &device_id,
        )
        .await
        {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                %err,
                "owned stream pull failed"
            );
            turn_error = Some(err);
        }
    }
    // Member proposals and direct writes land on the received outbox; upload
    // them before pulling so publish.lastAttempt reflects a real attempt.
    if let Some(err) = upload_received_outboxes(host, histories, &client).await {
        turn_error = Some(err);
    }
    if let Err(err) = pull_received_shares(host, histories, &client).await {
        turn_error = Some(err);
    }
    match turn_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn pull_owned(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    database_id: &str,
    node_key_hex: &str,
    device_id: &DeviceId,
) -> Result<(), String> {
    let Some(store) = materialize::store_for(host, database_id) else {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            "skip pull: no FoldDB store is open for this database"
        );
        return Ok(());
    };
    // DESIGN: persist per-stream read cursor; Phase 1 re-reads from 1 (idempotent Accept).
    let db = DatabaseCtx {
        home: &host.home,
        database_id,
        store: &store,
    };
    let _ = super::pull::pull_stream(
        client,
        histories,
        &db,
        node_key_hex,
        &device_id.to_string(),
        1,
    )
    .await?;
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
        if let Err(err) = pull_one_received(host, histories, client, &share, device).await {
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
    histories.with_mut(share.id.as_str(), |_| ())?;
    let from = histories
        .peek_with(share.id.as_str(), |history| {
            let stream = VerifiedHistory::stream(publisher, device);
            history
                .stream_head(&stream)
                .map_or(1, |head| head.writer_seq.saturating_add(1))
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
