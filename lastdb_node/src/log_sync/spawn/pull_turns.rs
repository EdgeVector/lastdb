//! Owned-shareable and received-share pull steps for the sync loop.

use cloud_backup_protocol::Id;
use mutation_log::NodeKey;

use crate::host::owned::RegisteredDatabase;
use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

use super::super::birth;
use super::super::history_store::HistoryMap;
use super::super::materialize::{self, DatabaseCtx};
use super::super::relay::RelayClient;

/// Phase c: pull the publisher and member streams of owned shareable databases,
/// then decide the proposals they carry. On the device that minted the database
/// (`birth.json` present), a history with no birth has no chain on the relay
/// yet and is skipped. On a second device of the same publisher (`birth.json`
/// absent), pulling discovers the birth and prior entries from the relay; an
/// unborn 403 before the first device uploads its birth is treated as an empty
/// relay. The personal database is pulled by `personal`. A sibling personal
/// database has no birth, and no pull. The history is rebuilt from the log when
/// this process has not loaded it yet; a log that cannot be read is this turn's
/// error.
pub(super) async fn pull_owned_shareables(
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
                host.cloud_sync
                    .record_download(&database.id, None, Some(err.clone()), true);
                turn_error = Some(err);
                continue;
            }
        };
        if !has_birth {
            match birth::load(&host.home, &database.id) {
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        id = %database.id,
                        %err,
                        "birth material could not be read"
                    );
                    host.cloud_sync
                        .record_download(&database.id, None, Some(err.clone()), true);
                    turn_error = Some(err);
                    continue;
                }
            }
        }
        match pull_owned(host, histories, client, &database.id, has_birth).await {
            Ok(true) => {
                let through = remote_applied_through(host, histories, &database.id);
                host.cloud_sync
                    .record_download(&database.id, through, None, true);
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database.id,
                    %err,
                    "owned stream pull failed"
                );
                host.cloud_sync
                    .record_download(&database.id, None, Some(err.clone()), true);
                turn_error = Some(err);
            }
        }
    }
    turn_error
}

async fn pull_owned(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    database_id: &str,
    has_birth: bool,
) -> Result<bool, String> {
    let Some(store) = materialize::store_for(host, database_id) else {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            "skip pull: no FoldDB store is open for this database"
        );
        return Ok(false);
    };
    let db = DatabaseCtx {
        database_id,
        store: &store,
    };
    let caller = NodeKey::from_bytes(host.keypair.public_key_bytes());
    if let Some(owner) = histories
        .peek_with(database_id, |history| history.owner())
        .flatten()
    {
        if owner != caller {
            return Err("this node is not the publisher of this database".into());
        }
    }
    let publisher_hex = caller.to_hex();
    let pulled = super::super::pull::pull_share_writers(
        client,
        histories,
        &db,
        caller,
        &publisher_hex,
        std::iter::empty(),
    )
    .await;
    if let Err(err) = &pulled {
        if !has_birth && super::super::personal::is_unborn_403(err) {
            return Ok(false);
        }
    }
    let persisted = keep_received(host, histories, database_id);
    pulled?;
    persisted?;
    super::super::incorporate::resolve_owned(host, histories, &store, database_id).await?;
    Ok(true)
}

/// Store what a pull accepted from other streams, so a restart rebuilds the
/// history without the relay. Entries stay in memory when the write fails, and
/// the next pull tries again.
fn keep_received(host: &Host, histories: &HistoryMap, database_id: &str) -> Result<(), String> {
    let stored = super::super::chain_author::identity_of(host)
        .map_err(|error| error.reply().message.to_owned())
        .and_then(|identity| {
            let own = mutation_log::VerifiedHistory::stream(identity.node_key, identity.device_id);
            histories.persist_received(database_id, &own)
        });
    if let Err(error) = &stored {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            %error,
            "received entries could not be stored"
        );
    }
    stored
}

/// Pull publisher and write/submit member streams for signed-log received DBs.
///
/// A grant-403 (or other error) on one received database must not stop the
/// turn from pulling the others, and must not fail the turn: a failed turn
/// takes the unconditional ACTIVE sleep (#1844) and leaves a newly authored
/// grant tip waiting instead of the wake-aware clean path.
pub(super) async fn pull_received_shares(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
) {
    let shares = match registered_share::list(&host.home) {
        Ok(shares) => shares,
        Err(err) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %err,
                "received share list failed"
            );
            return;
        }
    };
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        match pull_one_received(host, histories, client, &share).await {
            Ok(()) => {
                let through = remote_applied_through(host, histories, share.id.as_str());
                host.cloud_sync
                    .record_download(share.id.as_str(), through, None, false);
            }
            Err(err) => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %share.id.as_str(),
                    %err,
                    "received stream pull failed"
                );
                host.cloud_sync
                    .record_download(share.id.as_str(), None, Some(err), false);
            }
        }
        // A lease closing may have excluded direct changes of this device.
        if let Err(err) = super::super::lease_resign::resign_excluded(host, share.id.as_str()).await
        {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %share.id.as_str(),
                %err,
                "excluded changes could not be signed again as proposals"
            );
        }
    }
}

/// Load the history of each received signed-log database from the log on disk,
/// so a held write lease is known after a restart before any pull, and with no
/// relay at all.
pub(super) fn load_received_histories(host: &Host, histories: &HistoryMap) {
    for share in registered_share::list(&host.home).unwrap_or_default() {
        if share.signed_log && mutation_log::DatabaseId::from_hex(share.id.as_str()).is_ok() {
            if let Err(err) = histories.with_mut(share.id.as_str(), |_| ()) {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %share.id.as_str(),
                    %err,
                    "history could not be loaded from the log"
                );
            }
        }
    }
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
    let pulled = super::super::pull::pull_share_writers(
        client,
        histories,
        &db,
        caller,
        share.publisher.as_str(),
        known,
    )
    .await;
    let _ = keep_received(host, histories, share.id.as_str());
    pulled
}

fn remote_applied_through(host: &Host, histories: &HistoryMap, database_id: &str) -> Option<u64> {
    let identity = super::super::chain_author::identity_of(host).ok()?;
    let own = mutation_log::VerifiedHistory::stream(identity.node_key, identity.device_id);
    histories.peek_with(database_id, |history| {
        history
            .accepted_outside(&own)
            .into_iter()
            .map(|entry| entry.header().writer_seq)
            .max()
            .unwrap_or(0)
    })
}
