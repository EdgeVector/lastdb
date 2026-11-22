//! The personal database on several devices, and its restore.
//!
//! Every device of one owner signs with the same node key and has its own
//! device id, so the relay holds one stream for each device under that key. A
//! device reads all of them, skips its own, accepts the entries, opens the
//! sealed payloads, and applies the records to its store. Two devices that
//! change one record while apart agree afterwards: the higher position wins,
//! the Lamport number first and the device id second.
//!
//! A new device and a restore are the same operation. The recovery phrase gives
//! the node key, the personal database id and the read key. The device reads
//! every stream from its first entry, rebuilds the database, and then signs on
//! a new stream under a new device id. No device has to be online for that: the
//! relay holds the streams.
//!
//! The relay refuses to read a database whose birth it does not hold yet. For
//! the personal database that is the state of an owner who has not written
//! anything on any device, and it is not an error. It stays one only for a
//! device that holds a birth itself, because then the relay lost the database.

use mutation_log::{
    entry_hash, AcceptOutcome, BundleBody, BundleChange, DeviceId, EntryHeader, SignedEntry,
};

use super::blobs;
use super::device;
use super::history_store::HistoryMap;
use super::materialize::{self, DatabaseCtx};
use super::pull::rematerialize;
use super::relay::RelayClient;
use super::sealed::Sealer;
use crate::host::Host;

/// What one pull of the owner's streams accepted.
#[derive(Default)]
pub(super) struct Pulled {
    /// Entries that were new to this node.
    pub(super) accepted: u64,
    /// The highest `writer_seq` among them.
    pub(super) through: u64,
}

/// The id of the personal database of this node.
pub(super) fn personal_database_id(host: &Host) -> Result<String, String> {
    host.owned
        .owner()
        .map(|owner| owner.id)
        .map_err(|_| "the personal database is not registered".to_owned())
}

/// One sync turn of the personal database: read, accept, open, apply. The
/// result is the error of the turn, if any.
pub(super) async fn turn(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
) -> Option<String> {
    match pull(host, histories, client).await {
        Ok(_) => None,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "personal database pull failed");
            Some(error)
        }
    }
}

/// Read every stream of this node key, accept what is new, and apply it.
pub(super) async fn pull(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
) -> Result<Pulled, String> {
    let database_id = personal_database_id(host)?;
    let store = materialize::store_for(host, &database_id)
        .ok_or("no store is open for the personal database")?;
    let sealer = histories
        .sealer_of(&database_id)
        .ok_or("this node cannot seal the personal database")?;
    let own = device::device_id(&host.home)?;
    let mut entries = match read_streams(client, &database_id, &own).await {
        Ok(entries) => entries,
        Err(error)
            if error.contains("HTTP 403")
                && histories.with_mut(&database_id, |history| history.owner().is_none())? =>
        {
            return Ok(Pulled::default());
        }
        Err(error) => return Err(error),
    };
    // Dependencies come first: an entry's Lamport number is above its
    // predecessor's and above every entry it cites.
    entries.sort_by_key(|entry| {
        let header = entry.header();
        (
            header.lamport,
            header.node_key,
            header.device_id,
            header.writer_seq,
        )
    });
    let mut pulled = Pulled::default();
    for entry in entries {
        let writer_seq = entry.header().writer_seq;
        if accept(host, histories, client, &sealer, entry).await? {
            pulled.accepted += 1;
            pulled.through = pulled.through.max(writer_seq);
        }
    }
    super::spawn::keep_received(host, histories, &database_id);
    let db = DatabaseCtx {
        database_id: &database_id,
        store: &store,
    };
    rematerialize(histories, &db, pulled.accepted).await?;
    Ok(pulled)
}

/// Every entry of every stream of this node key except this device's own.
async fn read_streams(
    client: &RelayClient,
    database_id: &str,
    own: &DeviceId,
) -> Result<Vec<SignedEntry>, String> {
    let node_key = client.node_key_hex.clone();
    let mut found = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (entries, next) = client
            .read(database_id, &node_key, None, 1, cursor.as_deref(), 90)
            .await?;
        if entries.is_empty() {
            return Ok(found);
        }
        found.extend(
            entries
                .into_iter()
                .filter(|entry| entry.header().device_id != *own),
        );
        let Some(next) = next else {
            return Ok(found);
        };
        cursor = Some(next);
    }
}

/// Accept one entry. True when it was new. A bundle needs its blob first.
async fn accept(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
    sealer: &Sealer,
    entry: SignedEntry,
) -> Result<bool, String> {
    let database_id = entry.header().database_id.to_hex();
    let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
    if histories.with_mut(&database_id, |history| {
        history.get_accepted(&hash).is_some()
    })? {
        return Ok(false);
    }
    let changes = if let SignedEntry::Bundle { header, body, .. } = &entry {
        Some(bundle_changes(host, client, sealer, header, body).await?)
    } else {
        None
    };
    let outcome = histories.with_mut(&database_id, |history| match changes {
        Some(changes) => history.accept_bundle_with_changes(entry, &changes),
        None => history.accept_entry(entry),
    })?;
    match outcome {
        AcceptOutcome::Accept { duplicate, .. } => Ok(!duplicate),
        AcceptOutcome::Pending { .. } => Ok(false),
        AcceptOutcome::Reject { reason } => {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "relay entry rejected by AcceptEntry");
            Ok(false)
        }
    }
}

/// The changes of a bundle. The kept blob is used when there is one. Otherwise
/// the blob comes from the relay and is kept, so a restart does not need it.
async fn bundle_changes(
    host: &Host,
    client: &RelayClient,
    sealer: &Sealer,
    header: &EntryHeader,
    body: &BundleBody,
) -> Result<Vec<BundleChange>, String> {
    let database_id = header.database_id.to_hex();
    let blob = if let Some(blob) = blobs::load(&host.home, &database_id, &body.blob_sha256)? {
        blob
    } else {
        let blob = client
            .blob_get(&database_id, &body.blob_sha256.to_hex(), body.blob_size)
            .await?;
        blobs::save(&host.home, &database_id, &body.blob_sha256, &blob)?;
        blob
    };
    blobs::open_changes(sealer, header, &blob)
}

/// What a restore found.
pub struct RestoreReport {
    /// The id of the personal database.
    pub database_id: String,
    /// Entries that were new to the restored home.
    pub replayed: u64,
    /// The highest `writer_seq` among them.
    pub through: u64,
    /// Device streams that were read, not counting the stream of the birth.
    pub devices: usize,
}

/// Rebuild the personal database of `host` from the streams on the relay.
///
/// `host` is a fresh home made from the recovery phrase. It keeps the device id
/// that this call makes, and signs on that stream from then on.
pub async fn restore_personal_database(
    host: &Host,
    relay_url: &str,
) -> Result<RestoreReport, String> {
    let histories = super::histories_for(host);
    let client = RelayClient::for_host(host, relay_url)?;
    let pulled = pull(host, &histories, &client).await?;
    let database_id = personal_database_id(host)?;
    let devices = histories
        .with_mut(&database_id, |history| {
            history
                .owner()
                .map(|owner| history.stream_heads_of(owner).len())
        })?
        .ok_or("the relay holds no personal database for this recovery phrase")?;
    Ok(RestoreReport {
        database_id,
        replayed: pulled.accepted,
        through: pulled.through,
        // The birth stream is one of them.
        devices: devices.saturating_sub(1),
    })
}
