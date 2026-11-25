//! The personal database on the signed log.
//!
//! Every device of one owner signs its own stream under the one node key, and
//! the relay holds one stream for each device. A device does three things each
//! turn, and these use the machinery of the store:
//!
//! - [`outgoing`] sends the signed entries and sealed files from the spool of
//!   accepted writes (sealed and signed when each write commits) and retires them
//!   when the relay acknowledges.
//! - [`snapshot`] freezes a snapshot when one is due, and the snapshot lane uploads it while
//!   the turns above go on.
//! - [`reclaim`] sweeps after a snapshot is named: the relay drops every stored file that the
//!   newest snapshot and the entries after it do not name, so it holds what the store holds.
//! - [`incoming`] reads the other devices' streams from the head row of each,
//!   and applies every entry with the stamp that the store keeps beside each
//!   record. The head rows live in the store, so a snapshot carries them, and a
//!   restart replays nothing.
//!
//! A new device and a restore are the same operation as an ordinary turn: the
//! recovery phrase gives the node key, the personal database id and the read
//! key, and the first turn reads every stream from its first entry. The relay
//! refuses to list the streams of a database whose birth it does not hold. For
//! the personal database that is an owner who has not written anywhere yet,
//! and it is not an error for a device that has never uploaded.

mod apply;
mod apply_schema;
mod body;
mod files;
mod incoming;
mod outgoing;
mod reclaim;
mod reclaim_count;
mod reclaim_state;
mod reclaim_view;
mod send;
mod snapshot;
mod snapshot_entry;
mod snapshot_known;
mod snapshot_lane;
mod snapshot_manifest;
mod snapshot_restore;
mod snapshot_state;
mod snapshot_upload;
mod verify;

pub(crate) use reclaim_state::last as last_reclaim;
pub(crate) use snapshot_known::known as known_snapshot;
pub(super) use snapshot_lane::stop as stop_snapshot_lane;
pub use snapshot_restore::{install_newest_snapshot, SnapshotBase};
pub(crate) use snapshot_state::{
    reclaim_status, request as request_snapshot, request_reclaim, status as snapshot_status,
};

use std::sync::Arc;

use cloud_backup_protocol::{AccountId, CloudScope, DecimalU64};
use fold_db::fold_db_core::mutation_manager::ReplayStreamHead;
use fold_db::storage::disk_backup::accepted::UploadIdentity;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{AcceptedMeta, DatabaseId, DeviceId, EntryHash, SignedEntry, StreamHead};
use zeroize::Zeroizing;

use super::chain_author::{self, AuthorIdentity};
use super::relay::{RelayClient, StreamHead as RelayStream};
pub(super) use crate::database_birth::personal as birth_of;
use crate::host::Host;
use crate::personal_identity;
use crate::personal_seal::Sealer;

/// Everything one turn of the personal database needs, derived from the node seed.
pub(super) struct Ctx<'a> {
    pub(super) host: &'a Host,
    pub(super) database_id: String,
    pub(super) database: DatabaseId,
    pub(super) identity: AuthorIdentity,
    pub(super) node_hex: String,
    pub(super) device_hex: String,
    pub(super) birth_device_hex: String,
    /// The device id of the stream that holds the `snapshot` entries, and its hex.
    pub(super) snapshot_device: DeviceId,
    pub(super) snapshot_device_hex: String,
    /// The name of this device's stream in the head rows.
    pub(super) own_stream: String,
    pub(super) sealer: Sealer,
    /// The birth entry. It is the same on every device. A device uploads it
    /// when the relay lacks it, and every personal entry cites it.
    pub(super) birth_entry: SignedEntry,
    pub(super) birth: AcceptedMeta,
    pub(super) birth_hash: EntryHash,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) upload: Arc<UploadIdentity>,
}

impl<'a> Ctx<'a> {
    pub(super) fn open(host: &'a Host) -> Result<Self, String> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let database_id = personal_identity::database_id(&seed);
        let registered = host
            .owned
            .owner()
            .map_err(|_| "the personal database is not registered".to_owned())?
            .id;
        if registered != database_id {
            return Err("the personal database id is not the one of the recovery phrase".into());
        }
        let database = DatabaseId::from_hex(&database_id).map_err(|_| "database id is not hex")?;
        let identity =
            chain_author::identity_of(host).map_err(|error| error.reply().message.to_owned())?;
        let birth_device = DeviceId::from_bytes(personal_identity::birth_device(&seed));
        let (birth_entry, birth, birth_hash) = birth_of(&seed, &identity)?;
        let snapshot_device = DeviceId::from_bytes(personal_identity::snapshot_device(&seed));
        let node_hex = identity.node_key.to_hex();
        let device_hex = identity.device_id.to_hex();
        let account = AccountId::try_from(node_hex[..32].to_owned())
            .map_err(|_| "the writer key is not an account id".to_owned())?;
        // The upload position of the spool is bound to this identity. The relay
        // is named by a fixed word, not by its address, so moving to another
        // relay keeps the position: the stream is the same on every relay.
        let upload = Arc::new(UploadIdentity {
            scope: CloudScope {
                account_id: account,
                account_generation: DecimalU64::new(1),
            },
            environment: "relay".to_owned(),
            database_id: database_id.clone(),
            device_id: device_hex.clone(),
            writer_key: node_hex.clone(),
        });
        Ok(Self {
            host,
            sealer: Sealer::for_node_seed(&seed)?,
            birth_device_hex: birth_device.to_hex(),
            snapshot_device,
            snapshot_device_hex: snapshot_device.to_hex(),
            own_stream: format!("{node_hex}:{device_hex}"),
            runtime: host.db.db_ops().native_runtime(),
            database_id,
            database,
            identity,
            node_hex,
            device_hex,
            birth_entry,
            birth,
            birth_hash,
            upload,
        })
    }

    /// The name of a device stream of this owner in the head rows.
    pub(super) fn stream_name(&self, device_hex: &str) -> String {
        format!("{}:{device_hex}", self.node_hex)
    }

    /// The head row of this device's own stream: the last entry the relay
    /// acknowledged. `None` before the first one.
    pub(super) async fn own_head(&self) -> Result<Option<ReplayStreamHead>, String> {
        let mut rows = self
            .host
            .db
            .mutation_manager()
            .replay_stream_heads(std::slice::from_ref(&self.own_stream))
            .await
            .map_err(|error| format!("the own stream head could not be read: {error}"))?;
        Ok(rows.pop().flatten())
    }
}

/// The head row as the chain check reads it.
pub(super) fn chain_head(head: &ReplayStreamHead) -> Result<StreamHead, String> {
    Ok(StreamHead {
        entry_hash: EntryHash::from_hex(&head.hash)
            .map_err(|_| "a stream head row holds a bad hash".to_owned())?,
        writer_seq: head.seq,
        lamport: head.order,
    })
}

/// One sync turn of the personal database: list the streams, send what is in
/// the spool, apply what the other devices wrote. The result is the error of
/// the turn, if any.
pub(super) async fn turn(host: &Arc<Host>, client: &RelayClient) -> Option<String> {
    match run_turn(host, client).await {
        Ok(()) => None,
        Err(error) if error.starts_with(snapshot_upload::TOO_LARGE) => {
            // The refusal was logged when the relay gave it. The first snapshot of a home
            // repeats it each turn while the spool waits, which is not logged again.
            tracing::debug!(target: "lastdbd::log_sync", %error, "personal database sync is waiting for its first snapshot");
            Some(error)
        }
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "personal database sync failed");
            Some(error)
        }
    }
}

async fn run_turn(host: &Arc<Host>, client: &RelayClient) -> Result<(), String> {
    let ctx = Ctx::open(host)?;
    if !ctx.runtime.captures_accepted() {
        let error =
            "local capture is off: the node must restart with a relay configured".to_owned();
        let status = ctx.runtime.accepted_status();
        ctx.host.cloud_sync.record_upload(
            &ctx.database_id,
            status.head,
            status.acked,
            Some(error.clone()),
        );
        return Err(error);
    }
    let streams = match list_streams(&ctx, client).await {
        Ok(streams) => streams.unwrap_or_default(),
        Err(error) => {
            if is_payment_403(&error) {
                ctx.host
                    .cloud_sync
                    .record_error(crate::cloud::PAYMENT_REQUIRED_MESSAGE);
            } else {
                ctx.host
                    .cloud_sync
                    .record_download(&ctx.database_id, Some(error.clone()));
            }
            return Err(error);
        }
    };
    // A home that held data before it began to capture its writes names its first snapshot
    // before anything else goes up: until it does, the spool is not read for upload.
    if ctx.runtime.accepted_status().snapshot_required {
        if let Err(error) = snapshot::run_first(&ctx, client).await {
            let status = ctx.runtime.accepted_status();
            ctx.host.cloud_sync.record_upload(
                &ctx.database_id,
                status.head,
                status.acked,
                Some(error.clone()),
            );
            return Err(error);
        }
    }
    // Read the other devices' streams and send this device's spool; neither side's error stops
    // the other.
    let read = incoming::run(&ctx, client, &streams).await;
    // What the read failed with is in the status, so a device that is behind history the relay
    // dropped after a snapshot says so there and not only in a log that carries no text.
    ctx.host
        .cloud_sync
        .record_download(&ctx.database_id, read.as_ref().err().cloned());
    let sent = outgoing::run(&ctx, client, &streams).await;
    // A sweep that is due or asked for runs after the send step: the relay then drops every
    // stored file that the newest snapshot and the entries after it do not name.
    let swept = if sent.is_ok() {
        reclaim::run(&ctx, client, &streams).await
    } else {
        Ok(())
    };
    // A snapshot is frozen only after a clean turn: the spool is on the relay and the streams
    // are applied, so the cut holds exactly what the relay holds. Its upload then goes on in
    // the snapshot lane, and the next turns send and read as usual.
    let snapped = if sent.is_ok() && read.is_ok() {
        snapshot::run(&ctx, client, host).await
    } else {
        Ok(())
    };
    read.map(|_| ()).and(sent).and(swept).and(snapped)
}

/// The device streams of the owner on the relay. `None` when the relay does not
/// know the database yet, which is the state of an owner who has not written
/// anywhere. A payment refusal or a home that already holds stream heads must not
/// treat `HTTP 403` as an empty relay.
async fn list_streams(
    ctx: &Ctx<'_>,
    client: &RelayClient,
) -> Result<Option<Vec<RelayStream>>, String> {
    match client.streams(&ctx.database_id, &ctx.node_hex).await {
        Ok(streams) => Ok(Some(streams)),
        Err(error) => {
            if is_unborn_403(&error) && !has_any_stream_head(ctx).await? {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

async fn has_any_stream_head(ctx: &Ctx<'_>) -> Result<bool, String> {
    ctx.host
        .db
        .mutation_manager()
        .has_any_replay_stream_head()
        .await
        .map_err(|error| format!("the stream heads could not be checked: {error}"))
}

pub(super) fn is_unborn_403(error: &str) -> bool {
    if !error.contains("HTTP 403") || is_payment_403(error) {
        return false;
    }
    error
        .find('{')
        .and_then(|at| serde_json::from_str::<serde_json::Value>(&error[at..]).ok())
        .is_some_and(|body| body["code"].as_str() == Some("forbidden"))
}

fn is_payment_403(error: &str) -> bool {
    let body = error
        .split_once(": ")
        .map_or(error.as_bytes(), |(_, text)| text.as_bytes());
    crate::cloud::is_payment_subscription_error(403, body)
        || error.contains("payment.subscription")
        || error.contains(crate::cloud::PAYMENT_REQUIRED_MESSAGE)
}

/// True while the spool, a snapshot request, or a sweep holds work for the sync loop.
pub(super) fn pending(host: &Host) -> bool {
    let status = host.db.db_ops().native_runtime().accepted_status();
    let Ok(database_id) = host.owned.owner().map(|owner| owner.id) else {
        return false;
    };
    status.head > status.acked
        || status.snapshot_required
        // A request or sweep that waits behind a running snapshot does not wake the loop: it
        // would spin until the upload ends. The end of the lane is the next turn.
        || (!snapshot_lane::busy_for(&host.home)
            && (snapshot_state::requested(&host.home, &database_id)
                || reclaim::pending(&host.home, &database_id)))
}

/// What a restore found.
pub struct RestoreReport {
    /// The id of the personal database.
    pub database_id: String,
    /// Entries applied to the restored home.
    pub replayed: u64,
    /// The highest sequence number among them.
    pub through: u64,
    /// Device streams that were read, not counting the stream of the birth.
    pub devices: usize,
}

/// Rebuild the personal database of `host` from the relay.
///
/// `host` is a fresh home made from the recovery phrase. It keeps the device id
/// that this call makes, and signs on that stream from then on. When `base` is
/// given, the files of the newest snapshot are already installed in the home:
/// the call gives the store its own author stream, checks that the store holds
/// exactly the stream positions that the snapshot entry names, and then reads
/// each stream after its position. With no base it reads every stream from the
/// first entry.
pub async fn restore_personal_database(
    host: &Host,
    relay_url: &str,
    base: Option<&SnapshotBase>,
) -> Result<RestoreReport, String> {
    let ctx = Ctx::open(host)?;
    if let Some(base) = base {
        snapshot_restore::adopt(&ctx, base).await?;
    }
    let client = RelayClient::for_host(host, relay_url)?;
    let streams = list_streams(&ctx, &client)
        .await?
        .ok_or("the relay holds no personal database for this recovery phrase")?;
    let applied = incoming::run(&ctx, &client, &streams).await?;
    Ok(RestoreReport {
        database_id: ctx.database_id,
        replayed: applied.entries,
        through: applied.through,
        devices: applied.streams,
    })
}
