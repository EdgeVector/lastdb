//! The personal database on the signed log.
//!
//! Every device of one owner signs its own stream under the one node key, and
//! the relay holds one stream for each device. A device does two things each
//! turn, and both use the machinery of the store:
//!
//! - [`outgoing`] signs the writes of the spool of accepted writes as the next
//!   entries of its own stream and sends them. The spool is the upload queue. It
//!   trims when the relay acknowledges.
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
mod body;
mod files;
mod incoming;
mod outgoing;
mod saved;
mod snapshot;
mod snapshot_known;
mod snapshot_manifest;
mod snapshot_restore;
mod snapshot_state;
mod snapshot_upload;
mod verify;

pub(crate) use snapshot_known::known as known_snapshot;
pub use snapshot_restore::{install_newest_snapshot, SnapshotBase};
pub(crate) use snapshot_state::{request as request_snapshot, status as snapshot_status};

use std::sync::Arc;

use cloud_backup_protocol::{AccountId, CloudScope, DecimalU64};
use fold_db::fold_db_core::mutation_manager::ReplayStreamHead;
use fold_db::storage::disk_backup::accepted::UploadIdentity;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{
    position_of, AcceptedMeta, AuthorCtx, DatabaseId, DeviceId, EntryHash, SignedEntry, StreamHead,
};
use zeroize::Zeroizing;

use super::birth;
use super::chain_author::{self, AuthorIdentity};
use super::relay::{RelayClient, StreamHead as RelayStream};
use super::sealed::Sealer;
use crate::host::Host;
use crate::personal_identity;

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
    pub(super) content_key: Zeroizing<[u8; 32]>,
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
        let content_key = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| "the content key cannot be derived".to_owned())?
            .encryption_key();
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
            content_key: Zeroizing::new(content_key),
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

/// The birth entry of the personal database of the node with this seed, as an
/// accepted entry, and its hash. It is the same on every device.
fn birth_of(
    seed: &[u8; 32],
    identity: &AuthorIdentity,
) -> Result<(SignedEntry, AcceptedMeta, EntryHash), String> {
    let database = DatabaseId::from_hex(&personal_identity::database_id(seed))
        .map_err(|_| "database id is not hex")?;
    let birth_ctx = AuthorCtx {
        database_id: database,
        node_key: identity.node_key,
        device_id: DeviceId::from_bytes(personal_identity::birth_device(seed)),
        stream_head: None,
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: 0,
    };
    let entry = birth::entry(&birth::derived(seed), &birth_ctx, identity)?;
    let hash = mutation_log::entry_hash(&entry).map_err(|e| e.to_string())?;
    let meta = AcceptedMeta {
        position: position_of(&entry),
        is_publisher: true,
        grant_body: None,
        entry: entry.clone(),
    };
    Ok((entry, meta, hash))
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
pub(super) async fn turn(host: &Host, client: &RelayClient) -> Option<String> {
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

async fn run_turn(host: &Host, client: &RelayClient) -> Result<(), String> {
    let ctx = Ctx::open(host)?;
    let streams = list_streams(&ctx, client).await?.unwrap_or_default();
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
    // A failure of the sender must not stop the incoming side, or the reverse.
    let sent = outgoing::run(&ctx, client, &streams).await;
    let read = incoming::run(&ctx, client, &streams).await;
    // A snapshot is taken only after a clean turn: the spool is on the relay and
    // the streams are applied, so the cut holds exactly what the relay holds.
    let snapped = if sent.is_ok() && read.is_ok() {
        snapshot::run(&ctx, client).await
    } else {
        Ok(())
    };
    sent.and(read.map(|_| ())).and(snapped)
}

/// The device streams of the owner on the relay. `None` when the relay does not
/// know the database yet, which is the state of an owner who has not written
/// anywhere. A device that has uploaded before must not see that answer.
async fn list_streams(
    ctx: &Ctx<'_>,
    client: &RelayClient,
) -> Result<Option<Vec<RelayStream>>, String> {
    match client.streams(&ctx.database_id, &ctx.node_hex).await {
        Ok(streams) => Ok(Some(streams)),
        Err(error) => {
            if error.contains("HTTP 403") && ctx.own_head().await?.is_none() {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

/// True while the spool or the saved batch holds something the relay has not
/// acknowledged. The loop keeps waking while this is true.
pub(super) fn pending(host: &Host) -> bool {
    let status = host.db.db_ops().native_runtime().accepted_status();
    let Ok(database_id) = host.owned.owner().map(|owner| owner.id) else {
        return false;
    };
    status.head > status.acked
        || status.snapshot_required
        || !matches!(saved::load(&host.home, &database_id), Ok(None))
        || snapshot_state::requested(&host.home, &database_id)
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
    let client = RelayClient::for_host(host, relay_url)?;
    let ctx = Ctx::open(host)?;
    if let Some(base) = base {
        snapshot_restore::adopt(&ctx, base).await?;
    }
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
