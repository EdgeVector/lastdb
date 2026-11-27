//! Install the newest snapshot of the personal database into a new home.
//!
//! A restore lists the owner's streams on the relay. When the snapshot stream has
//! an entry, the newest one is read and checked (the relay keeps that one only): its signature is the owner's, it
//! is on the snapshot stream of this database, and it cites the birth. The
//! manifest it names is fetched and checked against the digest and length in the
//! entry, and each file is fetched and checked against the digest and length in
//! the manifest. The files go into the data directory of the new home while no
//! store is open on it. The home itself is staged by the caller and published by
//! one rename, so a kill at any point leaves no home at the destination or a
//! complete one, never a mix.
//!
//! After the store opens on the installed files, [`adopt`] gives it its own
//! author stream and checks that its head rows are exactly the positions the
//! entry names. The first entry read on each stream after that must chain to the
//! hash held in the head row, which the ordinary check of an incoming entry does.

use std::path::Path;
use std::time::Instant;

use app_identity_crypto::SigningKey;
use mutation_log::{DatabaseId, DeviceId, NodeKey, SnapshotThrough};

use super::snapshot_entry::{read_verified_head, SnapshotStream};
use super::snapshot_known::{self as known, Taken};
use super::snapshot_manifest::{self as manifest, Manifest, MANIFEST_LIMIT};
use super::{birth_of, Ctx};
use crate::host::cloud_sync::SnapshotState;
use crate::log_sync::chain_author::AuthorIdentity;
use crate::log_sync::device;
use crate::log_sync::relay::RelayClient;
use crate::personal_identity;

/// A snapshot whose files are installed in a home that has no store open yet.
pub struct SnapshotBase {
    /// The sequence number of the `snapshot` entry on the snapshot stream.
    pub snapshot_seq: u64,
    /// Files installed.
    pub files: usize,
    /// Total length of the files.
    pub bytes: u64,
    /// Milliseconds spent fetching and installing the files.
    pub download_ms: u64,
    through: Vec<SnapshotThrough>,
    /// When the snapshot entry was signed, the digest of its manifest, and the relay it came from.
    taken_at_ms: u64,
    manifest_sha256: String,
    relay: String,
}

impl SnapshotBase {
    /// The number of streams the snapshot covers.
    #[must_use]
    pub fn streams(&self) -> usize {
        self.through.len()
    }
}

/// Fetch the newest snapshot of the personal database of the node with `seed`
/// and install its files into `home/data`. `None` when the relay holds none, and
/// then nothing is written. `home` must not have a `data` directory.
pub async fn install_newest_snapshot(
    home: &Path,
    seed: &[u8; 32],
    relay_url: &str,
) -> Result<Option<SnapshotBase>, String> {
    let signing_key = SigningKey::from_bytes(seed);
    let node_key = NodeKey::from_bytes(signing_key.verifying_key().to_bytes());
    let node_hex = node_key.to_hex();
    let database_id = personal_identity::database_id(seed);
    let database = DatabaseId::from_hex(&database_id).map_err(|_| "database id is not hex")?;
    let device_id = device::device_id(home)?;
    let client = RelayClient::new(
        relay_url,
        SigningKey::from_bytes(seed),
        node_hex.clone(),
        &device_id,
    )?;
    let streams = match client.streams(&database_id, &node_hex).await {
        Ok(streams) => streams,
        // The relay does not know a database whose birth it does not hold.
        Err(error) if super::is_unborn_403(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let identity = AuthorIdentity {
        signing_key,
        node_key,
        device_id,
    };
    let (_, _, birth_hash) = birth_of(seed, &identity)?;
    let snapshot_device = DeviceId::from_bytes(personal_identity::snapshot_device(seed));
    let snapshot_device_hex = snapshot_device.to_hex();
    let stream = SnapshotStream {
        database_id: &database_id,
        database,
        node_hex: &node_hex,
        node_key,
        device: snapshot_device,
        device_hex: &snapshot_device_hex,
        birth_hash,
    };
    // The relay keeps one snapshot: the newest. There is no older one to fall back to.
    let Some(verified) = read_verified_head(&stream, &client, &streams).await? else {
        return Ok(None);
    };
    let started = Instant::now();
    let manifest_sha256 = verified.body.manifest_sha256.to_hex();
    let list = fetch_manifest(
        &client,
        &database_id,
        &manifest_sha256,
        verified.body.manifest_bytes,
    )
    .await?;
    let data = home.join("data");
    let _ = manifest::remove_tree(&data);
    if let Err(error) = install_files(&client, &database_id, &data, &list).await {
        let _ = manifest::remove_tree(&data);
        return Err(error);
    }
    Ok(Some(SnapshotBase {
        snapshot_seq: verified.head.writer_seq,
        files: list.files.len(),
        bytes: list.bytes(),
        download_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        through: verified.body.through,
        taken_at_ms: verified.authored_at_ms,
        manifest_sha256,
        relay: relay_url.to_owned(),
    }))
}

/// Fetch the manifest `sha256` of `bytes` bytes from the relay in memory and check its files.
pub(super) async fn fetch_manifest(
    client: &RelayClient,
    database_id: &str,
    sha256: &str,
    bytes: u64,
) -> Result<Manifest, String> {
    if bytes == 0 || bytes > MANIFEST_LIMIT {
        return Err("the snapshot entry names a manifest of an unusable length".into());
    }
    let raw = client.blob_get_bytes(database_id, sha256, bytes).await?;
    let list: Manifest = serde_json::from_slice(&raw).map_err(|_| "the manifest is invalid")?;
    manifest::validate(&list.files)?;
    Ok(list)
}

/// Fetch every file of the manifest into `data`, checking length and digest.
async fn install_files(
    client: &RelayClient,
    database_id: &str,
    data: &Path,
    list: &Manifest,
) -> Result<(), String> {
    manifest::create_layout(data).map_err(|error| error.to_string())?;
    for file in &list.files {
        client
            .blob_get(
                database_id,
                &file.sha256,
                file.bytes,
                &data.join(&file.path),
            )
            .await?;
    }
    manifest::sync_layout(data).map_err(|error| error.to_string())
}

/// Give the restored store its own author stream and check that it holds exactly
/// the stream positions that the snapshot entry names.
pub(super) async fn adopt(ctx: &Ctx<'_>, base: &SnapshotBase) -> Result<(), String> {
    ctx.runtime
        .adopt_restored_snapshot()
        .await
        .map_err(|error| {
            format!("the restored store cannot start its own author stream: {error}")
        })?;
    let names: Vec<String> = base
        .through
        .iter()
        .map(|position| ctx.stream_name(&position.device_id.to_hex()))
        .collect();
    let heads = ctx
        .host
        .db
        .mutation_manager()
        .replay_stream_heads(&names)
        .await
        .map_err(|error| format!("the stream heads could not be read: {error}"))?;
    for (position, head) in base.through.iter().zip(heads) {
        let same = head.is_some_and(|held| {
            held.seq == position.writer_seq
                && held.hash == position.entry_hash.to_hex()
                && held.order == position.order
        });
        if !same {
            return Err(
                "the installed snapshot does not hold the stream positions its entry names".into(),
            );
        }
    }
    known::record(
        ctx,
        &Taken {
            state: SnapshotState::Installed,
            relay_base: &base.relay,
            taken_at_ms: base.taken_at_ms,
            files: base.files,
            bytes: base.bytes,
            manifest_sha256: &base.manifest_sha256,
            snapshot_seq: base.snapshot_seq,
            through: &base.through,
            device: ctx.identity.device_id,
        },
    );
    Ok(())
}
