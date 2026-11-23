//! Install the newest snapshot of the personal database into a new home.
//!
//! A restore lists the owner's streams on the relay. When the snapshot stream has
//! an entry, the newest one is read and checked: its signature is the owner's, it
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
use mutation_log::{
    entry_hash, verify_entry_signature, DatabaseId, DeviceId, NodeKey, SignedEntry,
    SnapshotThrough, LAMPORT_LIMIT,
};

use super::snapshot_manifest::{self as manifest, Manifest, MANIFEST_LIMIT};
use super::{birth_of, Ctx};
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
    let snapshot_device = DeviceId::from_bytes(personal_identity::snapshot_device(seed));
    let streams = match client.streams(&database_id, &node_hex).await {
        Ok(streams) => streams,
        // The relay does not know a database whose birth it does not hold.
        Err(error) if error.contains("HTTP 403") => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(listed) = streams
        .iter()
        .find(|stream| stream.device_id == snapshot_device.to_hex() && stream.head_seq > 0)
    else {
        return Ok(None);
    };
    let identity = AuthorIdentity {
        signing_key,
        node_key,
        device_id,
    };
    let (_, _, birth_hash) = birth_of(seed, &identity)?;
    let (entries, _) = client
        .read(
            &database_id,
            &node_hex,
            Some(&snapshot_device.to_hex()),
            listed.head_seq,
            None,
            1,
        )
        .await?;
    let entry = entries
        .first()
        .ok_or("the relay lacks its newest snapshot entry")?;
    let stream = Stream {
        node_key,
        database,
        device: snapshot_device,
        birth_hash,
        seq: listed.head_seq,
        hash: &listed.head_hash,
    };
    let (manifest_sha256, manifest_bytes, through) = check_entry(&stream, entry)?;
    let started = Instant::now();
    let list = fetch_manifest(
        &client,
        &database_id,
        home,
        &manifest_sha256,
        manifest_bytes,
    )
    .await?;
    let data = home.join("data");
    let installed = install_files(&client, &database_id, &data, &list).await;
    if let Err(error) = installed {
        let _ = manifest::remove_tree(&data);
        return Err(error);
    }
    Ok(Some(SnapshotBase {
        snapshot_seq: listed.head_seq,
        files: list.files.len(),
        bytes: list.bytes(),
        download_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        through,
    }))
}

/// What the newest entry of the snapshot stream must be.
struct Stream<'a> {
    node_key: NodeKey,
    database: DatabaseId,
    device: DeviceId,
    birth_hash: mutation_log::EntryHash,
    seq: u64,
    hash: &'a str,
}

/// Check the entry and return its manifest digest, manifest length and positions.
fn check_entry(
    stream: &Stream<'_>,
    entry: &SignedEntry,
) -> Result<(String, u64, Vec<SnapshotThrough>), String> {
    let SignedEntry::Snapshot { header, body, .. } = entry else {
        return Err("the newest entry of the snapshot stream is not a snapshot entry".into());
    };
    verify_entry_signature(entry).map_err(|error| format!("the snapshot entry: {error}"))?;
    let hash = entry_hash(entry).map_err(|error| error.to_string())?;
    let valid = header.node_key == stream.node_key
        && header.database_id == stream.database
        && header.device_id == stream.device
        && header.writer_seq == stream.seq
        && hash.to_hex() == stream.hash
        && header.seen_heads == [stream.birth_hash]
        && header.lamport < LAMPORT_LIMIT
        && body.manifest_bytes > 0
        && body.manifest_bytes <= MANIFEST_LIMIT;
    if !valid {
        return Err("the newest snapshot entry is not the owner's entry of this stream".into());
    }
    Ok((
        body.manifest_sha256.to_hex(),
        body.manifest_bytes,
        body.through.clone(),
    ))
}

async fn fetch_manifest(
    client: &RelayClient,
    database_id: &str,
    home: &Path,
    sha256: &str,
    bytes: u64,
) -> Result<Manifest, String> {
    let dest = home.join(".snapshot-manifest");
    client.blob_get(database_id, sha256, bytes, &dest).await?;
    let read = std::fs::read(&dest).map_err(|error| error.to_string());
    let _ = std::fs::remove_file(&dest);
    let list: Manifest = serde_json::from_slice(&read?).map_err(|_| "the manifest is invalid")?;
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
    Ok(())
}
