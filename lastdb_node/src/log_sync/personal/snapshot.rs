//! Take a snapshot of the personal database and name it on the relay.
//!
//! A snapshot is the checkpoint of the signed log. It is the native files of the
//! store, frozen at one write boundary, and the stream positions that the store
//! holds at that boundary. A new device installs the files and reads each stream
//! after its position, so it does not replay the history.
//!
//! The steps, in the order that a crash leaves nothing half named:
//!
//! 1. Freeze. The store holds its write gate, checks that every accepted write is
//!    acknowledged by the relay and settled, pins the files, and reads the head
//!    rows of the streams. No commit runs in between, so the cut is exact.
//! 2. Upload each file that the previous snapshot of this device did not hold, as
//!    a blob named by its digest, then the manifest as a blob.
//! 3. Sign a `snapshot` entry on the snapshot stream and append it. Only now does
//!    the relay name the snapshot. A file or manifest that is on the relay without
//!    such an entry is unused and harmless.
//!
//! The files are encrypted at rest by the store (the keys come from the node
//! seed), so they are uploaded as they are. The relay sees file names, lengths
//! and digests in the manifest, and stream positions in the entry. It sees no
//! key, value or schema name.
//!
//! When a snapshot is taken: on request (`lastdb backup snapshot`), and
//! automatically once [`AUTO_SNAPSHOT_ENTRIES`] entries have been signed on this
//! device's stream since the last snapshot it took.

use std::io::Seek;
use std::path::Path;
use std::time::Instant;

use fold_db::storage::disk_pack::{snapshot::DatabaseSnapshot, NativeCloudWorkGuard};
use mutation_log::{
    author_snapshot, entry_hash, AuthorCtx, DeviceId, EntryHash, Hex32, SignedEntry, SnapshotBody,
    SnapshotThrough, StreamHead,
};
use sha2::{Digest, Sha256};

use super::snapshot_manifest::{self as manifest, FileEntry, Manifest};
use super::snapshot_state::{file_of, finish_request, load_last, Last, Report};
use super::{list_streams, Ctx};
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

/// A routine snapshot is taken once this many entries have been signed on this
/// device's stream since its last snapshot. This is the one place the number is set.
pub(super) const AUTO_SNAPSHOT_ENTRIES: u64 = 25_000;

enum Outcome {
    /// A write is not yet on the relay or not yet settled. Try again next turn.
    Waiting,
    Done(Report),
}

/// Take a snapshot if one was requested or the policy says one is due.
pub(super) async fn run(ctx: &Ctx<'_>, client: &RelayClient) -> Result<(), String> {
    let home = &ctx.host.home;
    let request = disk::read_limited(&file_of(home, &ctx.database_id, "snapshot-request"), 64)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok());
    if request.is_none() && !auto_due(ctx).await? {
        return Ok(());
    }
    match publish(ctx, client, false).await {
        Ok(Outcome::Waiting) => Ok(()),
        Ok(Outcome::Done(report)) => {
            tracing::info!(target: "lastdbd::log_sync", seq = report.snapshot_seq, files = report.files, uploaded = report.uploaded_files, "snapshot published");
            if let Some(nonce) = request {
                finish_request(ctx, &nonce, &report)?;
            }
            Ok(())
        }
        Err(error) => {
            if request.is_some() {
                let _ = disk::write_private(
                    &file_of(home, &ctx.database_id, "snapshot-error"),
                    error.as_bytes(),
                );
                let _ = disk::remove_private(&file_of(home, &ctx.database_id, "snapshot-request"));
            }
            Err(error)
        }
    }
}

async fn auto_due(ctx: &Ctx<'_>) -> Result<bool, String> {
    let own = ctx.own_head().await?.map_or(0, |head| head.seq);
    let last = load_last(&ctx.host.home, &ctx.database_id)?.map_or(0, |last| last.own_seq);
    Ok(own.saturating_sub(last) >= AUTO_SNAPSHOT_ENTRIES)
}

/// Publish the first snapshot of a home that held data before it began to capture writes.
///
/// The cut is the preserved copy of the files from before the first captured write (see
/// `PackRuntime::begin_first_snapshot`), so it holds no stream position. Every write since is
/// in the spool, and the spool is not read for upload until this snapshot is named on the
/// relay, so each earlier write is in the snapshot and each later one is sent as an entry,
/// never both and never neither. After the entry is on the relay the node records that the
/// history is covered, which opens the spool, and removes the preserved copy. A kill at any
/// point leaves the copy, and the next turn runs this again: the same cut gives the same
/// files, so the next turn finds the saved record and names nothing twice.
pub(super) async fn run_first(ctx: &Ctx<'_>, client: &RelayClient) -> Result<(), String> {
    let Outcome::Done(report) = publish(ctx, client, true).await? else {
        return Err("the preserved files of the first snapshot are missing".into());
    };
    let opened = ctx.runtime.accepted_status().opened_frontier;
    ctx.runtime
        .note_accepted_snapshot(opened)
        .map_err(|error| format!("the first snapshot could not be recorded: {error:?}"))?;
    ctx.runtime
        .release_initial_snapshot()
        .map_err(|error| format!("the preserved files could not be removed: {error}"))?;
    tracing::info!(target: "lastdbd::log_sync", seq = report.snapshot_seq, files = report.files, "first snapshot published");
    Ok(())
}

async fn publish(ctx: &Ctx<'_>, client: &RelayClient, first: bool) -> Result<Outcome, String> {
    let _work = NativeCloudWorkGuard::acquire().map_err(|error| error.to_string())?;
    let started = Instant::now();
    let db = ctx.host.db.db_ops();
    let captured = if first {
        db.capture_initial_snapshot().await
    } else {
        db.capture_synced_snapshot().await
    };
    let Some(cut) =
        captured.map_err(|error| format!("cannot freeze the database files: {error}"))?
    else {
        return Ok(Outcome::Waiting);
    };
    let through = through_of(ctx, &cut.heads)?;
    let (cut, files) = tokio::task::spawn_blocking(move || {
        let files = manifest::inventory(&cut)?;
        Ok::<_, String>((cut, files))
    })
    .await
    .map_err(|_| "the snapshot inventory stopped".to_owned())??;
    let capture_ms = elapsed_ms(started);
    let previous = load_last(&ctx.host.home, &ctx.database_id)?;
    if let Some(last) = previous
        .as_ref()
        .filter(|last| last.manifest.files == files && last.through == through)
    {
        // Nothing changed since the last snapshot. It still names the state.
        return Ok(Outcome::Done(last.report.clone()));
    }
    let uploading = Instant::now();
    let streams = list_streams(ctx, client).await?.unwrap_or_default();
    ensure_birth(ctx, client, &streams).await?;
    let (uploaded_files, uploaded_bytes) =
        upload_files(ctx, client, &cut, &files, previous.as_ref()).await?;
    let snapshot_bytes = files.iter().map(|file| file.bytes).sum();
    let list = Manifest {
        upload_id: uuid::Uuid::new_v4().to_string(),
        files,
    };
    drop(cut);
    let (sha256, length) = put_manifest(ctx, client, &list).await?;
    let own_seq = ctx.own_head().await?.map_or(0, |head| head.seq);
    let entry = sign(ctx, client, &streams, &sha256, length, &through).await?;
    let stored = client
        .append(&ctx.database_id, std::slice::from_ref(&entry))
        .await?;
    if stored.0 + stored.1 != 1 {
        return Err("the relay did not take the snapshot entry".into());
    }
    let report = Report {
        snapshot_seq: entry.header().writer_seq,
        files: list.files.len(),
        snapshot_bytes,
        uploaded_files,
        uploaded_bytes,
        streams: through.len(),
        capture_ms,
        upload_ms: elapsed_ms(uploading),
    };
    let last = Last {
        own_seq,
        through,
        manifest: list,
        report: report.clone(),
    };
    let bytes = serde_json::to_vec(&last).map_err(|_| "cannot encode the snapshot record")?;
    disk::write_private(
        &file_of(&ctx.host.home, &ctx.database_id, "snapshot-last.json"),
        &bytes,
    )
    .map_err(|_| "cannot save the snapshot record".to_owned())?;
    Ok(Outcome::Done(report))
}

/// Upload each file of the cut that the previous snapshot of this device did not hold, as a
/// blob named by its digest. Returns how many files and bytes went up.
async fn upload_files(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    files: &[FileEntry],
    previous: Option<&Last>,
) -> Result<(usize, u64), String> {
    let known: Vec<(&str, &str)> = previous.map_or_else(Vec::new, |last| {
        last.manifest
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.sha256.as_str()))
            .collect()
    });
    let (mut uploaded_files, mut uploaded_bytes) = (0, 0);
    for entry in files {
        if known.contains(&(entry.path.as_str(), entry.sha256.as_str())) {
            continue;
        }
        let mut file = cut
            .open_file(Path::new(&entry.path))
            .map_err(|error| error.to_string())?;
        file.rewind().map_err(|error| error.to_string())?;
        client
            .blob_put_file(
                &ctx.database_id,
                &entry.sha256,
                tokio::fs::File::from_std(file),
            )
            .await?;
        uploaded_files += 1;
        uploaded_bytes += entry.bytes;
    }
    Ok((uploaded_files, uploaded_bytes))
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// The stream positions of a cut, ascending by device id.
fn through_of(
    ctx: &Ctx<'_>,
    heads: &[(
        String,
        fold_db::fold_db_core::mutation_manager::ReplayStreamHead,
    )],
) -> Result<Vec<SnapshotThrough>, String> {
    let prefix = format!("{}:", ctx.node_hex);
    let mut through = Vec::new();
    for (stream, head) in heads {
        let device = stream
            .strip_prefix(&prefix)
            .ok_or("a stream head row names another writer")?;
        if head.seq == 0 {
            continue;
        }
        through.push(SnapshotThrough {
            device_id: DeviceId::from_hex(device)
                .map_err(|_| "a stream head row has a bad device")?,
            writer_seq: head.seq,
            entry_hash: EntryHash::from_hex(&head.hash)
                .map_err(|_| "a stream head row has a bad hash")?,
            order: head.order,
        });
    }
    through.sort_by_key(|position| position.device_id.to_bytes());
    Ok(through)
}

/// The relay takes a blob only for a database whose birth it holds.
async fn ensure_birth(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    if streams
        .iter()
        .any(|stream| stream.device_id == ctx.birth_device_hex)
    {
        return Ok(());
    }
    let (stored, duplicate) = client
        .append(&ctx.database_id, std::slice::from_ref(&ctx.birth_entry))
        .await?;
    if stored + duplicate == 1 {
        Ok(())
    } else {
        Err("the relay did not take the birth entry".into())
    }
}

/// Upload the manifest as a blob. Returns its digest and length.
async fn put_manifest(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    list: &Manifest,
) -> Result<(String, u64), String> {
    let bytes = serde_json::to_vec(list).map_err(|_| "cannot encode the manifest")?;
    let sha256 = app_identity_crypto::hex_lower(&Sha256::digest(&bytes));
    let path = file_of(&ctx.host.home, &ctx.database_id, "snapshot-manifest");
    disk::write_private(&path, &bytes).map_err(|_| "cannot save the manifest".to_owned())?;
    let result = client.blob_put(&ctx.database_id, &sha256, &path).await;
    let _ = disk::remove_private(&path);
    result?;
    Ok((sha256, bytes.len() as u64))
}

/// The head of the snapshot stream on the relay, with the order value of its entry.
async fn snapshot_head(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<Option<StreamHead>, String> {
    let Some(listed) = streams
        .iter()
        .find(|stream| stream.device_id == ctx.snapshot_device_hex && stream.head_seq > 0)
    else {
        return Ok(None);
    };
    let (entries, _) = client
        .read(
            &ctx.database_id,
            &ctx.node_hex,
            Some(&ctx.snapshot_device_hex),
            listed.head_seq,
            None,
            1,
        )
        .await?;
    let entry = entries
        .first()
        .ok_or("the relay lacks the head of the snapshot stream")?;
    let hash = entry_hash(entry).map_err(|error| error.to_string())?;
    if entry.header().writer_seq != listed.head_seq || hash.to_hex() != listed.head_hash {
        return Err("the head of the snapshot stream differs from the listing".into());
    }
    Ok(Some(StreamHead {
        entry_hash: hash,
        writer_seq: listed.head_seq,
        lamport: entry.header().lamport,
    }))
}

/// Sign the `snapshot` entry as the next entry of the snapshot stream.
async fn sign(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    manifest_sha256: &str,
    manifest_bytes: u64,
    through: &[SnapshotThrough],
) -> Result<SignedEntry, String> {
    let head = snapshot_head(ctx, client, streams).await?;
    // The order value of the entry is above every position it covers, so it
    // follows the normal rule and a reader sees it as made after them.
    let floor = through
        .iter()
        .map(|position| position.order)
        .max()
        .unwrap_or(0)
        + 1;
    let author = AuthorCtx {
        database_id: ctx.database,
        node_key: ctx.identity.node_key,
        device_id: ctx.snapshot_device,
        stream_head: head,
        seen_heads: vec![ctx.birth_hash],
        seen_lamports: vec![ctx.birth.position.lamport],
        authored_at_ms: fold_db::clock::unix_millis().max(floor),
    };
    let body = SnapshotBody {
        manifest_sha256: Hex32::from_hex(manifest_sha256).map_err(|_| "manifest digest")?,
        manifest_bytes,
        through: through.to_vec(),
    };
    author_snapshot(&author, &ctx.identity.signing_key, body).map_err(|error| error.to_string())
}
