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
//!    rows of the streams. No commit runs in between, so the cut is exact. This is the
//!    only step that needs a quiet instant, and it runs inside a sync turn. The pinned
//!    files and the positions stay valid however many writes follow.
//!
//! Steps 2 to 4 are slow and run in the snapshot lane (`snapshot_lane`), so entries are still
//! sent and acknowledged and the other devices are still read while they run. The first
//! snapshot of a home that held data before it began to capture runs them inside the turn,
//! because the spool is not sent until that snapshot is named.
//!
//! 2. Hash the files, and upload each file that the previous snapshot of this device did not hold, as
//!    a blob named by its digest, then the manifest as a blob.
//! 3. Sign a `snapshot` entry on the snapshot stream and append it. Only now does
//!    the relay name the snapshot. A file or manifest that is on the relay without
//!    such an entry is unused and harmless.
//! 4. Ask the relay to drop what the second newest snapshot covers (`trim_covered`). The relay
//!    keeps the two newest snapshots, so a restore from either still finds every entry it
//!    reads, and a failed or unknown trim only leaves the relay larger.
//!
//! The files are encrypted at rest by the store (the keys come from the node
//! seed), so they are uploaded as they are. The relay sees file names, lengths
//! and digests in the manifest, and stream positions in the entry. It sees no
//! key, value or schema name.
//!
//! When a snapshot is taken: on request (`lastdb backup snapshot`), and
//! automatically once [`AUTO_SNAPSHOT_ENTRIES`] entries have been signed on this
//! device's stream since the last snapshot it took.

use std::sync::Arc;
use std::time::Instant;

use fold_db::storage::disk_pack::snapshot::DatabaseSnapshot;
use fold_db::storage::disk_pack::NativeCloudWorkGuard;
use mutation_log::{
    author_snapshot, entry_hash, AuthorCtx, DeviceId, EntryHash, Hex32, SignedEntry, SnapshotBody,
    SnapshotThrough, StreamHead,
};

use super::snapshot_known::{self as known, Taken};
use super::snapshot_lane as lane;
use super::snapshot_manifest::{self as manifest, Manifest};
use super::snapshot_state::{fail_request, file_of, finish_request, load_last, Last, Report};
use super::snapshot_upload::{elapsed_ms, ensure_birth, put_manifest, upload_files, TOO_LARGE};
use super::{list_streams, Ctx};
use crate::host::cloud_sync::SnapshotState;
use crate::host::Host;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

/// A routine snapshot is taken once this many entries have been signed on this
/// device's stream since its last snapshot. This is the one place the number is set.
pub(super) const AUTO_SNAPSHOT_ENTRIES: u64 = 25_000;

/// Calls of one trim. The relay does a bounded amount of work in a call and says whether
/// it is done, so a large backlog takes several calls. What is left goes on at the next snapshot.
const TRIM_CALLS: usize = 64;

/// The files and positions of one frozen cut, and what the upload needs to finish and to
/// leave the status as it found it.
struct Frozen {
    cut: DatabaseSnapshot,
    through: Vec<SnapshotThrough>,
    started: Instant,
    prior: (SnapshotState, Option<String>),
    _work: NativeCloudWorkGuard,
}

/// Take a snapshot if one was requested or the policy says one is due. The freeze happens
/// here, inside the turn. The upload goes on in the lane, and the turn returns at once.
///
/// While the lane runs, a new request is not joined to it: its cut is older than the request.
/// The request file stays, and the turn after the lane ends takes a fresh snapshot for it.
pub(super) async fn run(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    host: &Arc<Host>,
) -> Result<(), String> {
    if lane::busy() {
        return Ok(());
    }
    let home = &ctx.host.home;
    let request = disk::read_limited(&file_of(home, &ctx.database_id, "snapshot-request"), 64)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok());
    if request.is_none() && (known::waiting().is_some() || !auto_due(ctx).await?) {
        return Ok(());
    }
    let frozen = match freeze(ctx, false).await {
        Ok(Some(frozen)) => frozen,
        // A write is not yet on the relay or not yet settled. Try again next turn.
        Ok(None) => return Ok(()),
        Err(error) => return failed(ctx, request.as_deref(), error),
    };
    ctx.host
        .cloud_sync
        .mark_snapshot(SnapshotState::InProgress, Some("hashing the files".into()));
    let (host, base) = (Arc::clone(host), client.base.clone());
    lane::start(async move { upload(host, base, frozen, request).await });
    Ok(())
}

/// The lane: hash, upload, name and trim one frozen snapshot, then record how it ended. It
/// returns nothing, because no turn waits for it.
async fn upload(host: Arc<Host>, base: String, frozen: Frozen, request: Option<String>) {
    let prior = frozen.prior.clone();
    let opened =
        Ctx::open(&host).and_then(|ctx| RelayClient::for_host(&host, &base).map(|c| (ctx, c)));
    let (ctx, client) = match opened {
        Ok(opened) => opened,
        Err(error) => {
            host.cloud_sync.mark_snapshot(prior.0, prior.1);
            tracing::warn!(target: "lastdbd::log_sync", %error, "snapshot upload could not start");
            return;
        }
    };
    match publish_cut(&ctx, &client, frozen).await {
        Ok((report, fresh)) => {
            tracing::info!(target: "lastdbd::log_sync", seq = report.snapshot_seq, files = report.files, uploaded = report.uploaded_files, "snapshot published");
            if !fresh {
                host.cloud_sync.mark_snapshot(prior.0, prior.1);
            }
            // The trim comes before the answer, so a person who waits for the snapshot finds the
            // relay already trimmed. A stop of the lane during the trim leaves the request in
            // place, and the next start takes the snapshot again; one of unchanged files names nothing twice.
            if fresh {
                trim_covered(&ctx, &client, report.snapshot_seq).await;
            }
            if let Some(nonce) = &request {
                if let Err(error) = finish_request(&ctx, nonce, &report) {
                    tracing::warn!(target: "lastdbd::log_sync", %error, "snapshot request could not be answered");
                }
            }
        }
        Err(error) => {
            host.cloud_sync.mark_snapshot(prior.0, prior.1);
            let _ = failed(&ctx, request.as_deref(), error.clone());
            if !error.starts_with(TOO_LARGE) {
                tracing::warn!(target: "lastdbd::log_sync", %error, "snapshot failed");
            }
        }
    }
}

/// A snapshot failed. The request it served, if it is still the current one, learns why and
/// ends. A relay limit is not a failed turn: entries and writes go on, and the status names
/// the problem. No new automatic attempt starts for a while.
fn failed(ctx: &Ctx<'_>, request: Option<&str>, error: String) -> Result<(), String> {
    if let Some(nonce) = request {
        fail_request(ctx, nonce, &error);
    }
    if error.starts_with(TOO_LARGE) {
        known::refused(ctx, &error);
        return Ok(());
    }
    Err(error)
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
    if let Some(message) = known::waiting() {
        return Err(message);
    }
    ctx.host
        .cloud_sync
        .mark_snapshot(SnapshotState::InProgress, None);
    let Some(frozen) = freeze(ctx, true).await? else {
        return Err("the preserved files of the first snapshot are missing".into());
    };
    let published = publish_cut(ctx, client, frozen).await;
    if let Err(error) = &published {
        if error.starts_with(TOO_LARGE) {
            // Until the first snapshot is named, the spool stays unsent: this home holds
            // earlier data that only the snapshot carries. The turn reports the refusal.
            known::refused(ctx, error);
        }
    }
    let (report, fresh) = published?;
    if fresh {
        trim_covered(ctx, client, report.snapshot_seq).await;
    }
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

/// Freeze the files at one write boundary. `None` when a write is not yet on the relay or not
/// yet settled, or when the preserved files of a first snapshot are gone.
async fn freeze(ctx: &Ctx<'_>, first: bool) -> Result<Option<Frozen>, String> {
    let work = NativeCloudWorkGuard::acquire().map_err(|error| error.to_string())?;
    let started = Instant::now();
    let held = ctx.host.cloud_sync.snapshot().snapshot;
    let db = ctx.host.db.db_ops();
    let captured = if first {
        db.capture_initial_snapshot().await
    } else {
        db.capture_synced_snapshot().await
    };
    let Some(cut) =
        captured.map_err(|error| format!("cannot freeze the database files: {error}"))?
    else {
        return Ok(None);
    };
    Ok(Some(Frozen {
        through: through_of(ctx, &cut.heads)?,
        cut,
        started,
        prior: (held.state, held.message),
        _work: work,
    }))
}

/// Hash, upload and name a frozen snapshot. Returns its report, and whether it is a new
/// snapshot (false when nothing changed since the last one, which still names the state).
///
/// Nothing here reads a position from the live store: the positions are those of the cut. After
/// the append of the `snapshot` entry there is no await until the records are written, so a
/// stop of the lane never falls between the relay naming the snapshot and the node knowing.
async fn publish_cut(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    frozen: Frozen,
) -> Result<(Report, bool), String> {
    let Frozen {
        cut,
        through,
        started,
        ..
    } = frozen;
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
        return Ok((last.report.clone(), false));
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
    // The upload may have taken minutes. Another device of this owner signs the same stream, so
    // the head of the snapshot stream is read again here, not taken from before the upload.
    let streams = list_streams(ctx, client).await?.unwrap_or_default();
    let entry = sign(ctx, client, &streams, &sha256, length, &through).await?;
    let stored = client
        .append(&ctx.database_id, std::slice::from_ref(&entry))
        .await?;
    if stored.0 + stored.1 != 1 {
        return Err("the relay did not take the snapshot entry".into());
    }
    // No await from here to the end of the function.
    let own_seq = through
        .iter()
        .find(|position| position.device_id == ctx.identity.device_id)
        .map_or(0, |position| position.writer_seq);
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
    known::record(
        ctx,
        &Taken {
            state: SnapshotState::Published,
            relay_base: &client.base,
            taken_at_ms: entry.header().authored_at_ms,
            files: last.manifest.files.len(),
            bytes: last.manifest.bytes(),
            manifest_sha256: &sha256,
            snapshot_seq: report.snapshot_seq,
            through: &last.through,
            device: ctx.identity.device_id,
        },
    );
    Ok((report, true))
}

/// Keep the two newest snapshots on the relay and drop what the older of them covers: the
/// entries up to its positions and the snapshot entries before it. It never drops a file or a
/// manifest. The relay reads that snapshot entry itself and checks it. A failure is
/// logged and not an error of the snapshot: the snapshot is already named, nothing was lost,
/// and the next snapshot asks again. A relay without the route answers 404, which is the same.
async fn trim_covered(ctx: &Ctx<'_>, client: &RelayClient, newest: u64) {
    let Some(basis) = newest.checked_sub(1).filter(|basis| *basis >= 1) else {
        return;
    };
    for _ in 0..TRIM_CALLS {
        match client
            .trim(&ctx.database_id, &ctx.snapshot_device_hex, basis)
            .await
        {
            Ok(true) => {
                tracing::info!(target: "lastdbd::log_sync", basis, "the relay dropped what the snapshot covers");
                return;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(target: "lastdbd::log_sync", %error, "the relay could not drop what the snapshot covers");
                return;
            }
        }
    }
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
