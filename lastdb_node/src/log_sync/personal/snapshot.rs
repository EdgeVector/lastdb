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
//! Steps 2 to 7 are slow and run in the snapshot lane (`snapshot_lane`), so entries are still
//! sent and acknowledged and the other devices are still read while they run. The first
//! snapshot of a home that held data before it began to capture runs them inside the turn,
//! because the spool is not sent until that snapshot is named.
//!
//! 2. Hash the files of the cut.
//! 3. Ask the relay what it holds (`blob.list`), upload each file of the manifest that it lacks
//!    by digest and length, then the manifest. The relay is the truth for what is stored: a
//!    record of this device is not asked.
//! 4. Sign a `snapshot` entry on the snapshot stream and append it. Only now does the relay
//!    name the snapshot.
//! 5. Verify: ask the relay again. Every file of the manifest and the manifest are there with
//!    the right length, and one that is gone is uploaded again from the cut, which stays pinned
//!    until here. Only then is the snapshot recorded as published. A sweep that ran before the
//!    entry was appended may have dropped a file that nothing named yet. A sweep after it keeps
//!    them, because the relay drops nothing once the head of the snapshot stream moved. From the
//!    append to the record a mark is on disk (`snapshot-unverified`): if the check does not end,
//!    the next clean turn takes a snapshot again, so an incomplete one does not stay the newest.
//! 6. Ask the relay to drop what this snapshot covers (`trim_covered`): the entries up to its
//!    positions and the older snapshot entries. The relay then holds one snapshot. A failed or
//!    unknown trim only leaves the relay larger.
//! 7. Mark a sweep as due for this snapshot (`reclaim`). A sync turn then drops every stored
//!    file that this snapshot and the entries after it do not name: the files of the snapshot
//!    before, and the sealed files of the entries the trim dropped. While a new snapshot
//!    uploads, the relay holds the old and the new one, and at no other time.
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
use mutation_log::{SignedEntry, SnapshotThrough};

use super::reclaim;
use super::snapshot_entry::{
    require_covers_latest, require_empty_for_first, sign, through_of, trim_covered,
};
use super::snapshot_known as known;
use super::snapshot_lane as lane;
use super::snapshot_manifest::{self as manifest, Manifest};
use super::snapshot_state::{
    clear_unverified, fail_request, fail_request_in, finish_request, load_last, mark_unverified,
    read_request, record_published, unverified, Last, PublishedCut, Report,
};
use super::snapshot_upload::{
    elapsed_ms, ensure_birth, put_manifest, upload_files, verify_named, Named, TOO_LARGE,
};
use super::{list_streams, Ctx};
use crate::host::cloud_sync::SnapshotState;
use crate::host::Host;
use crate::log_sync::relay::RelayClient;

/// A routine snapshot is taken once this many entries have been signed on this
/// device's stream since its last snapshot. This is the one place the number is set.
pub(super) const AUTO_SNAPSHOT_ENTRIES: u64 = 25_000;

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
    let home = &ctx.host.home;
    if lane::busy_for(home) {
        return Ok(());
    }
    let request = read_request(home, &ctx.database_id);
    // A snapshot this device named and could not verify is taken again, as a due one is.
    if request.is_none()
        && (known::waiting(home).is_some()
            || !(unverified(home, &ctx.database_id) || auto_due(ctx, client).await?))
    {
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
    lane::start_for(home, move |lane_id| {
        Box::pin(async move {
            let home = host.home.clone();
            upload(host, base, frozen, request).await;
            lane::finish_for(&home, lane_id);
            crate::log_sync::wake::request_wake(&home);
        })
    });
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
            if let (Ok(owner), Some(nonce)) = (host.owned.owner(), request.as_deref()) {
                fail_request_in(&host.home, &owner.id, nonce, &error);
            }
            tracing::warn!(target: "lastdbd::log_sync", %error, "snapshot upload could not start");
            return;
        }
    };
    match publish_cut(&ctx, &client, frozen, false).await {
        Ok((report, fresh)) => {
            tracing::info!(target: "lastdbd::log_sync", seq = report.snapshot_seq, files = report.files, uploaded = report.uploaded_files, "snapshot published");
            if fresh {
                trim_covered(&ctx, &client, report.snapshot_seq).await;
                reclaim::mark_due(&ctx, &client.base, report.snapshot_seq);
            } else {
                host.cloud_sync.mark_snapshot(prior.0, prior.1);
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

async fn auto_due(ctx: &Ctx<'_>, client: &RelayClient) -> Result<bool, String> {
    let own = ctx.own_head().await?.map_or(0, |head| head.seq);
    let last = load_last(&ctx.host.home, &ctx.database_id)?
        .filter(|last| same_relay(ctx, client, last))
        .map_or(0, |last| last.own_seq);
    Ok(own.saturating_sub(last) >= AUTO_SNAPSHOT_ENTRIES)
}

fn same_relay(ctx: &Ctx<'_>, client: &RelayClient, last: &Last) -> bool {
    let want = known::origin(&client.base);
    match &last.relay {
        Some(saved) => saved == &want,
        None => known::known(&ctx.host.home)
            .and_then(|status| status.relay)
            .is_some_and(|saved| saved == want),
    }
}

/// Publish the first snapshot of a home that held data before it began to capture writes.
pub(super) async fn run_first(ctx: &Ctx<'_>, client: &RelayClient) -> Result<(), String> {
    let request = read_request(&ctx.host.home, &ctx.database_id);
    if request.is_none() {
        if let Some(message) = known::waiting(&ctx.host.home) {
            return Err(message);
        }
    }
    let prior = ctx.host.cloud_sync.snapshot().snapshot;
    ctx.host
        .cloud_sync
        .mark_snapshot(SnapshotState::InProgress, None);
    let outcome = run_first_inner(ctx, client).await;
    match outcome {
        Ok(report) => {
            if let Some(nonce) = request.as_deref() {
                let _ = finish_request(ctx, nonce, &report);
            }
            Ok(())
        }
        Err(error) => {
            if let Some(nonce) = request.as_deref() {
                fail_request(ctx, nonce, &error);
            }
            if error.starts_with(TOO_LARGE) {
                known::refused(ctx, &error);
            } else {
                ctx.host
                    .cloud_sync
                    .mark_snapshot(prior.state, prior.message);
            }
            Err(error)
        }
    }
}

async fn run_first_inner(ctx: &Ctx<'_>, client: &RelayClient) -> Result<Report, String> {
    let Some(frozen) = freeze(ctx, true).await? else {
        return Err("the preserved files of the first snapshot are missing".into());
    };
    let (report, fresh) = publish_cut(ctx, client, frozen, true).await?;
    if fresh {
        trim_covered(ctx, client, report.snapshot_seq).await;
        reclaim::mark_due(ctx, &client.base, report.snapshot_seq);
    }
    let opened = ctx.runtime.accepted_status().opened_frontier;
    ctx.runtime
        .note_accepted_snapshot(opened)
        .map_err(|error| format!("the first snapshot could not be recorded: {error:?}"))?;
    ctx.runtime
        .release_initial_snapshot()
        .map_err(|error| format!("the preserved files could not be removed: {error}"))?;
    tracing::info!(target: "lastdbd::log_sync", seq = report.snapshot_seq, files = report.files, "first snapshot published");
    Ok(report)
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

/// Hash, upload, name and verify a frozen snapshot. Returns its report, and whether it is a new
/// snapshot (false when nothing changed since the last one, which still names the state).
async fn publish_cut(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    frozen: Frozen,
    first: bool,
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
    let unchanged = load_last(&ctx.host.home, &ctx.database_id)?
        .filter(|last| same_relay(ctx, client, last))
        .filter(|last| last.manifest.files == files && last.through == through);
    if let Some(last) = unchanged {
        return Ok((
            Report {
                uploaded_files: 0,
                uploaded_bytes: 0,
                capture_ms,
                upload_ms: 0,
                ..last.report
            },
            false,
        ));
    }
    let uploading = Instant::now();
    let streams = list_streams(ctx, client).await?.unwrap_or_default();
    if first {
        require_empty_for_first(ctx, client, &streams).await?;
    }
    require_covers_latest(ctx, client, &streams, &through).await?;
    ensure_birth(ctx, client, &streams).await?;
    let list = Manifest {
        upload_id: uuid::Uuid::new_v4().to_string(),
        files,
    };
    let held = client.blob_list(&ctx.database_id).await?;
    let (uploaded_files, uploaded_bytes) =
        upload_files(ctx, client, &cut, &list.files, &held).await?;
    let (sha256, length) = put_manifest(ctx, client, &list).await?;
    let entry = name(ctx, client, first, (sha256.as_str(), length), &through).await?;
    // The relay names the snapshot from here on, and a restore reads it alone. Until its files
    // are checked, the mark makes the next clean turn take a snapshot again.
    mark_unverified(ctx)?;
    // The cut stays pinned until the relay holds every file of the snapshot it now names.
    let named = Named {
        list: &list,
        sha256: &sha256,
        bytes: length,
    };
    let (again_files, again_bytes) = verify_named(ctx, client, &cut, &named).await?;
    drop(cut);
    let report = record_published(
        ctx,
        &client.base,
        PublishedCut {
            entry,
            sha256,
            list,
            through,
            uploaded_files: uploaded_files + again_files,
            uploaded_bytes: uploaded_bytes + again_bytes,
            capture_ms,
            upload_ms: elapsed_ms(uploading),
        },
    )?;
    clear_unverified(ctx);
    Ok((report, true))
}

/// Sign the `snapshot` entry for the manifest `(digest, length)` and append it.
async fn name(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    first: bool,
    manifest: (&str, u64),
    through: &[SnapshotThrough],
) -> Result<SignedEntry, String> {
    let streams = list_streams(ctx, client).await?.unwrap_or_default();
    if first {
        require_empty_for_first(ctx, client, &streams).await?;
    }
    let entry = sign(ctx, client, &streams, manifest.0, manifest.1, through).await?;
    let stored = client
        .append(&ctx.database_id, std::slice::from_ref(&entry))
        .await?;
    if stored.0 + stored.1 != 1 {
        return Err("the relay did not take the snapshot entry".into());
    }
    Ok(entry)
}
