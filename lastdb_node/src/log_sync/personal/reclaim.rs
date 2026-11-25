//! The sweep: drop every stored file that the newest snapshot and the entries after it do not
//! name, so the relay holds what the local store holds.
//!
//! After a snapshot is named and trimmed, the relay holds exactly: the files and the manifest of
//! that snapshot, the entries after its positions (with the birth and that one `snapshot` entry),
//! and the sealed files those entries name. Everything else is dropped: the files of the snapshot
//! before, the sealed files of entries a trim dropped, and the files of an upload that failed.
//!
//! The relay owns what is stored: `blob.list` is the truth. This node owns what is needed: the
//! manifest that the newest `snapshot` entry names, and the entries after its positions, read
//! back from the relay. The relay reads no manifest and no file. It drops only the digests this
//! node names, and only while the stream heads this node listed are still the heads
//! (`blob.drop`).
//!
//! One file is the danger: committed, and not yet named. A device uploads a sealed file and then
//! appends its entry. Two rules close it without a clock. The device that names a file asks for
//! it again after the append and uploads it when it is gone (`send`, and step 5 of `snapshot`).
//! And the drop is one step with the heads: an entry appended before the drop moved a head, so
//! the drop does nothing, and an entry appended after it finds its file gone and puts it back.
//!
//! A sweep runs inside a sync turn, after the send step, when the snapshot lane marked one as due
//! on this relay and the newest snapshot on the relay is at or past it, or when
//! `lastdb backup reclaim` asked for one. It finishes `log.trim` for that snapshot before dropping
//! any file, and drops nothing while the relay lacks any file of that snapshot or its manifest.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

use super::reclaim_count::Counter;
use super::reclaim_state as state;
pub(super) use super::reclaim_state::{mark_due, pending};
use super::reclaim_view::{manifest_of, read_tail, Seen};
use super::snapshot_entry::snapshot_head;
use super::snapshot_known as known;
use super::snapshot_lane as lane;
use super::snapshot_state::RECLAIM;
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::blob_transfer::{HeadAt, DROP_BATCH, DROP_HEADS};
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Passes of one sweep in one turn. A pass that dropped files is followed by one that lists
/// again, and the sweep is done when a pass finds nothing to drop. What is left goes on in the
/// next turn.
const PASSES: usize = 4;
/// Bounded `log.trim` calls per pass so covered entries are always trimmed before blobs drop.
const TRIM_CALLS_PER_PASS: usize = 16;
/// Turns in a row in which the relay may refuse the drops because a stream is not as this node
/// listed it. A stream that keeps an entry above its head is refused every time, so the sweep
/// stops trying after this many turns.
const REFUSED_TURNS: u32 = 30;
/// Why the sweep stopped trying, as the status and the answer to a request say it.
const REFUSED: &str = "the relay refused every drop: a stream of this database kept changing, or it holds an entry above its head. The next snapshot tries again, and so does `lastdb backup reclaim`";

/// Turns in a row that ended with a refused drop, for each home.
static REFUSALS: LazyLock<Mutex<HashMap<PathBuf, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

enum Pass {
    Clean(Box<ReclaimStatus>),
    Dropped,
    Trimming,
    Refused(u64),
    NotDue,
    TooManyStreams(u64),
    Incomplete { snapshot_seq: u64, files: usize },
}

enum Swept {
    Done(Box<ReclaimStatus>),
    Again,
    Refused(u64),
    NotDue,
    TooManyStreams(u64),
    Incomplete { snapshot_seq: u64, files: usize },
}

struct SweepTurn<'a> {
    due: Option<&'a state::Due>,
    asked: bool,
    trimmed_seq: Option<u64>,
}

/// Run the sweep when one is due or asked for. The caller is a sync turn, after its send step.
pub(super) async fn run(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    let (home, database_id) = (&ctx.host.home, &ctx.database_id);
    let request = RECLAIM.waiting(home, database_id);
    let due = state::refresh_due(ctx, client, streams);
    // A sweep that stopped trying waits for the next snapshot or for a request.
    let waits = due.as_ref().is_none_or(|due| due.held.is_some());
    if request.is_none() && waits {
        return Ok(());
    }
    // While a snapshot uploads, nothing names its files yet. The sweep runs when it is named.
    if lane::busy_for(home) {
        return Ok(());
    }
    let outcome = sweep(ctx, client, due.as_ref(), request.is_some()).await;
    if !matches!(outcome, Ok(Swept::Refused(_))) {
        refusals().remove(home);
    }
    match outcome {
        Ok(Swept::Done(status)) => {
            state::clear_due(ctx);
            state::record_last(ctx, &status);
            tracing::info!(target: "lastdbd::log_sync", snapshot_seq = ?status.snapshot_seq, files = ?status.cloud_files, dropped = ?status.dropped_files, "sweep done");
            match &request {
                Some(nonce) => RECLAIM.finish(home, database_id, nonce, &status),
                None => Ok(()),
            }
        }
        Ok(Swept::Again) => {
            crate::log_sync::wake::request_wake(home);
            Ok(())
        }
        Ok(Swept::NotDue) => {
            state::clear_due(ctx);
            Ok(())
        }
        Ok(Swept::Refused(snapshot_seq)) => {
            refused(ctx, client, snapshot_seq, request.as_deref());
            Ok(())
        }
        Ok(Swept::Incomplete {
            snapshot_seq,
            files,
        }) => {
            let reason = format!(
                "the relay does not list {files} files of the newest snapshot, so the sweep dropped nothing. Take a snapshot with `lastdb backup snapshot`; the sweep runs after it"
            );
            stop(ctx, client, snapshot_seq, request.as_deref(), &reason);
            Ok(())
        }
        Ok(Swept::TooManyStreams(snapshot_seq)) => {
            let reason = format!(
                "the relay lists more than {DROP_HEADS} streams of this database, and a sweep names every stream head"
            );
            stop(ctx, client, snapshot_seq, request.as_deref(), &reason);
            Ok(())
        }
        Err(error) => {
            if let Some(nonce) = &request {
                RECLAIM.fail(home, database_id, nonce, &error);
            }
            Err(error)
        }
    }
}

fn refused(ctx: &Ctx<'_>, client: &RelayClient, snapshot_seq: u64, request: Option<&str>) {
    let home = &ctx.host.home;
    let turns = {
        let mut refusals = refusals();
        let turns = refusals.entry(home.clone()).or_insert(0);
        *turns += 1;
        *turns
    };
    if turns < REFUSED_TURNS {
        return;
    }
    refusals().remove(home);
    stop(ctx, client, snapshot_seq, request, REFUSED);
}

fn stop(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    snapshot_seq: u64,
    request: Option<&str>,
    reason: &str,
) {
    tracing::warn!(target: "lastdbd::log_sync", %reason, "the sweep stopped");
    if let Some(nonce) = request {
        RECLAIM.fail(&ctx.host.home, &ctx.database_id, nonce, reason);
    }
    Counter::reset_totals(&ctx.host.home, &ctx.database_id);
    state::hold(ctx, &client.base, snapshot_seq, reason);
}

fn refusals() -> std::sync::MutexGuard<'static, HashMap<PathBuf, u32>> {
    REFUSALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn sweep(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    due: Option<&state::Due>,
    asked: bool,
) -> Result<Swept, String> {
    let mut counter = Counter::open(ctx, client)?;
    let mut turn = SweepTurn {
        due,
        asked,
        trimmed_seq: None,
    };
    for _ in 0..PASSES {
        match pass(ctx, &mut counter, &mut turn).await? {
            Pass::Clean(status) => return Ok(Swept::Done(status)),
            Pass::Dropped => {}
            Pass::Trimming => return Ok(Swept::Again),
            Pass::Refused(seq) => return Ok(Swept::Refused(seq)),
            Pass::NotDue => return Ok(Swept::NotDue),
            Pass::TooManyStreams(seq) => return Ok(Swept::TooManyStreams(seq)),
            Pass::Incomplete {
                snapshot_seq,
                files,
            } => {
                return Ok(Swept::Incomplete {
                    snapshot_seq,
                    files,
                });
            }
        }
    }
    Ok(Swept::Again)
}

async fn settle_not_due(ctx: &Ctx<'_>, counter: &mut Counter<'_>) -> Result<(), String> {
    if !counter.is_empty() {
        let listed = counter.client.blob_list(&ctx.database_id).await?;
        counter.settle(&listed)?;
    }
    counter.finish()
}

async fn trim_before_sweep(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    newest: u64,
) -> Result<bool, String> {
    if newest == 0 {
        return Ok(true);
    }
    for _ in 0..TRIM_CALLS_PER_PASS {
        if client
            .trim(&ctx.database_id, &ctx.snapshot_device_hex, newest)
            .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn other_heads(ctx: &Ctx<'_>, streams: &[RelayStream]) -> Vec<HeadAt> {
    streams
        .iter()
        .filter(|stream| stream.device_id != ctx.snapshot_device_hex)
        .map(|stream| HeadAt {
            device_id: stream.device_id.clone(),
            writer_seq: stream.head_seq,
        })
        .collect()
}

async fn pass(
    ctx: &Ctx<'_>,
    counter: &mut Counter<'_>,
    turn: &mut SweepTurn<'_>,
) -> Result<Pass, String> {
    let client = counter.client;
    let streams = client.streams(&ctx.database_id, &ctx.node_hex).await?;
    let Some((head, snapshot)) = snapshot_head(ctx, client, &streams).await? else {
        if turn.asked {
            return Err("the relay holds no snapshot of this database, so a sweep has nothing to keep; take one with `lastdb backup snapshot`".into());
        }
        settle_not_due(ctx, counter).await?;
        return Ok(Pass::NotDue);
    };
    let active = |due: &state::Due| {
        head.writer_seq >= due.snapshot_seq && due.relay == known::origin(&client.base)
    };
    if !turn.asked && !turn.due.is_some_and(active) {
        settle_not_due(ctx, counter).await?;
        return Ok(Pass::NotDue);
    }
    let heads = other_heads(ctx, &streams);
    if heads.len() > DROP_HEADS {
        return Ok(Pass::TooManyStreams(head.writer_seq));
    }
    let listed = client.blob_list(&ctx.database_id).await?;
    counter.settle(&listed)?;
    let manifest_sha256 = snapshot.manifest_sha256.to_hex();
    if listed.get(&manifest_sha256) != Some(&snapshot.manifest_bytes) {
        return Ok(Pass::Incomplete {
            snapshot_seq: head.writer_seq,
            files: 1,
        });
    }
    let list = manifest_of(ctx, client, &snapshot).await?;
    let files: BTreeMap<&str, u64> = list
        .files
        .iter()
        .map(|file| (file.sha256.as_str(), file.bytes))
        .collect();
    let missing_files = files
        .iter()
        .filter(|(digest, bytes)| listed.get(**digest) != Some(*bytes))
        .count();
    if missing_files > 0 {
        return Ok(Pass::Incomplete {
            snapshot_seq: head.writer_seq,
            files: missing_files,
        });
    }
    if turn.trimmed_seq != Some(head.writer_seq) {
        if !trim_before_sweep(ctx, client, head.writer_seq).await? {
            return Ok(Pass::Trimming);
        }
        turn.trimmed_seq = Some(head.writer_seq);
    }
    let tail = read_tail(ctx, client, &streams, &head, &snapshot).await?;
    let seen = Seen {
        snapshot_seq: head.writer_seq,
        manifest_sha256,
        manifest_bytes: snapshot.manifest_bytes,
        files,
        list: &list,
        tail: &tail,
        listed: &listed,
    };
    let dead: Vec<String> = listed
        .keys()
        .filter(|digest| !seen.needs(digest.as_str()))
        .cloned()
        .collect();
    if dead.is_empty() {
        let mut status = seen.status();
        status.dropped_files = Some(counter.files);
        status.released_bytes = Some(counter.bytes);
        counter.finish()?;
        return Ok(Pass::Clean(Box::new(status)));
    }
    drop_batches(ctx, counter, head.writer_seq, &heads, &listed, &dead).await
}

async fn drop_batches(
    ctx: &Ctx<'_>,
    counter: &mut Counter<'_>,
    snapshot_seq: u64,
    heads: &[HeadAt],
    listed: &BTreeMap<String, u64>,
    dead: &[String],
) -> Result<Pass, String> {
    let at = HeadAt {
        device_id: ctx.snapshot_device_hex.clone(),
        writer_seq: snapshot_seq,
    };
    for batch in dead.chunks(DROP_BATCH) {
        counter.naming(batch, listed)?;
        let answer = counter
            .client
            .blob_drop(&ctx.database_id, &at, heads, batch)
            .await?;
        counter.answered(batch, answer)?;
        if answer.is_none() {
            return Ok(Pass::Refused(snapshot_seq));
        }
    }
    Ok(Pass::Dropped)
}
