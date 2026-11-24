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
//! and the newest snapshot on the relay is still that one, or when `lastdb backup reclaim` asked
//! for one. It is a difference against a listing, so it also removes what earlier builds left.
//!
//! The relay refuses a drop (HTTP 409, code `heads`) when a stream is not as this node listed it:
//! a head moved, or the stream holds an entry above its head, which a reader cannot see. The next
//! turn lists again. A stream that keeps such an entry is refused every time, so after
//! [`REFUSED_TURNS`] turns in a row the sweep stops, keeps its mark, and says why in the status.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

use super::reclaim_state as state;
pub(super) use super::reclaim_state::mark_due;
use super::reclaim_view::{manifest_of, read_tail, Seen};
use super::snapshot_entry::snapshot_head;
use super::snapshot_known as known;
use super::snapshot_lane as lane;
use super::snapshot_state::RECLAIM;
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::blob_transfer::{HeadAt, DROP_BATCH, DROP_HEADS};
use crate::log_sync::relay::RelayClient;

/// Passes of one sweep in one turn. A pass that dropped files is followed by one that lists
/// again, and the sweep is done when a pass finds nothing to drop. What is left goes on in the
/// next turn.
const PASSES: usize = 4;
/// Turns in a row in which the relay may refuse the drops because a stream is not as this node
/// listed it. A stream that keeps an entry above its head is refused every time, so the sweep
/// stops trying after this many turns.
const REFUSED_TURNS: u32 = 30;
/// Why the sweep stopped trying, as the status and the answer to a request say it.
const REFUSED: &str = "the relay refused every drop: a stream of this database kept changing, or it holds an entry above its head. The next snapshot tries again, and so does `lastdb backup reclaim`";

/// Turns in a row that ended with a refused drop, for each home.
static REFUSALS: LazyLock<Mutex<HashMap<PathBuf, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// How one pass ended.
enum Pass {
    /// Nothing is left to drop. This is what the relay holds.
    Clean(Box<ReclaimStatus>),
    /// The relay dropped this many files and released this many bytes. List again.
    Dropped(u64, u64),
    /// The relay refused a drop: a stream is not as this pass listed it.
    Refused,
    /// The snapshot the sweep was due for is no longer the newest one on this relay.
    NotDue,
    /// The database has more streams than one drop can name.
    TooManyStreams,
}

/// How the sweep of one turn ended.
enum Swept {
    /// Nothing is left to drop. This is what the relay holds.
    Done(Box<ReclaimStatus>),
    /// It goes on in the next turn.
    Again,
    /// The relay refused a drop. The next turn computes the list again.
    Refused,
    /// The snapshot the sweep was due for is no longer the newest one on this relay.
    NotDue,
}

/// Run the sweep when one is due or asked for. The caller is a sync turn, after its send step.
pub(super) async fn run(ctx: &Ctx<'_>, client: &RelayClient) -> Result<(), String> {
    let (home, database_id) = (&ctx.host.home, &ctx.database_id);
    let request = RECLAIM.waiting(home, database_id);
    let due = state::due(home, database_id);
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
    if !matches!(outcome, Ok(Swept::Refused)) {
        refusals().remove(home);
    }
    match outcome {
        Ok(Swept::Done(status)) => {
            state::clear_due(home, database_id);
            state::record_last(ctx, &status);
            tracing::info!(target: "lastdbd::log_sync", snapshot_seq = ?status.snapshot_seq, files = ?status.cloud_files, dropped = ?status.dropped_files, "sweep done");
            match &request {
                Some(nonce) => RECLAIM.finish(home, database_id, nonce, &status),
                None => Ok(()),
            }
        }
        Ok(Swept::Again) => Ok(()),
        Ok(Swept::NotDue) => {
            state::clear_due(home, database_id);
            Ok(())
        }
        Ok(Swept::Refused) => {
            refused(ctx, due, request.as_deref());
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

/// The relay refused a drop in this turn. The next turn tries again, at most [`REFUSED_TURNS`]
/// turns in a row. Then the sweep stops: the mark stays and says why, the status of the node
/// shows it, a request learns it, and it is logged once.
fn refused(ctx: &Ctx<'_>, due: Option<state::Due>, request: Option<&str>) {
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
    tracing::warn!(target: "lastdbd::log_sync", turns, "the sweep stopped: the relay refused every drop");
    if let Some(nonce) = request {
        RECLAIM.fail(home, &ctx.database_id, nonce, REFUSED);
    }
    if let Some(due) = due {
        state::hold(ctx, due, REFUSED);
    }
}

fn refusals() -> std::sync::MutexGuard<'static, HashMap<PathBuf, u32>> {
    REFUSALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Passes until nothing is left to drop, a drop is refused, or the turn has done its share.
async fn sweep(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    due: Option<&state::Due>,
    asked: bool,
) -> Result<Swept, String> {
    let (mut dropped, mut released) = (0u64, 0u64);
    for _ in 0..PASSES {
        match pass(ctx, client, due, asked).await? {
            Pass::Clean(mut status) => {
                status.dropped_files = Some(dropped);
                status.released_bytes = Some(released);
                return Ok(Swept::Done(status));
            }
            Pass::Dropped(files, bytes) => {
                dropped += files;
                released += bytes;
            }
            Pass::Refused => return Ok(Swept::Refused),
            Pass::NotDue => return Ok(Swept::NotDue),
            Pass::TooManyStreams => {
                // A mark that can never be served must not fail every turn.
                state::clear_due(&ctx.host.home, &ctx.database_id);
                return Err(format!(
                    "the relay lists more than {DROP_HEADS} streams of this database, and a sweep names every stream head"
                ));
            }
        }
    }
    Ok(Swept::Again)
}

/// One pass: list, compute what nothing names, and drop it.
async fn pass(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    due: Option<&state::Due>,
    asked: bool,
) -> Result<Pass, String> {
    let streams = client.streams(&ctx.database_id, &ctx.node_hex).await?;
    let Some((head, snapshot)) = snapshot_head(ctx, client, &streams).await? else {
        if asked {
            return Err("the relay holds no snapshot of this database, so a sweep has nothing to keep; take one with `lastdb backup snapshot`".into());
        }
        return Ok(Pass::NotDue);
    };
    let same = |due: &state::Due| {
        due.snapshot_seq == head.writer_seq && due.relay == known::origin(&client.base)
    };
    if !asked && !due.is_some_and(same) {
        return Ok(Pass::NotDue);
    }
    let heads: Vec<HeadAt> = streams
        .iter()
        .filter(|stream| stream.device_id != ctx.snapshot_device_hex)
        .map(|stream| HeadAt {
            device_id: stream.device_id.clone(),
            writer_seq: stream.head_seq,
        })
        .collect();
    if heads.len() > DROP_HEADS {
        return Ok(Pass::TooManyStreams);
    }
    let listed = client.blob_list(&ctx.database_id).await?;
    let list = manifest_of(ctx, client, &snapshot).await?;
    let tail = read_tail(ctx, client, &streams, &snapshot).await?;
    let seen = Seen {
        snapshot_seq: head.writer_seq,
        manifest_sha256: snapshot.manifest_sha256.to_hex(),
        manifest_bytes: snapshot.manifest_bytes,
        files: list
            .files
            .iter()
            .map(|file| (file.sha256.as_str(), file.bytes))
            .collect(),
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
        return Ok(Pass::Clean(Box::new(seen.status())));
    }
    let at = HeadAt {
        device_id: ctx.snapshot_device_hex.clone(),
        writer_seq: head.writer_seq,
    };
    let (mut dropped, mut released) = (0, 0);
    for batch in dead.chunks(DROP_BATCH) {
        let Some(answer) = client
            .blob_drop(&ctx.database_id, &at, &heads, batch)
            .await?
        else {
            return Ok(Pass::Refused);
        };
        dropped += answer.dropped;
        released += answer.released_bytes;
    }
    Ok(Pass::Dropped(dropped, released))
}
