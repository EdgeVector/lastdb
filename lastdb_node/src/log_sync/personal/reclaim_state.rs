//! What a sweep keeps on disk beside the snapshot record.
//!
//! `reclaim-due.json` says that a trim and sweep are due for one snapshot. The snapshot lane
//! writes it as soon as it has named a snapshot, and the sweep removes it when nothing is left to
//! drop. When the relay refused the drops turn after turn, the mark stays and says why the sweep
//! stopped trying. `reclaim-last.json` is what the relay held after the last finished sweep, which
//! `lastdb status` shows. A request of `lastdb backup reclaim` uses the request files of
//! `snapshot_state`. None of them is part of the store, so a restored device starts with none.

use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::snapshot_known as known;
use super::snapshot_state::{file_of, RECLAIM};
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

const DUE: &str = "reclaim-due.json";
const LAST: &str = "reclaim-last.json";
const LIMIT: usize = 64 * 1024;

/// The snapshot that a sweep is due for.
#[derive(Serialize, Deserialize)]
pub(super) struct Due {
    /// The sequence number of its entry on the snapshot stream.
    pub(super) snapshot_seq: u64,
    /// The origin of the relay that holds it.
    pub(super) relay: String,
    /// Why the sweep stopped trying. It then waits for the next snapshot, which marks again, or
    /// for `lastdb backup reclaim`.
    #[serde(default)]
    pub(super) held: Option<String>,
}

/// True while a sweep request or an unheld due mark waits to run.
pub(super) fn pending(home: &Path, database_id: &str) -> bool {
    RECLAIM.waiting(home, database_id).is_some()
        || due(home, database_id).is_some_and(|due| due.held.is_none())
}

/// Mark a trim and sweep as due for the snapshot `snapshot_seq` on the relay at `relay_base`.
/// A failure to write is logged and does not fail the published snapshot: the next snapshot marks
/// again, and `lastdb backup reclaim` asks for a sweep without the mark.
pub(super) fn mark_due(ctx: &Ctx<'_>, relay_base: &str, snapshot_seq: u64) {
    let due = Due {
        snapshot_seq,
        relay: known::origin(relay_base),
        held: None,
    };
    if let Err(error) = write(&ctx.host.home, &ctx.database_id, DUE, &due) {
        tracing::warn!(target: "lastdbd::log_sync", %error, "the mark for a sweep could not be saved");
    }
    ctx.host.cloud_sync.mark_reclaim(None);
}

/// Schedule a sweep for `snapshot_seq` on `relay_base` if neither `reclaim-due.json` nor
/// `reclaim-last.json` already covers it.
pub(super) fn ensure_due_unless_swept(ctx: &Ctx<'_>, relay_base: &str, snapshot_seq: u64) {
    let origin = known::origin(relay_base);
    if due(&ctx.host.home, &ctx.database_id)
        .is_some_and(|cur| cur.relay == origin && cur.snapshot_seq >= snapshot_seq)
    {
        return;
    }
    let swept = read::<ReclaimStatus>(&file_of(&ctx.host.home, &ctx.database_id, LAST))
        .and_then(|last| last.snapshot_seq)
        .is_some_and(|seq| seq >= snapshot_seq);
    if !swept {
        mark_due(ctx, relay_base, snapshot_seq);
    }
}

/// The sweep stops trying for `snapshot_seq` on `relay_base`: keep the mark with `reason`, and
/// show the reason in the status of the node.
pub(super) fn hold(ctx: &Ctx<'_>, relay_base: &str, snapshot_seq: u64, reason: &str) {
    let message = Some(reason.to_owned());
    let due = Due {
        snapshot_seq,
        relay: known::origin(relay_base),
        held: message.clone(),
    };
    if let Err(error) = write(&ctx.host.home, &ctx.database_id, DUE, &due) {
        tracing::warn!(target: "lastdbd::log_sync", %error, "the mark for a sweep could not be saved");
    }
    ctx.host.cloud_sync.mark_reclaim(message);
}

/// The snapshot a sweep is due for, unheld when a newer snapshot has appeared on the same relay
/// or cleared when left from another relay.
pub(super) fn refresh_due(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Option<Due> {
    let current = due(&ctx.host.home, &ctx.database_id)?;
    if current.held.is_none() {
        return Some(current);
    }
    let origin = known::origin(&client.base);
    if current.relay != origin {
        clear_due(ctx);
        return None;
    }
    let relay_seq = streams
        .iter()
        .find(|stream| stream.device_id == ctx.snapshot_device_hex)
        .map_or(0, |stream| stream.head_seq);
    let known_seq = known::known(&ctx.host.home)
        .filter(|status| status.relay.as_deref() == Some(origin.as_str()))
        .and_then(|status| status.snapshot_seq)
        .unwrap_or(0);
    let newest = relay_seq.max(known_seq);
    if newest > current.snapshot_seq {
        mark_due(ctx, &client.base, newest);
        return Some(Due {
            snapshot_seq: newest,
            relay: origin,
            held: None,
        });
    }
    Some(current)
}

/// The snapshot a sweep is due for, if one is.
fn due(home: &Path, database_id: &str) -> Option<Due> {
    read(&file_of(home, database_id, DUE))
}

/// The sweep is done, or its snapshot is no longer due on this relay.
pub(super) fn clear_due(ctx: &Ctx<'_>) {
    let _ = disk::remove_private(&file_of(&ctx.host.home, &ctx.database_id, DUE));
    ctx.host.cloud_sync.mark_reclaim(None);
}

/// Keep what the relay held after a finished sweep, on disk and in the status of the node.
pub(super) fn record_last(ctx: &Ctx<'_>, status: &ReclaimStatus) {
    if let Err(error) = write(&ctx.host.home, &ctx.database_id, LAST, status) {
        tracing::warn!(target: "lastdbd::log_sync", %error, "the record of the sweep for status could not be saved");
    }
    ctx.host.cloud_sync.record_reclaim(status.clone());
}

/// The last finished sweep recorded in `home`, and why a sweep that is due waits, if either is
/// recorded. The personal database is the one database with such records, so the folders under
/// `log-sync` are the whole search.
pub(crate) fn last(home: &Path) -> Option<ReclaimStatus> {
    let dirs: Vec<_> = std::fs::read_dir(home.join("log-sync"))
        .ok()?
        .flatten()
        .collect();
    let held = dirs
        .iter()
        .find_map(|dir| read::<Due>(&dir.path().join(DUE)))
        .and_then(|due| due.held);
    let last: Option<ReclaimStatus> = dirs.iter().find_map(|dir| read(&dir.path().join(LAST)));
    if last.is_none() && held.is_none() {
        return None;
    }
    Some(ReclaimStatus {
        message: held,
        ..last.unwrap_or_default()
    })
}

fn write<T: Serialize>(
    home: &Path,
    database_id: &str,
    name: &str,
    value: &T,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    disk::write_private(&file_of(home, database_id, name), &bytes)
        .map_err(|error| error.to_string())
}

fn read<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let bytes = disk::read_limited(path, LIMIT).ok().flatten()?;
    serde_json::from_slice(&bytes).ok()
}
