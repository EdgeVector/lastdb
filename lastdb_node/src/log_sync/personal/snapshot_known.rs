//! What this node knows about its newest snapshot, for `lastdb status`.
//!
//! `snapshot-known.json` is written beside `snapshot-last.json` when this device publishes a
//! snapshot, and when a restore installs one. The daemon reads it at boot, so the status names
//! the snapshot without a call to the relay. It is not part of the store, and it is not a
//! source of truth: it only tells a person where the newest snapshot is.
//!
//! A refused snapshot is remembered here too, in memory only: after a relay refuses a file for
//! its size, no new attempt starts for [`RETRY`] unless a person asks for one, because each
//! attempt freezes the files and hashes all of them.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mutation_log::{DeviceId, SnapshotThrough};

use super::snapshot_state::file_of;
use super::Ctx;
use crate::host::cloud_sync::{SnapshotState, SnapshotStatus};
use crate::profile::private_fs as disk;

const FILE: &str = "snapshot-known.json";
const LIMIT: usize = 64 * 1024;
/// How long a snapshot that a relay refused for size waits before the next automatic attempt.
const RETRY: Duration = Duration::from_secs(10 * 60);

static REFUSED: Mutex<Option<(Instant, String)>> = Mutex::new(None);

/// The facts of one snapshot, as the entry and its manifest name them.
pub(super) struct Taken<'a> {
    pub(super) state: SnapshotState,
    pub(super) relay_base: &'a str,
    pub(super) taken_at_ms: u64,
    pub(super) files: usize,
    pub(super) bytes: u64,
    pub(super) manifest_sha256: &'a str,
    pub(super) snapshot_seq: u64,
    pub(super) through: &'a [SnapshotThrough],
    pub(super) device: DeviceId,
}

/// Keep the snapshot on disk and in the status of the node. A failure to write is logged and
/// does not fail the snapshot: the relay already holds it.
pub(super) fn record(ctx: &Ctx<'_>, taken: &Taken<'_>) {
    let status = SnapshotStatus {
        state: taken.state,
        taken_at_ms: Some(taken.taken_at_ms),
        files: Some(taken.files as u64),
        bytes: Some(taken.bytes),
        relay: Some(origin(taken.relay_base)),
        manifest_sha256: Some(taken.manifest_sha256.to_owned()),
        through_seq: Some(
            taken
                .through
                .iter()
                .find(|position| position.device_id == taken.device)
                .map_or(0, |position| position.writer_seq),
        ),
        snapshot_seq: Some(taken.snapshot_seq),
        streams: Some(taken.through.len() as u64),
        message: None,
    };
    let path = file_of(&ctx.host.home, &ctx.database_id, FILE);
    let written = serde_json::to_vec(&status)
        .map_err(|error| error.to_string())
        .and_then(|bytes| disk::write_private(&path, &bytes).map_err(|error| error.to_string()));
    if let Err(error) = written {
        tracing::warn!(target: "lastdbd::log_sync", %error, "the snapshot record for status could not be saved");
    }
    clear_refusal();
    ctx.host.cloud_sync.record_snapshot(status);
}

/// The snapshot recorded in `home`, if any. The personal database is the one database with
/// such a record, so the folders under `log-sync` are the whole search.
pub(crate) fn known(home: &Path) -> Option<SnapshotStatus> {
    let dirs = std::fs::read_dir(home.join("log-sync")).ok()?;
    dirs.flatten()
        .filter_map(|dir| {
            disk::read_limited(&dir.path().join(FILE), LIMIT)
                .ok()
                .flatten()
        })
        .find_map(|bytes| serde_json::from_slice(&bytes).ok())
}

/// The origin (scheme, host and port) of a relay URL. Another URL form is kept as it is.
pub(super) fn origin(base: &str) -> String {
    reqwest::Url::parse(base).map_or_else(
        |_| base.to_owned(),
        |url| url.origin().ascii_serialization(),
    )
}

/// The relay refused the snapshot: show `message` in the status and hold off new automatic
/// attempts for [`RETRY`].
pub(super) fn refused(ctx: &Ctx<'_>, message: &str) {
    tracing::warn!(target: "lastdbd::log_sync", %message, "snapshot cannot be published");
    *lock() = Some((Instant::now(), message.to_owned()));
    ctx.host
        .cloud_sync
        .mark_snapshot(SnapshotState::Blocked, Some(message.to_owned()));
}

/// The message of a refusal that is still recent, so no attempt should start yet.
pub(super) fn waiting() -> Option<String> {
    lock()
        .as_ref()
        .filter(|(at, _)| at.elapsed() < RETRY)
        .map(|(_, message)| message.clone())
}

fn clear_refusal() {
    *lock() = None;
}

fn lock() -> std::sync::MutexGuard<'static, Option<(Instant, String)>> {
    REFUSED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
