//! Send the entries of the spool to the relay.
//!
//! Every commit lands in the spool before the caller is told, and it lands
//! there finished: the store made the write the next entry of this device's
//! stream in the transaction that committed it, sealed with the database read
//! key and signed with the node key (`crate::personal_author`). A file that an
//! entry names is in the spool beside it, sealed. The sender changes nothing in
//! the store and signs nothing. It reads the entries past the acknowledged
//! position, sends their files and then the entries as one log segment (`send`), asks the relay for each
//! file again (a sweep drops a file that no entry named yet, and such a file goes
//! up again), writes the head row of its own stream, and tells the spool to
//! retire them.
//!
//! An entry is the same bytes however often it is read, so a retry sends what
//! the first try sent, and the relay counts an entry it already holds as a
//! duplicate. What a crash leaves, by window:
//!
//! - Before the relay acknowledges: the spool position is old. The next turn
//!   sends the same entries.
//! - After the relay acknowledges, before the head row: the same. The relay
//!   answers with duplicates.
//! - After the head row, before the spool confirms: the head row is ahead of
//!   the spool position. The next turn sends the entries once more, the relay
//!   answers with duplicates, and the spool confirms.

use std::sync::Arc;

use fold_db::storage::disk_backup::accepted::{
    Entry, EntryFile, Error as SpoolError, UploadIdentity, UploadPosition,
};
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{entry_hash, DataBody, SignedEntry};

use super::{apply, normalize_error, Ctx};
use crate::host::cloud_sync::BlockedUploadStatus;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::log_sync::send::{self, Transport};

/// The spool is read this many entries at a time.
const PAGE: u32 = 90;
/// A batch stops at 2 MiB of entries and files. The entries of a batch go up as one segment,
/// held in memory while it is written, and one spool confirmation covers the batch.
const BATCH_BYTES: u64 = 2 * 1024 * 1024;

/// The entries of one send, with the spool range they cover and the files they name.
pub(super) struct Batch {
    /// The spool position before the batch.
    pub(super) after: u64,
    /// The spool position after the batch.
    pub(super) through: u64,
    /// The signed entries, in stream order.
    pub(super) entries: Vec<SignedEntry>,
    /// The sealed files that the entries name.
    pub(super) files: Vec<EntryFile>,
}

/// Record a personal upload error before [`run`] starts (such as capture disabled or a blocked
/// first snapshot).
pub(super) fn record_pre_send_error(ctx: &Ctx<'_>, error: &str) {
    let message = normalize_error(error);
    let status = ctx.runtime.accepted_status();
    let oldest = oldest_blocked(status.head, status.acked, None, &message);
    ctx.host.cloud_sync.record_upload(
        &ctx.database_id,
        status.head,
        status.acked,
        Some(message),
        oldest,
        true,
    );
}

/// Record a `list_streams` failure on `download`, and also mark `upload` when local writes are
/// waiting in the spool with no earlier upload error recorded yet.
pub(super) fn record_streams_error(ctx: &Ctx<'_>, error: &str) {
    let message = normalize_error(error);
    ctx.host
        .cloud_sync
        .record_download(&ctx.database_id, None, Some(message.clone()), true);
    let status = ctx.runtime.accepted_status();
    if status.head > status.acked {
        ctx.host.cloud_sync.record_upload_if_unreported(
            &ctx.database_id,
            status.head,
            status.acked,
            message.clone(),
            BlockedUploadStatus {
                writer_seq: status.acked + 1,
                authored_at_ms: None,
                reason: message,
            },
            true,
        );
    }
}

/// Send everything the spool holds. Reports the spool position and oldest blocked upload to the
/// status of the node.
pub(super) async fn run(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    let result = drain(ctx, client, streams).await;
    let status = ctx.runtime.accepted_status();
    match result {
        Ok(()) => {
            ctx.host.cloud_sync.record_upload(
                &ctx.database_id,
                status.head,
                status.acked,
                None,
                None,
                true,
            );
            Ok(())
        }
        Err((error, first)) => {
            let message = normalize_error(&error);
            let oldest = oldest_blocked(status.head, status.acked, first, &message);
            ctx.host.cloud_sync.record_upload(
                &ctx.database_id,
                status.head,
                status.acked,
                Some(message),
                oldest,
                true,
            );
            Err(error)
        }
    }
}

fn oldest_blocked(
    head: u64,
    acked: u64,
    first: Option<(u64, Option<u64>)>,
    reason: &str,
) -> Option<BlockedUploadStatus> {
    if let Some((writer_seq, authored_at_ms)) = first {
        return Some(BlockedUploadStatus {
            writer_seq,
            authored_at_ms,
            reason: reason.to_owned(),
        });
    }
    (head > acked).then(|| BlockedUploadStatus {
        writer_seq: acked + 1,
        authored_at_ms: None,
        reason: reason.to_owned(),
    })
}

async fn drain(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), (String, Option<(u64, Option<u64>)>)> {
    if !ctx.runtime.captures_accepted() {
        return Err((
            "local capture is off: the node must restart with a relay configured".into(),
            None,
        ));
    }
    let mut birth_known = streams
        .iter()
        .any(|stream| stream.is(&ctx.publisher_hex, &ctx.birth_device_hex));
    let mut relay_head = streams
        .iter()
        .find(|stream| stream.is(&ctx.node_hex, &ctx.device_hex))
        .map_or(0, |stream| stream.head_seq);
    while let Some(batch) = next_batch(ctx, streams).await? {
        let first = batch.entries.first().map(|entry| {
            (
                entry.header().writer_seq,
                Some(entry.header().authored_at_ms),
            )
        });
        finish(ctx, client, &batch, &mut birth_known, &mut relay_head)
            .await
            .map_err(|err| (err, first))?;
    }
    Ok(())
}

/// Run blocking spool work off the async threads.
async fn blocking<T: Send + 'static>(
    ctx: &Ctx<'_>,
    work: impl FnOnce(&PackRuntime, &UploadIdentity) -> Result<T, SpoolError> + Send + 'static,
) -> Result<T, String> {
    let runtime = Arc::clone(&ctx.runtime);
    let identity = Arc::clone(&ctx.upload);
    tokio::task::spawn_blocking(move || work(runtime.as_ref(), identity.as_ref()))
        .await
        .map_err(|_| "the local upload queue worker stopped".to_owned())?
        .map_err(|error| format!("the local upload queue failed: {error:?}"))
}

/// The confirmed position of this device. A device with none starts at zero.
async fn position(ctx: &Ctx<'_>) -> Result<UploadPosition, String> {
    let found = blocking(ctx, |runtime, identity| {
        runtime.accepted_upload_position(identity)
    })
    .await?;
    if let Some(found) = found {
        return Ok(found);
    }
    // No upload was recorded yet. The spool starts at the head of this device's chain, which an
    // earlier spool already sent (zero for a new home), so the stream continues from there.
    let start = ctx.runtime.accepted_status().acked;
    let first = UploadPosition {
        writer_seq: start,
        spool_through: start,
    };
    confirm(ctx, start, first).await?;
    Ok(first)
}

/// Record an upload position in the spool, and retire the entries it covers.
async fn confirm(ctx: &Ctx<'_>, after: u64, position: UploadPosition) -> Result<(), String> {
    ctx.runtime
        .confirm_accepted_upload(&ctx.upload, after, position)
        .await
        .map_err(|error| format!("the local upload queue failed: {error:?}"))
}

/// The next entries of the spool, as many as fit in a batch. `None` when the spool is empty.
async fn next_batch(
    ctx: &Ctx<'_>,
    streams: &[RelayStream],
) -> Result<Option<Batch>, (String, Option<(u64, Option<u64>)>)> {
    let position = position(ctx).await.map_err(|err| (err, None))?;
    if position.writer_seq != position.spool_through {
        return Err((
            "the spool and the stream of this device number a write differently".into(),
            None,
        ));
    }
    let status = ctx.runtime.accepted_status();
    if status.gap {
        return Err(("local capture has a gap".into(), None));
    }
    // The relay may hold entries past the confirmed position: a turn that ended
    // between the relay's answer and the confirmation sends them again. It can
    // never hold more than this device has made.
    let relay_head = streams
        .iter()
        .find(|stream| stream.is(&ctx.node_hex, &ctx.device_hex))
        .map_or(0, |stream| stream.head_seq);
    if relay_head > status.head {
        return Err((
            "the relay holds entries of this device that the device did not make".into(),
            None,
        ));
    }
    let after = position.spool_through;
    let page = blocking(ctx, move |runtime, _| runtime.accepted_prefix(after, PAGE))
        .await
        .map_err(|err| (err, None))?;
    if page.items.is_empty() {
        return Ok(None);
    }
    let held = ctx
        .own_head()
        .await
        .map_err(|err| (err, None))?
        .map_or(0, |head| head.seq);
    if held < position.writer_seq {
        return Err((
            "the own stream head is behind the spool position".into(),
            None,
        ));
    }
    let mut batch = Batch {
        after,
        through: after,
        entries: Vec::new(),
        files: Vec::new(),
    };
    let mut bytes = 0u64;
    for item in page.items {
        let weight = item.signed.len() as u64 + item.file.as_ref().map_or(0, |file| file.bytes);
        if !batch.entries.is_empty() && bytes.saturating_add(weight) > BATCH_BYTES {
            break;
        }
        bytes = bytes.saturating_add(weight);
        let seq = item.sequence;
        batch.through = seq;
        let (entry, file) = signed_entry(ctx, item).map_err(|err| (err, Some((seq, None))))?;
        batch.entries.push(entry);
        batch.files.extend(file);
    }
    Ok(Some(batch))
}

/// The signed entry of a spool entry, and its file. The entry must be the one
/// this device signed at that position, and name the file the spool holds.
fn signed_entry(ctx: &Ctx<'_>, item: Entry) -> Result<(SignedEntry, Option<EntryFile>), String> {
    const BAD: &str = "a spool entry is not a signed entry of this device";
    let entry: SignedEntry = serde_json::from_slice(&item.signed).map_err(|_| BAD.to_owned())?;
    let header = entry.header();
    if header.writer_seq != item.sequence
        || header.node_key != ctx.identity.node_key
        || header.device_id != ctx.identity.device_id
    {
        return Err(BAD.into());
    }
    let SignedEntry::Data {
        body: DataBody { file: named, .. },
        ..
    } = &entry
    else {
        return Err(BAD.into());
    };
    let named = named.as_ref().map(|file| file.sha256.to_hex());
    if named != item.file.as_ref().map(|file| file.sha256.clone()) {
        return Err("a spool entry names another file than the spool holds".into());
    }
    Ok((entry, item.file))
}

/// Send one batch, move the own head row to its last entry, and retire it from the spool.
async fn finish(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    batch: &Batch,
    birth_known: &mut bool,
    relay_head: &mut u64,
) -> Result<(), String> {
    if !*birth_known {
        let stored = client
            .append(&ctx.database_id, std::slice::from_ref(&ctx.birth_entry))
            .await?;
        send::expect_all(stored, 1)?;
        *birth_known = true;
    }
    let files: Vec<_> = batch
        .files
        .iter()
        .map(|f| (f.sha256.as_str(), f.path.as_path()))
        .collect();
    send::send_batch(
        client,
        &ctx.database_id,
        &batch.entries,
        &files,
        Transport::SegmentAfter(*relay_head),
    )
    .await?;
    let last = batch.entries.last().ok_or("a batch holds no entry")?;
    *relay_head = (*relay_head).max(last.header().writer_seq);
    commit_head(ctx, last).await?;
    let position = UploadPosition {
        writer_seq: last.header().writer_seq,
        spool_through: batch.through,
    };
    confirm(ctx, batch.after, position).await
}

/// Move the own head row to an entry the relay acknowledged. It repeats safely.
async fn commit_head(ctx: &Ctx<'_>, last: &SignedEntry) -> Result<(), String> {
    let header = last.header();
    if ctx
        .own_head()
        .await?
        .is_some_and(|held| held.seq >= header.writer_seq)
    {
        return Ok(());
    }
    let hash = entry_hash(last).map_err(|e| e.to_string())?;
    ctx.store
        .mutation_manager()
        .set_replay_stream_head(&ctx.own_stream, &apply::head_of(header, &hash))
        .await
        .map_err(|error| format!("the own stream head could not be written: {error}"))
}
