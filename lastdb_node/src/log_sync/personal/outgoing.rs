//! Sign the accepted writes of the spool as the next entries of this device's
//! stream, and send them to the relay.
//!
//! Every commit already lands in the spool, in order, before the caller is
//! told, and the commit gave the write its place: its sequence number is its
//! number in the spool, its order value is the author time the spool recorded,
//! and the store wrote its stamp rows in the same transaction. The sender only
//! packages it. It takes the entries past the acknowledged position, opens each
//! canonical body, seals it with the database read key for that position, and
//! signs it. Nothing in the store changes when a write is sent. The signed
//! batch is saved before anything is sent, so a retry sends the same bytes.
//! When the relay acknowledges, the spool retires the batch and the saved
//! entries are deleted.
//!
//! What a crash leaves, by window, and how the next turn repairs it:
//!
//! - Before the batch is saved: nothing. The next turn signs the same spool
//!   entries again at the same sequence numbers, and nothing was sent.
//! - After the batch is saved, before the relay acknowledges: the saved file
//!   exists and the spool position is old. The next turn sends the saved
//!   bytes. The relay counts identical entries as duplicates.
//! - After the relay acknowledges, before the spool confirms: the same as the
//!   window before, and the relay answers with duplicates.
//! - After the spool confirms, before the own head row is written: the spool
//!   position is past the saved batch. The next turn writes the head row from
//!   the saved entries and deletes the file.
//! - After the head row, before the file is deleted: the next turn finds the
//!   head row at the saved batch, and deletes the file.

use std::sync::Arc;

use fold_db::fold_db_core::mutation_manager::ReplayStreamHead;
use fold_db::record_value::FileMarker;
use fold_db::storage::disk_backup::accepted::{
    Entry, Error as SpoolError, Page, UploadIdentity, UploadPosition,
};
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{
    author_data, entry_hash, AuthorCtx, DataBody, Hex32, MutationFileRef, SignedEntry, StreamHead,
    StreamKind, SEALED_NAMESPACE,
};

use super::body::{self, AcceptedBody, Change, RecordFile};
use super::saved::{self, Saved};
use super::{apply, files, send, Ctx};
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// The relay takes at most this many entries in one append.
const PAGE: u32 = 90;
/// A batch stops at 2 MiB of plaintext + file payload so saved batches and HTTP requests stay small.
const BATCH_BYTES: usize = 2 * 1024 * 1024;

/// Sign and send everything the spool holds. Reports the spool position to the
/// status of the node.
pub(super) async fn run(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    let result = drain(ctx, client, streams).await;
    let status = ctx.runtime.accepted_status();
    ctx.host.cloud_sync.record_upload(
        &ctx.database_id,
        status.head,
        status.acked,
        result.as_ref().err().cloned(),
    );
    result
}

async fn drain(ctx: &Ctx<'_>, client: &RelayClient, streams: &[RelayStream]) -> Result<(), String> {
    if !ctx.runtime.captures_accepted() {
        return Err("local capture is off: the node must restart with a relay configured".into());
    }
    let mut birth_known = streams
        .iter()
        .any(|stream| stream.device_id == ctx.birth_device_hex);
    loop {
        let saved = match saved::load(&ctx.host.home, &ctx.database_id)? {
            Some(saved) => saved,
            None => match next_batch(ctx, streams).await? {
                Some(saved) => saved,
                None => return Ok(()),
            },
        };
        finish(ctx, client, &saved, &mut birth_known).await?;
    }
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
    blocking(ctx, |runtime, identity| {
        if let Some(found) = runtime.accepted_upload_position(identity)? {
            return Ok(found);
        }
        if runtime.accepted_status().acked != 0 {
            return Err(SpoolError::Behind);
        }
        let first = UploadPosition {
            writer_seq: 0,
            spool_through: 0,
        };
        runtime.confirm_accepted_upload(identity, 0, first)?;
        Ok(first)
    })
    .await
}

/// Sign the next batch of the spool and save it. `None` when the spool is empty.
async fn next_batch(ctx: &Ctx<'_>, streams: &[RelayStream]) -> Result<Option<Saved>, String> {
    let position = position(ctx).await?;
    let relay_head = streams
        .iter()
        .find(|stream| stream.device_id == ctx.device_hex)
        .map_or(0, |stream| stream.head_seq);
    if relay_head > position.writer_seq {
        return Err("the relay holds entries of this device that it never confirmed".into());
    }
    if ctx.runtime.accepted_status().gap {
        return Err("local capture has a gap".into());
    }
    let after = position.spool_through;
    let page = blocking(ctx, move |runtime, _| runtime.accepted_prefix(after, PAGE)).await?;
    if page.items.is_empty() {
        return Ok(None);
    }
    let head = match ctx.own_head().await? {
        Some(held) if held.seq != position.writer_seq => {
            return Err("the own stream head and the spool position disagree".into());
        }
        Some(held) => Some(held),
        None if position.writer_seq != 0 => {
            return Err("the own stream head is missing".into());
        }
        None => None,
    };
    let saved = sign_batch(ctx, &page, after, head.as_ref())?;
    saved::store(&ctx.host.home, &ctx.database_id, &saved)?;
    Ok(Some(saved))
}

/// Sign the entries of a page, as many as fit in a batch.
fn sign_batch(
    ctx: &Ctx<'_>,
    page: &Page,
    after: u64,
    head: Option<&ReplayStreamHead>,
) -> Result<Saved, String> {
    let mut head = head.map(super::chain_head).transpose()?;
    let (mut entries, mut bytes, mut through) = (Vec::new(), 0usize, after);
    for item in &page.items {
        let (plain, marker) = open_item(ctx, item)?;
        let file_bytes = marker.as_ref().map_or(0usize, |m| {
            usize::try_from(m.size_bytes).unwrap_or(usize::MAX)
        });
        let next_bytes = item.envelope.len().saturating_add(file_bytes);
        if !entries.is_empty() && bytes.saturating_add(next_bytes) > BATCH_BYTES {
            break;
        }
        let entry = sign_one(ctx, item, &plain, marker.as_ref(), head)?;
        let header = entry.header();
        head = Some(StreamHead {
            entry_hash: entry_hash(&entry).map_err(|e| e.to_string())?,
            writer_seq: header.writer_seq,
            lamport: header.lamport,
        });
        bytes = bytes.saturating_add(next_bytes);
        through = item.sequence;
        entries.push(entry);
    }
    Ok(Saved {
        after,
        through,
        entries,
    })
}

fn open_item(ctx: &Ctx<'_>, item: &Entry) -> Result<(Vec<u8>, Option<FileMarker>), String> {
    let plain = fold_db::crypto::decrypt_envelope(&ctx.content_key, &item.envelope)
        .map_err(|_| "an accepted write cannot be opened".to_owned())?;
    let accepted: AcceptedBody = body::parse(&plain)?;
    if accepted.mutation_id != item.mutation_id {
        return Err("an accepted write names another mutation than its spool entry".into());
    }
    let marker = match body::classify(&accepted)? {
        Change::Record(record) => match record.file {
            RecordFile::Put(marker) => Some(marker),
            RecordFile::Keep | RecordFile::Clear => None,
        },
        Change::Schema(_) => None,
    };
    Ok((plain, marker))
}

/// The signed entry of one spool entry, after `head`.
fn sign_one(
    ctx: &Ctx<'_>,
    item: &Entry,
    plain: &[u8],
    marker: Option<&FileMarker>,
    head: Option<StreamHead>,
) -> Result<SignedEntry, String> {
    let identity = &ctx.identity;
    let writer_seq = head.map_or(1, |held| held.writer_seq + 1);
    // The store stamped this write at its commit with its number in the spool.
    if writer_seq != item.sequence {
        return Err("the spool and the stream of this device number a write differently".into());
    }
    let seal = |bytes: &[u8]| {
        ctx.sealer
            .seal(identity.node_key, identity.device_id, writer_seq, bytes)
    };
    let file = match marker {
        Some(marker) => {
            let plain_file = files::plain_of(
                &ctx.content_key,
                marker,
                item.file_ciphertext.as_deref(),
                item.file_reference.as_deref(),
            )?;
            let blob = seal(&plain_file)?;
            let sha256 = files::sha256_hex(&blob);
            saved::save_blob(&ctx.host.home, &ctx.database_id, &sha256, &blob)?;
            Some(MutationFileRef {
                file_handle: "file".to_owned(),
                sha256: Hex32::from_hex(&sha256)
                    .map_err(|_| "a file digest is not hex".to_owned())?,
            })
        }
        None => None,
    };
    // The order value is the time of the commit, taken from the store's author
    // clock. It is set when the write is made, so a write made offline keeps
    // the time it was made. The clock gives each captured write a millisecond of
    // its own, so this is the order value the entry gets, and the one the store
    // stamped the write with.
    let order = item
        .authored_at_ms
        .ok_or("an accepted write has no author time")?;
    let author = AuthorCtx {
        database_id: ctx.database,
        node_key: identity.node_key,
        device_id: identity.device_id,
        stream_head: head,
        seen_heads: vec![ctx.birth_hash],
        seen_lamports: vec![ctx.birth.position.lamport],
        authored_at_ms: order,
    };
    let data = DataBody {
        stream: StreamKind::Direct,
        namespace: SEALED_NAMESPACE.to_owned(),
        payload: seal(plain)?,
        file,
        incorporated_from: None,
        grant_at_device: None,
        grant_at_seq: None,
    };
    author_data(&author, &identity.signing_key, data).map_err(|e| e.to_string())
}

/// Send, confirm and forget one saved batch.
async fn finish(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    saved: &Saved,
    birth_known: &mut bool,
) -> Result<(), String> {
    if position(ctx).await?.spool_through < saved.through {
        send::send(ctx, client, saved, birth_known).await?;
        let (after, through) = (saved.after, saved.through);
        let writer_seq = saved
            .entries
            .last()
            .map(|entry| entry.header().writer_seq)
            .ok_or("a saved batch holds no entry")?;
        blocking(ctx, move |runtime, identity| {
            runtime.confirm_accepted_upload(
                identity,
                after,
                UploadPosition {
                    writer_seq,
                    spool_through: through,
                },
            )
        })
        .await?;
    }
    commit_head(ctx, saved).await?;
    let blobs: Vec<String> = saved
        .entries
        .iter()
        .filter_map(|entry| match entry {
            SignedEntry::Data {
                body: DataBody {
                    file: Some(file), ..
                },
                ..
            } => Some(file.sha256.to_hex()),
            _ => None,
        })
        .collect();
    saved::clear(&ctx.host.home, &ctx.database_id, &blobs)
}

/// Move the own head row to the last entry of the batch. It repeats safely.
async fn commit_head(ctx: &Ctx<'_>, saved: &Saved) -> Result<(), String> {
    let last = saved.entries.last().ok_or("a saved batch holds no entry")?;
    let header = last.header();
    if ctx
        .own_head()
        .await?
        .is_some_and(|held| held.seq >= header.writer_seq)
    {
        return Ok(());
    }
    let hash = entry_hash(last).map_err(|e| e.to_string())?;
    ctx.host
        .db
        .mutation_manager()
        .set_replay_stream_head(&ctx.own_stream, &apply::head_of(header, &hash))
        .await
        .map_err(|error| format!("the own stream head could not be written: {error}"))
}
