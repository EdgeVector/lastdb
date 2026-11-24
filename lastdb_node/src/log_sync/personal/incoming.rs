//! Read the other devices' streams from the relay and apply them.
//!
//! Each turn the node asks the relay for the heads of the owner's device
//! streams, which is one call and no entries. A stream whose relay head is above
//! the head row of this node is read from the entry after that row. The streams
//! are merged by order value (then node key, device id and sequence number), one
//! page of each at a time, so memory does not depend on how many entries there
//! are, and a schema change arrives before the records that use it.
//!
//! Every entry is verified (signature, chain to the head row, order rule), opened
//! and applied by [`apply::remote`], which writes the record, its winner row and
//! the head row of the stream in one store transaction. After a restart nothing
//! is replayed: the head rows say where each stream stands. A file that a record
//! names is fetched and checked before the record commits.

use std::collections::VecDeque;

use fold_db::fold_db_core::mutation_manager::ReplayStreamHead;
use mutation_log::{DataBody, SealOpener, SignedEntry};

use super::body::{self, Change, RecordFile};
use super::verify::{self, Verdict};
use super::{apply, files, Ctx};
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

/// The relay returns at most this many entries in one page.
const PAGE: u32 = 90;

/// What one run applied.
#[derive(Default)]
pub(super) struct Applied {
    /// Entries applied.
    pub(super) entries: u64,
    /// The highest sequence number among them.
    pub(super) through: u64,
    /// Device streams of other devices that the relay lists.
    pub(super) streams: usize,
}

/// One stream being read.
struct Cursor {
    device: String,
    name: String,
    /// The next sequence number to read.
    next: u64,
    /// The relay head when the run began. Nothing above it is read.
    limit: u64,
    head: Option<ReplayStreamHead>,
    page: VecDeque<SignedEntry>,
}

impl Cursor {
    /// Read a page when the buffer is empty and the relay holds more.
    async fn fill(&mut self, ctx: &Ctx<'_>, client: &RelayClient) -> Result<(), String> {
        if !self.page.is_empty() || self.next > self.limit {
            return Ok(());
        }
        let (entries, _) = client
            .read(
                &ctx.database_id,
                &ctx.node_hex,
                Some(self.device.as_str()),
                self.next,
                None,
                PAGE,
            )
            .await?;
        for entry in entries {
            let seq = entry.header().writer_seq;
            if seq != self.next || seq > self.limit {
                break;
            }
            self.next += 1;
            self.page.push_back(entry);
        }
        if self.page.is_empty() {
            return Err("the relay holds fewer entries of a stream than it reports".into());
        }
        Ok(())
    }

    /// The merge order of the entry in front.
    fn front(&self) -> Option<(u64, mutation_log::NodeKey, mutation_log::DeviceId, u64)> {
        let header = self.page.front()?.header();
        Some((
            header.lamport,
            header.node_key,
            header.device_id,
            header.writer_seq,
        ))
    }
}

/// Apply what the relay holds of the other devices that this node does not.
pub(super) async fn run(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<Applied, String> {
    clear_orphaned_incoming(ctx);
    let others: Vec<&RelayStream> = streams
        .iter()
        .filter(|stream| {
            stream.device_id != ctx.device_hex
                && stream.device_id != ctx.birth_device_hex
                && stream.device_id != ctx.snapshot_device_hex
        })
        .collect();
    let names: Vec<String> = others
        .iter()
        .map(|stream| ctx.stream_name(&stream.device_id))
        .collect();
    let heads = ctx
        .host
        .db
        .mutation_manager()
        .replay_stream_heads(&names)
        .await
        .map_err(|error| format!("the stream heads could not be read: {error}"))?;
    let mut applied = Applied {
        streams: others.len(),
        ..Applied::default()
    };
    let mut cursors = Vec::new();
    for ((stream, name), head) in others.iter().zip(names).zip(heads) {
        if let Some(held) = &head {
            if held.frozen {
                continue;
            }
            if held.seq == stream.head_seq && held.hash != stream.head_hash {
                return Err(format!(
                    "the relay's entry {} of a stream differs from the applied one",
                    held.seq
                ));
            }
        }
        let held = head.as_ref().map_or(0, |held| held.seq);
        if stream.head_seq > held {
            cursors.push(Cursor {
                device: stream.device_id.clone(),
                name,
                next: held + 1,
                limit: stream.head_seq,
                head,
                page: VecDeque::new(),
            });
        }
    }
    merge(ctx, client, &mut cursors, &mut applied).await?;
    Ok(applied)
}

/// Apply the entries of the cursors in merge order.
async fn merge(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cursors: &mut [Cursor],
    applied: &mut Applied,
) -> Result<(), String> {
    loop {
        for cursor in &mut *cursors {
            cursor.fill(ctx, client).await?;
        }
        let Some(at) = cursors
            .iter()
            .enumerate()
            .filter_map(|(at, cursor)| cursor.front().map(|key| (key, at)))
            .min()
            .map(|(_, at)| at)
        else {
            return Ok(());
        };
        let cursor = &mut cursors[at];
        let Some(entry) = cursor.page.pop_front() else {
            return Ok(());
        };
        apply_entry(ctx, client, cursor, &entry, applied).await?;
    }
}

async fn apply_entry(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cursor: &mut Cursor,
    entry: &SignedEntry,
    applied: &mut Applied,
) -> Result<(), String> {
    let head = cursor.head.as_ref().map(super::chain_head).transpose()?;
    let hash = match verify::check(ctx, &cursor.device, head, entry)? {
        Verdict::Next { hash } => hash,
        Verdict::Refused(reason) => return freeze(ctx, cursor, &reason).await,
    };
    let SignedEntry::Data {
        header, body: data, ..
    } = entry
    else {
        return freeze(ctx, cursor, "an entry is not a data entry").await;
    };
    let Some(plain) = ctx.sealer.open(header, &data.payload) else {
        return freeze(ctx, cursor, "a payload does not open").await;
    };
    let Ok(accepted) = body::parse(&plain) else {
        return freeze(ctx, cursor, "a payload is not an accepted write").await;
    };
    let Ok(change) = body::classify(&accepted) else {
        return freeze(ctx, cursor, "a payload is not a valid change").await;
    };
    if let Some(reason) = invalid_file_reason(data, &change) {
        return freeze(ctx, cursor, reason).await;
    }
    let next = apply::head_of(header, &hash);
    if apply::superseded(ctx, header, &change).await? {
        apply::skip_remote(ctx, &cursor.name, &next, header).await?;
    } else {
        let blob = fetch_file(ctx, client, data, &change).await?;
        let outcome =
            apply::remote(ctx, &cursor.name, &next, header, change, blob.as_deref()).await;
        if let Some(path) = &blob {
            let _ = disk::remove_private(path);
        }
        if let apply::ApplyOutcome::InvalidEntry(reason) = outcome? {
            return freeze(ctx, cursor, reason).await;
        }
    }
    applied.entries += 1;
    applied.through = applied.through.max(header.writer_seq);
    cursor.head = Some(next);
    Ok(())
}

/// Remove any temporary or leftover incoming blob files from an interrupted turn.
fn clear_orphaned_incoming(ctx: &Ctx<'_>) {
    let dir = ctx
        .host
        .home
        .join("log-sync")
        .join(&ctx.database_id)
        .join("incoming");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_file()) {
            let _ = disk::remove_private(&entry.path());
        }
    }
}

fn invalid_file_reason(data: &DataBody, change: &Change) -> Option<&'static str> {
    let Change::Record(record) = change else {
        return None;
    };
    let RecordFile::Put(marker) = &record.file else {
        return None;
    };
    if data.file.is_none() {
        return Some("a record sets a file that its entry does not name");
    }
    if marker.size_bytes > fold_db::storage::MAX_RECORD_FILE_BYTES {
        return Some("a record file exceeds the 32 MiB store value limit");
    }
    None
}

/// Fetch the sealed blob of the file a record write sets, into a file.
async fn fetch_file(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    data: &DataBody,
    change: &Change,
) -> Result<Option<std::path::PathBuf>, String> {
    let Change::Record(record) = change else {
        return Ok(None);
    };
    let RecordFile::Put(marker) = &record.file else {
        return Ok(None);
    };
    let file = data
        .file
        .as_ref()
        .ok_or("a record sets a file that its entry does not name")?;
    let sha256 = file.sha256.to_hex();
    let dest = ctx
        .host
        .home
        .join("log-sync")
        .join(&ctx.database_id)
        .join("incoming")
        .join(&sha256);
    client
        .blob_get(
            &ctx.database_id,
            &sha256,
            marker.size_bytes + files::ENVELOPE_OVERHEAD,
            &dest,
        )
        .await?;
    Ok(Some(dest))
}

/// Mark a stream that carried an entry that is not valid. It is never read again.
async fn freeze(ctx: &Ctx<'_>, cursor: &mut Cursor, reason: &str) -> Result<(), String> {
    tracing::warn!(
        target: "lastdbd::log_sync",
        stream = %cursor.name,
        reason,
        "a device stream carried an invalid entry and is frozen"
    );
    let frozen = ReplayStreamHead {
        frozen: true,
        ..cursor.head.clone().unwrap_or(ReplayStreamHead {
            seq: 0,
            hash: mutation_log::Hex32::ZERO.to_hex(),
            order: 0,
            frozen: false,
        })
    };
    ctx.host
        .db
        .mutation_manager()
        .set_replay_stream_head(&cursor.name, &frozen)
        .await
        .map_err(|error| format!("a frozen stream could not be recorded: {error}"))?;
    cursor.head = Some(frozen);
    cursor.page.clear();
    cursor.next = cursor.limit + 1;
    Ok(())
}
