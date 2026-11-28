//! What one pass of the sweep reads: the manifest of the newest snapshot, the entries after its
//! positions, and what the relay then holds.

use std::collections::{BTreeMap, BTreeSet};

use mutation_log::{
    entry_hash, named_blobs, verify_entry_signature, EntryHash, Hex32, SignedEntry, SnapshotBody,
    StreamHead,
};

use super::snapshot_manifest::Manifest;
use super::snapshot_restore::fetch_manifest;
use super::snapshot_state::SNAPSHOT;
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Items (`ReadItem::Entry` or `ReadItem::Segment`) requested per `log.read` page during a sweep.
const PAGE: u32 = 16;
/// Empty cursor pages tolerated in a row before treating `log.read` as stalled.
const MAX_EMPTY_PAGES: u32 = 64;

/// The entries after the positions of a snapshot.
pub(super) struct Tail {
    /// The digests they name.
    named: BTreeSet<String>,
    entries: u64,
    entry_bytes: u64,
    /// The hash of the snapshot entry that the tail follows.
    snapshot_hash: EntryHash,
    /// For each stream, the last entry read and its hash, so that a later pass of the same sweep
    /// reads only the entries appended since.
    read_to: BTreeMap<String, Position>,
}

/// The last entry read of a stream, and the hash the next entry chains to.
type Position = (u64, Option<EntryHash>);

/// The manifest that the newest snapshot entry names, fetched from the relay and checked
/// against the digest and the length in the entry.
pub(super) async fn manifest_of(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    snapshot: &SnapshotBody,
) -> Result<Manifest, String> {
    fetch_manifest(
        client,
        &ctx.database_id,
        &snapshot.manifest_sha256.to_hex(),
        snapshot.manifest_bytes,
    )
    .await
}

/// Read from the relay every entry after the positions of `snapshot`, up to the listed heads,
/// and the snapshot entry itself. Every entry is verified by signature and hash chain before the
/// sweep trusts what it names.
///
/// `previous` is the tail an earlier pass of the same sweep read. When it follows the same
/// snapshot entry, the read goes on from where it ended: a stream only grows after the snapshot,
/// so the entries it read are still the entries the relay holds. Without this, each pass read the
/// whole tail again; after an old snapshot that is thousands of segments for every pass, and the
/// sync turn waited for all of them before it sent anything.
pub(super) async fn read_tail(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    snapshot_head: &StreamHead,
    snapshot: &SnapshotBody,
    previous: Option<Tail>,
) -> Result<Option<Tail>, String> {
    check_stream_coverage(streams, snapshot)?;
    let mut tail = previous
        .filter(|tail| tail.snapshot_hash == snapshot_head.entry_hash)
        .unwrap_or_else(|| Tail {
            named: BTreeSet::new(),
            entries: 0,
            entry_bytes: 0,
            snapshot_hash: snapshot_head.entry_hash,
            read_to: BTreeMap::new(),
        });
    let resume = std::mem::take(&mut tail.read_to);
    let mut reader = TailReader {
        ctx,
        client,
        snapshot_head,
        snapshot,
        tail,
    };
    for stream in streams {
        let at = resume.get(&stream.device_id).copied();
        if !reader.read_stream(stream, at).await? {
            return Ok(None);
        }
    }
    Ok(Some(reader.tail))
}

/// Only the read-only tail fetch is interruptible. A fenced trim or drop is
/// allowed to finish; no partially read tail is ever used to delete files.
async fn snapshot_requested(ctx: &Ctx<'_>) {
    loop {
        if SNAPSHOT.waiting(&ctx.host.home, &ctx.database_id).is_some() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

fn check_stream_coverage(streams: &[RelayStream], snapshot: &SnapshotBody) -> Result<(), String> {
    if snapshot.through.iter().any(|at| at.writer_key.is_some()) {
        return Err("reclaim needs the stream listing of every snapshot writer".into());
    }
    let mut seen_devices = BTreeSet::new();
    for stream in streams {
        if !seen_devices.insert(stream.device_id.as_str()) {
            return Err("the relay listed a device stream more than once".into());
        }
    }
    for at in snapshot.through.iter().filter(|at| at.writer_seq > 0) {
        let hex = at.device_id.to_hex();
        let covered = streams
            .iter()
            .any(|stream| stream.device_id == hex && stream.head_seq >= at.writer_seq);
        if !covered {
            return Err(format!(
                "the relay lacks stream {hex} through entry {} that the newest snapshot covers",
                at.writer_seq
            ));
        }
    }
    Ok(())
}

struct TailReader<'a, 'b> {
    ctx: &'a Ctx<'b>,
    client: &'a RelayClient,
    snapshot_head: &'a StreamHead,
    snapshot: &'a SnapshotBody,
    tail: Tail,
}

impl TailReader<'_, '_> {
    fn start_for(&self, stream: &RelayStream) -> Result<(u64, Option<EntryHash>), String> {
        let (position, expected_prev) = if stream.device_id == self.ctx.snapshot_device_hex {
            let pos = stream.head_seq.saturating_sub(1);
            (pos, (pos == 0).then_some(EntryHash::ZERO))
        } else {
            match self
                .snapshot
                .through
                .iter()
                .find(|at| at.device_id.to_hex() == stream.device_id && at.writer_seq > 0)
            {
                Some(at) => (at.writer_seq, Some(at.entry_hash)),
                None => (0, Some(EntryHash::ZERO)),
            }
        };
        if position > stream.head_seq {
            return Err(format!(
                "the newest snapshot covers entry {position} of a stream that the relay holds through entry {} only",
                stream.head_seq
            ));
        }
        if position > 0
            && position == stream.head_seq
            && expected_prev.is_some_and(|prev| prev.to_hex() != stream.head_hash)
        {
            return Err(format!(
                "stream {} at entry {position} does not match the hash in the newest snapshot",
                stream.device_id
            ));
        }
        Ok((position, expected_prev))
    }

    /// Where a stream read earlier in this sweep goes on: after the last entry read, which the
    /// relay must still hold with the same hash when it is the head.
    fn resume_at(stream: &RelayStream, at: Position) -> Result<Position, String> {
        let (position, prev) = at;
        if position > stream.head_seq {
            return Err(format!(
                "the relay holds stream {} through entry {} only, below entry {position} that this sweep read",
                stream.device_id, stream.head_seq
            ));
        }
        if position == stream.head_seq && prev.is_some_and(|hash| hash.to_hex() != stream.head_hash)
        {
            return Err(format!(
                "stream {} at entry {position} does not match the entry this sweep read",
                stream.device_id
            ));
        }
        Ok(at)
    }

    async fn read_stream(
        &mut self,
        stream: &RelayStream,
        at: Option<Position>,
    ) -> Result<bool, String> {
        let (position, mut expected_prev) = match at {
            Some(at) => Self::resume_at(stream, at)?,
            None => self.start_for(stream)?,
        };
        let mut next = position + 1;
        let mut cursor: Option<String> = None;
        let mut empty_pages: u32 = 0;
        while next <= stream.head_seq {
            let want = (stream.head_seq - next + 1).min(u64::from(PAGE)) as u32;
            let page = tokio::select! {
                biased;
                () = snapshot_requested(self.ctx) => return Ok(false),
                result = self.client.read_page(
                    &self.ctx.database_id,
                    &self.ctx.node_hex,
                    Some(&stream.device_id),
                    next,
                    cursor.as_deref(),
                    want,
                ) => result?,
            };
            // A segment's blob is needed while its item is stored, like a file an entry names.
            self.tail.named.extend(page.segments);
            let (entries, next_cursor) = (page.entries, page.cursor);
            if entries.is_empty() {
                if let Some(advance) = next_cursor.filter(|c| {
                    !c.is_empty()
                        && cursor.as_deref() != Some(c.as_str())
                        && empty_pages < MAX_EMPTY_PAGES
                }) {
                    empty_pages += 1;
                    cursor = Some(advance);
                    continue;
                }
                return Err(format!(
                    "the relay returned no entry {next} of a stream whose head is {}",
                    stream.head_seq
                ));
            }
            cursor = None;
            empty_pages = 0;
            for item in entries {
                if next > stream.head_seq {
                    break;
                }
                let hash = self.verify_entry(stream, next, expected_prev, &item.entry)?;
                expected_prev = Some(hash);
                self.tail
                    .named
                    .extend(named_blobs(&item.entry).iter().map(Hex32::to_hex));
                self.tail.entries = self.tail.entries.saturating_add(1);
                self.tail.entry_bytes = self.tail.entry_bytes.saturating_add(item.inline_bytes);
                next += 1;
            }
        }
        self.tail
            .read_to
            .insert(stream.device_id.clone(), (next - 1, expected_prev));
        Ok(true)
    }

    fn verify_entry(
        &self,
        stream: &RelayStream,
        next: u64,
        expected_prev: Option<EntryHash>,
        entry: &SignedEntry,
    ) -> Result<EntryHash, String> {
        let header = entry.header();
        if header.database_id != self.ctx.database
            || header.node_key != self.ctx.identity.node_key
            || header.device_id.to_hex() != stream.device_id
            || header.writer_seq != next
        {
            return Err(format!(
                "the relay returned another entry than entry {next}"
            ));
        }
        if (next == 1) != (header.prev_hash == EntryHash::ZERO)
            || expected_prev.is_some_and(|prev| header.prev_hash != prev)
        {
            return Err(format!(
                "entry {next} of stream {} does not chain to its predecessor",
                stream.device_id
            ));
        }
        verify_entry_signature(entry)
            .map_err(|error| format!("entry {next} of stream {}: {error}", stream.device_id))?;
        let hash = entry_hash(entry).map_err(|error| error.to_string())?;
        if stream.device_id == self.ctx.snapshot_device_hex
            && next == self.snapshot_head.writer_seq
            && hash != self.snapshot_head.entry_hash
        {
            return Err("the snapshot entry changed from the verified head".into());
        }
        if next == stream.head_seq && hash.to_hex() != stream.head_hash {
            return Err(format!(
                "entry {next} of stream {} does not match its listed head hash",
                stream.device_id
            ));
        }
        Ok(hash)
    }
}

/// What one pass saw: the newest snapshot, the entries after it, and the listing of the relay.
pub(super) struct Seen<'a> {
    pub(super) snapshot_seq: u64,
    pub(super) manifest_sha256: String,
    pub(super) manifest_bytes: u64,
    /// The files of the manifest, one for each digest, with their length.
    pub(super) files: BTreeMap<&'a str, u64>,
    pub(super) list: &'a Manifest,
    pub(super) tail: &'a Tail,
    pub(super) listed: &'a BTreeMap<String, u64>,
}

impl Seen<'_> {
    /// True when the snapshot or an entry after it names `digest`.
    pub(super) fn needs(&self, digest: &str) -> bool {
        digest == self.manifest_sha256
            || self.files.contains_key(digest)
            || self.tail.named.contains(digest)
    }

    /// What the relay holds, for a pass that found nothing to drop.
    pub(super) fn status(&self) -> ReclaimStatus {
        // The files the entries name and the snapshot does not, with their length on the relay.
        let named: Vec<Option<u64>> = self
            .tail
            .named
            .iter()
            .filter(|digest| {
                **digest != self.manifest_sha256 && !self.files.contains_key(digest.as_str())
            })
            .map(|digest| self.listed.get(digest).copied())
            .collect();
        let missing = named.iter().filter(|held| held.is_none()).count();
        ReclaimStatus {
            swept_at_ms: Some(fold_db::clock::unix_millis()),
            snapshot_seq: Some(self.snapshot_seq),
            snapshot_files: Some(self.list.files.len() as u64),
            snapshot_bytes: Some(self.list.bytes()),
            snapshot_blobs: Some(self.files.len() as u64),
            snapshot_blob_bytes: Some(self.files.values().copied().fold(0, u64::saturating_add)),
            manifest_bytes: Some(self.manifest_bytes),
            named_files: Some(named.len() as u64),
            named_bytes: Some(named.iter().flatten().copied().fold(0, u64::saturating_add)),
            missing_files: Some(missing as u64),
            entries: Some(self.tail.entries),
            entry_bytes: Some(self.tail.entry_bytes),
            cloud_files: Some(self.listed.len() as u64),
            cloud_bytes: Some(self.listed.values().copied().fold(0, u64::saturating_add)),
            dropped_files: None,
            released_bytes: None,
            message: None,
        }
    }
}
