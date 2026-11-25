//! What one pass of the sweep reads: the manifest of the newest snapshot, the entries after its
//! positions, and what the relay then holds.

use std::collections::{BTreeMap, BTreeSet};

use mutation_log::{
    entry_hash, named_blobs, verify_entry_signature, EntryHash, Hex32, SignedEntry, SnapshotBody,
    StreamHead,
};

use super::snapshot_manifest::{Manifest, MANIFEST_LIMIT};
use super::snapshot_restore::fetch_manifest;
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Entries in one read of a stream.
const PAGE: u32 = 90;
/// Empty cursor pages tolerated in a row before treating `log.read` as stalled.
const MAX_EMPTY_PAGES: u32 = 64;

/// The entries after the positions of a snapshot.
pub(super) struct Tail {
    /// The digests they name.
    named: BTreeSet<String>,
    entries: u64,
    entry_bytes: u64,
}

/// The manifest that the newest snapshot entry names, fetched from the relay and checked
/// against the digest and the length in the entry.
pub(super) async fn manifest_of(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    snapshot: &SnapshotBody,
) -> Result<Manifest, String> {
    if snapshot.manifest_bytes == 0 || snapshot.manifest_bytes > MANIFEST_LIMIT {
        return Err("the newest snapshot entry names a manifest of an unusable length".into());
    }
    let dir = ctx.host.home.join("log-sync").join(&ctx.database_id);
    fetch_manifest(
        client,
        &ctx.database_id,
        &dir,
        &snapshot.manifest_sha256.to_hex(),
        snapshot.manifest_bytes,
    )
    .await
}

/// Read from the relay every entry after the positions of `snapshot`, up to the listed heads,
/// and the snapshot entry itself. Every entry is verified by signature and hash chain before the
/// sweep trusts what it names.
pub(super) async fn read_tail(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    snapshot_head: &StreamHead,
    snapshot: &SnapshotBody,
) -> Result<Tail, String> {
    check_stream_coverage(streams, snapshot)?;
    let mut reader = TailReader {
        ctx,
        client,
        snapshot_head,
        snapshot,
        tail: Tail {
            named: BTreeSet::new(),
            entries: 0,
            entry_bytes: 0,
        },
    };
    for stream in streams {
        reader.read_stream(stream).await?;
    }
    Ok(reader.tail)
}

fn check_stream_coverage(streams: &[RelayStream], snapshot: &SnapshotBody) -> Result<(), String> {
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

    async fn read_stream(&mut self, stream: &RelayStream) -> Result<(), String> {
        let (position, mut expected_prev) = self.start_for(stream)?;
        let mut next = position + 1;
        let mut cursor: Option<String> = None;
        let mut empty_pages: u32 = 0;
        while next <= stream.head_seq {
            let want = (stream.head_seq - next + 1).min(u64::from(PAGE)) as u32;
            let (entries, next_cursor) = self
                .client
                .read(
                    &self.ctx.database_id,
                    &self.ctx.node_hex,
                    Some(&stream.device_id),
                    next,
                    cursor.as_deref(),
                    want,
                )
                .await?;
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
            for entry in &entries {
                if next > stream.head_seq {
                    return Err("the relay returned entries above the listed stream head".into());
                }
                let hash = self.verify_entry(stream, next, expected_prev, entry)?;
                expected_prev = Some(hash);
                self.tail
                    .named
                    .extend(named_blobs(entry).iter().map(Hex32::to_hex));
                self.tail.entries = self.tail.entries.saturating_add(1);
                let len = serde_json::to_vec(entry)
                    .map_err(|_| "cannot measure an entry")?
                    .len() as u64;
                self.tail.entry_bytes = self.tail.entry_bytes.saturating_add(len);
                next += 1;
            }
        }
        Ok(())
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

    /// How many files of the snapshot the relay does not list with their length: its manifest
    /// and the files the manifest names. A sweep drops nothing while this is above zero.
    pub(super) fn lacking(&self) -> usize {
        let lacks = |digest: &str, bytes: u64| self.listed.get(digest) != Some(&bytes);
        self.files
            .iter()
            .filter(|(digest, bytes)| lacks(digest, **bytes))
            .count()
            + usize::from(lacks(&self.manifest_sha256, self.manifest_bytes))
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
        let missing = named.iter().filter(|held| held.is_none()).count() + self.lacking();
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
