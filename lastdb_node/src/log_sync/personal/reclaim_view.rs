//! What one pass of the sweep reads: the manifest of the newest snapshot, the entries after its
//! positions, and what the relay then holds.

use std::collections::{BTreeMap, BTreeSet};

use mutation_log::{named_blobs, Hex32, SnapshotBody};

use super::snapshot_manifest::{Manifest, MANIFEST_LIMIT};
use super::snapshot_restore::fetch_manifest;
use super::Ctx;
use crate::host::cloud_sync::ReclaimStatus;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Entries in one read of a stream.
const PAGE: u32 = 90;

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
    fetch_manifest(
        client,
        &ctx.database_id,
        &ctx.host.home,
        &snapshot.manifest_sha256.to_hex(),
        snapshot.manifest_bytes,
    )
    .await
}

/// Read from the relay every entry after the positions of `snapshot`, up to the listed heads,
/// and the snapshot entry itself. A read that cannot return every one of them is an error, and
/// then the sweep drops nothing: a gap, an entry out of place, or a position at or below what a
/// trim dropped (the relay answers 410).
pub(super) async fn read_tail(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    snapshot: &SnapshotBody,
) -> Result<Tail, String> {
    let mut tail = Tail {
        named: BTreeSet::new(),
        entries: 0,
        entry_bytes: 0,
    };
    for stream in streams {
        let position = if stream.device_id == ctx.snapshot_device_hex {
            stream.head_seq.saturating_sub(1)
        } else {
            snapshot
                .through
                .iter()
                .find(|at| at.device_id.to_hex() == stream.device_id)
                .map_or(0, |at| at.writer_seq)
        };
        if position > stream.head_seq {
            return Err(format!(
                "the newest snapshot covers entry {position} of a stream that the relay holds through entry {} only",
                stream.head_seq
            ));
        }
        let mut next = position + 1;
        while next <= stream.head_seq {
            let (entries, _) = client
                .read(
                    &ctx.database_id,
                    &ctx.node_hex,
                    Some(&stream.device_id),
                    next,
                    None,
                    PAGE,
                )
                .await?;
            if entries.is_empty() {
                return Err(format!(
                    "the relay returned no entry {next} of a stream whose head is {}",
                    stream.head_seq
                ));
            }
            for entry in &entries {
                let header = entry.header();
                if header.writer_seq != next || header.device_id.to_hex() != stream.device_id {
                    return Err(format!(
                        "the relay returned another entry than entry {next}"
                    ));
                }
                tail.named
                    .extend(named_blobs(entry).iter().map(Hex32::to_hex));
                tail.entries += 1;
                tail.entry_bytes += serde_json::to_vec(entry)
                    .map_err(|_| "cannot measure an entry")?
                    .len() as u64;
                next += 1;
            }
        }
    }
    Ok(tail)
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
        let lacks = |digest: &str, bytes: u64| self.listed.get(digest) != Some(&bytes);
        let missing = named.iter().filter(|held| held.is_none()).count()
            + self
                .files
                .iter()
                .filter(|(digest, bytes)| lacks(digest, **bytes))
                .count()
            + usize::from(lacks(&self.manifest_sha256, self.manifest_bytes));
        ReclaimStatus {
            swept_at_ms: Some(fold_db::clock::unix_millis()),
            snapshot_seq: Some(self.snapshot_seq),
            snapshot_files: Some(self.list.files.len() as u64),
            snapshot_bytes: Some(self.list.bytes()),
            snapshot_blobs: Some(self.files.len() as u64),
            snapshot_blob_bytes: Some(self.files.values().sum()),
            manifest_bytes: Some(self.manifest_bytes),
            named_files: Some(named.len() as u64),
            named_bytes: Some(named.iter().flatten().sum()),
            missing_files: Some(missing as u64),
            entries: Some(self.tail.entries),
            entry_bytes: Some(self.tail.entry_bytes),
            cloud_files: Some(self.listed.len() as u64),
            cloud_bytes: Some(self.listed.values().sum()),
            dropped_files: None,
            released_bytes: None,
            message: None,
        }
    }
}
