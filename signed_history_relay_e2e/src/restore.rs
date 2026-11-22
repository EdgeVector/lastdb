//! Restore from start with stream discovery, AcceptEntry, and bundle blobs.
//!
//! The invite names the publisher node and its first device stream. Every other
//! stream is found from publisher entries: `heads` tips name the
//! publisher's other devices and an incorporation names the member device
//! whose proposal it carries. Each
//! stream is read from `writer_seq = 1`; a bundle entry is applied only after
//! its blob downloads, hashes to the entry's digest, and decrypts.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use mutation_log::{
    AcceptOutcome, BundleBody, BundleChange, DeviceId, Hex32, NodeKey, SignedEntry, VerifiedHistory,
};
use sha2::{Digest, Sha256};

use crate::blob::{decode_changes, BundleCipher};
use crate::relay::{RelayClient, READ_LIMIT};
use crate::upload::Invite;

pub struct RestoreReport {
    pub history: VerifiedHistory,
    pub streams: usize,
    pub bundles: usize,
}

type Deferred = (SignedEntry, Vec<BundleChange>);

struct Ctx<'a> {
    client: &'a RelayClient,
    cipher: Arc<BundleCipher>,
    concurrency: usize,
}

pub async fn restore(
    client: &RelayClient,
    invite: &Invite,
    concurrency: usize,
) -> Result<RestoreReport, String> {
    let ctx = Ctx {
        client,
        cipher: Arc::new(BundleCipher::new(&invite.secret, invite.database_id)?),
        concurrency,
    };
    let mut history = VerifiedHistory::new(invite.database_id);
    let mut queue = VecDeque::from([(invite.owner, invite.root_device)]);
    let mut seen: BTreeSet<(NodeKey, DeviceId)> = queue.iter().copied().collect();
    let mut deferred = Vec::new();
    let mut bundles = 0;
    while let Some((writer, device)) = queue.pop_front() {
        let entries = pull_stream(client, writer, device).await?;
        bundles += apply_stream(&ctx, &mut history, &mut deferred, &entries).await?;
        if history.owner() != Some(invite.owner) {
            return Err("the invited owner has no accepted birth entry".into());
        }
        for stream in discover(&history, &entries) {
            if seen.insert(stream) {
                queue.push_back(stream);
            }
        }
    }
    verify_complete(&history, &deferred)?;
    Ok(RestoreReport {
        history,
        streams: seen.len(),
        bundles,
    })
}

async fn pull_stream(
    client: &RelayClient,
    writer: NodeKey,
    device: DeviceId,
) -> Result<Vec<SignedEntry>, String> {
    let (writer_hex, device_hex) = (writer.to_hex(), device.to_hex());
    let mut entries = Vec::new();
    let mut from_seq = 1u64;
    loop {
        let page = client.read(&writer_hex, &device_hex, from_seq).await?;
        let Some(last_seq) = page.entries.last().map(|e| e.header().writer_seq) else {
            break;
        };
        if last_seq < from_seq {
            return Err(format!(
                "relay returned seq {last_seq} for a read from {from_seq}"
            ));
        }
        let short = !page.more && (page.entries.len() as u64) < READ_LIMIT;
        entries.extend(page.entries);
        if short {
            break;
        }
        from_seq = last_seq + 1;
    }
    Ok(entries)
}

async fn apply_stream(
    ctx: &Ctx<'_>,
    history: &mut VerifiedHistory,
    deferred: &mut Vec<Deferred>,
    entries: &[SignedEntry],
) -> Result<usize, String> {
    let prepared = stream::iter(entries.iter().cloned())
        .map(|entry| prepare(ctx, entry))
        .buffered(ctx.concurrency);
    let mut prepared = Box::pin(prepared);
    let mut bundles = 0;
    while let Some(item) = prepared.next().await {
        let (entry, changes) = item?;
        bundles += usize::from(changes.is_some());
        apply_one(history, deferred, entry, changes)?;
    }
    Ok(bundles)
}

async fn prepare(
    ctx: &Ctx<'_>,
    entry: SignedEntry,
) -> Result<(SignedEntry, Option<Vec<BundleChange>>), String> {
    let changes = if let SignedEntry::Bundle { body, .. } = &entry {
        Some(fetch_changes(ctx, body).await?)
    } else {
        None
    };
    Ok((entry, changes))
}

async fn fetch_changes(ctx: &Ctx<'_>, body: &BundleBody) -> Result<Vec<BundleChange>, String> {
    let blob = ctx
        .client
        .blob_get(&body.blob_sha256.to_hex(), body.blob_size)
        .await?;
    let cipher = Arc::clone(&ctx.cipher);
    let (expected_sha, expected_size) = (body.blob_sha256, body.blob_size);
    tokio::task::spawn_blocking(move || {
        if blob.len() as u64 != expected_size {
            return Err("blob length differs from its bundle entry".to_owned());
        }
        let digest: [u8; 32] = Sha256::digest(&blob).into();
        if Hex32::from_bytes(digest) != expected_sha {
            return Err("blob sha256 differs from its bundle entry".to_owned());
        }
        decode_changes(&cipher.open(&blob)?)
    })
    .await
    .map_err(|e| format!("bundle worker: {e}"))?
}

fn apply_one(
    history: &mut VerifiedHistory,
    deferred: &mut Vec<Deferred>,
    entry: SignedEntry,
    changes: Option<Vec<BundleChange>>,
) -> Result<(), String> {
    if let Some(changes) = changes {
        if let Some(held) = accept_bundle(history, entry, changes)? {
            deferred.push(held);
        }
    } else if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
        return Err(format!("AcceptEntry rejected an entry: {reason:?}"));
    }
    retry_deferred(history, deferred)
}

fn accept_bundle(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
    changes: Vec<BundleChange>,
) -> Result<Option<Deferred>, String> {
    match history.accept_bundle_with_changes(entry.clone(), &changes) {
        AcceptOutcome::Accept { .. } => Ok(None),
        AcceptOutcome::Pending { .. } => Ok(Some((entry, changes))),
        AcceptOutcome::Reject { reason } => {
            Err(format!("AcceptEntry rejected a bundle: {reason:?}"))
        }
    }
}

fn retry_deferred(
    history: &mut VerifiedHistory,
    deferred: &mut Vec<Deferred>,
) -> Result<(), String> {
    loop {
        let waiting = std::mem::take(deferred);
        let before = waiting.len();
        for (entry, changes) in waiting {
            if let Some(held) = accept_bundle(history, entry, changes)? {
                deferred.push(held);
            }
        }
        if deferred.len() == before {
            return Ok(());
        }
    }
}

/// Streams named by entries the publisher signed: `heads` tips (the publisher's
/// other devices) and `incorporated_from` (the member device that signed a
/// proposal the publisher incorporated). Either entry may wait on a stream that
/// is not read yet, so a stream is named before its entry is accepted;
/// AcceptEntry still judges every entry and a bad one fails the restore.
fn discover(history: &VerifiedHistory, entries: &[SignedEntry]) -> Vec<(NodeKey, DeviceId)> {
    let mut streams = Vec::new();
    for entry in entries {
        let owner = entry.header().node_key;
        if history.owner() != Some(owner) {
            continue;
        }
        match entry {
            SignedEntry::Heads { body, .. } => {
                streams.extend(
                    body.publisher_heads
                        .iter()
                        .map(|tip| (owner, tip.device_id)),
                );
            }
            SignedEntry::Data { body, .. } => {
                streams.extend(
                    body.incorporated_from
                        .iter()
                        .map(|from| (from.node_key, from.device_id)),
                );
            }
            _ => {}
        }
    }
    streams
}

/// Nothing pending, deferred or rejected, and every tip the publisher's latest
/// `heads` entry cites is held at its cited hash (a relay cannot hide an entry).
fn verify_complete(history: &VerifiedHistory, deferred: &[Deferred]) -> Result<(), String> {
    if !deferred.is_empty() {
        return Err(format!(
            "{} bundle entries still wait on dependencies",
            deferred.len()
        ));
    }
    if history.pending_len() > 0 {
        return Err(format!(
            "{} entries are still pending",
            history.pending_len()
        ));
    }
    if !history.rejects().is_empty() {
        return Err(format!("{} entries were rejected", history.rejects().len()));
    }
    let owner = history.owner().ok_or("no accepted birth entry")?;
    for tip in history.publisher_heads() {
        let stream = VerifiedHistory::stream(owner, tip.device_id);
        if history.entry_hash_at(&stream, tip.writer_seq) != Some(tip.entry_hash) {
            return Err(format!(
                "publisher heads cite device {} seq {} that the relay did not serve",
                tip.device_id, tip.writer_seq
            ));
        }
    }
    Ok(())
}
