//! The relay side of a snapshot: the positions of a cut, the signed `snapshot` entry, and the trim.

use mutation_log::{
    author_snapshot, entry_hash, verify_entry_signature, AuthorCtx, DeviceId, EntryHash, Hex32,
    SignedEntry, SnapshotBody, SnapshotThrough, StreamHead,
};

use super::Ctx;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Calls of one trim. The relay does a bounded amount of work in a call and says whether
/// it is done, so a large backlog takes several calls. What is left goes on at the next snapshot.
const TRIM_CALLS: usize = 64;

/// Ask the relay to drop what the newest snapshot covers: the entries up to its positions and
/// the snapshot entries before it. The relay then holds one snapshot and the entries after it.
/// The relay reads that snapshot entry itself and checks it. A trim drops no file: the sweep
/// (`reclaim`) drops the files that nothing names any more. A failure is logged and is not an
/// error of the snapshot: the snapshot is already named, nothing was lost, and the next snapshot
/// asks again.
pub(super) async fn trim_covered(ctx: &Ctx<'_>, client: &RelayClient, newest: u64) {
    if newest == 0 {
        return;
    }
    for _ in 0..TRIM_CALLS {
        match client
            .trim(&ctx.database_id, &ctx.snapshot_device_hex, newest)
            .await
        {
            Ok(true) => {
                tracing::info!(target: "lastdbd::log_sync", newest, "the relay dropped what the snapshot covers");
                return;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(target: "lastdbd::log_sync", %error, "the relay could not drop what the snapshot covers");
                return;
            }
        }
    }
}

/// The stream positions of a cut, ascending by device id.
pub(super) fn through_of(
    ctx: &Ctx<'_>,
    heads: &[(
        String,
        fold_db::fold_db_core::mutation_manager::ReplayStreamHead,
    )],
) -> Result<Vec<SnapshotThrough>, String> {
    let prefix = format!("{}:", ctx.node_hex);
    let mut through = Vec::new();
    for (stream, head) in heads {
        let device = stream
            .strip_prefix(&prefix)
            .ok_or("a stream head row names another writer")?;
        if head.seq == 0 {
            continue;
        }
        through.push(SnapshotThrough {
            device_id: DeviceId::from_hex(device)
                .map_err(|_| "a stream head row has a bad device")?,
            writer_seq: head.seq,
            entry_hash: EntryHash::from_hex(&head.hash)
                .map_err(|_| "a stream head row has a bad hash")?,
            order: head.order,
        });
    }
    through.sort_by_key(|position| position.device_id.to_bytes());
    Ok(through)
}

/// Refuse an initial (`through = []`) snapshot when the relay already holds device entries or a
/// snapshot that covers any stream. A retry of this device's own initial snapshot (only `birth`
/// and at most a `through = []` snapshot entry) is allowed.
pub(super) async fn require_empty_for_first(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    let has_data_stream = streams.iter().any(|stream| {
        stream.head_seq > 0
            && stream.device_id != ctx.birth_device_hex
            && stream.device_id != ctx.snapshot_device_hex
    });
    if has_data_stream {
        return Err(
            "cannot publish an initial snapshot: the relay already holds device streams for this personal database".into(),
        );
    }
    if let Some((_, previous)) = snapshot_head(ctx, client, streams).await? {
        if !previous.through.is_empty() {
            return Err(
                "cannot publish an initial snapshot: the relay already holds a snapshot covering device streams".into(),
            );
        }
    }
    Ok(())
}

/// The head of the snapshot stream on the relay, with the order value and the body of its entry:
/// the newest snapshot. The entry must be the owner's, on this stream, with the hash the relay
/// listed.
pub(super) async fn snapshot_head(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<Option<(StreamHead, SnapshotBody)>, String> {
    let Some(listed) = streams
        .iter()
        .find(|stream| stream.device_id == ctx.snapshot_device_hex && stream.head_seq > 0)
    else {
        return Ok(None);
    };
    let (entries, _) = client
        .read(
            &ctx.database_id,
            &ctx.node_hex,
            Some(&ctx.snapshot_device_hex),
            listed.head_seq,
            None,
            1,
        )
        .await?;
    let entry = entries
        .first()
        .ok_or("the relay lacks the head of the snapshot stream")?;
    let hash = entry_hash(entry).map_err(|error| error.to_string())?;
    let SignedEntry::Snapshot { header, body, .. } = entry else {
        return Err("the head of the snapshot stream is not a snapshot entry".into());
    };
    if header.writer_seq != listed.head_seq || hash.to_hex() != listed.head_hash {
        return Err("the head of the snapshot stream differs from the listing".into());
    }
    verify_entry_signature(entry).map_err(|error| format!("the snapshot entry: {error}"))?;
    if header.node_key != ctx.identity.node_key
        || header.database_id != ctx.database
        || header.device_id != ctx.snapshot_device
    {
        return Err(
            "the head of the snapshot stream is not the owner's entry of this stream".into(),
        );
    }
    Ok(Some((
        StreamHead {
            entry_hash: hash,
            writer_seq: listed.head_seq,
            lamport: header.lamport,
        },
        body.clone(),
    )))
}

/// Ensure `through` covers every stream position of the relay's current snapshot head.
pub(super) async fn require_covers_latest(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    through: &[SnapshotThrough],
) -> Result<Option<StreamHead>, String> {
    let Some((head, previous)) = snapshot_head(ctx, client, streams).await? else {
        return Ok(None);
    };
    for prev in &previous.through {
        let covered = through
            .iter()
            .find(|pos| pos.device_id == prev.device_id)
            .is_some_and(|pos| pos.writer_seq >= prev.writer_seq);
        if !covered {
            return Err(
                "another device published a newer snapshot; catch up before taking a snapshot"
                    .into(),
            );
        }
    }
    Ok(Some(head))
}

/// Sign the `snapshot` entry as the next entry of the snapshot stream.
async fn sign(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    manifest_sha256: &str,
    manifest_bytes: u64,
    through: &[SnapshotThrough],
) -> Result<SignedEntry, String> {
    let head = require_covers_latest(ctx, client, streams, through).await?;
    // The order value of the entry is above every position it covers and above the
    // previous snapshot entry, so a reader sees it as made after them.
    let floor = through
        .iter()
        .map(|position| position.order)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(head.as_ref().map_or(0, |h| h.lamport.saturating_add(1)));
    let author = AuthorCtx {
        database_id: ctx.database,
        node_key: ctx.identity.node_key,
        device_id: ctx.snapshot_device,
        stream_head: head,
        seen_heads: vec![ctx.birth_hash],
        seen_lamports: vec![ctx.birth.position.lamport],
        authored_at_ms: fold_db::clock::unix_millis().max(floor),
    };
    let body = SnapshotBody {
        manifest_sha256: Hex32::from_hex(manifest_sha256).map_err(|_| "manifest digest")?,
        manifest_bytes,
        through: through.to_vec(),
    };
    author_snapshot(&author, &ctx.identity.signing_key, body).map_err(|error| error.to_string())
}

/// Sign the `snapshot` entry for `manifest` `(digest, length)` and append it, marking
/// `snapshot-unverified` before sending so an uncertain failure still triggers re-verification.
pub(super) async fn append_snapshot(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    first: bool,
    manifest: (&str, u64),
    through: &[SnapshotThrough],
) -> Result<SignedEntry, String> {
    let streams = super::list_streams(ctx, client).await?.unwrap_or_default();
    if first {
        require_empty_for_first(ctx, client, &streams).await?;
    }
    let entry = sign(ctx, client, &streams, manifest.0, manifest.1, through).await?;
    super::snapshot_state::mark_unverified(ctx)?;
    let stored = match client
        .append(&ctx.database_id, std::slice::from_ref(&entry))
        .await
    {
        Ok(stored) => stored,
        Err(error) => {
            if super::snapshot_upload::definitive_append_rejection(&error) {
                super::snapshot_state::clear_unverified(ctx);
            }
            return Err(error);
        }
    };
    if stored.0 + stored.1 != 1 {
        return Err("the relay did not take the snapshot entry".into());
    }
    Ok(entry)
}
