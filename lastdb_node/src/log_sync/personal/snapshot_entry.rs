//! The relay side of a snapshot: the positions of a cut, the signed `snapshot` entry, and the trim.

use mutation_log::{
    author_snapshot, entry_hash, AuthorCtx, DeviceId, EntryHash, Hex32, SignedEntry, SnapshotBody,
    SnapshotThrough, StreamHead,
};

use super::Ctx;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};

/// Calls of one trim. The relay does a bounded amount of work in a call and says whether
/// it is done, so a large backlog takes several calls. What is left goes on at the next snapshot.
const TRIM_CALLS: usize = 64;

/// Keep the two newest snapshots on the relay and drop what the older of them covers: the
/// entries up to its positions and the snapshot entries before it. It never drops a file or a
/// manifest. The relay reads that snapshot entry itself and checks it. A failure is
/// logged and not an error of the snapshot: the snapshot is already named, nothing was lost,
/// and the next snapshot asks again. A relay without the route answers 404, which is the same.
pub(super) async fn trim_covered(ctx: &Ctx<'_>, client: &RelayClient, newest: u64) {
    let Some(basis) = newest.checked_sub(1).filter(|basis| *basis >= 1) else {
        return;
    };
    for _ in 0..TRIM_CALLS {
        match client
            .trim(&ctx.database_id, &ctx.snapshot_device_hex, basis)
            .await
        {
            Ok(true) => {
                tracing::info!(target: "lastdbd::log_sync", basis, "the relay dropped what the snapshot covers");
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

/// The head of the snapshot stream on the relay, with the order value of its entry.
async fn snapshot_head(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<Option<StreamHead>, String> {
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
    if entry.header().writer_seq != listed.head_seq || hash.to_hex() != listed.head_hash {
        return Err("the head of the snapshot stream differs from the listing".into());
    }
    Ok(Some(StreamHead {
        entry_hash: hash,
        writer_seq: listed.head_seq,
        lamport: entry.header().lamport,
    }))
}

/// Sign the `snapshot` entry as the next entry of the snapshot stream.
pub(super) async fn sign(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    manifest_sha256: &str,
    manifest_bytes: u64,
    through: &[SnapshotThrough],
) -> Result<SignedEntry, String> {
    let head = snapshot_head(ctx, client, streams).await?;
    // The order value of the entry is above every position it covers, so it
    // follows the normal rule and a reader sees it as made after them.
    let floor = through
        .iter()
        .map(|position| position.order)
        .max()
        .unwrap_or(0)
        + 1;
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
