//! Send a signed personal batch to the relay as one log segment.
//!
//! The entries of a batch go up as one blob, the segment, and one small `log.append` names it
//! (`docs/designs/relay-protocol.md`, "Log segments"). The relay keeps an item for the segment and
//! never the bytes of the entries, so a batch costs the relay a few small writes however large its
//! entries are.

use mutation_log::{entry_hash, SignedEntry};
use serde_json::json;

use super::files::sha256_hex;
use super::outgoing::Batch;
use super::Ctx;
use crate::log_sync::relay::RelayClient;
use crate::profile::private_fs as disk;

/// The file a segment is written to before it goes up. One sender runs for a database at a time.
const SEGMENT_FILE: &str = "segment.out";

/// Send a batch: the birth when the relay lacks it, the files, the entries the relay does not hold
/// yet as one segment, then a second look at the files. `relay_head` is the head of this device's
/// stream on the relay as far as this turn knows it, and moves to the end of the batch.
pub(super) async fn send(
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
        expect_all(stored, 1)?;
        *birth_known = true;
    }
    // A file is on the relay before the entry that names it. The bytes go from the spool.
    for file in &batch.files {
        client
            .blob_put(&ctx.database_id, &file.sha256, &file.path)
            .await?;
    }
    // The relay can hold the first entries of the batch already: a turn that ended between the
    // relay's answer and the confirmation of the spool. A segment continues the head, so it starts
    // after them.
    let (held, unsent): (Vec<&SignedEntry>, Vec<&SignedEntry>) = batch
        .entries
        .iter()
        .partition(|entry| entry.header().writer_seq <= *relay_head);
    // The turn that sent `held` can have ended before it counted them in the usage ledger. This
    // build sends entries only in segments, so they count as segment entries; the ledger skips
    // every entry it counted already.
    if !held.is_empty() {
        client.segment_entries_held(&ctx.database_id, &held)?;
    }
    if !unsent.is_empty() {
        send_segment(ctx, client, &unsent).await?;
    }
    if let Some(last) = batch.entries.last() {
        *relay_head = (*relay_head).max(last.header().writer_seq);
    }
    // The device that names a file asks for it again after it named it. A sweep that ran between
    // the upload of a file and the append of its entry dropped the file, because nothing named it
    // yet. `blob.put` begin answers at once for a file the relay holds, and a file that is gone
    // goes up again. The caller retires the entries from the spool only after this.
    for file in &batch.files {
        client
            .blob_put(&ctx.database_id, &file.sha256, &file.path)
            .await?;
    }
    Ok(())
}

/// Write `entries` as one segment, put it on the relay, and append the item that names it. A
/// sweep can drop the blob between the upload and the append, because nothing names it yet; the
/// relay then answers 404 for `segment.sha256` and the blob goes up once more.
async fn send_segment(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    entries: &[&SignedEntry],
) -> Result<(), String> {
    let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
        return Ok(());
    };
    let mut bytes = Vec::new();
    for entry in entries {
        serde_json::to_writer(&mut bytes, entry)
            .map_err(|_| "cannot encode signed entry".to_owned())?;
        bytes.push(b'\n');
    }
    let sha256 = sha256_hex(&bytes);
    let header = last.header();
    let segment = json!({
        "device_id": header.device_id.to_hex(),
        "first_seq": first.header().writer_seq,
        "last_seq": header.writer_seq,
        "last_hash": entry_hash(last).map_err(|e| e.to_string())?.to_hex(),
        "sha256": sha256,
        "bytes": bytes.len(),
    });
    let path = ctx
        .host
        .home
        .join("log-sync")
        .join(&ctx.database_id)
        .join(SEGMENT_FILE);
    let file = path.clone();
    // A staging copy that `blob_put` reads back in this turn and that is removed after it.
    // The spool holds the entries, so a lost copy is built again by the next turn: nothing
    // waits for the drive.
    tokio::task::spawn_blocking(move || disk::write_private_with(&file, &bytes, disk::Flush::None))
        .await
        .map_err(|_| "the segment writer stopped".to_owned())?
        .map_err(|error| format!("cannot write the log segment: {error}"))?;
    let mut tried = 0;
    let result = loop {
        tried += 1;
        if let Err(error) = client.blob_put(&ctx.database_id, &sha256, &path).await {
            break Err(error);
        }
        match client
            .append_segment(&ctx.database_id, &segment, entries)
            .await
        {
            Ok(stored) => break expect_all(stored, entries.len()),
            Err(error) if tried < 2 && error.contains("HTTP 404") => {}
            Err(error) => break Err(error),
        }
    };
    let _ = disk::remove_private_with(&path, disk::Flush::None);
    result
}

fn expect_all((stored, duplicate): (u64, u64), sent: usize) -> Result<(), String> {
    if stored + duplicate == sent as u64 {
        Ok(())
    } else {
        Err(format!(
            "the relay stored {stored} and knew {duplicate} of {sent} entries"
        ))
    }
}
