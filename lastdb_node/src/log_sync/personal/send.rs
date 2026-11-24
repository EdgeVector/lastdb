//! Send a signed personal batch to the relay in bounded JSON chunks.

use mutation_log::SignedEntry;

use super::outgoing::Batch;
use super::Ctx;
use crate::log_sync::relay::RelayClient;

/// Relay page cap for one `log/append` call.
const APPEND_MAX_ENTRIES: usize = 90;
/// Maximum serialized JSON body size per `log/append` call, safely below Lambda's 6 MB limit.
const APPEND_MAX_BYTES: usize = 3 * 1024 * 1024;
/// Envelope overhead for `{"database_id":"...","entries":[...]}`.
const APPEND_WRAPPER_BYTES: usize = 128;

/// Send a batch: the birth when the relay lacks it, the files, the entries in chunks, then a
/// second look at the files.
pub(super) async fn send(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    batch: &Batch,
    birth_known: &mut bool,
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
    for chunk in entry_chunks(&batch.entries)? {
        let stored = client.append(&ctx.database_id, chunk).await?;
        expect_all(stored, chunk.len())?;
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

fn entry_chunks(entries: &[SignedEntry]) -> Result<Vec<&[SignedEntry]>, String> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start;
        let mut bytes = APPEND_WRAPPER_BYTES;
        while end < entries.len() && end - start < APPEND_MAX_ENTRIES {
            let encoded = serde_json::to_vec(&entries[end])
                .map_err(|_| "cannot encode signed entry".to_owned())?;
            let next = encoded.len().saturating_add(1);
            if end > start && bytes.saturating_add(next) > APPEND_MAX_BYTES {
                break;
            }
            bytes = bytes.saturating_add(next);
            end += 1;
        }
        chunks.push(&entries[start..end]);
        start = end;
    }
    Ok(chunks)
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
