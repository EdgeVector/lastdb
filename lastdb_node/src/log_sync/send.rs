//! Send a batch of the database's signed entries and files to the relay.

use super::record_files::sha256_hex;
use super::relay::RelayClient;
use mutation_log::{entry_hash, SignedEntry};
use relay_protocol::SegmentAppend;
use std::path::Path;

/// Entries that need the relay's individual admission checks travel inline. A publisher
/// sending a contiguous run of sealed entries can put it in one segment after the known head.
pub(super) enum Transport {
    Inline,
    SegmentAfter(u64),
}

pub(super) async fn send_batch(
    client: &RelayClient,
    database_id: &str,
    entries: &[SignedEntry],
    files: &[(&str, &Path)],
    transport: Transport,
) -> Result<(), String> {
    put_files(client, database_id, files).await?;
    match transport {
        Transport::Inline => {
            expect_all(client.append(database_id, entries).await?, entries.len())?;
        }
        Transport::SegmentAfter(head) => {
            let (held, unsent): (Vec<_>, Vec<_>) = entries
                .iter()
                .partition(|entry| entry.header().writer_seq <= head);
            if !held.is_empty() {
                client.segment_entries_held(database_id, &held)?;
            }
            if !unsent.is_empty() {
                send_segment(database_id, client, &unsent).await?;
            }
        }
    }
    // Reclaim may have run between the first upload and the append that names the blob.
    // Retire the local spool only after every named file is present after acknowledgment.
    put_files(client, database_id, files).await
}

async fn put_files(
    client: &RelayClient,
    database_id: &str,
    files: &[(&str, &Path)],
) -> Result<(), String> {
    for (sha256, path) in files {
        client.blob_put(database_id, sha256, path).await?;
    }
    Ok(())
}

/// Encode `entries` as one segment in memory, put it on the relay, and append the item that names
/// it. A sweep can drop the blob between the upload and the append, because nothing names it yet;
/// the relay then answers 404 for `segment.sha256` and the blob goes up once more.
async fn send_segment(
    database_id: &str,
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
    let header = last.header();
    let segment = SegmentAppend {
        device_id: header.device_id.to_hex(),
        first_seq: first.header().writer_seq,
        last_seq: header.writer_seq,
        last_hash: entry_hash(last).map_err(|e| e.to_string())?.to_hex(),
        sha256: sha256_hex(&bytes),
        bytes: bytes.len() as u64,
    };
    let mut tried = 0;
    loop {
        tried += 1;
        client
            .blob_put_bytes(database_id, &segment.sha256, &bytes)
            .await?;
        match client.append_segment(database_id, &segment, entries).await {
            Ok(stored) => return expect_all(stored, entries.len()),
            Err(error) if tried < 2 && error.contains("HTTP 404") => {}
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn expect_all((stored, duplicate): (u64, u64), sent: usize) -> Result<(), String> {
    if stored + duplicate == sent as u64 {
        Ok(())
    } else {
        Err(format!(
            "the relay stored {stored} and knew {duplicate} of {sent} entries"
        ))
    }
}
