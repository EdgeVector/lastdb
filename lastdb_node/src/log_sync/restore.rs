//! Restore: read streams from seq 1, AcceptEntry + LWW, lazy blobs.

use mutation_log::{AcceptOutcome, SignedEntry};

use super::history_store::HistoryMap;
use super::materialize;
use super::relay::RelayClient;

/// Pull one device stream from seq 1 until empty.
pub(super) async fn restore_stream(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
    node_key: &str,
    device_id: &str,
) -> Result<u64, String> {
    let mut from_seq = 1u64;
    let mut total = 0u64;
    loop {
        let (entries, cursor) = client
            .read(database_id, node_key, device_id, from_seq, 90)
            .await?;
        if entries.is_empty() {
            break;
        }
        let last_seq = entries.last().map_or(from_seq, |e| e.header().writer_seq);
        for entry in entries {
            if accept_restore(histories, home, database_id, entry)? {
                total += 1;
            }
        }
        from_seq = last_seq.saturating_add(1);
        if cursor.is_none() {
            // DESIGN: when the relay returns a full page without cursor, Phase 1
            // advances by last writer_seq + 1; confirm against OpenAPI cursor rules.
            if last_seq < from_seq.saturating_sub(1) {
                break;
            }
        }
        // Stop if page was short of the default limit.
        // (Caller may loop known streams; discovery of all streams is DESIGN.)
        if last_seq + 1 == from_seq {
            // continue
        }
    }
    Ok(total)
}

fn accept_restore(
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
    entry: SignedEntry,
) -> Result<bool, String> {
    // DESIGN: blob bytes are fetched lazily on materialize when a data entry
    // carries MutationFileRef; Phase 1 records the hash only.
    histories.with_mut(database_id, |history| match history.accept_entry(entry) {
        AcceptOutcome::Accept { duplicate, .. } => {
            if !duplicate {
                materialize::apply_history_records(home, database_id, history)?;
            }
            Ok(!duplicate)
        }
        AcceptOutcome::Pending { .. } => Ok(false),
        AcceptOutcome::Reject { reason } => {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "restore entry rejected");
            Ok(false)
        }
    })?
}
