//! Pull signed entries from the relay and run AcceptEntry.

use mutation_log::{AcceptOutcome, SignedEntry};

use super::history_store::{self, HistoryMap};
use super::materialize;
use super::relay::RelayClient;

pub(super) async fn pull_stream(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
    node_key: &str,
    device_id: &str,
    from_seq: u64,
) -> Result<u64, String> {
    let (entries, _cursor) = client
        .read(database_id, node_key, device_id, from_seq, 90)
        .await?;
    let mut accepted = 0u64;
    for entry in entries {
        match accept_one(histories, home, database_id, entry)? {
            true => accepted += 1,
            false => {}
        }
    }
    Ok(accepted)
}

fn accept_one(
    histories: &HistoryMap,
    home: &std::path::Path,
    database_id: &str,
    entry: SignedEntry,
) -> Result<bool, String> {
    histories.with_mut(database_id, |history| match history.accept_entry(entry) {
        AcceptOutcome::Accept { duplicate, .. } => {
            if !duplicate {
                materialize::apply_history_records(home, database_id, history)?;
            }
            history_store::persist_outbox(home, database_id, history.outbox())?;
            Ok(!duplicate)
        }
        AcceptOutcome::Pending { .. } => Ok(false),
        AcceptOutcome::Reject { reason } => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                ?reason,
                "relay entry rejected by AcceptEntry"
            );
            Ok(false)
        }
    })??
}
