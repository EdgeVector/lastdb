//! Pull signed entries from the relay and run AcceptEntry.

use mutation_log::{AcceptOutcome, SignedEntry};

use super::history_store::{self, HistoryMap};
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;

pub(super) async fn pull_stream(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    node_key: &str,
    device_id: &str,
    from_seq: u64,
) -> Result<u64, String> {
    let (entries, _cursor) = client
        .read(db.database_id, node_key, device_id, from_seq, 90)
        .await?;
    let mut accepted = 0u64;
    for entry in entries {
        if accept_one(histories, db, entry)? {
            accepted += 1;
        }
    }
    // Rematerialize whenever birth is present so a prior failed apply (for
    // example a missing schema) can heal on a later turn with accepted=0.
    let history = histories.snapshot(db.database_id)?;
    if history.owner().is_some() {
        materialize::apply_history_records(db.store, &history).await?;
    }
    Ok(accepted)
}

fn accept_one(
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    entry: SignedEntry,
) -> Result<bool, String> {
    histories.with_mut(db.database_id, |history| {
        match history.accept_entry(entry) {
            AcceptOutcome::Accept { duplicate, .. } => {
                history_store::persist_outbox(db.home, db.database_id, history.outbox())?;
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
        }
    })?
}
