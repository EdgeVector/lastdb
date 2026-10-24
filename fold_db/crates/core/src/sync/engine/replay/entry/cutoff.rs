//! A home-wide owner cutoff changes through offline maintenance, never replay.

use super::*;
use crate::db_operations::atom_store::{AtomStore, DELETE_REPLAY_CUTOFF_KEY};

impl SyncEngine {
    pub(super) async fn require_physical_replay_cutoff(&self, entry: &LogEntry) -> SyncResult<()> {
        if matches!(
            &entry.op,
            LogOp::MutationIntent { .. } | LogOp::PhysicalDigest { .. }
        ) {
            return Ok(());
        }
        // Check the complete entry before any effect. In particular an
        // ordinary home cannot import a floor behind its startup-time cache.
        if changes_cutoff_metadata(&entry.op) {
            return Err(refused(entry, ReplayCause::DeleteReplayCutoff));
        }
        let atoms = self.atom_store.lock().await.clone();
        let Some(atoms) = atoms else {
            return Err(refused(entry, ReplayCause::MissingMutationApplier));
        };
        if atoms.minimum_replay_time().await? > 0 {
            // Physical rows have no complete original author clock.
            return Err(refused(entry, ReplayCause::DeleteReplayCutoff));
        }
        Ok(())
    }
}

fn refused(entry: &LogEntry, cause: ReplayCause) -> SyncError {
    SyncError::replay_refused(
        "delete_replay_cutoff",
        entry.seq,
        ReplayOperation::from(&entry.op),
        cause,
    )
}

fn cutoff_key(namespace: &str, encoded: &str) -> bool {
    if namespace != "main" {
        return false;
    }
    // Invalid framing retains the existing per-operation poison handling.
    LogOp::decode_bytes(encoded).is_ok_and(|key| {
        key == AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes()
            || key == DELETE_REPLAY_CUTOFF_KEY.as_bytes()
    })
}

fn changes_cutoff_metadata(operation: &LogOp) -> bool {
    match operation {
        LogOp::Put { namespace, key, .. } | LogOp::Delete { namespace, key } => {
            cutoff_key(namespace, key)
        }
        LogOp::BatchPut { namespace, items } => {
            items.iter().any(|(key, _)| cutoff_key(namespace, key))
        }
        LogOp::BatchDelete { namespace, keys } => keys.iter().any(|key| cutoff_key(namespace, key)),
        LogOp::LogicalCommit { changes } => changes
            .iter()
            .any(|change| cutoff_key(&change.namespace, &change.key)),
        _ => false,
    }
}
