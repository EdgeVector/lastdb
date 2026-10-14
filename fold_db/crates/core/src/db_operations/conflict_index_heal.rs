//! Self-heal writes of the `mcc:{molecule}` id list, under the recorder gate.
//!
//! The conflict recorder reads, extends and writes the list while it holds the
//! gate of the atom store. A reader that also writes the list must hold the
//! same gate and must write from a fresh read. Otherwise a reader that read
//! the list before a record, and wrote it after, drops the id of that record,
//! and the cache then says "no conflict" for a molecule that has one.
//!
//! A reader holds the gate only for its write, never for a scan: the gate is
//! in the path of a replay write, and a scan of many conflict rows is long.

use std::collections::HashSet;

use super::core::DbOperations;

impl DbOperations {
    /// Remove the ids that this reader found resolved or missing. The list is
    /// read again under the gate, so an id that the recorder added after the
    /// reader's first read stays.
    pub(super) async fn prune_conflict_index(&self, index_key: &str, gone: &[String]) {
        let _gate = self.atoms().lock_conflict_record_gate().await;
        let held = std::time::Instant::now();
        let Ok(Some(mut current)) = self.atoms().raw().get_item::<Vec<String>>(index_key).await
        else {
            return;
        };
        let gone: HashSet<&str> = gone.iter().map(String::as_str).collect();
        let before = current.len();
        current.retain(|id| !gone.contains(id.as_str()));
        if current.len() != before {
            let _ = self.atoms().raw().put_item(index_key, &current).await;
        }
        log_hold("prune", held);
    }

    /// Write the list that a scan found, if no record call wrote a conflict
    /// row since the scan began (`epoch` was read before the scan). A record
    /// call that landed in between would be missing from `ids`, and a list
    /// without its id says "no conflict". In that case the list stays absent,
    /// and the next read scans again.
    pub(super) async fn stamp_conflict_index(&self, index_key: &str, ids: &[String], epoch: u64) {
        let _gate = self.atoms().lock_conflict_record_gate().await;
        let held = std::time::Instant::now();
        if self.atoms().conflict_record_epoch() == epoch {
            let _ = self.atoms().raw().put_item(index_key, &ids).await;
        }
        log_hold("stamp", held);
    }
}

fn log_hold(what: &str, held: std::time::Instant) {
    tracing::debug!(
        target: "fold_db::conflict_index",
        what,
        gate_held_us = u64::try_from(held.elapsed().as_micros()).unwrap_or(u64::MAX),
        "reader held the conflict record gate"
    );
}
