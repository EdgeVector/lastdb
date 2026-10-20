//! Self-heal writes of the `mcc:{molecule}` id list, inside a write scope.
//!
//! The conflict recorder reads, extends and writes the list inside the native
//! write request scope that also writes its tip. A reader that also writes the
//! list does the same, from a fresh read. The scope holds the write gate, so
//! the two cannot interleave. Otherwise a reader that read the list before a
//! record, and wrote it after, would drop the id of that record, and the cache
//! would then say "no conflict" for a molecule that has one.
//!
//! A reader takes the gate only for its write, never for a scan: a scan of
//! many conflict rows is long.

use std::collections::HashSet;

use super::core::DbOperations;

impl DbOperations {
    /// Remove the ids that this reader found resolved or missing. The list is
    /// read again inside the scope, so an id that the recorder added after the
    /// reader's first read stays.
    pub(super) async fn prune_conflict_index(&self, index_key: &str, gone: &[String]) {
        let atoms = self.atoms();
        let held = std::time::Instant::now();
        let _ = atoms
            .in_write_scope(async {
                let Some(mut current) = atoms.raw().get_item::<Vec<String>>(index_key).await?
                else {
                    return Ok(());
                };
                let gone: HashSet<&str> = gone.iter().map(String::as_str).collect();
                let before = current.len();
                current.retain(|id| !gone.contains(id.as_str()));
                if current.len() != before {
                    atoms.raw().put_item(index_key, &current).await?;
                }
                Ok::<(), crate::schema::SchemaError>(())
            })
            .await;
        log_hold("prune", held);
    }

    /// Write the list that a scan found, if no record call wrote a conflict
    /// row since the scan began (`epoch` was read before the scan). A record
    /// call that landed in between would be missing from `ids`, and a list
    /// without its id says "no conflict". In that case the list stays absent,
    /// and the next read scans again.
    pub(super) async fn stamp_conflict_index(&self, index_key: &str, ids: &[String], epoch: u64) {
        let atoms = self.atoms();
        let held = std::time::Instant::now();
        let _ = atoms
            .in_write_scope(async {
                if atoms.conflict_record_epoch() == epoch {
                    atoms.raw().put_item(index_key, &ids).await?;
                }
                Ok::<(), crate::schema::SchemaError>(())
            })
            .await;
        log_hold("stamp", held);
    }
}

fn log_hold(what: &str, held: std::time::Instant) {
    tracing::debug!(
        target: "fold_db::conflict_index",
        what,
        elapsed_us = u64::try_from(held.elapsed().as_micros()).unwrap_or(u64::MAX),
        "reader ran a conflict index write scope"
    );
}
