//! Self-heal writes of the `mcc:{molecule}` id list, under the recorder gate.
//!
//! The conflict recorder reads, extends and writes the list while it holds the
//! gate of the atom store. A reader that also writes the list must hold the
//! same gate and must write from a fresh read. Otherwise a reader that read
//! the list before a record, and wrote it after, drops the id of that record,
//! and the cache then says "no conflict" for a molecule that has one.

use std::collections::HashSet;

use super::core::DbOperations;

impl DbOperations {
    /// Remove the ids that this reader found resolved or missing. The list is
    /// read again under the gate, so an id that the recorder added after the
    /// reader's first read stays.
    pub(super) async fn prune_conflict_index(&self, index_key: &str, gone: &[String]) {
        let _gate = self.atoms().lock_conflict_record_gate().await;
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
    }
}
