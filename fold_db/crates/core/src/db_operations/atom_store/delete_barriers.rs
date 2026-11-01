use std::collections::HashMap;

use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::schema::SchemaError;

use super::AtomStore;

impl AtomStore {
    /// Read exact Delete winners in one batch while the caller holds the tip locks.
    /// A negative result is a read by key; it is not a collection scan.
    pub(crate) async fn winning_delete_barriers(
        &self,
        molecule_keys: &[String],
    ) -> Result<HashMap<String, DeleteBarrier>, SchemaError> {
        if molecule_keys.is_empty() {
            return Ok(HashMap::new());
        }
        let mut keys = molecule_keys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let storage_keys: Vec<String> = keys
            .iter()
            .map(|key| delete_barrier_key(key.as_bytes()))
            .collect();
        let captured = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
            .is_some_and(|runtime| runtime.captured_view_active());
        // Native views read their own captured disk and private staged barriers.
        // The global pending map can contain a different Delete that later
        // fails, so it cannot establish an absence in a captured typed model.
        // Other readers still take pending first across its durable handoff.
        let pending: Vec<Option<DeleteBarrier>> = if captured {
            vec![None; keys.len()]
        } else {
            let guard = self
                .pending_delete_barriers
                .lock()
                .expect("pending_delete_barriers poisoned");
            keys.iter().map(|key| guard.get(key).cloned()).collect()
        };
        let durable: Vec<Option<DeleteBarrier>> = self
            .main_store
            .get_items(&storage_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read durable Delete barriers: {error}"))
            })?;
        let mut winners = HashMap::new();
        for ((key, pending), durable) in keys.iter().zip(pending).zip(durable) {
            if durable
                .as_ref()
                .is_some_and(|barrier| !barrier.matches_key(key.as_bytes()))
            {
                return Err(SchemaError::InvalidData(
                    "durable Delete barrier key identity differs from the molecule key".into(),
                ));
            }
            let winner = match (pending, durable) {
                (Some(pending), Some(durable)) if pending.is_newer_than(&durable) => Some(pending),
                (Some(_), Some(durable)) => Some(durable),
                (Some(pending), None) => Some(pending),
                (None, durable) => durable,
            };
            if let Some(winner) = winner {
                winners.insert(key.clone(), winner);
            }
        }
        Ok(winners)
    }
}
