use std::collections::HashMap;

use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::schema::SchemaError;

use super::{AtomStore, PerKeyRecord};

impl AtomStore {
    /// Read exact Delete target tips in one batch.
    pub(crate) async fn delete_target_tips(
        &self,
        molecule_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        self.main_store
            .get_items(molecule_keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read Delete target tips: {error}")))
    }

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
        // Read pending first. Converge may flush the durable barrier and then
        // clear pending while the disk read runs. The snapshot spans that handoff.
        let pending: Vec<Option<DeleteBarrier>> = {
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

    /// Read the barrier winners and eligible target tips for Delete convergence.
    /// The caller holds the tip locks across both batch reads and the barrier write.
    pub(crate) async fn delete_converge_targets(
        &self,
        planned: &[DeleteBarrier],
    ) -> Result<Vec<(DeleteBarrier, Option<PerKeyRecord>)>, SchemaError> {
        let keys: Vec<String> = planned
            .iter()
            .map(|barrier| barrier.mk_key.clone())
            .collect();
        let winners = self.winning_delete_barriers(&keys).await?;
        let eligible: Vec<DeleteBarrier> = planned
            .iter()
            .filter(|barrier| {
                !winners
                    .get(&barrier.mk_key)
                    .is_some_and(|winner| winner.is_newer_than(barrier))
            })
            .cloned()
            .collect();
        let eligible_keys: Vec<String> = eligible
            .iter()
            .map(|barrier| barrier.mk_key.clone())
            .collect();
        let tips = self.delete_target_tips(&eligible_keys).await?;
        Ok(eligible.into_iter().zip(tips).collect())
    }
}
