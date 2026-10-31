//! Idempotent-mutation filtering against current state.
//!
//! Gather: one batch read of the cached ids, then one current-state check
//! per cache hit that needs it. Decide: [`super::plan::idempotency`].

use crate::db_operations::AtomStore;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;

use super::plan::idempotency::{self, FilteredMutations, RetryStep};
use super::MutationManager;

impl MutationManager {
    /// Filters out already-processed mutations using content-hash idempotency.
    /// Keeps each surviving mutation in the same position as its content hash.
    pub(super) async fn filter_idempotent_mutations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<FilteredMutations, SchemaError> {
        let hashes: Vec<_> = mutations.iter().map(Mutation::content_hash).collect();
        let keys: Vec<_> = hashes
            .iter()
            .map(|hash| idempotency::cache_key(storage_prefix, hash))
            .collect();
        // The fingerprint includes the exact DB scope. All selected cache
        // buckets are fetched together; a cache failure is a safe miss.
        let cached = self
            .db_ops
            .metadata()
            .get_idempotency_ids(&keys)
            .await
            .unwrap_or_else(|_| vec![None; mutations.len()]);
        let mut plan = FilteredMutations::default();
        for ((mutation, hash), cached_id) in mutations.into_iter().zip(hashes).zip(cached) {
            match plan.step(&mutation, cached_id) {
                RetryStep::Apply => {}
                RetryStep::ReapplyAfterSameKey(cached_id) => {
                    tracing::debug!(
                        "Idempotency hit for mutation hash {}, returning cached id {}",
                        hash,
                        cached_id
                    );
                    tracing::debug!(
                        "Idempotency hit for mutation hash {} follows an earlier same-key write in this batch; reapplying mutation {}",
                        hash,
                        mutation.uuid
                    );
                }
                RetryStep::CheckState(cached_id) => {
                    tracing::debug!(
                        "Idempotency hit for mutation hash {}, returning cached id {}",
                        hash,
                        cached_id
                    );
                    if self
                        .idempotency_hit_matches_current_state(&mutation, storage_prefix)
                        .await?
                    {
                        plan.replay(cached_id);
                        continue;
                    }
                    tracing::debug!(
                        "Idempotency hit for mutation hash {} is stale against current molecule heads; reapplying mutation {}",
                        hash,
                        mutation.uuid
                    );
                }
            }
            plan.apply(mutation, hash);
        }
        Ok(plan)
    }

    /// Gather the restored heads of the written fields for one cache hit,
    /// then decide with [`idempotency::replay_matches_current`].
    ///
    /// The helper reads current logical tips, including accepted changes that
    /// still await flush. A disk-only comparison could skip the retry.
    ///
    /// Only Create/Update reach here: `write_mutations_batch_async` peels both
    /// hard-erasure verbs off before the idempotency filter runs.
    pub(super) async fn idempotency_hit_matches_current_state(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let Some(mut schema) = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
        else {
            return Ok(false);
        };
        super::helpers::apply_storage_prefix_to_schema(&mut schema, storage_prefix);

        let key_value =
            self.resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        let changed_keys = super::plan::molecules::changed_keys_by_field(
            &schema,
            std::slice::from_ref(mutation),
            std::slice::from_ref(&key_value),
        );
        self.restore_missing_molecules(&mut schema, &changed_keys)
            .await?;

        idempotency::replay_matches_current(mutation, &schema, &key_value, |value| {
            AtomStore::create_atom(
                &mutation.schema_name,
                value.clone(),
                mutation.source_file_name.clone(),
                mutation.metadata.clone(),
            )
        })
    }
}
