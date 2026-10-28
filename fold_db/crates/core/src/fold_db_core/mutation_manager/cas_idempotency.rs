//! Idempotent-mutation filtering against current state.

use crate::db_operations::AtomStore;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;

use super::MutationManager;

/// The accepted retry decisions and the surviving writes with their hashes.
pub(super) struct FilteredMutations {
    pub(super) already_seen_ids: Vec<String>,
    pub(super) mutations: Vec<Mutation>,
    pub(super) hashes: Vec<String>,
}

impl MutationManager {
    /// Filters out already-processed mutations using content-hash idempotency.
    /// Keeps each surviving mutation in the same position as its content hash.
    pub(super) async fn filter_idempotent_mutations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<FilteredMutations, SchemaError> {
        let mut already_seen_ids: Vec<String> = Vec::new();
        let mut new_mutations: Vec<Mutation> = Vec::new();
        let mut new_hashes: Vec<String> = Vec::new();

        let hashes: Vec<_> = mutations.iter().map(Mutation::content_hash).collect();
        let keys: Vec<_> = hashes
            .iter()
            .map(|hash| {
                crate::schema::types::field::build_storage_key(
                    storage_prefix,
                    &format!("idem:{hash}"),
                )
            })
            .collect();
        // The fingerprint includes the exact DB scope. All selected cache
        // buckets are fetched together; a cache failure is a safe miss.
        let cached = self
            .db_ops
            .metadata()
            .get_idempotency_ids(&keys)
            .await
            .unwrap_or_else(|_| vec![None; mutations.len()]);
        for ((mutation, hash), cached_id) in mutations.into_iter().zip(hashes).zip(cached) {
            if let Some(cached_id) = cached_id {
                tracing::debug!(
                    "Idempotency hit for mutation hash {}, returning cached id {}",
                    hash,
                    cached_id
                );
                if Self::has_prior_batch_write_for_same_key(&new_mutations, &mutation) {
                    tracing::debug!(
                        "Idempotency hit for mutation hash {} follows an earlier same-key write in this batch; reapplying mutation {}",
                        hash,
                        mutation.uuid
                    );
                } else if self
                    .idempotency_hit_matches_current_state(&mutation, storage_prefix)
                    .await?
                {
                    already_seen_ids.push(cached_id);
                    continue;
                } else {
                    tracing::debug!(
                        "Idempotency hit for mutation hash {} is stale against current molecule heads; reapplying mutation {}",
                        hash,
                        mutation.uuid
                    );
                }
            }
            new_hashes.push(hash);
            new_mutations.push(mutation);
        }

        Ok(FilteredMutations {
            already_seen_ids,
            mutations: new_mutations,
            hashes: new_hashes,
        })
    }

    fn has_prior_batch_write_for_same_key(prior: &[Mutation], mutation: &Mutation) -> bool {
        prior.iter().any(|candidate| {
            candidate.schema_name == mutation.schema_name
                && candidate.key_value == mutation.key_value
        })
    }

    /// A prior mutation-content hash also requires matching current values.
    /// Atom UUID equality below checks only the source schema and value;
    /// it does not compare the tip's author clock or per-key metadata.
    ///
    /// Without this state check, Create(A) → Delete → Create(A) is misread as
    /// a duplicate of the first Create and the live heads stay dead. The atom
    /// UUIDs are content-addressed, so reapplying the second Create still
    /// reuses the original atom and advances its tip with the new author clock.
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
        let mutation_key_values = vec![key_value.clone()];
        let single_mutation = vec![mutation.clone()];
        let changed_keys =
            Self::changed_keys_by_field(&schema, &single_mutation, &mutation_key_values);
        self.restore_missing_molecules(&mut schema, &changed_keys)
            .await?;

        for (field_name, value) in &mutation.fields_and_values {
            let Some(schema_field) = schema.runtime_fields.get(field_name) else {
                return Ok(false);
            };
            let expected_atom = AtomStore::create_atom(
                &mutation.schema_name,
                value.clone(),
                mutation.source_file_name.clone(),
                mutation.metadata.clone(),
            )?;
            let current_uuid = schema_field.current_atom_uuid(&key_value);
            if current_uuid.as_deref() != Some(expected_atom.uuid()) {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
