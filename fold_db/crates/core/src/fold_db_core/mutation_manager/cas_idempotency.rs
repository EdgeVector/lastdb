//! Idempotent-mutation filtering against current state.

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
                } else if mutation.fields_and_values.is_empty() {
                    // A record merges by stamp, so a repeated mutation leaves
                    // its record as it is. A hit without field values (a
                    // delete) is already seen; a hit with field values merges
                    // again and its equal stamp changes nothing.
                    already_seen_ids.push(cached_id);
                    continue;
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
}
