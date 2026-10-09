//! Idempotent-mutation filtering against current state.

use crate::db_operations::AtomStore;
use crate::schema::types::{KeyValue, Mutation};
use crate::schema::SchemaError;

use super::helpers::current_atom_uuid;
use super::MutationManager;

impl MutationManager {
    /// Filters out already-processed mutations using content-hash idempotency.
    /// Returns (already_seen_ids, new_mutations, new_hashes).
    pub(super) async fn filter_idempotent_mutations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<(Vec<String>, Vec<Mutation>, Vec<String>), SchemaError> {
        let mut already_seen_ids: Vec<String> = Vec::new();
        let mut new_mutations: Vec<Mutation> = Vec::new();
        let mut new_hashes: Vec<String> = Vec::new();

        for mutation in mutations {
            let hash = mutation.content_hash();
            // Scope idempotency by DB handle so the same content in personal vs
            // org DB (or two org DBs) is not treated as a cross-DB duplicate.
            let key = crate::schema::types::field::build_storage_key(
                storage_prefix,
                &format!("idem:{hash}"),
            );
            if let Ok(Some(cached_id)) = self
                .db_ops
                .metadata()
                .get_idempotency_item::<String>(&key)
                .await
            {
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
                    new_hashes.push(hash);
                    new_mutations.push(mutation);
                } else if self
                    .idempotency_hit_matches_current_state(&mutation, storage_prefix)
                    .await?
                {
                    already_seen_ids.push(cached_id);
                } else {
                    tracing::debug!(
                        "Idempotency hit for mutation hash {} is stale against current molecule heads; reapplying mutation {}",
                        hash,
                        mutation.uuid
                    );
                    new_hashes.push(hash);
                    new_mutations.push(mutation);
                }
            } else {
                new_hashes.push(hash);
                new_mutations.push(mutation);
            }
        }

        Ok((already_seen_ids, new_mutations, new_hashes))
    }

    pub(super) fn has_prior_batch_write_for_same_key(
        prior: &[Mutation],
        mutation: &Mutation,
    ) -> bool {
        prior.iter().any(|candidate| {
            candidate.schema_name == mutation.schema_name
                && candidate.key_value == mutation.key_value
        })
    }

    /// A content-hash idempotency hit is only safe to skip when the
    /// targeted molecule heads already match the mutation's expected atoms.
    ///
    /// Without this state check, Create(A) → Delete → Create(A) is misread as
    /// a duplicate of the first Create and the live heads stay dead. The atom
    /// UUIDs are content-addressed, so reapplying the second Create still
    /// reuses the original atom; it just advances the molecule heads back to
    /// it. After Delete the overlay is serving truth while the catalog still
    /// names atom A, so the helper must consult that overlay the same way
    /// [`Self::read_current_head`] does — otherwise a catalog-only comparison
    /// skips the retry.
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

        let key_value = Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
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
            let current_uuid =
                self.resident_preferred_atom_uuid(schema_field, &key_value, storage_prefix);
            if current_uuid.as_deref() != Some(expected_atom.uuid()) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// The current atom uuid at `key_value` for `field`, preferring the
    /// acknowledged resident tip over the schema catalog head.
    ///
    /// Mirrors the same preference [`Self::read_current_head`] applies: a
    /// same-node write acks and advances the resident tip before its
    /// deferred durable persist reloads the schema catalog, so a lookup that
    /// only ever consults the catalog can observe pre-write state for a
    /// window after the write that produced it already returned. Without
    /// this, a byte-identical retry of that write reads its own prior
    /// attempt as absent and gets reapplied instead of recognized as an
    /// idempotent no-op.
    ///
    /// A hard-erase stamps the resident tombstone overlay and drops the
    /// resident tip before ack, while the catalog head still names the
    /// pre-delete atom until the deferred converge lane runs. Without an
    /// overlay check here, a byte-identical Create after Delete falls
    /// through to that stale catalog uuid and skips as a duplicate.
    pub(super) fn resident_preferred_atom_uuid(
        &self,
        field: &crate::schema::types::field::FieldVariant,
        key_value: &KeyValue,
        storage_prefix: Option<&str>,
    ) -> Option<String> {
        // Overlay and resident tip are not prefix-gated: org Delete stamps
        // the same molecule-slot overlay as personal. The prefix still
        // scopes the catalog fallback via `current_atom_uuid` (schema clone).
        let _ = storage_prefix;
        if let (Some(mol_uuid), Some(changed)) = (
            field.common().molecule_uuid(),
            field.changed_key_for(key_value),
        ) {
            if self.db_ops.resident().is_key_tombstoned(
                mol_uuid,
                changed.disk_hash(),
                changed.disk_range(),
            ) {
                return None;
            }
            if let Some(tip) = self.db_ops.resident().resolve_tip(
                mol_uuid,
                changed.disk_hash(),
                changed.disk_range(),
            ) {
                return Some(tip.value.atom_uuid);
            }
        }
        current_atom_uuid(field, key_value)
    }
}
