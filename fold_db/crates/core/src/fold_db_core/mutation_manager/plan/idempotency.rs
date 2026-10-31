//! Content-hash retry decisions: apply, reapply, or replay a cached id.
//!
//! The caller gathers every cached id with one batch read, then walks the
//! batch in input order. A cache hit that does not follow an earlier
//! same-key write in this batch needs one current-state check before it can
//! replay; that check reads restored molecule heads, so it stays a caller
//! step between [`FilteredMutations::step`] and [`FilteredMutations::replay`].

use crate::atom::Atom;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

/// The idempotency cache key of one mutation content hash in one DB scope.
/// The read before the gate and the write in the schema envelope share it.
pub(in crate::fold_db_core::mutation_manager) fn cache_key(
    storage_prefix: Option<&str>,
    hash: &str,
) -> String {
    build_storage_key(storage_prefix, &format!("idem:{hash}"))
}

/// What the filter does with one mutation after its cache lookup.
pub(in crate::fold_db_core::mutation_manager) enum RetryStep {
    /// No cached id: a new write.
    Apply,
    /// A cached id follows an earlier same-key write in this batch, so the
    /// current state cannot prove the retry. Apply it again.
    ReapplyAfterSameKey(String),
    /// A cached id needs a current-state check before it can replay.
    CheckState(String),
}

/// The accepted retry decisions and the surviving writes with their hashes.
/// Keeps each surviving mutation in the same position as its content hash.
#[derive(Default)]
pub(in crate::fold_db_core::mutation_manager) struct FilteredMutations {
    pub(in crate::fold_db_core::mutation_manager) already_seen_ids: Vec<String>,
    pub(in crate::fold_db_core::mutation_manager) mutations: Vec<Mutation>,
    pub(in crate::fold_db_core::mutation_manager) hashes: Vec<String>,
}

impl FilteredMutations {
    /// Decide the next step for one mutation from its cached id and the
    /// writes this batch already kept.
    pub(in crate::fold_db_core::mutation_manager) fn step(
        &self,
        mutation: &Mutation,
        cached_id: Option<String>,
    ) -> RetryStep {
        let Some(cached_id) = cached_id else {
            return RetryStep::Apply;
        };
        if self.has_prior_write_for_same_key(mutation) {
            RetryStep::ReapplyAfterSameKey(cached_id)
        } else {
            RetryStep::CheckState(cached_id)
        }
    }

    /// Keep a write with its content hash.
    pub(in crate::fold_db_core::mutation_manager) fn apply(
        &mut self,
        mutation: Mutation,
        hash: String,
    ) {
        self.hashes.push(hash);
        self.mutations.push(mutation);
    }

    /// Answer a retry with the id its earlier accepted write received.
    pub(in crate::fold_db_core::mutation_manager) fn replay(&mut self, cached_id: String) {
        self.already_seen_ids.push(cached_id);
    }

    fn has_prior_write_for_same_key(&self, mutation: &Mutation) -> bool {
        self.mutations.iter().any(|candidate| {
            candidate.schema_name == mutation.schema_name
                && candidate.key_value == mutation.key_value
        })
    }
}

/// A prior mutation-content hash also requires matching current values.
/// Atom UUID equality checks only the source schema and value; it does not
/// compare the tip's author clock or per-key metadata.
///
/// Without this state check, Create(A) → Delete → Create(A) is misread as a
/// duplicate of the first Create and the live heads stay dead. The atom UUIDs
/// are content-addressed, so reapplying the second Create still reuses the
/// original atom and advances its tip with the new author clock.
///
/// `schema` carries the restored heads of the written fields. `expected_atom`
/// builds the content-addressed atom of one written value; the caller owns
/// that step because atom creation enforces the content limit. It runs once
/// per field, in field order, and stops at the first mismatch.
pub(in crate::fold_db_core::mutation_manager) fn replay_matches_current<F>(
    mutation: &Mutation,
    schema: &Schema,
    key_value: &KeyValue,
    mut expected_atom: F,
) -> Result<bool, SchemaError>
where
    F: FnMut(&serde_json::Value) -> Result<Atom, SchemaError>,
{
    for (field_name, value) in &mutation.fields_and_values {
        let Some(schema_field) = schema.runtime_fields.get(field_name) else {
            return Ok(false);
        };
        let expected = expected_atom(value)?;
        let current_uuid = schema_field.current_atom_uuid(key_value);
        if current_uuid.as_deref() != Some(expected.uuid()) {
            return Ok(false);
        }
    }
    Ok(true)
}
