//! CAS and must-exist precondition gates.
//!
//! Gather: one current-head or row read per guarded mutation, in input
//! order. Decide: [`super::plan::cas`]. The first refusal stops the gate
//! before any later read.

use crate::schema::types::Mutation;
use crate::schema::SchemaError;

use super::plan::cas;
use super::MutationManager;

impl MutationManager {
    /// Verify each supplied CAS mutation against the current
    /// head. The caller holds the native write gate through comparison and
    /// publication for request writes and both erasure verbs. Return the first
    /// [`SchemaError::CasConflict`] before this
    /// phase publishes. A mixed request has separate update and delete phases;
    /// this helper does not make the whole mixed request atomic.
    pub(super) async fn check_cas_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for mutation in mutations {
            let Some(expected) = &mutation.expected else {
                continue;
            };
            let head = self
                .read_current_head(mutation, expected.field(), storage_prefix)
                .await?;
            cas::check_expectation(mutation, expected, head)?;
        }
        Ok(())
    }

    /// Refuse every `must_exist: true` update whose key carries no live value
    /// in this schema yet.
    ///
    /// Must run inside the batch's native write gate, like CAS. Returns the
    /// first missing target — the whole batch is rejected, matching the
    /// all-or-nothing contract `check_cas_preconditions` already applies.
    /// [`cas::must_exist_probes`] names the probe order and why it widens to
    /// every schema field.
    pub(super) async fn check_must_exist_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for mutation in mutations {
            if !cas::is_must_exist_update(mutation) {
                continue;
            }
            let row_exists = self
                .row_has_any_live_field(mutation, storage_prefix)
                .await?;
            cas::must_exist_verdict(mutation, row_exists)?;
        }
        Ok(())
    }

    /// Does the mutation's key carry at least one live field value?
    ///
    /// A missing selected body is an error. The row reader reports Absent when
    /// every selected body is a tombstone. Corrupt means a live field and a
    /// tombstone field coexist, so this presence-only check still sees a row.
    async fn row_has_any_live_field(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let schema = self
            .schema_manager
            .require_schema_metadata(&mutation.schema_name)?;
        for fields in cas::must_exist_probes(mutation, &schema) {
            if fields.is_empty() {
                continue;
            }
            if self
                .read_current_row_fields(mutation, &fields, storage_prefix)
                .await?
                .has_live_field()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
