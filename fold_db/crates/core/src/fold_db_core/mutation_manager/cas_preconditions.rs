//! CAS and must-exist precondition checks.

use crate::schema::types::Mutation;
use crate::schema::SchemaError;

use super::cas_rules;
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
            cas_rules::check_expectation(mutation, expected, head)?;
        }
        Ok(())
    }

    /// Is this a user `Update` carrying `must_exist: true`?
    ///
    /// The one predicate both the lock set and the precondition gate key on,
    /// so they can never disagree about which mutations are guarded.
    pub(super) fn is_must_exist_update(mutation: &Mutation) -> bool {
        matches!(
            mutation.mutation_type,
            crate::schema::types::operations::MutationType::Update
        ) && mutation.must_exist == Some(true)
    }

    /// Refuse every `must_exist: true` update whose key carries no live value
    /// in this schema yet.
    ///
    /// Must run inside the batch's native write gate, like CAS. Returns the
    /// first missing target — the whole batch is rejected, matching the
    /// all-or-nothing contract `check_cas_preconditions` already applies.
    ///
    /// # Why "any live field", not "the written fields"
    ///
    /// A row is absent iff NO field of the schema resolves at its key. Keying
    /// the check on the fields this mutation writes would refuse the legitimate
    /// heal — writing field `d` onto an existing row that never had `d` — which
    /// is the exact operation the projection rule (a row is returned only when
    /// every projected field has an atom) makes necessary. So the check widens
    /// to the schema's fields rather than narrowing to the mutation's.
    ///
    /// Cost is paid in that order for the same reason it is cheap: the fields
    /// this mutation writes are read FIRST and their molecules are already
    /// restored by the write path, so an existing row normally settles on the
    /// first probe. The full-schema sweep runs only when that probe found
    /// nothing — the refuse-or-heal case.
    pub(super) async fn check_must_exist_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for mutation in mutations {
            if !Self::is_must_exist_update(mutation) {
                continue;
            }
            if self
                .row_has_any_live_field(mutation, storage_prefix)
                .await?
            {
                continue;
            }
            return Err(SchemaError::InvalidData(format!(
                "update target not found: schema '{}', key {} — refusing to silently create a \
                 row from a partial update",
                mutation.schema_name,
                mutation.key_value.to_storage_key(),
            )));
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

        // Probe the written fields first — their molecules are already
        // restored by this write, so a live row usually answers here.
        let written = cas_rules::written_fields(mutation, &schema);
        if !written.is_empty()
            && self
                .read_current_row_fields(mutation, &written, storage_prefix)
                .await?
                .has_live_field()
        {
            return Ok(true);
        }

        // Nothing written resolved. Widen to the rest of the schema before
        // calling the row absent, so a heal write to a field the row never
        // carried is not mistaken for a phantom-row create.
        let rest = cas_rules::remaining_fields(&schema, &written);
        if rest.is_empty() {
            return Ok(false);
        }
        Ok(self
            .read_current_row_fields(mutation, &rest, storage_prefix)
            .await?
            .has_live_field())
    }
}
