//! CAS and must-exist precondition checks.

use crate::schema::types::cas::CasExpectation;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;
use super::MutationManager;

impl MutationManager {
    /// Verify every CAS mutation's precondition against the current persisted
    /// head. Must run while the batch holds the relevant CAS locks (see
    /// [`Self::acquire_cas_locks`]). Returns the first
    /// [`SchemaError::CasConflict`] encountered — the whole batch is rejected,
    /// matching the all-or-nothing contract the write pipeline already applies
    /// to access-control and type-validation failures, so a client never sees a
    /// batch where some CAS writes landed and others were rejected.
    pub(super) async fn check_cas_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for mutation in mutations {
            let Some(expected) = &mutation.expected else {
                continue;
            };
            self.evaluate_cas(mutation, expected, storage_prefix)
                .await?;
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
    /// Must run while the batch holds the relevant locks (see
    /// [`Self::acquire_cas_locks`]), for the same reason CAS does. Returns the
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
    /// `CurrentRowFields::Corrupt` counts as PRESENT: it is returned only after
    /// a tip was seen, so the row exists even though one body did not resolve.
    /// A `must_exist` update must not read an unresolvable atom as an absent
    /// row and refuse a write to a row that is really there.
    async fn row_has_any_live_field(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let Some(schema) = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
        else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                mutation.schema_name
            )));
        };

        // Probe the written fields first — their molecules are already
        // restored by this write, so a live row usually answers here.
        let mut written: Vec<String> = mutation
            .fields_and_values
            .keys()
            .filter(|field| schema.runtime_fields.contains_key(*field))
            .cloned()
            .collect();
        written.sort_unstable();
        if !written.is_empty()
            && self
                .row_present_over_fields(mutation, &written, storage_prefix)
                .await?
        {
            return Ok(true);
        }

        // Nothing written resolved. Widen to the rest of the schema before
        // calling the row absent, so a heal write to a field the row never
        // carried is not mistaken for a phantom-row create.
        let mut rest: Vec<String> = schema
            .runtime_fields
            .keys()
            .filter(|field| !written.contains(field))
            .cloned()
            .collect();
        rest.sort_unstable();
        if rest.is_empty() {
            return Ok(false);
        }
        self.row_present_over_fields(mutation, &rest, storage_prefix)
            .await
    }

    /// One presence probe over an exact field set.
    ///
    /// A field the point read cannot key or resolve is not evidence of
    /// absence, so those errors are swallowed into "this probe found nothing"
    /// rather than failing the write — the caller widens or refuses on the
    /// evidence it actually has.
    async fn row_present_over_fields(
        &self,
        mutation: &Mutation,
        fields: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let row = match self
            .read_current_row_fields(mutation, fields, storage_prefix)
            .await
        {
            Ok(row) => row,
            // Not evidence of a live row, and not a reason to fail the write:
            // let the caller widen the probe, or refuse on the evidence it has.
            Err(SchemaError::InvalidField(_) | SchemaError::InvalidData(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(matches!(
            row,
            CurrentRowFields::Present(_) | CurrentRowFields::Corrupt { .. }
        ))
    }

    /// Compare one CAS mutation's expectation against the live head at its key,
    /// returning `Ok(())` on a match and [`SchemaError::CasConflict`] on a miss.
    pub(super) async fn evaluate_cas(
        &self,
        mutation: &Mutation,
        expected: &CasExpectation,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let field = expected.field();
        let (current_uuid, current_value) = self
            .read_current_head(mutation, field, storage_prefix)
            .await?;

        let conflict = |exp: String, actual: Option<String>| SchemaError::CasConflict {
            schema: mutation.schema_name.clone(),
            field: field.to_string(),
            key: mutation.key_value.to_storage_key(),
            expected: exp,
            actual,
        };

        match expected {
            CasExpectation::Absent { .. } => {
                // A live value present where the caller expected none.
                if let Some(value) = current_value {
                    return Err(conflict("<absent>".to_string(), Some(value.to_string())));
                }
                Ok(())
            }
            CasExpectation::Value { value, .. } => match current_value {
                Some(current) if &current == value => Ok(()),
                Some(current) => Err(conflict(value.to_string(), Some(current.to_string()))),
                None => Err(conflict(value.to_string(), None)),
            },
            CasExpectation::ContentHash { hash, .. } => match current_uuid {
                Some(current) if &current == hash => Ok(()),
                Some(current) => Err(conflict(hash.clone(), Some(current))),
                None => Err(conflict(hash.clone(), None)),
            },
        }
    }
}
