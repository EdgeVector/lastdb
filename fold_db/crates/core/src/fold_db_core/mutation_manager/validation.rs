//! Preconditions, lookup-key normalization, field validation, and schema loading for writes.

use std::collections::{BTreeMap, HashSet};

use lastdb_model::record_value::StoredRecord;

use super::MutationManager;
use crate::schema::types::{cas::CasExpectation, KeyValue, Mutation, MutationType, Schema};
use crate::schema::{SchemaError, SchemaState};

pub(super) fn check_expectation(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    let current =
        record.filter(|record| record.is_live() || mutation.mutation_type == MutationType::Purge);
    let actual = current.map(|record| record.stamp.revision());
    let matches = match &mutation.expected {
        CasExpectation::Absent => !record.is_some_and(StoredRecord::is_live),
        CasExpectation::Revision { revision } => actual.as_ref() == Some(revision),
    };
    if !matches {
        return Err(SchemaError::CasConflict {
            schema: mutation.schema_name.clone(),
            key: mutation.key_value.to_storage_key(),
            expected: match &mutation.expected {
                CasExpectation::Absent => "<absent>".into(),
                CasExpectation::Revision { revision } => revision.clone(),
            },
            actual,
        });
    }
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) && current
        .filter(|record| record.is_live())
        .is_some_and(|record| record.alternates != mutation.alternates)
    {
        let message = "deletion must supply the record's complete current alternate keys";
        if mutation.contribution.is_some() {
            return Err(SchemaError::InvalidField(message.into()));
        }
        return Err(SchemaError::InvalidData(message.into()));
    }
    if mutation.erase_written_before_nanos.is_some_and(|cutoff| {
        record
            .and_then(StoredRecord::written_at)
            .is_some_and(|written| written >= cutoff)
    }) {
        return Err(SchemaError::InvalidData(
            "record is newer than the erasure cutoff".into(),
        ));
    }
    Ok(())
}

/// Replace and delete from a verified contributor may commit only when the live
/// row was stamped with that writer's grant digest. Absence stays a CAS conflict.
pub(super) fn require_contribution_author(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    if !matches!(
        mutation.mutation_type,
        MutationType::Replace | MutationType::Delete
    ) {
        return Ok(());
    }
    let Some(writer) = mutation
        .contribution
        .as_ref()
        .map(|admission| admission.writer.as_str())
    else {
        return Ok(());
    };
    let Some(live) = record.filter(|record| record.is_live()) else {
        return Ok(());
    };
    let author = live
        .meta
        .as_ref()
        .and_then(|meta| meta.metadata.get("share_author"))
        .map(String::as_str);
    if author == Some(writer) {
        return Ok(());
    }
    Err(SchemaError::InvalidField(
        "share contribution can change only a record its writer created".into(),
    ))
}

/// Only a declared field can receive a value.
pub(super) fn validate_field_value(
    schema_name: &str,
    schema: &Schema,
    field_name: &str,
    value: &serde_json::Value,
) -> Result<bool, SchemaError> {
    if !schema.has_field(field_name) {
        return Err(SchemaError::InvalidData(format!(
            "field '{field_name}' is not declared in schema '{schema_name}'"
        )));
    }
    let field_type = schema.get_field_type(field_name);
    if let Err(type_err) = field_type.validate(value) {
        return Err(SchemaError::InvalidData(format!(
            "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
            field_name,
            schema_name,
            type_err,
            field_type,
            serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
        )));
    }
    Ok(true)
}

impl MutationManager {
    pub(super) fn resolve_mutation_key_value(
        &self,
        _schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        let record_keys = schema.require_record_keys()?;
        mutation
            .target()
            .validate(record_keys)
            .map_err(SchemaError::InvalidData)?;
        let profile = record_keys
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let mut key = mutation.key_value.clone();
        crate::schema::types::key_comparison::canonical_key(profile, &mut key)?;
        Ok(key)
    }

    pub(super) fn validate_mutation_batch(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let mut identities = HashSet::new();
        for mutation in mutations {
            if !identities.insert(&mutation.uuid) {
                return Err(SchemaError::InvalidData(format!(
                    "mutation batch repeats identity '{}'",
                    mutation.uuid
                )));
            }
        }
        for field in mutations.iter().flat_map(|m| m.record_fields.keys()) {
            Schema::validate_field_name(field).map_err(SchemaError::InvalidData)?;
        }
        let mut schemas = BTreeMap::new();
        for mutation in mutations {
            let name = mutation.schema_name.as_str();
            let schema = match schemas.entry(name) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let schema = self.schema_manager.require_schema_metadata(name)?;
                    crate::schema::require_logical_layout(&schema.schema_type)?;
                    entry.insert(schema)
                }
            };
            self.resolve_mutation_key_value(name, schema, mutation)?;
        }
        Ok(())
    }

    pub(super) fn reject_blocked_mutation_targets(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let states = self.schema_manager.get_schema_states()?;
        for mutation in mutations {
            if states
                .get(&mutation.schema_name)
                .copied()
                .unwrap_or_default()
                == SchemaState::Blocked
            {
                return Err(SchemaError::Blocked(format!(
                    "Schema '{}' is blocked and cannot be mutated (a drop is running)",
                    mutation.schema_name
                )));
            }
        }
        Ok(())
    }

    /// Load the schema's current field definitions for private mutation preparation.
    pub(super) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
    ) -> Result<Schema, SchemaError> {
        let (mut schema, mut state) = self.schema_manager.get_schema_and_state(schema_name)?;
        if schema.is_none() && state != SchemaState::Blocked {
            schema = self.db_ops.schemas().get_schema(schema_name).await?;
            if schema.is_some() {
                state = self
                    .db_ops
                    .schemas()
                    .get_schema_state(schema_name)
                    .await?
                    .unwrap_or_default();
            }
        }
        if state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
            )));
        }
        let schema = schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        Ok(schema)
    }
}
