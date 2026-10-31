//! Normalize omitted declared key fields in the same private field batch.

use super::super::super::MutationManager;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use std::collections::HashMap;

use super::super::super::plan::key_fields::{key_field_pairs, validated_materialized_key_field};

impl MutationManager {
    /// Check declared key fields before any source batch can prepare or publish.
    /// Preparation repeats the same check while
    /// it adds omitted fields to its private mutation copy.
    pub(in crate::fold_db_core::mutation_manager) fn validate_key_field_payloads(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let mut schemas = HashMap::new();
        for mutation in mutations {
            if !schemas.contains_key(&mutation.schema_name) {
                let mut schema = self
                    .schema_manager
                    .require_schema_metadata(&mutation.schema_name)?;
                if schema.runtime_fields.is_empty() {
                    schema.populate_runtime_fields()?;
                }
                schemas.insert(mutation.schema_name.clone(), schema);
            }
            let schema = &schemas[&mutation.schema_name];
            let key_value =
                self.resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            self.validate_key_fields(&mutation.schema_name, schema, mutation, &key_value)?;
        }
        Ok(())
    }

    /// Make omitted declared key fields part of the same field batch as
    /// the caller's payload.
    ///
    /// A mutation can carry its coordinates only in `key_value`. Before this
    /// normalization, that shape advanced payload molecules but left the
    /// declared hash/range-field molecules sparse. Mutating the prepared copy
    /// here keeps the request and mutation-intent wire shapes unchanged while
    /// making atom preparation, changed-key restore, resident apply, Search,
    /// and durable persistence use one complete atomic field set.
    pub(super) fn materialize_key_fields(
        &self,
        schema_name: &str,
        schema: &Schema,
        mutation: &mut Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        for (field_name, coordinate) in key_field_pairs(schema_name, schema, mutation, key_value)?
            .into_iter()
            .flatten()
        {
            if let Some(value) = validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                self.db_ops.molecule_keys().schema_profile(schema)?,
            )? {
                mutation
                    .fields_and_values
                    .insert(field_name.to_string(), value);
            }
        }
        Ok(())
    }

    fn validate_key_fields(
        &self,
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        for (field_name, coordinate) in key_field_pairs(schema_name, schema, mutation, key_value)?
            .into_iter()
            .flatten()
        {
            validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                self.db_ops.molecule_keys().schema_profile(schema)?,
            )?;
        }
        Ok(())
    }
}
