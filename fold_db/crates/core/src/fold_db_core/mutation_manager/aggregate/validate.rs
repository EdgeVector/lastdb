//! Aggregate contract and enrollment validation.
// lint:file-size-ok moved verbatim from aggregate.rs; one method family per file

use super::*;

impl MutationManager {
    // lint:fn-size-ok verbatim move from aggregate.rs; splitting this function is separate work
    pub(super) fn validate_aggregate_contract(
        &self,
        source: &Mutation,
        aggregate: &AggregateSet,
    ) -> Result<(KeyValue, AggregateSet), SchemaError> {
        aggregate.validate().map_err(SchemaError::InvalidData)?;
        if !matches!(
            source.mutation_type,
            MutationType::Create | MutationType::Update
        ) {
            return Err(SchemaError::InvalidData(
                "aggregate_set supports Create or Update source mutations only; use an explicit zero Update as the logical tombstone"
                    .into(),
                ));
        }
        let source_schema = self
            .schema_manager
            .get_schema_metadata(&source.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate source schema '{}' not found",
                    source.schema_name
                ))
            })?;
        if source_schema.schema_type != DeclarativeSchemaType::HashRange {
            return Err(SchemaError::InvalidData(
                "aggregate source schema must be HashRange".into(),
            ));
        }
        if source.schema_name == aggregate.member_schema_name {
            return Err(SchemaError::InvalidData(
                "aggregate source and member schemas must be distinct".into(),
            ));
        }
        if source_schema.runtime_fields.keys().any(|field| {
            field == AGGREGATE_VALID_FIELD
                || field == AGGREGATE_GUARD_TOKEN_FIELD
                || AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field.as_str())
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate source schema '{}' declares a reserved aggregate field",
                source.schema_name
            )));
        }
        for (field, value) in &source.fields_and_values {
            if !source_schema.runtime_fields.contains_key(field) {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate source schema '{}' does not declare payload field '{field}'",
                    source.schema_name
                )));
            }
            if crate::atom::is_tombstone_value(value) {
                return Err(SchemaError::InvalidData(
                    "aggregate source mutation rejects tombstone-valued fields; write a live logical removal value with an explicit zero contribution"
                        .into(),
                ));
            }
            let field_type = source_schema.get_field_type(field);
            if let Err(type_err) = field_type.validate(value) {
                return Err(SchemaError::InvalidData(format!(
                    "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
                    field,
                    source.schema_name,
                    type_err,
                    field_type,
                    serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
                )));
            }
        }
        let source_key_fields: std::collections::HashSet<&str> = source_schema
            .key
            .as_ref()
            .into_iter()
            .flat_map(|key| {
                key.hash_field
                    .as_deref()
                    .into_iter()
                    .chain(key.range_field.as_deref())
            })
            .collect();
        if !source.fields_and_values.keys().any(|field| {
            !source_key_fields.contains(field.as_str())
                && source.fields_and_values.contains_key(field)
        }) {
            return Err(SchemaError::InvalidData(
                "aggregate source mutation must persist at least one live declared non-key field"
                    .into(),
            ));
        }
        let source_key =
            Self::resolve_mutation_key_value(&source.schema_name, &source_schema, source)?;
        let source_hash = source_key.hash.clone().ok_or_else(|| {
            SchemaError::InvalidData("aggregate source key has no hash partition".into())
        })?;

        let target_schema = self
            .schema_manager
            .get_schema_metadata(&aggregate.target_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate target schema '{}' not found",
                    aggregate.target_schema_name
                ))
            })?;
        if target_schema.schema_type != DeclarativeSchemaType::Hash {
            return Err(SchemaError::InvalidData(
                "aggregate target schema must be Hash".into(),
            ));
        }
        if target_schema.key.as_ref().is_some_and(|key| {
            key.hash_field
                .as_deref()
                .into_iter()
                .chain(key.range_field.as_deref())
                .any(|field| {
                    field == AGGREGATE_VALID_FIELD
                        || field == AGGREGATE_GUARD_TOKEN_FIELD
                        || aggregate.contribution.contains_key(field)
                })
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate target schema '{}' uses an aggregate-owned field as a key",
                aggregate.target_schema_name
            )));
        }
        let target_probe = Self::aggregate_probe(
            aggregate.target_schema_name.clone(),
            aggregate.target_key_value.clone(),
        );
        let target_key = Self::resolve_mutation_key_value(
            &aggregate.target_schema_name,
            &target_schema,
            &target_probe,
        )?;
        for field in aggregate.contribution.keys().map(String::as_str) {
            if !target_schema.runtime_fields.contains_key(field) {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate target schema '{}' must declare field '{field}'",
                    aggregate.target_schema_name
                )));
            }
            if target_schema.get_field_type(field) != &FieldValueType::Integer {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate metric field '{}.{field}' must have type Integer",
                    aggregate.target_schema_name
                )));
            }
        }
        for (field, expected) in [
            (AGGREGATE_VALID_FIELD, FieldValueType::Integer),
            (AGGREGATE_GUARD_TOKEN_FIELD, FieldValueType::String),
        ] {
            if !target_schema.runtime_fields.contains_key(field)
                || target_schema.get_field_type(field) != &expected
            {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate target field '{}.{field}' must have type {expected:?}",
                    aggregate.target_schema_name
                )));
            }
        }

        let member_schema = self
            .schema_manager
            .get_schema_metadata(&aggregate.member_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate member schema '{}' not found",
                    aggregate.member_schema_name
                ))
            })?;
        if member_schema.schema_type != DeclarativeSchemaType::HashRange {
            return Err(SchemaError::InvalidData(format!(
                "aggregate member schema '{}' must be HashRange",
                aggregate.member_schema_name
            )));
        }
        if member_schema.key.as_ref().is_some_and(|key| {
            key.hash_field
                .as_deref()
                .into_iter()
                .chain(key.range_field.as_deref())
                .any(|field| AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field))
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate member schema '{}' uses a reserved aggregate field as a key",
                aggregate.member_schema_name
            )));
        }
        let member_probe = Self::aggregate_probe(
            aggregate.member_schema_name.clone(),
            aggregate.member_key_value.clone(),
        );
        let member_key = Self::resolve_mutation_key_value(
            &aggregate.member_schema_name,
            &member_schema,
            &member_probe,
        )?;
        let canonical_member_key = source_key.clone();
        if member_key != canonical_member_key {
            return Err(SchemaError::InvalidData(format!(
                "aggregate member_key_value must use the canonical source identity {}",
                canonical_member_key.to_storage_key()
            )));
        }
        for field in AGGREGATE_MEMBER_RESERVED_FIELDS {
            if !member_schema.runtime_fields.contains_key(*field) {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member schema '{}' must declare reserved field '{field}'",
                    aggregate.member_schema_name
                )));
            }
        }
        for field in [
            crate::schema::types::aggregate::AGGREGATE_WINNER_COUNTER_FIELD,
            crate::schema::types::aggregate::AGGREGATE_WINNER_WRITTEN_AT_FIELD,
        ] {
            if member_schema.get_field_type(field) != &FieldValueType::String {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member field '{}.{field}' must have type String",
                    aggregate.member_schema_name
                )));
            }
        }
        if member_schema.get_field_type(crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD)
            != &FieldValueType::Any
        {
            return Err(SchemaError::InvalidField(format!(
                "aggregate member field '{}.{}' must have type Any",
                aggregate.member_schema_name,
                crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD
            )));
        }
        for field in AGGREGATE_MEMBER_RESERVED_FIELDS
            .iter()
            .copied()
            .filter(|field| {
                !matches!(
                    *field,
                    crate::schema::types::aggregate::AGGREGATE_WINNER_COUNTER_FIELD
                        | crate::schema::types::aggregate::AGGREGATE_WINNER_WRITTEN_AT_FIELD
                        | crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD
                )
            })
        {
            if member_schema.get_field_type(field) != &FieldValueType::String {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member field '{}.{field}' must have type String",
                    aggregate.member_schema_name
                )));
            }
        }

        let normalized = AggregateSet {
            target_schema_name: aggregate.target_schema_name.clone(),
            target_key_value: target_key,
            member_schema_name: aggregate.member_schema_name.clone(),
            member_key_value: member_key,
            contribution: aggregate.contribution.clone(),
        };
        let _ = source_hash;
        Ok((source_key, normalized))
    }

    #[cfg(feature = "cloud-sync")]
    pub(in crate::fold_db_core::mutation_manager) fn preflight_replayed_aggregate_set(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if mutations.len() != 1 {
            return Err(SchemaError::InvalidData(
                "aggregate replay must contain exactly one source mutation".into(),
            ));
        }
        let source = &mutations[0];
        let aggregate = source.aggregate_set.as_ref().ok_or_else(|| {
            SchemaError::InvalidData("aggregate replay source has no aggregate_set".into())
        })?;
        self.validate_aggregate_contract(source, aggregate)?;
        Ok(())
    }

    // lint:fn-size-ok verbatim move from aggregate.rs; splitting this function is separate work
    pub(super) fn validate_enrolled_aggregate_schemas(
        &self,
        guard: &AggregateGuard,
    ) -> Result<(), SchemaError> {
        let source_schema = self
            .schema_manager
            .get_schema_metadata(&guard.source_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate source schema '{}' not found",
                    guard.source_schema_name
                ))
            })?;
        if source_schema.schema_type != DeclarativeSchemaType::HashRange
            || guard.source_schema_name == guard.member_schema_name
        {
            return Err(SchemaError::InvalidData(
                "enrolled aggregate source must remain a distinct HashRange schema".into(),
            ));
        }
        if source_schema.runtime_fields.keys().any(|field| {
            field == AGGREGATE_VALID_FIELD
                || field == AGGREGATE_GUARD_TOKEN_FIELD
                || AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field.as_str())
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate source schema '{}' declares a reserved aggregate field",
                guard.source_schema_name
            )));
        }

        let target_schema = self
            .schema_manager
            .get_schema_metadata(&guard.target_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate target schema '{}' not found",
                    guard.target_schema_name
                ))
            })?;
        if target_schema.schema_type != DeclarativeSchemaType::Hash {
            return Err(SchemaError::InvalidData(
                "enrolled aggregate target must remain a Hash schema".into(),
            ));
        }
        if target_schema.key.as_ref().is_some_and(|key| {
            key.hash_field
                .as_deref()
                .into_iter()
                .chain(key.range_field.as_deref())
                .any(|field| {
                    field == AGGREGATE_VALID_FIELD
                        || field == AGGREGATE_GUARD_TOKEN_FIELD
                        || guard.metric_fields.iter().any(|metric| metric == field)
                })
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate target schema '{}' uses an aggregate-owned field as a key",
                guard.target_schema_name
            )));
        }
        for field in &guard.metric_fields {
            if !target_schema.runtime_fields.contains_key(field)
                || target_schema.get_field_type(field) != &FieldValueType::Integer
            {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate target metric '{}.{field}' must remain Integer",
                    guard.target_schema_name
                )));
            }
        }
        for (field, expected) in [
            (AGGREGATE_VALID_FIELD, FieldValueType::Integer),
            (AGGREGATE_GUARD_TOKEN_FIELD, FieldValueType::String),
        ] {
            if !target_schema.runtime_fields.contains_key(field)
                || target_schema.get_field_type(field) != &expected
            {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate target field '{}.{field}' must remain {expected:?}",
                    guard.target_schema_name
                )));
            }
        }

        let member_schema = self
            .schema_manager
            .get_schema_metadata(&guard.member_schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate member schema '{}' not found",
                    guard.member_schema_name
                ))
            })?;
        if member_schema.schema_type != DeclarativeSchemaType::HashRange {
            return Err(SchemaError::InvalidData(
                "enrolled aggregate member must remain a HashRange schema".into(),
            ));
        }
        if member_schema.key.as_ref().is_some_and(|key| {
            key.hash_field
                .as_deref()
                .into_iter()
                .chain(key.range_field.as_deref())
                .any(|field| AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field))
        }) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate member schema '{}' uses a reserved aggregate field as a key",
                guard.member_schema_name
            )));
        }
        for field in AGGREGATE_MEMBER_RESERVED_FIELDS {
            if !member_schema.runtime_fields.contains_key(*field) {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member schema '{}' is missing reserved field '{field}'",
                    guard.member_schema_name
                )));
            }
        }
        for field in [
            crate::schema::types::aggregate::AGGREGATE_WINNER_COUNTER_FIELD,
            crate::schema::types::aggregate::AGGREGATE_WINNER_WRITTEN_AT_FIELD,
        ] {
            if member_schema.get_field_type(field) != &FieldValueType::String {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member field '{}.{field}' must remain String",
                    guard.member_schema_name
                )));
            }
        }
        if member_schema.get_field_type(crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD)
            != &FieldValueType::Any
        {
            return Err(SchemaError::InvalidField(format!(
                "aggregate member field '{}.{}' must remain Any",
                guard.member_schema_name,
                crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD
            )));
        }
        for field in AGGREGATE_MEMBER_RESERVED_FIELDS
            .iter()
            .copied()
            .filter(|field| {
                !matches!(
                    *field,
                    crate::schema::types::aggregate::AGGREGATE_WINNER_COUNTER_FIELD
                        | crate::schema::types::aggregate::AGGREGATE_WINNER_WRITTEN_AT_FIELD
                        | crate::schema::types::aggregate::AGGREGATE_VALUES_FIELD
                )
            })
        {
            if member_schema.get_field_type(field) != &FieldValueType::String {
                return Err(SchemaError::InvalidField(format!(
                    "aggregate member field '{}.{field}' must remain String",
                    guard.member_schema_name
                )));
            }
        }
        Ok(())
    }

    pub(super) async fn resolve_aggregate_storage_prefix(
        &self,
        source_schema_name: &str,
        aggregate: &AggregateSet,
        access_context: &crate::access::AccessContext,
    ) -> Result<Option<String>, SchemaError> {
        let mut resolved = None;
        for schema_name in [
            source_schema_name,
            aggregate.target_schema_name.as_str(),
            aggregate.member_schema_name.as_str(),
        ] {
            let prefix = self
                .db_ops
                .db_catalog()
                .resolve_storage_prefix(
                    access_context.db_locator.as_deref(),
                    schema_name,
                    access_context.storage_prefix.as_deref(),
                )
                .await?;
            if let Some(existing) = &resolved {
                if existing != &prefix {
                    return Err(SchemaError::InvalidData(
                        "aggregate source, member, and target resolve to different catalog instances"
                            .into(),
                    ));
                }
            } else {
                resolved = Some(prefix);
            }
        }
        Ok(resolved.flatten())
    }
}
