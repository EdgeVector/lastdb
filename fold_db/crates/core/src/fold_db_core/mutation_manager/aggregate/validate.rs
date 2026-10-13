//! Aggregate contract and enrollment validation.

use super::*;

mod schema_checks;

impl MutationManager {
    pub(super) fn validate_aggregate_contract(
        &self,
        source: &Mutation,
        aggregate: &AggregateSet,
    ) -> Result<(KeyValue, AggregateSet), SchemaError> {
        aggregate.validate().map_err(SchemaError::InvalidData)?;
        let source_key = self.validate_aggregate_source(source, aggregate)?;
        let target_key = self.validate_aggregate_target(aggregate)?;
        let member_key = self.validate_aggregate_member(aggregate, &source_key)?;
        let normalized = AggregateSet {
            target_schema_name: aggregate.target_schema_name.clone(),
            target_key_value: target_key,
            member_schema_name: aggregate.member_schema_name.clone(),
            member_key_value: member_key,
            contribution: aggregate.contribution.clone(),
        };
        Ok((source_key, normalized))
    }

    /// Validates the source mutation and schema; returns the resolved source key.
    fn validate_aggregate_source(
        &self,
        source: &Mutation,
        aggregate: &AggregateSet,
    ) -> Result<KeyValue, SchemaError> {
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
        if schema_checks::declares_reserved_aggregate_field(&source_schema) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate source schema '{}' declares a reserved aggregate field",
                source.schema_name
            )));
        }
        schema_checks::check_source_payload(source, &source_schema)?;
        schema_checks::check_source_has_non_key_field(source, &source_schema)?;
        let source_key =
            Self::resolve_mutation_key_value(&source.schema_name, &source_schema, source)?;
        if source_key.hash.is_none() {
            return Err(SchemaError::InvalidData(
                "aggregate source key has no hash partition".into(),
            ));
        }
        Ok(source_key)
    }

    /// Validates the target schema and metric fields; returns the resolved target key.
    fn validate_aggregate_target(&self, aggregate: &AggregateSet) -> Result<KeyValue, SchemaError> {
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
        if schema_checks::target_key_uses_owned_field(&target_schema, |field| {
            aggregate.contribution.contains_key(field)
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
        schema_checks::check_target_owned_fields(
            &aggregate.target_schema_name,
            &target_schema,
            "have type",
        )?;
        Ok(target_key)
    }

    /// Validates the member schema against the canonical source key; returns the member key.
    fn validate_aggregate_member(
        &self,
        aggregate: &AggregateSet,
        source_key: &KeyValue,
    ) -> Result<KeyValue, SchemaError> {
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
        if schema_checks::member_key_uses_reserved_field(&member_schema) {
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
        if &member_key != source_key {
            return Err(SchemaError::InvalidData(format!(
                "aggregate member_key_value must use the canonical source identity {}",
                source_key.to_storage_key()
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
        schema_checks::check_member_field_types(
            &aggregate.member_schema_name,
            &member_schema,
            "have type",
        )?;
        Ok(member_key)
    }

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

    pub(super) fn validate_enrolled_aggregate_schemas(
        &self,
        guard: &AggregateGuard,
    ) -> Result<(), SchemaError> {
        self.validate_enrolled_source_schema(guard)?;
        self.validate_enrolled_target_schema(guard)?;
        self.validate_enrolled_member_schema(guard)
    }

    fn validate_enrolled_source_schema(&self, guard: &AggregateGuard) -> Result<(), SchemaError> {
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
        if schema_checks::declares_reserved_aggregate_field(&source_schema) {
            return Err(SchemaError::InvalidData(format!(
                "aggregate source schema '{}' declares a reserved aggregate field",
                guard.source_schema_name
            )));
        }
        Ok(())
    }

    fn validate_enrolled_target_schema(&self, guard: &AggregateGuard) -> Result<(), SchemaError> {
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
        if schema_checks::target_key_uses_owned_field(&target_schema, |field| {
            guard.metric_fields.iter().any(|metric| metric == field)
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
        schema_checks::check_target_owned_fields(
            &guard.target_schema_name,
            &target_schema,
            "remain",
        )
    }

    fn validate_enrolled_member_schema(&self, guard: &AggregateGuard) -> Result<(), SchemaError> {
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
        if schema_checks::member_key_uses_reserved_field(&member_schema) {
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
        schema_checks::check_member_field_types(&guard.member_schema_name, &member_schema, "remain")
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
