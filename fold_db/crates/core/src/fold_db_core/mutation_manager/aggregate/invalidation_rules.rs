//! Invalidation decisions use captured guards and values, without database effects.

use super::*;

pub(super) struct InvalidationTarget {
    pub(super) guard: AggregateGuard,
    pub(super) winner: AggregateWinner,
    pub(super) token: String,
    pub(super) pub_key: String,
}

/// Reject a source write that an enrolled aggregate guard cannot accept.
pub(super) fn check_enrolled_source(
    guard: &AggregateGuard,
    source: &Mutation,
    source_hash: &str,
) -> Result<(), SchemaError> {
    if guard.source_schema_name != source.schema_name || guard.source_partition_hash != source_hash
    {
        return Err(SchemaError::InvalidData(
            "aggregate guard identity does not match its storage key".into(),
        ));
    }
    if matches!(
        source.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        return Err(SchemaError::InvalidData(
            "enrolled aggregate sources reject Delete and Purge; write a live logical removal value with an explicit zero AggregateSet"
                .into(),
        ));
    }
    if source
        .fields_and_values
        .values()
        .any(crate::atom::is_tombstone_value)
    {
        return Err(SchemaError::InvalidData(
            "enrolled aggregate sources reject field tombstones; write a live logical removal value with an explicit zero AggregateSet"
                .into(),
        ));
    }
    Ok(())
}

pub(super) fn check_target_fields(
    schema: &crate::schema::Schema,
    target_schema_name: &str,
) -> Result<(), SchemaError> {
    for field in [AGGREGATE_VALID_FIELD, AGGREGATE_GUARD_TOKEN_FIELD] {
        if !schema.runtime_fields.contains_key(field) {
            return Err(SchemaError::InvalidField(format!(
                "aggregate target schema '{target_schema_name}' must declare field '{field}'"
            )));
        }
    }
    Ok(())
}

pub(super) fn guard_targets(
    guards: &[&AggregateGuard],
) -> BTreeMap<(String, String), std::collections::BTreeSet<String>> {
    let mut targets: BTreeMap<(String, String), std::collections::BTreeSet<String>> =
        BTreeMap::new();
    for guard in guards {
        targets
            .entry((guard.target_schema_name.clone(), guard.target_key.clone()))
            .or_default()
            .extend(guard.metric_fields.iter().cloned());
    }
    targets
}

pub(super) fn missing_metric_values(
    metric_names: &[String],
    current: &CurrentRowFields,
) -> HashMap<String, serde_json::Value> {
    let mut values = HashMap::new();
    match current {
        CurrentRowFields::Absent | CurrentRowFields::Corrupt { .. } => {
            values.extend(
                metric_names
                    .iter()
                    .cloned()
                    .map(|field| (field, serde_json::Value::from(0))),
            );
        }
        CurrentRowFields::Present(current) => {
            values.extend(
                metric_names
                    .iter()
                    .filter(|field| {
                        current
                            .get(*field)
                            .and_then(serde_json::Value::as_i64)
                            .is_none()
                    })
                    .map(|field| (field.clone(), serde_json::Value::from(0))),
            );
        }
    }
    values
}

pub(super) fn check_marker(status: &CurrentRowFields, token: &str) -> Result<(), SchemaError> {
    let CurrentRowFields::Present(status) = status else {
        return Err(SchemaError::InvalidData(
            "aggregate invalidation marker did not become readable".into(),
        ));
    };
    if status
        .get(AGGREGATE_VALID_FIELD)
        .and_then(serde_json::Value::as_i64)
        != Some(0)
        || status
            .get(AGGREGATE_GUARD_TOKEN_FIELD)
            .and_then(serde_json::Value::as_str)
            != Some(token)
    {
        return Err(SchemaError::InvalidData(
            "aggregate invalidation marker did not become current".into(),
        ));
    }
    Ok(())
}
