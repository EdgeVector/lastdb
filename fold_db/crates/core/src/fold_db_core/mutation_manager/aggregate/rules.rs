//! Shared aggregate rules and mutation construction, without storage or locks.

use super::*;

pub(super) fn association_matches(
    guard: &AggregateGuard,
    source: &str,
    source_hash: &str,
    target: &str,
    target_key: &KeyValue,
    member: &str,
) -> bool {
    guard.source_schema_name == source
        && guard.source_partition_hash == source_hash
        && guard.target_schema_name == target
        && guard.target_key == target_key.to_storage_key()
        && guard.member_schema_name == member
        && guard.committed
}

pub(super) fn target_owner_matches(owner: &AggregateGuard, guard: &AggregateGuard) -> bool {
    owner.same_association(guard) && owner.cmp_enrollment(guard) == std::cmp::Ordering::Equal
}

pub(super) fn summary_mutation(
    schema: String,
    key: KeyValue,
    mut fields: HashMap<String, serde_json::Value>,
    valid: bool,
    token: String,
    pub_key: String,
) -> Mutation {
    fields.insert(
        AGGREGATE_VALID_FIELD.into(),
        serde_json::Value::from(if valid { 1 } else { 0 }),
    );
    fields.insert(
        AGGREGATE_GUARD_TOKEN_FIELD.into(),
        serde_json::Value::String(token),
    );
    let mut mutation = Mutation::new(schema, fields, key, pub_key, MutationType::Update);
    mutation.aggregate_derived_internal = true;
    mutation.synchronous = Some(true);
    mutation
}
