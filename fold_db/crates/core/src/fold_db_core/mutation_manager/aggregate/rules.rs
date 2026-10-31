//! Derived aggregate mutation construction, without storage or locks.
//! Each constructor mints a fresh mutation id, so it stays out of `plan`.

use super::*;

pub(super) fn derived_mutation(
    schema: String,
    fields: HashMap<String, serde_json::Value>,
    key: KeyValue,
    pub_key: String,
    mutation_type: MutationType,
) -> Mutation {
    let mut mutation = Mutation::new(schema, fields, key, pub_key, mutation_type);
    mutation.aggregate_derived_internal = true;
    mutation.synchronous = Some(true);
    mutation
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
    derived_mutation(schema, fields, key, pub_key, MutationType::Update)
}
