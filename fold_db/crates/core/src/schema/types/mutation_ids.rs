//! Caller-supplied mutation identity and complete record target.
use super::{Mutation, MutationType};
use lastdb_model::record_replacement::{RecordExpectation, RecordTarget};
use serde_json::Value;
use std::collections::HashMap;

pub trait MutationIds {
    fn new(
        id: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
        pub_key: String,
        mutation_type: MutationType,
        expected: RecordExpectation,
    ) -> Self;
}
impl MutationIds for Mutation {
    fn new(
        id: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
        pub_key: String,
        mutation_type: MutationType,
        expected: RecordExpectation,
    ) -> Self {
        Self::with_uuid(id, target, record_fields, pub_key, mutation_type, expected)
    }
}
