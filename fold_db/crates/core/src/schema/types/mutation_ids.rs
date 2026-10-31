//! The random mutation id. The model takes the id as an argument.

use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use super::{KeyValue, Mutation, MutationType};

/// Construct a [`Mutation`] with a fresh uuid v4 id.
pub trait MutationIds {
    /// A new mutation with a random uuid v4 id. See
    /// [`Mutation::with_uuid`] for a caller-chosen id.
    fn new(
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self;
}

impl MutationIds for Mutation {
    fn new(
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self {
        Self::with_uuid(
            Uuid::new_v4().to_string(),
            schema_name,
            fields_and_values,
            key_value,
            pub_key,
            mutation_type,
        )
    }
}
