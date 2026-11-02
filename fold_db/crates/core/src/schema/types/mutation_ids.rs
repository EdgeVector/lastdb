//! Caller-supplied mutation ids. The model stores the id it is given.
//! Callers draw a uuid v4 and pass it in.

use std::collections::HashMap;

use serde_json::Value;

use super::{KeyValue, Mutation, MutationType};

/// Construct a [`Mutation`] with a caller-supplied id.
pub trait MutationIds {
    /// A new mutation with the caller's id. [`Mutation::with_uuid`] stores it.
    fn new(
        id: String,
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self;
}

impl MutationIds for Mutation {
    fn new(
        id: String,
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self {
        Self::with_uuid(
            id,
            schema_name,
            fields_and_values,
            key_value,
            pub_key,
            mutation_type,
        )
    }
}
