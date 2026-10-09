use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Represents the schema-level type information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub enum DeclarativeSchemaType {
    /// Single schema without range semantics
    Single,
    /// Schema keyed by a single hash key (unordered collection)
    Hash,
    /// Schema that stores data in a key range
    Range,
    /// Schema that uses hashed and ranged keys for partitioning
    HashRange,
}
