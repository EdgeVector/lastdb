use serde::{Deserialize, Serialize};

use crate::schema::types::schema::DeclarativeSchemaType;

/// Discriminator for a field's key shape. Mirrors
/// [`DeclarativeSchemaType`] and selects key-shape filter/load semantics for the unified HashRange field key
/// `write_mutation` / `refresh_from_db` produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldKind {
    /// No key dimensions — one record per field (empty `("", "")` HashRange slot).
    Single,
    /// Hash-keyed collection (unordered).
    Hash,
    /// Range-keyed ordered collection.
    Range,
    /// Composite (hash, range) collection.
    HashRange,
}

impl From<DeclarativeSchemaType> for FieldKind {
    fn from(schema_type: DeclarativeSchemaType) -> Self {
        match schema_type {
            DeclarativeSchemaType::Single => Self::Single,
            DeclarativeSchemaType::Hash => Self::Hash,
            DeclarativeSchemaType::Range => Self::Range,
            DeclarativeSchemaType::HashRange => Self::HashRange,
        }
    }
}
