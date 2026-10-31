use serde::{Deserialize, Serialize};

use crate::atom::molecule_slot::{FilterLayout, OneDSlot};
use crate::schema::types::schema::DeclarativeSchemaType;

/// Discriminator for a field's key shape. Mirrors
/// [`DeclarativeSchemaType`] and selects key-shape filter/load semantics for the unified HashRange molecule
/// `write_mutation` / `refresh_from_db` produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldKind {
    /// No key dimensions — one atom per field (empty `("", "")` HashRange slot).
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

impl FieldKind {
    /// Filter/load layout for the unified HashRange store (1-D Hash / 1-D Range /
    /// composite HashRange). Single uses the empty `("", "")` slot.
    #[must_use]
    pub fn filter_layout(self) -> FilterLayout {
        match self {
            Self::Hash => FilterLayout::OneD(OneDSlot::Hash),
            Self::Range => FilterLayout::OneD(OneDSlot::Range),
            Self::Single | Self::HashRange => FilterLayout::Composite,
        }
    }
}
