use serde::{Deserialize, Serialize};

use crate::db_operations::{FilterLayout, MoleculeData, OneDSlot};
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
    pub(crate) fn filter_layout(self) -> FilterLayout {
        match self {
            Self::Hash => FilterLayout::OneD(OneDSlot::Hash),
            Self::Range => FilterLayout::OneD(OneDSlot::Range),
            Self::Single | Self::HashRange => FilterLayout::Composite,
        }
    }

    /// Optional 1-D retype for dual-read flat slots.
    #[must_use]
    pub(crate) fn retype_slot(self) -> Option<OneDSlot> {
        match self {
            Self::Hash => Some(OneDSlot::Hash),
            Self::Range => Some(OneDSlot::Range),
            Self::Single | Self::HashRange => None,
        }
    }

    /// True when this field uses the per-key (`mk:`/`mh:`) layout.
    #[must_use]
    pub(crate) fn is_per_key(self) -> bool {
        true
    }

    /// Whether `data` matches this field kind.
    ///
    /// All field kinds accept the unified HashRange molecule shape.
    #[must_use]
    pub(crate) fn matches_data(self, _data: &MoleculeData) -> bool {
        // All field kinds hydrate the unified HashRange molecule shape.
        true
    }
}
