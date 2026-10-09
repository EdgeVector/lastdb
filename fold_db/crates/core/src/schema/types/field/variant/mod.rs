//! Unified schema field runtime type.
//!
//! Historically each of Single / Hash / Range / HashRange had its own field
//! struct (`SingleField`, `HashField`, …) wrapped in a four-way
//! [`FieldVariant`] enum. That doubled the type taxonomy already expressed by
//! [`crate::db_operations::MoleculeData`] and forced match-boilerplate at every
//! call site.
//!
//! This module collapses the field layer to **one** struct keyed by
//! [`FieldKind`], holding an optional hydrated molecule
//! ([`MoleculeData`] = [`crate::atom::MoleculeHashRange`]). On-disk layout is
//! always HashRange. Filter/load slot shape (Hash / Range / HashRange) is
//! expressed via [`FieldKind`] / filter layout.

mod access;
mod filter;
mod history;
mod kind;
mod read;
mod value;
mod write;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::db_operations::MoleculeData;
use crate::schema::types::declarative_schemas::FieldMapper;
use crate::schema::types::field::FieldCommon;
use crate::schema::types::schema::DeclarativeSchemaType;

pub use kind::FieldKind;
pub use value::FieldValue;

/// Unified runtime field: common metadata + key-shape kind + optional
/// hydrated molecule. Replaces the former four-way enum of field structs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldVariant {
    #[serde(flatten)]
    pub inner: FieldCommon,
    /// Key shape — fixed at construction from the schema's
    /// [`DeclarativeSchemaType`].
    pub kind: FieldKind,
    /// Transient hydrated molecule cache. Not a durability root — the
    /// durable link is `inner.molecule_uuid` + on-disk `mk:`/`ref:` records.
    /// Skipped in serde so a schema clone never accidentally rehydrates
    /// stale molecule bytes from a serialized snapshot.
    #[serde(skip)]
    pub molecule: Option<MoleculeData>,
}

impl FieldVariant {
    /// Construct an empty field of the given kind.
    #[must_use]
    pub fn new(kind: FieldKind, field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self {
            inner: FieldCommon::new(field_mappers),
            kind,
            molecule: None,
        }
    }

    /// Construct from a declarative schema type (the usual construction path).
    #[must_use]
    pub fn from_schema_type(
        schema_type: DeclarativeSchemaType,
        field_mappers: HashMap<String, FieldMapper>,
    ) -> Self {
        Self::new(FieldKind::from(schema_type), field_mappers)
    }

    /// Convenience constructors used by tests and a few call sites that
    /// already know the concrete kind.
    #[must_use]
    pub fn single(field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self::new(FieldKind::Single, field_mappers)
    }

    #[must_use]
    pub fn hash(field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self::new(FieldKind::Hash, field_mappers)
    }

    #[must_use]
    pub fn range(field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self::new(FieldKind::Range, field_mappers)
    }

    #[must_use]
    pub fn hash_range(field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self::new(FieldKind::HashRange, field_mappers)
    }

    /// Gets the common field data.
    pub fn common(&self) -> &FieldCommon {
        &self.inner
    }

    /// Gets the common field data mutably.
    pub fn common_mut(&mut self) -> &mut FieldCommon {
        &mut self.inner
    }

    /// Clone the field's *structure* without deep-copying the in-memory
    /// hydrated molecule (left `None`). See prior `cloned_without_molecule`
    /// docs — this keeps per-query clone O(1) in field data size.
    #[must_use]
    pub fn cloned_without_molecule(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            kind: self.kind,
            molecule: None,
        }
    }
}
