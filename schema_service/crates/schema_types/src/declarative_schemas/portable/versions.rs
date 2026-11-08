//! Later definitions of one shared schema name
//! (`decision-2026-11-07-lastdb-schema-name-slot-definition-per-record`).
//! The name stays the storage slot. A later definition is compatible when it
//! keeps the key, the alternate keys and every field it shares with the
//! current definition; it may only add fields, as a local declare may.

use super::{DeclarativeSchemaDefinition, PortableSchemaDefinition, PortableSchemaId};
use std::collections::BTreeSet;

const INCOMPATIBLE: &str = "a later shared schema definition changes the key, the alternate keys or an existing field; declare a new schema name";

impl PortableSchemaDefinition {
    /// The app-scoped name this definition installs under. Verify the
    /// definition before it decides where a definition installs.
    #[must_use]
    pub fn canonical_name(&self) -> &str {
        &self.definition.name
    }

    /// The definition a name holds once it has accepted every one of
    /// `accepted`. One accepted definition that declares every accepted field
    /// replaces the others. Without exactly one, two branches each added a
    /// field, and the name holds their union in field-name order. Either way
    /// the result depends only on the set, so every home that accepted the
    /// same definitions holds the same one, whatever the order of arrival.
    pub fn current_of(accepted: &[Self]) -> Result<Self, String> {
        let (first, rest) = accepted
            .split_first()
            .ok_or("a shared schema name has no definition")?;
        let base = first.verify(&first.id)?;
        let mut union = base.clone();
        let mut names: BTreeSet<&str> = field_names(base).collect();
        for later in rest {
            let incoming = later.verify(&later.id)?;
            if incoming.name != base.name || !compatible(&union, incoming)? {
                return Err(INCOMPATIBLE.into());
            }
            for field in field_names(incoming) {
                if names.insert(field) {
                    copy_field(incoming, &mut union, field);
                }
            }
        }
        let mut complete = accepted.iter().filter(|definition| {
            field_names(&definition.definition).collect::<BTreeSet<_>>() == names
        });
        if let (Some(only), None) = (complete.next(), complete.next()) {
            return Ok(only.clone());
        }
        union.fields = Some(names.into_iter().map(str::to_owned).collect());
        union.identity_hash = None;
        union.identity_hash_algo_version = None;
        union.compute_identity_hash();
        let catalog_id = union
            .identity_hash
            .clone()
            .ok_or("the union of shared schema definitions has no identity")?;
        Self::from_catalog(&catalog_id, union)
    }

    /// Bind a record's definition to the installed schema of its name. The
    /// installed schema may hold later fields. It keeps this definition's
    /// key, alternate keys and every field this definition declares.
    pub fn bind_local_version(
        &self,
        expected: &PortableSchemaId,
        local: &DeclarativeSchemaDefinition,
    ) -> Result<&DeclarativeSchemaDefinition, String> {
        let definition = self.verify(expected)?;
        if local.name.is_empty() {
            return Err("portable schema requires an explicit local runtime name".into());
        }
        let mut candidate = local.clone();
        candidate.name.clone_from(&definition.name);
        candidate.identity_hash = None;
        candidate.identity_hash_algo_version = None;
        candidate.compute_identity_hash();
        let catalog_id = candidate
            .identity_hash
            .clone()
            .ok_or("the installed shared schema has no identity")?;
        let installed = Self::from_catalog(&catalog_id, candidate)?;
        let fields: BTreeSet<&str> = field_names(&installed.definition).collect();
        if !field_names(definition).all(|field| fields.contains(field))
            || !compatible(definition, &installed.definition)?
        {
            return Err(
                "the installed schema does not extend this shared schema definition".into(),
            );
        }
        Ok(definition)
    }
}

fn field_names(definition: &DeclarativeSchemaDefinition) -> impl Iterator<Item = &str> {
    definition.fields.iter().flatten().map(String::as_str)
}

/// Equal apart from fields that only one of the two declares.
fn compatible(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> Result<bool, String> {
    if shape(a)? != shape(b)? {
        return Ok(false);
    }
    let fields: BTreeSet<&str> = field_names(b).collect();
    for field in field_names(a).filter(|field| fields.contains(field)) {
        if field_entry(a, field)? != field_entry(b, field)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Everything but the name, the fields and the identity derived from them.
fn shape(definition: &DeclarativeSchemaDefinition) -> Result<serde_json::Value, String> {
    let mut shape = definition.clone();
    shape.name.clear();
    shape.fields = None;
    shape.field_classifications.clear();
    shape.field_descriptions.clear();
    shape.field_data_classifications.clear();
    shape.field_interest_categories.clear();
    shape.field_types.clear();
    shape.field_hashes.clear();
    shape.field_versions.clear();
    shape.field_declarations.clear();
    shape.identity_hash = None;
    shape.identity_hash_algo_version = None;
    shape.hash = None;
    shape.superseded_by = None;
    serde_json::to_value(&shape).map_err(|_| "cannot encode a shared schema definition".into())
}

/// Every declaration of one field.
fn field_entry(
    definition: &DeclarativeSchemaDefinition,
    field: &str,
) -> Result<serde_json::Value, String> {
    serde_json::to_value((
        definition.field_types.get(field),
        definition.field_descriptions.get(field),
        definition.field_classifications.get(field),
        definition.field_data_classifications.get(field),
        definition.field_interest_categories.get(field),
        definition.field_hashes.get(field),
        definition.field_versions.get(field),
        definition.field_declarations.get(field),
    ))
    .map_err(|_| "cannot encode a shared schema field".into())
}

fn copy_field(
    from: &DeclarativeSchemaDefinition,
    to: &mut DeclarativeSchemaDefinition,
    field: &str,
) {
    if let Some(value) = from.field_types.get(field) {
        to.field_types.insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_descriptions.get(field) {
        to.field_descriptions
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_classifications.get(field) {
        to.field_classifications
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_data_classifications.get(field) {
        to.field_data_classifications
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_interest_categories.get(field) {
        to.field_interest_categories
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_hashes.get(field) {
        to.field_hashes.insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_versions.get(field) {
        to.field_versions.insert(field.to_owned(), *value);
    }
    if let Some(value) = from.field_declarations.get(field) {
        to.field_declarations
            .insert(field.to_owned(), value.clone());
    }
}
