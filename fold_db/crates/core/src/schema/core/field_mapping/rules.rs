//! One mapper step without storage or cache access.

use crate::schema::types::Schema;

pub(super) enum ChainStep {
    Follow(String, String),
    Resolved(Option<String>),
    MissingSource,
    MissingField,
}

pub(super) fn chain_step(source: Option<&Schema>, field_name: &str) -> ChainStep {
    let Some(schema) = source else {
        return ChainStep::MissingSource;
    };
    // Intermediate runtime/persisted UUIDs can contain a single-hop fallback.
    // Follow the mapper first to reach the actual unmapped chain root.
    if let Some(mapper) = schema
        .field_mappers
        .as_ref()
        .and_then(|m| m.get(field_name))
    {
        return ChainStep::Follow(
            mapper.source_schema().to_string(),
            mapper.source_field().to_string(),
        );
    }
    if let Some(uuid) = schema
        .field_molecule_uuids
        .as_ref()
        .and_then(|m| m.get(field_name))
    {
        return ChainStep::Resolved(Some(uuid.clone()));
    }
    match schema.runtime_fields.get(field_name) {
        Some(field) => ChainStep::Resolved(field.common().molecule_uuid().cloned()),
        None => ChainStep::MissingField,
    }
}
