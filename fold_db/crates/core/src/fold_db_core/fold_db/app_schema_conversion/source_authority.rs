//! A legacy mapper can describe an already-private, differently keyed molecule.

use super::*;

pub(super) fn validate(
    source: &Schema,
    installed: &std::collections::HashMap<String, Schema>,
    selected: &BTreeSet<String>,
) -> Result<(), SchemaError> {
    let Some(mappings) = source.field_mappers.as_ref().filter(|map| !map.is_empty()) else {
        return Ok(());
    };
    if source.uses_independent_fields() {
        return Err(invalid(
            "an independent source cannot carry legacy value mappers",
        ));
    }
    let mut source = source.clone_for_read_shared();
    source.populate_runtime_fields()?;
    let stored = source
        .field_molecule_uuids
        .as_ref()
        .ok_or_else(|| invalid("mapped source conversion requires stored molecule identities"))?;
    if stored.keys().collect::<BTreeSet<_>>()
        != source.runtime_fields.keys().collect::<BTreeSet<_>>()
    {
        return Err(invalid("mapped source molecule identities are incomplete"));
    }
    let mut references = std::collections::HashMap::new();
    for mapper in mappings.values() {
        if !selected.contains(mapper.source_schema()) {
            return Err(invalid(
                "every mapped source authority must be explicitly kept in this conversion",
            ));
        }
        if !references.contains_key(mapper.source_schema()) {
            let mut reference = installed
                .get(mapper.source_schema())
                .ok_or_else(|| invalid("a mapped source authority is absent"))?
                .clone_for_read_shared();
            reference.populate_runtime_fields()?;
            references.insert(mapper.source_schema().to_string(), reference);
        }
    }
    for (name, field) in &source.runtime_fields {
        if stored.get(name).filter(|id| !id.is_empty()) != field.common().molecule_uuid() {
            return Err(invalid(
                "mapped source runtime and stored molecule identities differ",
            ));
        }
    }
    for (name, mapper) in mappings {
        let reference = &references[mapper.source_schema()];
        let field = source
            .runtime_fields
            .get(name)
            .ok_or_else(|| invalid("mapped source field is absent"))?;
        let referenced = reference
            .runtime_fields
            .get(mapper.source_field())
            .ok_or_else(|| invalid("mapped authority field is absent"))?;
        let identity = source.field_hashes.get(name).filter(|id| !id.is_empty());
        if source.owner_app_id != reference.owner_app_id
            || crate::schema::SchemaCore::same_coordinates(&source, reference)
            || field.common().molecule_uuid().is_none()
            || referenced.common().molecule_uuid().is_none()
            || field.common().molecule_uuid() == referenced.common().molecule_uuid()
            || reference
                .field_molecule_uuids
                .as_ref()
                .and_then(|ids| ids.get(mapper.source_field()))
                != referenced.common().molecule_uuid()
            || identity.is_none()
            || identity != reference.field_hashes.get(mapper.source_field())
            || !source.field_types.contains_key(name)
            || source.field_types.get(name) != reference.field_types.get(mapper.source_field())
        {
            return Err(invalid("mapped source conversion requires verified private molecules at different key coordinates with the same app, field identity and type"));
        }
    }
    Ok(())
}
