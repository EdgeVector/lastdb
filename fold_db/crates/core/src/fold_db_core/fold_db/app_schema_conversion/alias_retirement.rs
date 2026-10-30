//! Retirement cannot discard values outside the kept sources' coverage audit.

use super::*;

pub(super) fn validate(alias: &Schema, targets: &[Target]) -> Result<(), SchemaError> {
    let mut alias = alias.clone_for_read_shared();
    alias.populate_runtime_fields()?;
    let mappings = alias.field_mappers.as_ref().ok_or_else(|| {
        invalid("keep and convert an ordinary old layout; retirement only accepts a fully backed mapper alias")
    })?;
    if alias.uses_independent_fields()
        || alias
            .transform_fields
            .as_ref()
            .is_some_and(|fields| !fields.is_empty())
        || mappings.is_empty()
        || mappings.keys().collect::<BTreeSet<_>>()
            != alias.runtime_fields.keys().collect::<BTreeSet<_>>()
    {
        return Err(invalid(
            "a retired alias cannot have independent, transformed, or unmapped fields",
        ));
    }
    let source_name = mappings
        .values()
        .next()
        .expect("nonempty mappings")
        .source_schema();
    let source = &targets
        .iter()
        .find(|target| target.source.name == source_name)
        .ok_or_else(|| invalid("a retired alias must reference a kept source"))?
        .source;
    if alias.owner_app_id != source.owner_app_id || alias.schema_type != source.schema_type {
        return Err(invalid(
            "a retired alias must retain the kept source's owner and schema type",
        ));
    }
    let mut fields = BTreeMap::new();
    for (name, mapper) in mappings {
        let from = &alias.runtime_fields[name];
        let to = source
            .runtime_fields
            .get(mapper.source_field())
            .ok_or_else(|| invalid("a retired alias references a field outside its kept source"))?;
        if mapper.source_schema() != source.name
            || from.common().molecule_uuid().is_none()
            || from.common().molecule_uuid() != to.common().molecule_uuid()
            || alias.get_field_type(name) != source.get_field_type(mapper.source_field())
        {
            return Err(invalid(
                "a retired alias has an unverified molecule, field type, or source authority",
            ));
        }
        fields.insert(name.clone(), mapper.source_field().to_string());
    }
    let mut key = alias.key.clone();
    if let Some(key) = &mut key {
        key.hash_field = key
            .hash_field
            .as_deref()
            .map(|name| mapped_fields::key_field(name, &fields))
            .transpose()?;
        key.range_field = key
            .range_field
            .as_deref()
            .map(|name| mapped_fields::key_field(name, &fields))
            .transpose()?;
    }
    if key != source.key {
        return Err(invalid(
            "a retired alias cannot change its kept source's primary key roles or paths",
        ));
    }
    Ok(())
}
