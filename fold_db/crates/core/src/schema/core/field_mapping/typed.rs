//! Typed molecules share storage only when their complete key coordinates agree.

use std::collections::{HashMap, HashSet};

use super::super::SchemaCore;
use crate::atom::deterministic_molecule_uuid;
use crate::schema::types::{Schema, SchemaError};

impl SchemaCore {
    /// Resolve before minting a typed molecule contract. Cross-key siblings keep
    /// their own molecules; the normal field-identity pass binds their proteins.
    pub(in crate::schema::core) async fn resolve_typed_field_mappers(
        &self,
        schema: &mut Schema,
        stored: Option<&Schema>,
    ) -> Result<(), SchemaError> {
        // Refuse an invalid key profile before any mapper resolution.
        self.db_ops.molecule_keys().schema_profile(schema)?;
        // Only this process's completed publication can establish the fast
        // path. An older binary may have stored a collapsed cross-key map.
        if self.coherence_memo(&schema.name)?.is_some()
            && stored.is_some_and(|old| {
                schema.field_mappers == old.field_mappers
                    && schema.owner_app_id == old.owner_app_id
                    && schema.key == old.key
                    && schema.field_types == old.field_types
                    && schema.field_hashes == old.field_hashes
                    && schema.field_molecule_uuids.is_some()
                    && schema.field_molecule_uuids == old.field_molecule_uuids
            })
        {
            return Ok(());
        }
        let fields: Vec<_> = schema
            .field_mappers
            .iter()
            .flat_map(|m| m.keys())
            .cloned()
            .collect();
        let mut sources = HashMap::new();
        for field in fields {
            let (molecule, identity) = self
                .typed_mapper_molecule(schema, &field, stored, &mut sources)
                .await?;
            schema
                .runtime_fields
                .get_mut(&field)
                .ok_or_else(|| invalid("mapper target field is absent"))?
                .common_mut()
                .set_molecule_uuid(molecule);
            schema.field_hashes.insert(field, identity);
        }
        schema.sync_molecule_uuids();
        Ok(())
    }

    async fn typed_mapper_molecule(
        &self,
        schema: &Schema,
        field: &str,
        stored: Option<&Schema>,
        sources: &mut HashMap<String, Schema>,
    ) -> Result<(String, String), SchemaError> {
        let mut current = schema.clone_for_read_shared();
        let mut field = field.to_owned();
        let mut first = true;
        let mut visited = HashSet::new();
        loop {
            if !visited.insert((current.name.clone(), field.clone())) {
                return Err(invalid("cyclic typed field mapper"));
            }
            let Some(mapper) = current.field_mappers.as_ref().and_then(|m| m.get(&field)) else {
                let molecule = current
                    .runtime_fields
                    .get(&field)
                    .and_then(|field| field.common().molecule_uuid())
                    .cloned()
                    .ok_or_else(|| invalid("typed mapper source field is absent"))?;
                let identity = current
                    .field_hashes
                    .get(&field)
                    .filter(|id| !id.is_empty())
                    .cloned()
                    .ok_or_else(|| invalid("typed mapper source identity is absent"))?;
                return Ok((molecule, identity));
            };
            if !sources.contains_key(mapper.source_schema()) {
                // Name the source: a declaring app can load it and retry.
                let source = self
                    .db_ops
                    .get_schema(mapper.source_schema())
                    .await?
                    .ok_or_else(|| {
                        invalid(&format!(
                            "typed mapper source schema '{}' is not installed",
                            mapper.source_schema()
                        ))
                    })?;
                sources.insert(mapper.source_schema().to_owned(), source);
            }
            let source = &sources[mapper.source_schema()];
            if !source.alternate_keys.is_empty() {
                return Err(invalid(
                    "alternate schemas cannot supply mutable field mappers in the spike",
                ));
            }
            if !same_coordinates(&current, source) {
                let old = if first { stored } else { Some(&current) };
                let molecule =
                    cross_key_molecule(&current, &field, source, mapper.source_field(), old)?;
                return Ok((molecule, current.field_hashes[&field].clone()));
            }
            field = mapper.source_field().to_owned();
            current = source.clone_for_read_shared();
            first = false;
        }
    }
}

/// Translate app aliases before comparing key names. Payload field types do not
/// decide molecule order; the hash/range fields and their types do.
fn same_coordinates(target: &Schema, source: &Schema) -> bool {
    let names = |schema: &Schema| {
        schema
            .key
            .as_ref()
            .map(|key| (key.hash_field.clone(), key.range_field.clone()))
            .unwrap_or_default()
    };
    let (target_hash, target_range) = names(target);
    let (source_hash, source_range) = names(source);
    let same =
        |target_path: Option<&str>, source_path: Option<&str>| match (target_path, source_path) {
            (None, None) => true,
            (Some(target_path), Some(source_path)) => {
                let mapped = target
                    .field_mappers
                    .as_ref()
                    .and_then(|m| m.get(target_path));
                let name = mapped
                    .filter(|m| m.source_schema() == source.name)
                    .map_or(target_path, schema_types::FieldMapper::source_field);
                name == source_path
                    && target.key_field_type(target_path) == source.key_field_type(source_path)
            }
            _ => false,
        };
    same(target_hash.as_deref(), source_hash.as_deref())
        && same(target_range.as_deref(), source_range.as_deref())
}

fn cross_key_molecule(
    target: &Schema,
    field: &str,
    source: &Schema,
    source_field: &str,
    stored: Option<&Schema>,
) -> Result<String, SchemaError> {
    if stored.is_some_and(|old| !same_coordinates(target, old)) {
        return Err(invalid(
            "existing molecule coordinates require explicit conversion",
        ));
    }
    let identity = target.field_hashes.get(field).filter(|id| !id.is_empty());
    if identity.is_none() || identity != source.field_hashes.get(source_field) {
        return Err(invalid(
            "cross-key mapper requires matching canonical field identities",
        ));
    }
    if let Some(molecule) = stored
        .and_then(|s| s.field_molecule_uuids.as_ref())
        .and_then(|m| m.get(field))
    {
        if source
            .runtime_fields
            .get(source_field)
            .and_then(|f| f.common().molecule_uuid())
            == Some(molecule)
        {
            return Err(invalid(
                "existing cross-key molecule sharing requires explicit conversion",
            ));
        }
        return Ok(molecule.clone());
    }
    Ok(deterministic_molecule_uuid(&target.name, field))
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.to_owned())
}
