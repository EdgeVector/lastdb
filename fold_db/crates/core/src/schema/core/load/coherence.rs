//! Bind equal canonical fields from every schema into one durable protein.

use super::super::locks::read_map;
use super::super::{CoherenceBinding, SchemaCore};
use crate::atom::deterministic_molecule_uuid;
use crate::protein::ProteinMember;
use crate::schema::field_hash_coherence::KeyLayoutFingerprint;
use crate::schema::types::{Schema, SchemaError};
use std::collections::{HashMap, HashSet};

impl SchemaCore {
    /// Bind this schema's fields and matching fields of any stored schema.
    /// The catalog holds definitions, not record keys or atom values. Including
    /// existing definitions joins legacy proteins even before a peer is queried.
    pub async fn apply_field_hash_coherence_on_load(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        let Some(loaded) = self.get_schema_metadata(schema_name)? else {
            return Ok(());
        };
        if loaded.uses_independent_fields() {
            return Ok(());
        }
        let bindings = {
            let schemas = read_map(&self.schemas, "schemas")?;
            let staged = self.staged_schemas()?;
            let mut definitions: HashMap<_, _> = schemas
                .iter()
                .map(|(name, schema)| (name.as_str(), schema))
                .collect();
            definitions.extend(
                staged
                    .iter()
                    .map(|(name, cached)| (name.as_str(), &cached.schema)),
            );
            definitions.insert(schema_name, &loaded);
            coherence_bindings(&loaded, &definitions)?
        };
        if self.coherence_memo(schema_name)?.as_ref() == Some(&bindings) {
            return Ok(());
        }
        if !bindings.is_empty() {
            self.db_ops.atoms().bind_field_proteins(&bindings).await?;
        }
        self.publish_coherence_memo(schema_name, bindings)
    }
}

/// Field identity, including an explicit mapper reference, selects membership.
/// App owners, local field names, key differences and schema overlap do not.
fn coherence_bindings(
    loaded: &Schema,
    definitions: &HashMap<&str, &Schema>,
) -> Result<Vec<CoherenceBinding>, SchemaError> {
    let mut bindings = field_bindings(loaded, definitions, None)?;
    let identities: HashSet<_> = bindings
        .iter()
        .map(|binding| binding.field_identity.clone())
        .collect();
    for peer in definitions.values().filter(|peer| peer.name != loaded.name) {
        bindings.extend(field_bindings(peer, definitions, Some(&identities))?);
    }
    bindings.sort_by(|a, b| {
        (
            &a.field_identity,
            &a.member.molecule_uuid,
            &a.member.hash_field,
            &a.member.range_field,
        )
            .cmp(&(
                &b.field_identity,
                &b.member.molecule_uuid,
                &b.member.hash_field,
                &b.member.range_field,
            ))
    });
    bindings.dedup();
    // A protein relates distinct field molecules. Do not allocate a protein
    // or add write-path metadata for a field that has no peer yet.
    let mut counts = HashMap::new();
    for binding in &bindings {
        *counts
            .entry(binding.field_identity.clone())
            .or_insert(0usize) += 1;
    }
    bindings.retain(|binding| counts[&binding.field_identity] > 1);
    Ok(bindings)
}

fn field_bindings(
    schema: &Schema,
    definitions: &HashMap<&str, &Schema>,
    wanted: Option<&HashSet<String>>,
) -> Result<Vec<CoherenceBinding>, SchemaError> {
    if schema.uses_independent_fields() {
        return Ok(Vec::new());
    }
    let layout = KeyLayoutFingerprint::from_schema(schema);
    let Some(hash_field) = layout.hash_field.filter(|hash| !hash.is_empty()) else {
        return Ok(Vec::new());
    };
    let mut bindings = Vec::new();
    for (field, identity) in &schema.field_hashes {
        let mapped = schema
            .field_mappers
            .as_ref()
            .is_some_and(|mappers| mappers.contains_key(field));
        if !mapped && wanted.is_some_and(|wanted| !wanted.contains(identity)) {
            continue;
        }
        let identity = mapped_identity(schema, field, definitions)?;
        if wanted.is_some_and(|wanted| !wanted.contains(&identity)) {
            continue;
        }
        bindings.push(CoherenceBinding {
            field_identity: identity,
            member: ProteinMember::new(
                schema
                    .field_molecule_uuids
                    .as_ref()
                    .and_then(|ids| ids.get(field))
                    .cloned()
                    .unwrap_or_else(|| deterministic_molecule_uuid(&schema.name, field)),
                &hash_field,
                layout.range_field.clone(),
            ),
        });
    }
    Ok(bindings)
}

/// A mapper references its source field; a local rename does not create a new
/// identity for that molecule. Resolve old stored aliases too, without a reload.
fn mapped_identity(
    schema: &Schema,
    field: &str,
    definitions: &HashMap<&str, &Schema>,
) -> Result<String, SchemaError> {
    let mut current = schema;
    let mut name = field;
    let mut visited = HashSet::new();
    loop {
        let Some(mapper) = current
            .field_mappers
            .as_ref()
            .and_then(|mappers| mappers.get(name))
        else {
            return current
                .field_hashes
                .get(name)
                .filter(|identity| !identity.is_empty())
                .cloned()
                .ok_or_else(|| {
                    SchemaError::InvalidData("mapped canonical field identity is absent".into())
                });
        };
        if !visited.insert((current.name.clone(), name.to_owned())) {
            return Err(SchemaError::InvalidData(
                "cyclic field identity mapper".into(),
            ));
        }
        name = mapper.source_field();
        current = definitions
            .get(mapper.source_schema())
            .copied()
            .ok_or_else(|| {
                SchemaError::InvalidData("mapped canonical source schema is absent".into())
            })?;
    }
}
