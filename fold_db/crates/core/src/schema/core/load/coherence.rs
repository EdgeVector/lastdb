//! Bind equal canonical fields from every schema into one durable protein.

use super::super::locks::read_map;
use super::super::{CoherenceBinding, SchemaCore};
use crate::atom::deterministic_molecule_uuid;
use crate::protein::ProteinMember;
use crate::schema::field_hash_coherence::KeyLayoutFingerprint;
use crate::schema::types::{Schema, SchemaError};
use std::collections::HashSet;

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
        let identities: HashSet<_> = loaded
            .field_hashes
            .values()
            .filter(|identity| !identity.is_empty())
            .map(String::as_str)
            .collect();
        let mut bindings = field_bindings(&loaded, &identities);
        {
            let schemas = read_map(&self.schemas, "schemas")?;
            let staged = self.staged_schemas()?;
            let committed = schemas.iter().filter(|(name, _)| {
                *name != schema_name && !staged.iter().any(|(staged_name, _)| staged_name == *name)
            });
            let staged = staged
                .iter()
                .filter(|(name, _)| name != schema_name)
                .map(|(name, cached)| (name, &cached.schema));
            for (_, peer) in committed.chain(staged) {
                bindings.extend(field_bindings(peer, &identities));
            }
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
        if self.coherence_memo(schema_name)?.as_ref() == Some(&bindings) {
            return Ok(());
        }
        if !bindings.is_empty() {
            self.db_ops.atoms().bind_field_proteins(&bindings).await?;
        }
        self.publish_coherence_memo(schema_name, bindings)
    }
}

fn field_bindings(schema: &Schema, wanted: &HashSet<&str>) -> Vec<CoherenceBinding> {
    let layout = KeyLayoutFingerprint::from_schema(schema);
    let Some(hash_field) = layout.hash_field.filter(|hash| !hash.is_empty()) else {
        return Vec::new();
    };
    schema
        .field_hashes
        .iter()
        .filter(|(_, identity)| wanted.contains(identity.as_str()))
        .map(|(field, identity)| CoherenceBinding {
            field_identity: identity.clone(),
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
        })
        .collect()
}
