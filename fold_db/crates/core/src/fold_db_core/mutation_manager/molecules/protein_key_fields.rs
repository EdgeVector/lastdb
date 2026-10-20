//! Protein key coordinates come from the complete addressed record, in batches.

use super::super::MutationManager;
use super::protein_batch::{schema_storage_prefix, ProteinFoldMetadata};
use crate::atom::Atom;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use std::collections::{BTreeSet, HashMap};

pub(super) type KeyFields = HashMap<usize, HashMap<String, String>>;

pub(super) struct ProteinKeyFields {
    pub(super) before: KeyFields,
    pub(super) after: KeyFields,
}

impl MutationManager {
    pub(super) async fn protein_key_fields(
        &self,
        schema: &Schema,
        mutations: &[Mutation],
        keys: &[KeyValue],
        eligible: &[(usize, &Atom, &str)],
        metadata: &ProteinFoldMetadata,
    ) -> Result<ProteinKeyFields, SchemaError> {
        let indices: BTreeSet<_> = eligible.iter().map(|(idx, _, _)| *idx).collect();
        let required = required_fields(metadata, &indices);
        let names: BTreeSet<_> = required.iter().map(|(_, name)| name.clone()).collect();
        let mut before = HashMap::new();
        for &idx in &indices {
            before.insert(idx, addressed_fields(schema, &keys[idx]));
        }
        self.fill_protein_key_fields(schema, keys, &required, &mut before)
            .await?;
        let after = after_fields(schema, mutations, keys, eligible, &names, &before);
        Ok(ProteinKeyFields { before, after })
    }

    async fn fill_protein_key_fields(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        required: &BTreeSet<(usize, String)>,
        fields: &mut KeyFields,
    ) -> Result<(), SchemaError> {
        let mut requests = Vec::new();
        let mut groups = Vec::new();
        for (idx, name) in required {
            if fields[idx].contains_key(name) {
                continue;
            }
            let Some(field) = schema.runtime_fields.get(name) else {
                continue;
            };
            let Some(molecule) = field.common().molecule_uuid() else {
                continue;
            };
            let Some((hash, range)) = field.slot_coordinates(&keys[*idx]) else {
                continue;
            };
            groups.push(
                self.db_ops
                    .atoms()
                    .key_codec_for_molecule(molecule)?
                    .api_hash_range_record_keys_for_read(molecule, &hash, &range)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
            );
            requests.push((*idx, name));
        }
        if requests.is_empty() {
            return Ok(());
        }
        let scope = schema_storage_prefix(schema, None);
        let records = self
            .db_ops
            .atoms()
            .load_first_present_candidate_with_key(scope, &groups)
            .await?;
        if records.len() != requests.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein key-field winner count differs".into(),
            });
        }
        let selected: Vec<_> = requests
            .into_iter()
            .zip(records)
            .filter_map(|(request, record)| {
                record.map(|(_, record)| (request, record.entry.atom_uuid))
            })
            .collect();
        let uuids: Vec<&str> = selected.iter().map(|(_, uuid)| uuid.as_str()).collect();
        let atoms = self.db_ops.atoms().get_atoms(&uuids, scope).await?;
        if atoms.len() != selected.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein key-field atom count differs".into(),
            });
        }
        for (((idx, name), _), atom) in selected.into_iter().zip(atoms) {
            let atom = atom.ok_or_else(|| SchemaError::StorageRead {
                detail: "protein key-field tip has no atom".into(),
            })?;
            if let Some(value) = scalar_key_coordinate(atom.content()) {
                fields
                    .get_mut(&idx)
                    .expect("protein source key")
                    .insert(name.clone(), value.into_owned());
            }
        }
        Ok(())
    }
}

fn addressed_fields(schema: &Schema, key: &KeyValue) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    if let Some(layout) = &schema.key {
        for (name, value) in [
            (&layout.hash_field, &key.hash),
            (&layout.range_field, &key.range),
        ] {
            if let (Some(name), Some(value)) = (name, value) {
                fields.insert(name.clone(), value.clone());
            }
        }
    }
    fields
}

fn after_fields(
    schema: &Schema,
    mutations: &[Mutation],
    keys: &[KeyValue],
    eligible: &[(usize, &Atom, &str)],
    names: &BTreeSet<String>,
    before: &KeyFields,
) -> KeyFields {
    let indices: BTreeSet<_> = eligible.iter().map(|(idx, _, _)| *idx).collect();
    let mut heads: HashMap<&KeyValue, HashMap<String, String>> = HashMap::new();
    for &idx in &indices {
        let fields = heads
            .entry(&keys[idx])
            .or_insert_with(|| before[&idx].clone());
        // Payload-only names are routing inputs, not writable source fields.
        // Registered fields below must use their accepted atom instead.
        for name in names {
            if !schema.runtime_fields.contains_key(name) {
                if let Some(value) = mutations[idx].fields_and_values.get(name) {
                    set_coordinate(fields, name, value);
                }
            }
        }
    }
    let mut names_by_molecule: HashMap<&str, Vec<&String>> = HashMap::new();
    for name in names {
        if let Some(uuid) = schema
            .runtime_fields
            .get(name)
            .and_then(|field| field.common().molecule_uuid())
        {
            names_by_molecule.entry(uuid).or_default().push(name);
        }
    }
    // All winning fields for one addressed key share its final coordinates.
    // An earlier winning field must not route through an intermediate key.
    for &(idx, atom, molecule) in eligible {
        let fields = heads.get_mut(&keys[idx]).expect("protein source key");
        for name in names_by_molecule.get(molecule).into_iter().flatten() {
            set_coordinate(fields, name, atom.content());
        }
    }
    indices
        .into_iter()
        .map(|idx| {
            let mut fields = heads[&keys[idx]].clone();
            fields.extend(addressed_fields(schema, &keys[idx]));
            (idx, fields)
        })
        .collect()
}

fn set_coordinate(fields: &mut HashMap<String, String>, name: &str, value: &serde_json::Value) {
    if let Some(value) = scalar_key_coordinate(value) {
        fields.insert(name.to_owned(), value.into_owned());
    } else {
        fields.remove(name);
    }
}

pub(super) fn member_ids(schema: &Schema) -> Vec<&str> {
    schema
        .runtime_fields
        .values()
        .filter_map(|field| field.common().molecule_uuid().map(String::as_str))
        .collect()
}

fn required_fields(
    metadata: &ProteinFoldMetadata,
    indices: &BTreeSet<usize>,
) -> BTreeSet<(usize, String)> {
    let names: BTreeSet<_> = metadata
        .1
        .values()
        .flatten()
        .flat_map(|protein| protein.members.iter())
        .flat_map(|member| std::iter::once(&member.hash_field).chain(member.range_field.as_ref()))
        .cloned()
        .collect();
    indices
        .iter()
        .flat_map(|idx| names.iter().map(move |name| (*idx, name.clone())))
        .collect()
}
