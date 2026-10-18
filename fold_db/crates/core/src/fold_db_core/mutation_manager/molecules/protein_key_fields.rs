//! Protein key coordinates come from the complete addressed record, in batches.

use super::super::MutationManager;
use super::protein_batch::{schema_storage_prefix, ProteinFoldMetadata};
use crate::atom::{Atom, AtomPartition};
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
        atoms: &[(usize, String, Atom)],
        eligible: &[(usize, &Atom, &str)],
        metadata: &ProteinFoldMetadata,
    ) -> Result<ProteinKeyFields, SchemaError> {
        let documents: HashMap<_, _> = atoms
            .iter()
            .filter(|(_, name, _)| crate::record_molecule::is_record_molecule_field(name))
            .map(|(idx, _, atom)| (*idx, atom))
            .collect();
        let indices: BTreeSet<_> = eligible.iter().map(|(idx, _, _)| *idx).collect();
        let mut required = required_fields(metadata, &indices);
        let names: BTreeSet<_> = required.iter().map(|(_, name)| name.clone()).collect();
        let mut before = HashMap::new();
        for &idx in &indices {
            before.insert(idx, addressed_fields(schema, &keys[idx]));
        }
        let scope = schema_storage_prefix(schema, None);
        let native = self.db_ops.namespaced_store().disk_pack_runtime().is_some();
        if native && !documents.is_empty() {
            if let Some(molecule) = schema.molecule_uuid.as_deref() {
                let old_keys: Vec<_> = documents.keys().map(|idx| keys[*idx].clone()).collect();
                let old = crate::record_molecule::load_record_tip_atoms(
                    &self.db_ops,
                    molecule,
                    &old_keys,
                    scope,
                    Some(schema),
                )
                .await?;
                for &idx in &indices {
                    if let Some(fields) = old.get(&keys[idx]).and_then(|atom| {
                        crate::record_molecule::parse_document_fields(atom.content())
                    }) {
                        for (name, value) in fields {
                            if !names.contains(name) {
                                continue;
                            }
                            if let Some(value) = scalar(value) {
                                before
                                    .get_mut(&idx)
                                    .expect("protein source key")
                                    .entry(name.clone())
                                    .or_insert(value);
                            }
                        }
                    }
                }
            }
        }
        if !native {
            required.retain(|(idx, name)| {
                !mutations[*idx].fields_and_values.contains_key(name)
                    && !documents
                        .get(idx)
                        .and_then(|atom| {
                            crate::record_molecule::parse_document_fields(atom.content())
                        })
                        .is_some_and(|fields| fields.contains_key(name))
            });
        }
        self.fill_protein_key_fields(schema, keys, &required, &mut before)
            .await?;
        let after = after_fields(
            schema, mutations, keys, &indices, &names, &documents, &before,
        );
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
            let Some((hash, range)) = field.disk_slot_for_key(&keys[*idx]) else {
                continue;
            };
            groups.push(
                self.db_ops
                    .atoms()
                    .key_codec_for_molecule(molecule)
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
                record.map(|(base, record)| {
                    (
                        request,
                        record.entry.atom_uuid,
                        AtomPartition::from_record_key(&base),
                    )
                })
            })
            .collect();
        let slots: Vec<_> = selected
            .iter()
            .map(|(_, uuid, partition)| (uuid.as_str(), partition.clone()))
            .collect();
        let atoms = self.db_ops.atoms().get_atoms_located(&slots, scope).await?;
        if atoms.len() != selected.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein key-field atom count differs".into(),
            });
        }
        for (((idx, name), _, _), atom) in selected.into_iter().zip(atoms) {
            let atom = atom.ok_or_else(|| SchemaError::StorageRead {
                detail: "protein key-field tip has no atom".into(),
            })?;
            if let Some(value) = scalar(atom.content()) {
                fields
                    .get_mut(&idx)
                    .expect("protein source key")
                    .insert(name.clone(), value);
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
    indices: &BTreeSet<usize>,
    names: &BTreeSet<String>,
    documents: &HashMap<usize, &Atom>,
    before: &KeyFields,
) -> KeyFields {
    let mut after = HashMap::new();
    let mut heads: HashMap<&KeyValue, HashMap<String, String>> = HashMap::new();
    for &idx in indices {
        // Later mutations inherit earlier private key changes. The BEFORE
        // map still describes the actual committed old coordinates, because
        // intermediate source tips can lose at final batch rebase.
        let mut fields = heads.get(&keys[idx]).unwrap_or(&before[&idx]).clone();
        let document = documents
            .get(&idx)
            .and_then(|atom| crate::record_molecule::parse_document_fields(atom.content()));
        let direct_document = mutations[idx].fields_and_values.len() == 1
            && mutations[idx]
                .fields_and_values
                .contains_key(crate::record_molecule::RECORD_SENTINEL);
        for name in names {
            let value = mutations[idx].fields_and_values.get(name).or_else(|| {
                direct_document
                    .then(|| document.and_then(|fields| fields.get(name)))
                    .flatten()
            });
            if let Some(value) = value {
                if let Some(value) = scalar(value) {
                    fields.insert(name.clone(), value);
                } else {
                    fields.remove(name);
                }
            }
        }
        fields.extend(addressed_fields(schema, &keys[idx]));
        heads.insert(&keys[idx], fields.clone());
        after.insert(idx, fields);
    }
    after
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

fn scalar(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}
