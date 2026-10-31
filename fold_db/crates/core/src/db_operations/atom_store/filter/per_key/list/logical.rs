//! The schema list reuses the same selected-tip and field-value warm contract.

use super::*;
use crate::atom::MoleculeKeyCodec;
use crate::schema::types::field::{field_value_from_atom, FieldValue, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::logical_paths::field;

pub(super) struct ListedValue {
    pub(super) field: FieldValue,
    // A cold decode remains owned here until schema membership selects its key.
    // A warm hit already belongs to the core and needs no second body owner.
    atom: Option<crate::atom::Atom>,
}

impl<S: FilterPorts> Filter<'_, S> {
    pub(super) async fn list_field_values(
        &self,
        request: &KeyList<'_>,
        codec: &MoleculeKeyCodec,
        rows: &[(String, PerKeyRecord)],
    ) -> Result<Vec<Option<ListedValue>>, SchemaError> {
        let schema = matches!(request.membership, Membership::Schema(_));
        // A blinded hash needs the key value to recover its API coordinate.
        let needs_bodies = schema || request.hash.is_none();
        let mut values: Vec<_> = (0..rows.len()).map(|_| None).collect();
        if !needs_bodies {
            return Ok(values);
        }
        let keys = rows
            .iter()
            .map(|(base, _)| {
                request
                    .hash
                    .map(|hash| {
                        values::decoded_key(request, codec, base)
                            .map(|(_, range)| KeyValue::new(Some(hash.to_owned()), Some(range)))
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        if schema && request.hash.is_some() {
            self.certify_listed_tips(request, rows, &keys).await?;
        }
        let runtime = self.store.native_runtime();
        let mut misses = Vec::new();
        for (index, ((_, record), key)) in rows.iter().zip(&keys).enumerate() {
            if schema {
                if let Some(key) = key {
                    let entry = listed_entry(key.clone(), record);
                    values[index] = field::lookup(
                        runtime.clone(),
                        request.scope,
                        Some(request.molecule),
                        &field::AtomSelection {
                            primary: self.store.body_storage_key(&entry.1, request.scope),
                            entry: &entry,
                        },
                    )?
                    .map(|field| ListedValue { field, atom: None });
                }
            }
            if values[index].is_none() {
                misses.push(index);
            }
        }
        let ids: Vec<&str> = misses
            .iter()
            .map(|&index| rows[index].1.entry.atom_uuid.as_str())
            .collect();
        let atoms = self
            .store
            .get_atoms(&ids, request.scope)
            .await
            .map_err(SchemaError::read_failure)?;
        let atoms = if schema {
            crate::db_operations::tip_atoms::require_tip_atoms(atoms, ids.len())?
                .into_iter()
                .map(Some)
                .collect()
        } else {
            atoms
        };
        for (index, atom) in misses.into_iter().zip(atoms) {
            let Some(atom) = atom else {
                continue;
            };
            let record = &rows[index].1;
            values[index] = Some(ListedValue {
                field: field_value_from_atom(
                    &atom,
                    record.entry.atom_uuid.clone(),
                    record.meta.clone(),
                    Some(record.entry.writer_pubkey().to_string()),
                ),
                atom: schema.then_some(atom),
            });
        }
        Ok(values)
    }

    pub(super) async fn certify_listed_tips(
        &self,
        request: &KeyList<'_>,
        rows: &[(String, PerKeyRecord)],
        keys: &[Option<KeyValue>],
    ) -> Result<(), SchemaError> {
        let requested: Vec<_> = keys.iter().filter_map(Clone::clone).collect();
        let selected = self
            .store
            .load_query_tip_records(request.scope, request.molecule, &requested)
            .await?;
        let mut selected = selected.into_iter();
        for ((base, expected), key) in rows.iter().zip(keys) {
            let Some(key) = key else { continue };
            let Some((_, actual)) = selected.next().flatten() else {
                return Err(list_tip_disagreement());
            };
            if actual.entry != expected.entry || actual.meta != expected.meta {
                return Err(list_tip_disagreement());
            }
            self.store.note_selected_query_tip(
                request.scope,
                request.molecule,
                key,
                expected,
                base,
            )?;
        }
        Ok(())
    }

    pub(super) fn admit_listed_values(
        &self,
        request: &KeyList<'_>,
        keys: &[Option<KeyValue>],
        values: Vec<Option<ListedValue>>,
    ) -> Result<(), SchemaError> {
        let runtime = self.store.native_runtime();
        for (key, value) in keys.iter().zip(values) {
            let (Some(key), Some(value)) = (key, value) else {
                continue;
            };
            let Some(atom) = value.atom else { continue };
            field::admit(
                runtime.clone(),
                request.scope,
                Some(request.molecule),
                key,
                atom,
            )?;
        }
        Ok(())
    }
}

fn listed_entry(key: KeyValue, record: &PerKeyRecord) -> KeyedAtomMatch {
    (
        key,
        record.entry.atom_uuid.clone(),
        record.meta.clone(),
        Some(record.entry.writer_pubkey().to_string()),
    )
}

fn list_tip_disagreement() -> SchemaError {
    SchemaError::StorageRead {
        detail: "listed molecule tip disagrees with its exact key".into(),
    }
}
