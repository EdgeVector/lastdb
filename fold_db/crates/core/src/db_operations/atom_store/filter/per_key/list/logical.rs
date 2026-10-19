//! The schema list reuses the same selected-tip and field-value warm contract.

use super::*;
use crate::atom::{AtomPartition, MoleculeKeyCodec};
use crate::schema::types::field::{field_value_from_atom, FieldValue, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::logical_paths::field;

impl AtomStore {
    pub(super) async fn list_field_values(
        &self,
        request: &KeyList<'_>,
        codec: &MoleculeKeyCodec,
        rows: &[(String, PerKeyRecord)],
    ) -> Result<Vec<Option<FieldValue>>, SchemaError> {
        let schema = matches!(request.membership, Membership::Schema { .. });
        let needs_bodies = schema
            || (request.hash.is_none()
                && codec.encoding() == crate::atom::HashKeyEncoding::BlindV1);
        let mut values = vec![None; rows.len()];
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
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let mut misses = Vec::new();
        for (index, ((base, record), key)) in rows.iter().zip(&keys).enumerate() {
            if schema {
                if let Some(key) = key {
                    let entry = listed_entry(key.clone(), base, record);
                    values[index] = field::lookup(
                        runtime.clone(),
                        request.scope,
                        Some(request.molecule),
                        &field::AtomSelection {
                            primary: self.primary_atom_key(
                                &entry.1,
                                entry.4.as_ref(),
                                request.scope,
                            ),
                            entry: &entry,
                        },
                    )?;
                }
            }
            if values[index].is_none() {
                misses.push(index);
            }
        }
        let slots: Vec<_> = misses
            .iter()
            .map(|&index| {
                (
                    rows[index].1.entry.atom_uuid.as_str(),
                    AtomPartition::from_record_key(&rows[index].0),
                )
            })
            .collect();
        let atoms = self
            .get_atoms_located(&slots, request.scope)
            .await
            .map_err(SchemaError::read_failure)?;
        for (index, atom) in misses.into_iter().zip(atoms) {
            let Some(atom) = atom else {
                if schema && request.scope.is_none() {
                    return Err(SchemaError::MissingLocalAtom);
                }
                continue;
            };
            let record = &rows[index].1;
            values[index] = Some(field_value_from_atom(
                &atom,
                record.entry.atom_uuid.clone(),
                record.meta.clone(),
                Some(record.entry.writer_pubkey.clone()),
            ));
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
            self.note_selected_query_tip(request.scope, request.molecule, key, expected, base)?;
        }
        Ok(())
    }

    pub(super) fn admit_listed_values(
        &self,
        request: &KeyList<'_>,
        rows: &[(String, PerKeyRecord)],
        keys: &[Option<KeyValue>],
        values: &[Option<FieldValue>],
    ) -> Result<(), SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        for (((base, _), key), value) in rows.iter().zip(keys).zip(values) {
            let (Some(key), Some(value)) = (key, value) else {
                continue;
            };
            field::admit(
                runtime.clone(),
                request.scope,
                Some(request.molecule),
                key,
                value,
                AtomPartition::from_record_key(base).as_ref(),
            )?;
        }
        Ok(())
    }
}

fn listed_entry(key: KeyValue, base: &str, record: &PerKeyRecord) -> KeyedAtomMatch {
    (
        key,
        record.entry.atom_uuid.clone(),
        record.meta.clone(),
        Some(record.entry.writer_pubkey.clone()),
        AtomPartition::from_record_key(base),
    )
}

fn list_tip_disagreement() -> SchemaError {
    SchemaError::StorageRead {
        detail: "listed molecule tip disagrees with its exact key".into(),
    }
}
