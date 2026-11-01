//! The key values of listed tips, read to recover a blinded API hash.

use super::*;
use crate::schema::types::field::{field_value_from_atom, FieldValue};

pub(super) struct ListedValue {
    pub(super) field: FieldValue,
}

impl AtomStore {
    pub(super) async fn list_field_values(
        &self,
        request: &KeyList<'_>,
        rows: &[(String, PerKeyRecord)],
    ) -> Result<Vec<Option<ListedValue>>, SchemaError> {
        // A blinded hash needs the key value to recover its API coordinate.
        if request.hash.is_some() {
            return Ok((0..rows.len()).map(|_| None).collect());
        }
        let ids: Vec<&str> = rows
            .iter()
            .map(|(_, record)| record.entry.atom_uuid.as_str())
            .collect();
        let atoms = self
            .get_atoms(&ids, request.scope)
            .await
            .map_err(SchemaError::read_failure)?;
        Ok(rows
            .iter()
            .zip(atoms)
            .map(|((_, record), atom)| {
                atom.map(|atom| ListedValue {
                    field: field_value_from_atom(
                        &atom,
                        record.entry.atom_uuid.clone(),
                        record.meta.clone(),
                        Some(record.entry.writer_pubkey().to_string()),
                    ),
                })
            })
            .collect())
    }
}
