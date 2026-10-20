//! Certify the source's declared key fields at their selected atom locations.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn source_coordinates_match(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<bool>, SchemaError> {
        let mut valid = vec![true; keys.len()];
        let Some(layout) = schema.key.as_ref() else {
            return Ok(vec![false; keys.len()]);
        };
        let mut requests = Vec::new();
        let mut groups = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let mut count = 0;
            for (name, coordinate) in [
                (&layout.hash_field, &key.hash),
                (&layout.range_field, &key.range),
            ] {
                let Some(name) = name.as_ref() else {
                    continue;
                };
                count += 1;
                let Some((field, coordinate)) =
                    schema.runtime_fields.get(name).zip(coordinate.as_ref())
                else {
                    valid[index] = false;
                    continue;
                };
                let Some(molecule) = field.common().molecule_uuid() else {
                    valid[index] = false;
                    continue;
                };
                let Some((hash, range)) = field.slot_coordinates(key) else {
                    valid[index] = false;
                    continue;
                };
                groups.push(
                    self.db_ops
                        .atoms()
                        .key_codec_for_molecule(molecule)?
                        .api_hash_range_record_keys_for_read(molecule, &hash, &range)
                        .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
                );
                requests.push((index, coordinate));
            }
            valid[index] &= count > 0;
        }
        let selected = self
            .db_ops
            .atoms()
            .load_first_present_candidate_with_key(storage_prefix, &groups)
            .await?;
        if selected.len() != requests.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein source-coordinate tip count differs".into(),
            });
        }
        let selected: Vec<_> = requests
            .into_iter()
            .zip(selected)
            .map(|((index, coordinate), record)| {
                if let Some((_, record)) = record {
                    Ok(Some((index, coordinate, record.entry.atom_uuid)))
                } else {
                    valid[index] = false;
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, SchemaError>>()?
            .into_iter()
            .flatten()
            .collect();
        let tips: Vec<_> = selected.iter().map(|(_, _, uuid)| uuid.as_str()).collect();
        let atoms = crate::db_operations::tip_atoms::get_atoms_for_tips(
            &self.db_ops,
            &tips,
            storage_prefix,
        )
        .await?;
        for ((index, coordinate, _), atom) in selected.into_iter().zip(atoms) {
            valid[index] &=
                scalar_key_coordinate(atom.content()).as_deref() == Some(coordinate.as_str());
        }
        Ok(valid)
    }
}
