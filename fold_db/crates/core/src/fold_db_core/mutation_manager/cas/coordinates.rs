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
        let profile = self.db_ops.molecule_keys().schema_profile(schema)?;
        let mut requests = Vec::new();
        let mut slot_keys = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let mut count = 0;
            for (name, coordinate, is_range) in [
                (&layout.hash_field, &key.hash, false),
                (&layout.range_field, &key.range, true),
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
                slot_keys.push(
                    self.db_ops
                        .atoms()
                        .key_codec_for_molecule(molecule)?
                        .api_hash_range_record_key(molecule, &hash, &range)
                        .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
                );
                requests.push((index, coordinate, is_range));
            }
            valid[index] &= count > 0;
        }
        let selected = self
            .db_ops
            .atoms()
            .load_visible_per_key_records(storage_prefix, &slot_keys)
            .await?;
        if selected.len() != requests.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein source-coordinate tip count differs".into(),
            });
        }
        let selected: Vec<_> = requests
            .into_iter()
            .zip(selected)
            .map(|((index, coordinate, is_range), record)| {
                if let Some(record) = record {
                    Ok(Some((index, coordinate, is_range, record.entry.atom_uuid)))
                } else {
                    valid[index] = false;
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, SchemaError>>()?
            .into_iter()
            .flatten()
            .collect();
        let tips: Vec<_> = selected
            .iter()
            .map(|(_, _, _, uuid)| uuid.as_str())
            .collect();
        let atoms = crate::db_operations::tip_atoms::get_atoms_for_tips(
            &self.db_ops,
            &tips,
            storage_prefix,
        )
        .await?;
        for ((index, coordinate, is_range, _), atom) in selected.into_iter().zip(atoms) {
            valid[index] &= coordinate_matches(profile, coordinate, is_range, atom.content())?;
        }
        Ok(valid)
    }
}

fn coordinate_matches(
    profile: Option<schema_types::molecule_range_profile::MoleculeRangeProfile>,
    coordinate: &str,
    is_range: bool,
    value: &serde_json::Value,
) -> Result<bool, SchemaError> {
    let Some(value) = scalar_key_coordinate(value) else {
        return Ok(false);
    };
    if !is_range {
        return Ok(value == coordinate);
    }
    use crate::schema::types::key_comparison::normalize::canonical_range;
    Ok(canonical_range(profile, &value)? == canonical_range(profile, coordinate)?)
}
