//! Exact point probe of one receipt-named tip in a dropped schema.

use super::super::helpers::schema_index_codec;
use super::super::PerKeyRecord;
use super::{AtomStore, DroppedSchemaTipProbe};
use crate::db_operations::key_fingerprint;
use crate::kind_partition::form_twin;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

impl AtomStore {
    /// Point-read one receipt-named `mk:` tip for a clone pilot proof.
    /// The caller must confirm that the schema is dropped and the receipt
    /// contains this molecule before it calls this method.
    pub async fn probe_dropped_schema_tip(
        &self,
        schema_name: &str,
        molecule_uuid: &str,
        key_hash: &str,
        key_range: &str,
        expected_key_fingerprint: Option<&str>,
    ) -> Result<DroppedSchemaTipProbe, SchemaError> {
        let keys = self
            .key_codec_for_molecule(molecule_uuid)?
            .api_hash_range_record_keys_for_read(molecule_uuid, key_hash, key_range)
            .map_err(|error| SchemaError::InvalidField(format!("encode probe key: {error}")))?;
        if keys.is_empty() || keys.len() > 16 {
            return Err(SchemaError::InvalidData(
                "exact probe resolved no key or more than 16 key forms".to_string(),
            ));
        }
        let fingerprints: Vec<String> = keys
            .iter()
            .map(|key| key_fingerprint(schema_name, key))
            .collect();
        if let Some(expected) = expected_key_fingerprint {
            if !fingerprints
                .iter()
                .any(|fingerprint| fingerprint == expected)
            {
                return Err(SchemaError::InvalidField(
                    "expected key fingerprint does not match this exact probe key".to_string(),
                ));
            }
        }
        let found = self
            .find_single_tip(&keys, &fingerprints, expected_key_fingerprint)
            .await?;
        let selected_index = found
            .as_ref()
            .map(|(index, _)| *index)
            .or_else(|| {
                expected_key_fingerprint.and_then(|expected| {
                    fingerprints
                        .iter()
                        .position(|fingerprint| fingerprint == expected)
                })
            })
            .unwrap_or(0);
        let mut result = DroppedSchemaTipProbe {
            schema: schema_name.to_string(),
            molecule_uuid: molecule_uuid.to_string(),
            key_fingerprint: fingerprints[selected_index].clone(),
            candidate_fingerprints: fingerprints,
            tip_present: found.is_some(),
            atom_body_present: None,
            atom_source_schema_matches: None,
            schema_index_present: None,
            protein_bound: self.protein_of_molecule(molecule_uuid).await?.is_some(),
            ref_edges_present_conservative: self
                .has_any_molecule_ref_edges(molecule_uuid, None)
                .await?,
            molecule_edges_complete: self.molecule_ref_edges_complete(None).await?,
        };
        if let Some((_, record)) = found {
            self.fill_atom_evidence(&mut result, schema_name, &record.entry.atom_uuid)
                .await?;
        }
        Ok(result)
    }

    /// Read every key form and return the one that holds the tip. More than
    /// one holder, or a holder that differs from the expected fingerprint, is
    /// an error.
    async fn find_single_tip(
        &self,
        keys: &[String],
        fingerprints: &[String],
        expected_key_fingerprint: Option<&str>,
    ) -> Result<Option<(usize, PerKeyRecord)>, SchemaError> {
        let mut found = None;
        for (index, key) in keys.iter().enumerate() {
            let record = self
                .main_store
                .get_item::<PerKeyRecord>(key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("point-read dropped tip: {error}"))
                })?;
            if let Some(record) = record {
                if found.is_some() {
                    return Err(SchemaError::InvalidData(
                        "multiple storage forms hold this exact tip".to_string(),
                    ));
                }
                found = Some((index, record));
            }
        }
        if let (Some(expected), Some((index, _))) = (expected_key_fingerprint, &found) {
            if fingerprints[*index] != expected {
                return Err(SchemaError::InvalidField(
                    "a different storage form holds this exact tip".to_string(),
                ));
            }
        }
        Ok(found)
    }

    /// Record whether the tip's atom body and schema index marker are present.
    async fn fill_atom_evidence(
        &self,
        result: &mut DroppedSchemaTipProbe,
        schema_name: &str,
        atom_uuid: &str,
    ) -> Result<(), SchemaError> {
        let atom = self.get_atom_by_uuid(atom_uuid, None).await?;
        result.atom_body_present = Some(atom.is_some());
        result.atom_source_schema_matches = atom
            .as_ref()
            .map(|atom| atom.source_schema_name() == schema_name);
        let index_key = build_storage_key(
            None,
            &schema_index_codec::record_key(schema_name, atom_uuid),
        );
        let mut index_forms = vec![index_key.clone()];
        if let Some(twin) = form_twin(&index_key) {
            if twin != index_key {
                index_forms.push(twin);
            }
        }
        let mut index_present = false;
        for form in index_forms {
            index_present |= self
                .schema_index_store
                .inner()
                .exists(form.as_bytes())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("point-read dropped schema index: {error}"))
                })?;
        }
        result.schema_index_present = Some(index_present);
        Ok(())
    }
}
