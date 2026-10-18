//! Exact query resolution retains every readable candidate before projection.

use super::*;
use crate::schema::types::key_value::KeyValue;

impl AtomStore {
    /// A Hash field always uses the empty range, including every read fallback.
    pub(crate) async fn get_query_hash_tip(
        &self,
        molecule: &str,
        hash: &str,
        scope: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        let api_key = KeyValue::new(Some(hash.to_owned()), None);
        let candidates = self
            .key_codec_for_molecule(molecule)
            .api_hash_range_record_keys_for_read(molecule, hash, EMPTY_KEY_COMPONENT)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let groups = [candidates];
        let hit = self
            .load_first_present_candidate_with_key(scope, &groups)
            .await?
            .pop()
            .flatten();
        if let Some((base, record)) = &hit {
            self.note_selected_query_tip_candidates(
                scope, molecule, &api_key, record, base, &groups[0],
            )?;
        } else {
            self.note_missing_query_key(scope, molecule, &api_key, &groups[0])?;
        }
        Ok(hit)
    }

    /// Use only the field's own dimension and echo that API key in the result.
    /// The winner resolver checks all spellings and final Delete barriers.
    pub(super) async fn load_1d_multi_get(
        &self,
        target: &OneDPageTarget<'_>,
        slot: OneDSlot,
        keys: &[(String, String)],
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        let molecule = target.molecule_uuid;
        let codec = self.key_codec_for_molecule(molecule);
        let mut groups = Vec::with_capacity(keys.len());
        let mut api_keys = Vec::with_capacity(keys.len());
        for (hash, range) in keys {
            let (hash, range) = match slot {
                OneDSlot::Hash => (hash.as_str(), EMPTY_KEY_COMPONENT),
                OneDSlot::Range => (EMPTY_KEY_COMPONENT, range.as_str()),
            };
            groups.push(
                codec
                    .api_hash_range_record_keys_for_read(molecule, hash, range)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
            );
            api_keys.push((hash.to_owned(), range.to_owned()));
        }
        let hits = self
            .load_first_present_candidate_with_key(target.storage_prefix, &groups)
            .await?;
        let mut records = Vec::with_capacity(keys.len());
        for (((hash, range), candidates), hit) in api_keys.into_iter().zip(&groups).zip(hits) {
            if matches!(slot, OneDSlot::Hash) {
                let api_key = KeyValue::new(Some(hash.clone()), None);
                if let Some((base, record)) = &hit {
                    self.note_selected_query_tip_candidates(
                        target.storage_prefix,
                        molecule,
                        &api_key,
                        record,
                        base,
                        candidates,
                    )?;
                } else {
                    self.note_missing_query_key(
                        target.storage_prefix,
                        molecule,
                        &api_key,
                        candidates,
                    )?;
                }
            }
            if let Some((_, record)) = hit {
                records.push((hash, range, record.entry, record.meta));
            }
        }
        Ok(records)
    }
}
