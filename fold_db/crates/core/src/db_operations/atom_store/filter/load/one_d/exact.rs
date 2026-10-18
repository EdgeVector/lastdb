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
        let hit = self
            .load_query_tip_records(scope, molecule, &[api_key])
            .await?
            .pop()
            .flatten();
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
        let mut api_keys = Vec::with_capacity(keys.len());
        for (hash, range) in keys {
            let (hash, range) = match slot {
                OneDSlot::Hash => (hash.as_str(), EMPTY_KEY_COMPONENT),
                OneDSlot::Range => (EMPTY_KEY_COMPONENT, range.as_str()),
            };
            api_keys.push(KeyValue::new(Some(hash.to_owned()), Some(range.to_owned())));
        }
        let hits = self
            .load_query_tip_records(target.storage_prefix, molecule, &api_keys)
            .await?;
        let mut records = Vec::with_capacity(keys.len());
        for (api_key, hit) in api_keys.into_iter().zip(hits) {
            if let Some((_, record)) = hit {
                records.push((
                    api_key.hash.unwrap_or_default(),
                    api_key.range.unwrap_or_default(),
                    record.entry,
                    record.meta,
                ));
            }
        }
        Ok(records)
    }
}
