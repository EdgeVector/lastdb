//! Exact tip records for an already selected page of molecule keys.

use crate::atom::{molecule_key_codec, AtomEntry, KeyMetadata};
use crate::schema::SchemaError;

use super::super::AtomStore;

impl AtomStore {
    /// Read current tip records in one batch and retain their selected order.
    pub(crate) async fn load_hash_range_page_records(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        page_keys: Vec<(String, String)>,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        if page_keys.is_empty() {
            return Ok(Vec::new());
        }
        let base_keys: Vec<String> = page_keys
            .iter()
            .map(|(hash, range)| {
                molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range)
            })
            .collect();
        let fetched = self
            .load_exact_per_key_records(storage_prefix, &base_keys)
            .await?;
        let candidates = base_keys
            .iter()
            .cloned()
            .zip(fetched)
            .filter_map(|(base_key, record)| record.map(|record| (base_key, record)))
            .collect();
        let mut visible = self
            .filter_delete_barriers_for_records(candidates, storage_prefix)
            .await?
            .into_iter()
            .peekable();
        let mut records = Vec::with_capacity(page_keys.len());
        for ((hash, range), base_key) in page_keys.into_iter().zip(base_keys) {
            if visible
                .peek()
                .is_some_and(|(visible_key, _)| visible_key == &base_key)
            {
                let (_, record) = visible.next().expect("checked visible record");
                records.push((hash, range, record.entry, record.meta));
            }
        }
        debug_assert!(visible.next().is_none(), "unmatched visible page record");
        Ok(records)
    }
}
