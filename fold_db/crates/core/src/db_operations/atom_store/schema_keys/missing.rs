//! Confirm absence across every readable spelling before retaining a key miss.

use super::*;
use std::collections::HashMap;

impl AtomStore {
    pub(super) async fn retain_missing_preloaded_keys(
        &self,
        scope: Option<&str>,
        keys: &[ReadyTipKey],
        missing: &[String],
        known: &HashMap<String, PerKeyRecord>,
    ) -> Result<(), SchemaError> {
        let mut requested = Vec::new();
        let mut groups = Vec::new();
        for key in keys {
            if missing.binary_search(&key.base).is_err() || known.contains_key(&key.base) {
                continue;
            }
            let Some(api) = &key.api_key else { continue };
            let group = self
                .key_codec_for_molecule(&key.molecule)?
                .api_hash_range_record_keys_for_read(
                    &key.molecule,
                    api.hash.as_deref().unwrap_or_default(),
                    api.range.as_deref().unwrap_or_default(),
                )
                .map_err(key_error)?;
            if group.first() != Some(&key.base) {
                continue;
            }
            requested.push(key);
            groups.push(group);
        }
        if requested.is_empty() {
            return Ok(());
        }
        let records = self.load_first_present_candidate(scope, &groups).await?;
        for ((key, candidates), record) in requested.into_iter().zip(groups).zip(records) {
            if record.is_none() {
                self.note_missing_query_key(
                    scope,
                    &key.molecule,
                    key.api_key.as_ref().expect("selected API key"),
                    &candidates,
                )?;
            }
        }
        Ok(())
    }
}
