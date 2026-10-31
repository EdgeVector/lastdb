//! Confirm the exact key is absent before retaining a key miss.

use super::*;
use std::collections::HashMap;

impl<S: SchemaKeyPorts> SchemaKeys<'_, S> {
    pub(super) async fn retain_missing_preloaded_keys(
        &self,
        scope: Option<&str>,
        keys: &[ReadyTipKey],
        missing: &[String],
        known: &HashMap<String, PerKeyRecord>,
    ) -> Result<(), SchemaError> {
        let mut requested = Vec::new();
        let mut slot_keys = Vec::new();
        for key in keys {
            if missing.binary_search(&key.base).is_err() || known.contains_key(&key.base) {
                continue;
            }
            let Some(api) = &key.api_key else { continue };
            let base = self
                .store
                .key_codec_for_molecule(&key.molecule)?
                .api_hash_range_record_key(
                    &key.molecule,
                    api.hash.as_deref().unwrap_or_default(),
                    api.range.as_deref().unwrap_or_default(),
                )
                .map_err(key_error)?;
            if base != key.base {
                continue;
            }
            requested.push(key);
            slot_keys.push(base);
        }
        if requested.is_empty() {
            return Ok(());
        }
        let records = self
            .store
            .load_visible_per_key_records(scope, &slot_keys)
            .await?;
        for ((key, base), record) in requested.into_iter().zip(slot_keys).zip(records) {
            if record.is_none() {
                self.note_missing_query_key(
                    scope,
                    &key.molecule,
                    key.api_key.as_ref().expect("selected API key"),
                    &base,
                )?;
            }
        }
        Ok(())
    }
}
