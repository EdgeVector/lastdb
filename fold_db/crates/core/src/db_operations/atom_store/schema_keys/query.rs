//! Exact semantic reads resolve each typed key before one disk miss batch.

use super::*;

impl AtomStore {
    pub(crate) async fn load_query_tip_records(
        &self,
        scope: Option<&str>,
        molecule: &str,
        keys: &[KeyValue],
    ) -> Result<Vec<Option<TipRow>>, SchemaError> {
        let runtime = self.schema_keys_runtime();
        let codec = self.key_codec_for_molecule(molecule)?;
        let mut output = vec![None; keys.len()];
        let mut groups = Vec::new();
        let mut misses = Vec::new();
        for (at, key) in keys.iter().enumerate() {
            let known = runtime
                .as_ref()
                .map(|runtime| runtime.schema_key_tip(scope, molecule, key))
                .transpose()?
                .flatten();
            match known {
                Some(Some(record)) => {
                    let base = codec
                        .api_hash_range_record_key(
                            molecule,
                            key.hash.as_deref().unwrap_or_default(),
                            key.range.as_deref().unwrap_or_default(),
                        )
                        .map_err(key_error)?;
                    output[at] = Some((base, record));
                }
                Some(None) => {}
                None => {
                    groups.push(
                        codec
                            .api_hash_range_record_keys_for_read(
                                molecule,
                                key.hash.as_deref().unwrap_or_default(),
                                key.range.as_deref().unwrap_or_default(),
                            )
                            .map_err(key_error)?,
                    );
                    misses.push(at);
                }
            }
        }
        let fetched = self
            .load_first_present_candidate_with_key(scope, &groups)
            .await?;
        for ((at, candidates), hit) in misses.into_iter().zip(&groups).zip(fetched) {
            if let Some((base, record)) = &hit {
                self.note_selected_query_tip_candidates(
                    scope, molecule, &keys[at], record, base, candidates,
                )?;
            } else {
                self.note_missing_query_key(scope, molecule, &keys[at], candidates)?;
            }
            output[at] = hit;
        }
        Ok(output)
    }
}
