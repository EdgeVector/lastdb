//! One exact fetch and one delete-barrier batch for all unknown selected tips.

use super::*;
use std::collections::HashMap;

impl AtomStore {
    /// Native narrowed molecules project their API keys after this location
    /// receipt. A full receipt cannot silently become a UUID-only body lookup.
    pub(crate) fn note_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
    ) -> Result<(), SchemaError> {
        let runtime = self.schema_keys_runtime();
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(());
        }
        if runtime
            .as_ref()
            .map(|runtime| runtime.schema_key_tip_for_record(scope, molecule, api_key, record))
            .transpose()?
            .unwrap_or(false)
        {
            return Ok(());
        }
        if self.has_selected_query_tip(scope, molecule, api_key, record)? {
            return Ok(());
        }
        // A range row has a physical location without a point-read source.
        // Keep that location first; candidates cannot grant missing authority.
        self.retain_selected_query_tip(scope, molecule, api_key, record, base)?;
        let sourced = crate::storage::disk_pack::logical_paths::core_read::selected_tip(
            runtime.clone(),
            scope,
            molecule,
            api_key,
            &record.entry.atom_uuid,
        )?
        .is_some_and(|selected| {
            selected.entry == record.entry
                && selected.metadata == record.meta
                && selected.tip_source.is_some()
        });
        if !sourced {
            return Ok(());
        }
        let candidates = self
            .key_codec_for_molecule(molecule)
            .api_hash_range_record_keys_for_read(
                molecule,
                api_key.hash.as_deref().unwrap_or_default(),
                api_key.range.as_deref().unwrap_or_default(),
            )
            .map_err(key_error)?;
        crate::storage::disk_pack::logical_paths::core_read::note_candidates(
            runtime,
            scope,
            molecule,
            api_key,
            &record.entry.atom_uuid,
            &candidates,
            self.atom_key_encoding(),
        )?;
        Ok(())
    }

    pub(in crate::db_operations::atom_store) fn note_selected_query_tip_candidates(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
        candidates: &[String],
    ) -> Result<(), SchemaError> {
        if self
            .schema_keys_runtime()
            .as_ref()
            .map(|runtime| runtime.schema_key_tip_for_record(scope, molecule, api_key, record))
            .transpose()?
            .unwrap_or(false)
        {
            return Ok(());
        }
        if self.has_selected_query_tip(scope, molecule, api_key, record)? {
            return Ok(());
        }
        self.retain_selected_query_tip(scope, molecule, api_key, record, base)?;
        crate::storage::disk_pack::logical_paths::core_read::note_candidates(
            self.schema_keys_runtime(),
            scope,
            molecule,
            api_key,
            &record.entry.atom_uuid,
            candidates,
            self.atom_key_encoding(),
        )?;
        Ok(())
    }

    /// A reconstructed API key must not replace the already selected atom copy.
    fn has_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
    ) -> Result<bool, SchemaError> {
        use crate::storage::disk_pack::logical_paths::core_read;
        let runtime = self.schema_keys_runtime();
        let Some(selected) = core_read::selected_tip(
            runtime.clone(),
            scope,
            molecule,
            api_key,
            &record.entry.atom_uuid,
        )?
        else {
            return Ok(false);
        };
        if selected.entry != record.entry
            || selected.metadata != record.meta
            || (selected.sources.is_none() && selected.tip_source.is_some())
        {
            return Ok(false);
        }
        Ok(!self.atom_key_encoding().writes_partition_prefix()
            || core_read::selected_partition(
                runtime,
                scope,
                molecule,
                api_key,
                &record.entry.atom_uuid,
            )?
            .is_some())
    }

    /// Location-only rows can reuse bodies without direct-tip authority.
    fn retain_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
    ) -> Result<(), SchemaError> {
        let runtime = self.schema_keys_runtime();
        let required = self.atom_key_encoding().writes_partition_prefix()
            && crate::storage::disk_pack::read_snapshot_active(runtime.clone());
        let key = crate::schema::types::field::build_storage_key(scope, base);
        let retained = crate::storage::disk_pack::logical_paths::core_read::note_winning_tip(
            runtime,
            scope,
            molecule,
            api_key,
            &record.entry,
            record.meta.as_ref(),
            &key,
        )?;
        if required && !retained {
            return Err(SchemaError::ReadResourceLimit {
                detail: "native query cannot retain its selected atom location".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn note_missing_query_key(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        candidates: &[String],
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::logical_paths::absence::admit(
            self.schema_keys_runtime(),
            scope,
            molecule,
            api_key,
            candidates,
        )?;
        Ok(())
    }

    pub(super) async fn resolve_preloaded_tip_keys(
        &self,
        runtime: &Arc<PackRuntime>,
        scope: Option<&str>,
        keys: Vec<ReadyTipKey>,
    ) -> Result<Vec<TipRow>, SchemaError> {
        let mut known = HashMap::new();
        let mut missing = Vec::new();
        for key in &keys {
            let primary = key
                .api_key
                .as_ref()
                .map(|api_key| {
                    self.key_codec_for_molecule(&key.molecule)
                        .api_hash_range_record_key(
                            &key.molecule,
                            api_key.hash.as_deref().unwrap_or_default(),
                            api_key.range.as_deref().unwrap_or_default(),
                        )
                        .map_err(key_error)
                })
                .transpose()?;
            let warm = match &key.api_key {
                Some(api_key) if primary.as_deref() == Some(key.base.as_str()) => runtime
                    .schema_key_tip(scope, &key.molecule, api_key)
                    .map_err(SchemaError::from)?,
                _ => None,
            };
            match warm {
                Some(Some(record)) => {
                    known.insert(key.base.clone(), record);
                }
                Some(None) => {}
                None => missing.push(key.base.clone()),
            }
        }
        missing.sort();
        missing.dedup();
        let records = self.load_exact_per_key_records(scope, &missing).await?;
        let rows = missing
            .iter()
            .cloned()
            .zip(records)
            .filter_map(|(base, record)| record.map(|record| (base, record)))
            .collect();
        known.extend(self.filter_delete_barriers_for_records(rows, scope).await?);
        self.retain_missing_preloaded_keys(scope, &keys, &missing, &known)
            .await?;
        Ok(keys
            .into_iter()
            .filter_map(|key| {
                known
                    .get(&key.base)
                    .cloned()
                    .map(|record| (key.base, record))
            })
            .collect())
    }
}
