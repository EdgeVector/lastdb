//! Captured typed changes join disk membership before counts and windows.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

type Changes = BTreeMap<String, Option<PerKeyRecord>>;

impl AtomStore {
    pub(crate) fn merge_captured_exact_tips(
        &self,
        scope: Option<&str>,
        keys: &[String],
        molecules: BTreeMap<&str, Vec<usize>>,
        records: &mut [Option<PerKeyRecord>],
    ) -> Result<(), SchemaError> {
        for (molecule, slots) in molecules {
            let changes = self.captured_tip_changes(scope, molecule)?;
            for slot in slots {
                if let Some(value) = changes.get(&keys[slot]) {
                    records[slot] = value.clone();
                }
            }
        }
        Ok(())
    }

    pub(crate) fn captured_tip_changes(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<Changes, SchemaError> {
        let Some(model) = self
            .schema_keys_runtime()
            .and_then(|runtime| runtime.captured_core_model())
        else {
            return Ok(BTreeMap::new());
        };
        if model.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut changes = BTreeMap::new();
        for uid in crate::atom::molecule_uuid_read_candidates(molecule) {
            let codec = self.key_codec_for_molecule(&uid);
            for (hash, range, absent) in model.changes(scope, &uid) {
                let candidates = codec
                    .api_hash_range_record_keys_for_read(&uid, &hash, &range)
                    .map_err(key_error)?;
                for candidate in candidates {
                    changes.insert(candidate, None);
                }
                if absent {
                    continue;
                }
                let Some(value) = model.lookup_key(scope, &uid, &hash, &range).into_tip() else {
                    continue;
                };
                let primary = codec
                    .api_hash_range_record_key(&uid, &hash, &range)
                    .map_err(key_error)?;
                changes.insert(
                    primary,
                    Some(PerKeyRecord {
                        entry: value.entry,
                        meta: value.metadata,
                    }),
                );
            }
        }
        Ok(changes)
    }

    pub(crate) fn merge_captured_tip_prefixes(
        &self,
        scope: Option<&str>,
        prefixes: &[String],
        rows: Vec<TipRow>,
    ) -> Result<Vec<TipRow>, SchemaError> {
        let Some(molecule) = prefixes
            .first()
            .and_then(|prefix| molecule_key_codec::molecule_uuid_from_storage_key(prefix))
        else {
            return Ok(rows);
        };
        let changes = self.captured_tip_changes(scope, molecule)?;
        Ok(merge_rows(
            rows,
            changes
                .into_iter()
                .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix))),
        ))
    }

    pub(crate) fn merge_captured_tip_range(
        &self,
        scope: Option<&str>,
        start: &str,
        end: &str,
        rows: Vec<TipRow>,
    ) -> Result<Vec<TipRow>, SchemaError> {
        let Some(molecule) = molecule_key_codec::molecule_uuid_from_storage_key(start) else {
            return Ok(rows);
        };
        let changes = self.captured_tip_changes(scope, molecule)?;
        Ok(merge_rows(
            rows,
            changes
                .into_iter()
                .filter(|(key, _)| key.as_str() >= start && key.as_str() < end),
        ))
    }

    pub(crate) fn merge_captured_tip_keys(
        &self,
        scope: Option<&str>,
        molecule: &str,
        keys: Vec<String>,
    ) -> Result<Vec<String>, SchemaError> {
        let changes = self.captured_tip_changes(scope, molecule)?;
        if changes.is_empty() {
            return Ok(keys);
        }
        let prefix = molecule_key_codec::molecule_record_prefix(molecule);
        let mut keys: BTreeSet<_> = keys.into_iter().collect();
        for (base, record) in changes {
            if !base.starts_with(&prefix) {
                continue;
            }
            let key = crate::schema::types::field::build_storage_key(scope, &base);
            if record.is_some() {
                keys.insert(key);
            } else {
                keys.remove(&key);
            }
        }
        Ok(keys.into_iter().collect())
    }

    /// Refill from the retained disk boundary until the merged prefix is known.
    pub(crate) async fn captured_tip_page(
        &self,
        scope: Option<&str>,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let Some(molecule) = molecule_key_codec::molecule_uuid_from_storage_key(start) else {
            return Ok(None);
        };
        let changes: Changes = self
            .captured_tip_changes(scope, molecule)?
            .into_iter()
            .filter(|(key, _)| key.as_str() >= start && key.as_str() < end)
            .collect();
        if changes.is_empty() {
            return Ok(None);
        }
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let mut selected: BTreeMap<_, _> = changes
            .iter()
            .filter_map(|(key, value)| value.as_ref().map(|value| (key.clone(), value.clone())))
            .collect();
        let mut cursor = start.to_string();
        let mut after = None;
        loop {
            let ask = limit
                .saturating_add(changes.len())
                .saturating_add(1)
                .clamp(2, 10_000);
            let disk: Vec<TipRow> = self
                .main_store
                .scan_items_in_range_paged(
                    &crate::schema::types::field::build_storage_key(scope, &cursor),
                    &crate::schema::types::field::build_storage_key(scope, end),
                    ask,
                )
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("captured molecule key page: {error}"))
                })?;
            let exhausted = disk.len() < ask;
            let disk = Self::strip_per_key_storage_prefix(scope, disk);
            let last = disk.last().map(|row| row.0.clone());
            for (key, value) in disk {
                if after.as_ref().is_some_and(|after| key <= *after) || changes.contains_key(&key) {
                    continue;
                }
                selected.insert(key, value);
            }
            let frontier = selected.keys().nth(limit.saturating_sub(1));
            if exhausted
                || frontier
                    .zip(last.as_ref())
                    .is_some_and(|(frontier, last)| frontier <= last)
            {
                break;
            }
            let Some(last) = last.filter(|last| after.as_ref().is_none_or(|after| last > after))
            else {
                break;
            };
            cursor = last.clone();
            after = Some(last);
        }
        Ok(Some(selected.into_iter().take(limit).collect()))
    }
}

fn merge_rows(
    rows: Vec<TipRow>,
    changes: impl Iterator<Item = (String, Option<PerKeyRecord>)>,
) -> Vec<TipRow> {
    let mut rows: BTreeMap<_, _> = rows.into_iter().collect();
    for (key, value) in changes {
        match value {
            Some(value) => {
                rows.insert(key, value);
            }
            None => {
                rows.remove(&key);
            }
        }
    }
    rows.into_iter().collect()
}
