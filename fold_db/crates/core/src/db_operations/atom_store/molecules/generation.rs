//! Barrierless immutable molecule generations.

use crate::atom::{incoming_wins_lww, molecule_key_codec};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;
#[cfg(feature = "sharing")]
use serde_json::Value;
#[cfg(feature = "sharing")]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};
#[cfg(feature = "sharing")]
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::{
    AtomStore, MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot,
    PerKeyRecord,
};

/// A read-only cut captured before a complete generation snapshot is built.
/// Ordinary writes continue after this value is created.
#[cfg(feature = "sharing")]
pub(crate) struct PreparedMoleculeGeneration {
    generation: String,
    observed: HashMap<(String, String), PerKeyRecord>,
    previous_generation: Option<String>,
    reclaim_generation: Option<String>,
}

/// One complete immutable body and the rows that select it.
#[cfg(feature = "sharing")]
pub(crate) struct PreparedMoleculeGenerationActivation {
    pub(crate) molecule_uuid: String,
    pub(crate) prepared: PreparedMoleculeGeneration,
    pub(crate) base_records: Vec<(String, PerKeyRecord)>,
    pub(crate) activation_items: Vec<(String, Value)>,
}

#[cfg(feature = "sharing")]
struct ReadyMoleculeGenerationActivation {
    molecule_uuid: String,
    prepared: PreparedMoleculeGeneration,
    generation: String,
    stale_direct: Vec<((String, String), PerKeyRecord)>,
    activation_items: Vec<(String, Value)>,
}

fn incoming_record_wins(incoming: &PerKeyRecord, current: &PerKeyRecord) -> bool {
    incoming_wins_lww(incoming.entry.lww_key(), current.entry.lww_key())
}

pub(crate) const MATERIALIZE_PAGE_ROWS: usize = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaterializeReadCost {
    pub(crate) page_count: u64,
    pub(crate) row_count: u64,
    pub(crate) value_bytes: u64,
    pub(crate) max_page_rows: u64,
}

impl MaterializeReadCost {
    fn record<T>(&mut self, page: &[(Vec<u8>, T)], value_bytes: u64) {
        self.page_count = self.page_count.saturating_add(1);
        self.row_count = self.row_count.saturating_add(page.len() as u64);
        self.value_bytes = self.value_bytes.saturating_add(value_bytes);
        self.max_page_rows = self.max_page_rows.max(page.len() as u64);
    }

    pub(super) fn merge(&mut self, other: Self) {
        self.page_count = self.page_count.saturating_add(other.page_count);
        self.row_count = self.row_count.saturating_add(other.row_count);
        self.value_bytes = self.value_bytes.saturating_add(other.value_bytes);
        self.max_page_rows = self.max_page_rows.max(other.max_page_rows);
    }
}

pub(super) struct PagedRows<T> {
    pub(super) rows: Vec<(String, T)>,
    pub(super) cost: MaterializeReadCost,
}

impl AtomStore {
    /// Read an exact physical prefix through bounded continuation pages.
    pub(super) async fn scan_exact_prefix_pages_with_limit<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
        what: &str,
        page_rows: usize,
    ) -> Result<PagedRows<T>, SchemaError> {
        let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
        self.scan_range_pages_with_limit(&start, &end, what, page_rows)
            .await
    }

    async fn scan_range_pages_with_limit<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        what: &str,
        page_rows: usize,
    ) -> Result<PagedRows<T>, SchemaError> {
        if page_rows == 0 {
            return Err(SchemaError::InvalidData(format!(
                "scan {what} requires a positive page size"
            )));
        }
        let mut cursor = start.as_bytes().to_vec();
        let mut decoded = Vec::new();
        let mut cost = MaterializeReadCost::default();
        loop {
            let page = self
                .main_store
                .inner()
                .scan_range_paged(&cursor, end.as_bytes(), page_rows)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("scan {what}: {error}")))?;
            cost.record(
                &page,
                page.iter().map(|(_, value)| value.len() as u64).sum(),
            );
            if page.is_empty() {
                break;
            }
            if page.len() > page_rows
                || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
                || page.iter().any(|(key, _)| {
                    key.as_slice() < cursor.as_slice() || key.as_slice() >= end.as_bytes()
                })
            {
                return Err(SchemaError::InvalidData(format!(
                    "scan {what} returned an invalid or unordered page"
                )));
            }
            for (key, value) in &page {
                let value = serde_json::from_slice(value).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "decode {what} {}: {error}",
                        String::from_utf8_lossy(key)
                    ))
                })?;
                decoded.push((String::from_utf8_lossy(key).into_owned(), value));
            }

            let last_key = page.last().expect("non-empty page").0.clone();
            // A short page is not proof of the end of the range: the store may
            // cut a page early, and rows may be committed behind the cursor by
            // concurrent writers while we read. Only an empty page ends the
            // scan (see the `page.is_empty()` break above), so every row that is
            // present when its page is read is returned. The unbounded page
            // size reads the whole range in one call and ends here.
            if page_rows == usize::MAX {
                break;
            }
            cursor = last_key;
            cursor.push(0);
        }
        Ok(PagedRows {
            rows: decoded,
            cost,
        })
    }

    pub(crate) async fn active_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationPointer>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_generation_pointer_key(molecule_uuid),
        );
        self.main_store.get_item(&key).await.map_err(|error| {
            SchemaError::InvalidData(format!(
                "read generation pointer for molecule {molecule_uuid}: {error}"
            ))
        })
    }

    /// Select one logical slot from the immutable base and the live change row.
    pub(crate) fn merge_generation_slot(
        live: Option<PerKeyRecord>,
        base: Option<MoleculeGenerationSlot>,
    ) -> Option<PerKeyRecord> {
        match base {
            None
            | Some(MoleculeGenerationSlot {
                record: None,
                shadow: None,
            }) => live,
            Some(MoleculeGenerationSlot {
                record: Some(base), ..
            }) => match live {
                Some(live) if incoming_record_wins(&live, &base) => Some(live),
                _ => Some(base),
            },
            Some(MoleculeGenerationSlot {
                record: None,
                shadow: Some(shadow),
            }) => live
                .filter(|candidate| incoming_wins_lww(candidate.entry.lww_key(), shadow.lww_key())),
        }
    }

    pub(crate) async fn generation_slot_for_record_key(
        &self,
        base_key: &str,
        generation: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationSlot>, SchemaError> {
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_key)
        else {
            return Ok(None);
        };
        let Some(generation_key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
            molecule_uuid,
            generation,
            base_key,
        ) else {
            return Ok(None);
        };
        let storage_key = build_storage_key(storage_prefix, &generation_key);
        self.main_store
            .get_item(&storage_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read molecule generation slot {storage_key}: {error}"
                ))
            })
    }

    pub(crate) async fn generation_delete_for_record_key(
        &self,
        base_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationDelete>, SchemaError> {
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_key)
        else {
            return Ok(None);
        };
        let Some(key) = molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
            molecule_uuid,
            base_key,
        ) else {
            return Ok(None);
        };
        self.main_store
            .get_item(&build_storage_key(storage_prefix, &key))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read generation delete for molecule {molecule_uuid}: {error}"
                ))
            })
    }

    pub(crate) fn apply_generation_delete(
        record: Option<PerKeyRecord>,
        deletion: Option<MoleculeGenerationDelete>,
    ) -> Option<PerKeyRecord> {
        match (record, deletion) {
            (Some(record), Some(deletion))
                if !incoming_wins_lww(record.entry.lww_key(), deletion.shadow.lww_key()) =>
            {
                None
            }
            (record, _) => record,
        }
    }

    async fn filter_generation_deletes(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        Ok(self
            .filter_generation_deletes_with_cost(molecule_uuid, delete_prefix, storage_prefix, rows)
            .await?
            .0)
    }

    async fn filter_generation_deletes_with_cost(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        self.filter_generation_deletes_with_page_rows(
            molecule_uuid,
            delete_prefix,
            storage_prefix,
            rows,
            MATERIALIZE_PAGE_ROWS,
        )
        .await
    }

    async fn filter_generation_deletes_with_page_rows(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
        page_rows: usize,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        let storage_delete_prefix = build_storage_key(storage_prefix, delete_prefix);
        let deletes = self
            .scan_exact_prefix_pages_with_limit::<MoleculeGenerationDelete>(
                &storage_delete_prefix,
                &format!("generation deletes for molecule {molecule_uuid}"),
                page_rows,
            )
            .await?;
        let cost = deletes.cost;
        let strip = build_storage_key(storage_prefix, "");
        let deletes: HashMap<String, MoleculeGenerationDelete> = deletes
            .rows
            .into_iter()
            .filter_map(|(key, deletion)| {
                let bare = key.strip_prefix(&strip).unwrap_or(&key);
                molecule_key_codec::molecule_record_key_from_generation_delete_key(
                    molecule_uuid,
                    bare,
                )
                .map(|key| (key, deletion))
            })
            .collect();
        Ok((
            rows.into_iter()
                .filter(|(key, record)| {
                    deletes.get(key).is_none_or(|deletion| {
                        incoming_wins_lww(record.entry.lww_key(), deletion.shadow.lww_key())
                    })
                })
                .collect(),
            cost,
        ))
    }

    async fn filter_generation_deletes_range(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let (Some(start), Some(end)) = (
            molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                base_start,
            ),
            molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                base_end,
            ),
        ) else {
            return Ok(rows);
        };
        let start = build_storage_key(storage_prefix, &start);
        let end = build_storage_key(storage_prefix, &end);
        let deletes: Vec<(String, MoleculeGenerationDelete)> = self
            .main_store
            .scan_items_in_range(&start, &end)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan generation delete range {start}..{end}: {error}"
                ))
            })?;
        let strip = build_storage_key(storage_prefix, "");
        let deletes: HashMap<String, MoleculeGenerationDelete> = deletes
            .into_iter()
            .filter_map(|(key, deletion)| {
                let bare = key.strip_prefix(&strip).unwrap_or(&key);
                molecule_key_codec::molecule_record_key_from_generation_delete_key(
                    molecule_uuid,
                    bare,
                )
                .map(|key| (key, deletion))
            })
            .collect();
        Ok(rows
            .into_iter()
            .filter(|(key, record)| {
                deletes.get(key).is_none_or(|deletion| {
                    incoming_wins_lww(record.entry.lww_key(), deletion.shadow.lww_key())
                })
            })
            .collect())
    }

    pub(super) async fn merge_generation_rows_with_page_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
        page_rows: usize,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok((live_rows, MaterializeReadCost::default()));
        };
        let prefix = molecule_key_codec::molecule_generation_record_prefix(
            molecule_uuid,
            &pointer.generation,
        );
        let storage_prefix_key = build_storage_key(storage_prefix, &prefix);
        let generation_rows = self
            .scan_exact_prefix_pages_with_limit::<MoleculeGenerationSlot>(
                &storage_prefix_key,
                &format!("molecule generation {molecule_uuid}"),
                page_rows,
            )
            .await?;
        let mut cost = generation_rows.cost;
        let merged = self.merge_generation_rows_from_selected(
            molecule_uuid,
            storage_prefix,
            &pointer.generation,
            live_rows,
            generation_rows.rows,
        )?;
        let (merged, delete_cost) = self
            .filter_generation_deletes_with_page_rows(
                molecule_uuid,
                &molecule_key_codec::molecule_generation_delete_prefix(molecule_uuid),
                storage_prefix,
                merged,
                page_rows,
            )
            .await?;
        cost.merge(delete_cost);
        Ok((merged, cost))
    }

    /// Merge rows for one bounded logical prefix without scanning any other
    /// hash partition in the generation.
    pub(crate) async fn merge_generation_prefix_rows(
        &self,
        molecule_uuid: &str,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(live_rows);
        };
        let Some(generation_prefix) =
            molecule_key_codec::molecule_generation_bound_for_record_bound(
                molecule_uuid,
                &pointer.generation,
                base_prefix,
            )
        else {
            return Ok(live_rows);
        };
        let storage_generation_prefix = build_storage_key(storage_prefix, &generation_prefix);
        let rows: Vec<(String, MoleculeGenerationSlot)> = self
            .main_store
            .scan_items_with_prefix(&storage_generation_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan generation prefix {storage_generation_prefix}: {error}"
                ))
            })?;
        let merged = self.merge_generation_rows_from_selected(
            molecule_uuid,
            storage_prefix,
            &pointer.generation,
            live_rows,
            rows,
        )?;
        let delete_prefix = molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
            molecule_uuid,
            base_prefix,
        )
        .unwrap_or_else(|| molecule_key_codec::molecule_generation_delete_prefix(molecule_uuid));
        self.filter_generation_deletes(molecule_uuid, &delete_prefix, storage_prefix, merged)
            .await
    }

    /// Merge rows for one bounded logical key range.
    pub(crate) async fn merge_generation_range_rows(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(live_rows);
        };
        let (Some(start), Some(end)) = (
            molecule_key_codec::molecule_generation_bound_for_record_bound(
                molecule_uuid,
                &pointer.generation,
                base_start,
            ),
            molecule_key_codec::molecule_generation_bound_for_record_bound(
                molecule_uuid,
                &pointer.generation,
                base_end,
            ),
        ) else {
            return Ok(live_rows);
        };
        let start = build_storage_key(storage_prefix, &start);
        let end = build_storage_key(storage_prefix, &end);
        let rows: Vec<(String, MoleculeGenerationSlot)> = self
            .main_store
            .scan_items_in_range(&start, &end)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("scan generation range {start}..{end}: {error}"))
            })?;
        let merged = self.merge_generation_rows_from_selected(
            molecule_uuid,
            storage_prefix,
            &pointer.generation,
            live_rows,
            rows,
        )?;
        self.filter_generation_deletes_range(
            molecule_uuid,
            base_start,
            base_end,
            storage_prefix,
            merged,
        )
        .await
    }

    fn merge_generation_rows_from_selected(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        generation: &str,
        live_rows: Vec<(String, PerKeyRecord)>,
        generation_rows: Vec<(String, MoleculeGenerationSlot)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let strip = build_storage_key(storage_prefix, "");
        let mut merged: BTreeMap<String, (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>)> =
            BTreeMap::new();
        for (key, record) in live_rows {
            merged.entry(key).or_default().0 = Some(record);
        }
        for (key, slot) in generation_rows {
            let bare = key.strip_prefix(&strip).unwrap_or(&key);
            let Some(record_key) = molecule_key_codec::molecule_record_key_from_generation_key(
                molecule_uuid,
                generation,
                bare,
            ) else {
                continue;
            };
            merged.entry(record_key).or_default().1 = Some(slot);
        }
        Ok(merged
            .into_iter()
            .filter_map(|(key, (live, base))| {
                Self::merge_generation_slot(live, base).map(|record| (key, record))
            })
            .collect())
    }

    /// Capture the complete logical row set at the generation cut.
    #[cfg(feature = "sharing")]
    pub(crate) async fn prepare_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<PreparedMoleculeGeneration, SchemaError> {
        let generation = uuid::Uuid::new_v4().simple().to_string();
        let live_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule_uuid),
        );
        let observed_live: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_with_prefix(&live_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "capture live rows for molecule generation {molecule_uuid}: {error}"
                ))
            })?;
        let observed_live = Self::strip_per_key_storage_prefix(storage_prefix, observed_live);
        let (observed_live, _) = self
            .merge_generation_rows_with_page_rows(
                molecule_uuid,
                storage_prefix,
                observed_live,
                MATERIALIZE_PAGE_ROWS,
            )
            .await?;
        let mut observed: HashMap<(String, String), PerKeyRecord> = HashMap::new();
        for (key, record) in observed_live {
            if let Some(slot) = molecule_key_codec::decode_hash_range(&key, molecule_uuid) {
                observed.insert(slot, record);
            }
        }
        let current = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?;
        let previous_generation = current.as_ref().map(|value| value.generation.clone());
        let reclaim_generation = current.and_then(|value| value.previous_generation);
        Ok(PreparedMoleculeGeneration {
            generation,
            observed,
            previous_generation,
            reclaim_generation,
        })
    }

    /// Build immutable bases, then select every field generation in one batch.
    /// Ordinary writers continue to use `mk:` rows while each base is built.
    #[cfg(feature = "sharing")]
    pub(crate) async fn activate_prepared_molecule_generations(
        &self,
        activations: Vec<PreparedMoleculeGenerationActivation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        if activations.is_empty() {
            return Ok(Vec::new());
        }

        let _coverage = self.invalidate_coverage_during(
            activations
                .iter()
                .map(|activation| activation.molecule_uuid.as_str()),
            storage_prefix,
        );

        let automatic_gc_uuids: Vec<String> = activations
            .iter()
            .flat_map(|activation| {
                self.automatic_gc_tip_reference_uuids(&activation.activation_items, storage_prefix)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let automatic_gc_guards = self.lock_automatic_gc_atoms(&automatic_gc_uuids).await;
        let mut generation_items = Vec::new();
        let mut ready = Vec::with_capacity(activations.len());
        for activation in activations {
            let PreparedMoleculeGenerationActivation {
                molecule_uuid,
                mut prepared,
                base_records,
                activation_items,
            } = activation;
            let generation = prepared.generation.clone();
            for (base_key, record) in base_records {
                let Some((hash, range)) =
                    molecule_key_codec::decode_hash_range(&base_key, &molecule_uuid)
                else {
                    return Err(SchemaError::InvalidData(format!(
                        "generation base contains a malformed molecule key: {base_key}"
                    )));
                };
                prepared.observed.remove(&(hash.clone(), range.clone()));
                let key = build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_record_key(
                        &molecule_uuid,
                        &generation,
                        &hash,
                        &range,
                    ),
                );
                generation_items.push((
                    key,
                    serde_json::to_value(MoleculeGenerationSlot {
                        record: Some(record),
                        shadow: None,
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize molecule generation slot: {error}"
                        ))
                    })?,
                ));
            }
            let stale_direct: Vec<_> = prepared.observed.drain().collect();
            for ((hash, range), old) in &stale_direct {
                let key = build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_record_key(
                        &molecule_uuid,
                        &generation,
                        hash,
                        range,
                    ),
                );
                generation_items.push((
                    key,
                    serde_json::to_value(MoleculeGenerationSlot {
                        record: None,
                        shadow: Some(old.entry.clone()),
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize molecule generation tombstone: {error}"
                        ))
                    })?,
                ));
            }
            ready.push(ReadyMoleculeGenerationActivation {
                molecule_uuid,
                prepared,
                generation,
                stale_direct,
                activation_items,
            });
        }

        if !generation_items.is_empty() {
            self.main_store
                .batch_put_items(generation_items)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("write molecule generation batch: {error}"))
                })?;
            self.main_store.inner().flush().await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "flush molecule generation bodies before activation: {error}"
                ))
            })?;
        }

        // Only the selector batch takes exact-slot locks. One LastStore
        // transaction activates every field or restores every prior selector.
        let mut tip_keys = Vec::new();
        for item in &ready {
            tip_keys.extend(
                item.activation_items
                    .iter()
                    .filter(|(key, _)| key.contains("mk:"))
                    .map(|(key, _)| key.clone()),
            );
            tip_keys.extend(item.stale_direct.iter().map(|((hash, range), _)| {
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::hash_range_record_key(&item.molecule_uuid, hash, range),
                )
            }));
        }
        tip_keys.sort_unstable();
        tip_keys.dedup();
        let tip_guards = self.lock_tip_commits(&tip_keys).await;
        let publication_guards = self.lock_tip_publications(&tip_keys).await;

        let mut activation_items: Vec<(String, Value)> = ready
            .iter_mut()
            .flat_map(|item| std::mem::take(&mut item.activation_items))
            .collect();
        self.retain_durable_tip_winners(&mut activation_items)
            .await?;

        for item in &ready {
            let stale_keys: Vec<String> = item
                .stale_direct
                .iter()
                .map(|((hash, range), _)| {
                    build_storage_key(
                        storage_prefix,
                        &molecule_key_codec::hash_range_record_key(
                            &item.molecule_uuid,
                            hash,
                            range,
                        ),
                    )
                })
                .collect();
            let current_stale = self
                .main_store
                .get_items::<PerKeyRecord>(&stale_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "read stale direct rows for molecule {}: {error}",
                        item.molecule_uuid
                    ))
                })?;
            for ((((hash, range), old), key), current) in
                item.stale_direct.iter().zip(stale_keys).zip(current_stale)
            {
                let Some(mut current) = current else {
                    continue;
                };
                if current.entry.lww_key() != old.entry.lww_key() {
                    continue;
                }
                current.meta.get_or_insert_with(Default::default).tombstoned = true;
                activation_items.push(self.inactive_atom_ref_tip_edge_item(
                    &item.molecule_uuid,
                    hash,
                    range,
                    &old.entry,
                    storage_prefix,
                )?);
                activation_items.push((
                    key,
                    serde_json::to_value(current).map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize stale direct tombstone for molecule {}: {error}",
                            item.molecule_uuid
                        ))
                    })?,
                ));
            }

            let activated_at_unix_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| {
                    SchemaError::InvalidData(format!("system clock before epoch: {error}"))
                })?
                .as_nanos() as u64;
            let pointer = MoleculeGenerationPointer {
                generation: item.generation.clone(),
                previous_generation: item.prepared.previous_generation.clone(),
                activated_at_unix_nanos,
            };
            activation_items.push((
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_pointer_key(&item.molecule_uuid),
                ),
                serde_json::to_value(pointer).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "serialize molecule generation pointer: {error}"
                    ))
                })?,
            ));
        }

        self.batch_put_items_with_atom_ref_v2(activation_items, storage_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("activate molecule generation batch: {error}"))
            })?;
        drop(publication_guards);
        drop(tip_guards);
        drop(automatic_gc_guards);

        let generations = ready.iter().map(|item| item.generation.clone()).collect();
        for item in ready {
            let Some(reclaim_generation) = item.prepared.reclaim_generation else {
                continue;
            };
            let reclaim_prefix = build_storage_key(
                storage_prefix,
                &molecule_key_codec::molecule_generation_record_prefix(
                    &item.molecule_uuid,
                    &reclaim_generation,
                ),
            );
            let keys: Vec<String> = self
                .main_store
                .scan_items_with_prefix::<MoleculeGenerationSlot>(&reclaim_prefix)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "list expired molecule generation {reclaim_generation}: {error}"
                    ))
                })?
                .into_iter()
                .map(|(key, _)| key)
                .collect();
            if !keys.is_empty() {
                self.main_store
                    .batch_delete_keys(keys)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "reclaim expired molecule generation {reclaim_generation}: {error}"
                        ))
                    })?;
            }
        }
        Ok(generations)
    }
}
