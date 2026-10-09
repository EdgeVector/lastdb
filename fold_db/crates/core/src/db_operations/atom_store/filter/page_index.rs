//! HashRange page-index rebuild and paged scans.

use crate::atom::{molecule_key_codec, AtomEntry, KeyMetadata};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;

use super::super::helpers::{
    hash_range_page_index_complete_item, hash_range_page_index_item, page_index_built_at,
};
use super::super::types::MoleculeHeader;
use super::super::AtomStore;

impl AtomStore {
    pub(crate) async fn hash_range_page_index_complete(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_page_index_complete_key(molecule_uuid),
        );
        self.main_store.exists_item(&key).await.map_err(|e| {
            SchemaError::InvalidData(format!(
                "probe HashRange page index marker {molecule_uuid}: {e}"
            ))
        })
    }

    /// The `(version, updated_at)` the stored completion marker was built from,
    /// or `None` when the marker is absent or is the legacy bare `true`.
    pub(crate) async fn hash_range_page_index_built_at(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<super::super::helpers::PageIndexBuiltAt>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_page_index_complete_key(molecule_uuid),
        );
        let value: Option<Value> = self.main_store.get_item(&key).await.map_err(|e| {
            SchemaError::InvalidData(format!(
                "read HashRange page index marker {molecule_uuid}: {e}"
            ))
        })?;
        Ok(value.as_ref().and_then(page_index_built_at))
    }

    /// Build the `mhr:` index because it is missing or was built from a
    /// different header.
    ///
    /// Skips when the marker already records this exact `(version,
    /// updated_at)`: the index is a pure function of the molecule's `mk:` key
    /// set, and the production write paths (`store_molecule_per_key`,
    /// `store_molecule_changed_keys`) co-write the `mh:` header and `mk:`
    /// records in one ordered batch. After a successful return, an unchanged
    /// header means an unchanged key set and a byte-identical rebuild. A
    /// cross-group crash remains outside this fast-path guarantee.
    pub(crate) async fn rebuild_hash_range_page_index(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        header: &MoleculeHeader,
    ) -> Result<(), SchemaError> {
        if !super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
            let _ = (molecule_uuid, storage_prefix, header);
            return Ok(());
        }
        if let Some(built) = self
            .hash_range_page_index_built_at(molecule_uuid, storage_prefix)
            .await?
        {
            if built.version == header.version && built.updated_at == header.updated_at {
                return Ok(());
            }
        }
        self.write_hash_range_page_index(molecule_uuid, storage_prefix, header, false)
            .await
    }

    /// Rebuild in response to a **stale** page window — an index entry naming a
    /// `mk:` record that is no longer there.
    ///
    /// This one deliberately runs even at an unchanged header, because the
    /// unchanged-header argument above only covers the production write paths.
    /// A record deleted out of band leaves an orphan index row behind, the
    /// header never moves, and this rebuild is the only thing that reaps it.
    ///
    /// The budget is **one repair per header**. Without a budget, a
    /// listing/fetch disagreement — `list_keys_with_prefix` returning a key
    /// `get_per_key` cannot fetch — is unfixable by rebuilding, so every paged
    /// read would answer stale with another full `mk:{M}:` sweep, forever. The
    /// first repair at a header sets `repaired`; a second stale window at that
    /// same header has already been shown not to be an orphan, and is skipped.
    pub(crate) async fn repair_hash_range_page_index(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        header: &MoleculeHeader,
    ) -> Result<(), SchemaError> {
        if !super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
            let _ = (molecule_uuid, storage_prefix, header);
            return Ok(());
        }
        if let Some(built) = self
            .hash_range_page_index_built_at(molecule_uuid, storage_prefix)
            .await?
        {
            if built.repaired
                && built.version == header.version
                && built.updated_at == header.updated_at
            {
                return Ok(());
            }
        }
        self.write_hash_range_page_index(molecule_uuid, storage_prefix, header, true)
            .await
    }

    /// Derive the whole `mhr:` index from the molecule's `mk:` key set and
    /// stamp the completion marker. Unconditional — the callers above own the
    /// decision about whether it should run.
    async fn write_hash_range_page_index(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        header: &MoleculeHeader,
        repaired: bool,
    ) -> Result<(), SchemaError> {
        if !super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
            let _ = (molecule_uuid, storage_prefix, header, repaired);
            return Ok(());
        }
        let record_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule_uuid),
        );
        let strip = build_storage_key(storage_prefix, "");
        let record_keys = self
            .main_store
            .list_keys_with_prefix(&record_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "scan HashRange records for page-index rebuild {molecule_uuid}: {e}"
                ))
            })?;

        // Sweep the live prefix *and* the pre-partition-pinned one. A home
        // indexed before `mhr:{M}:` became `mhr:{M}\0` still holds rows the
        // live prefix cannot see, and this rebuild is the only thing that ever
        // visits them — skip them and the migration leaks a marker per record
        // instead of reclaiming it.
        let mut delete_keys: Vec<Vec<u8>> = Vec::new();
        for prefix in [
            molecule_key_codec::hash_range_page_index_prefix(molecule_uuid),
            molecule_key_codec::legacy_hash_range_page_index_prefix(molecule_uuid),
        ] {
            let page_index_prefix = build_storage_key(storage_prefix, &prefix);
            let stale_index_keys = self
                .main_store
                .list_keys_with_prefix(&page_index_prefix)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "scan stale HashRange page index {molecule_uuid}: {e}"
                    ))
                })?;
            delete_keys.extend(stale_index_keys.into_iter().map(String::into_bytes));
        }
        for marker in [
            molecule_key_codec::hash_range_page_index_complete_key(molecule_uuid),
            molecule_key_codec::legacy_hash_range_page_index_complete_key(molecule_uuid),
        ] {
            delete_keys.push(build_storage_key(storage_prefix, &marker).into_bytes());
        }
        self.main_store
            .inner()
            .batch_delete(delete_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "delete stale HashRange page index {molecule_uuid}: {e}"
                ))
            })?;

        let mut items = Vec::with_capacity(record_keys.len() + 1);
        for key in record_keys {
            let base = key.strip_prefix(&strip).unwrap_or(&key);
            let (hash, range) = molecule_key_codec::decode_hash_range(base, molecule_uuid)
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!("malformed hash-range record key: {key}"))
                })?;
            items.push(hash_range_page_index_item(
                molecule_uuid,
                &hash,
                &range,
                storage_prefix,
            ));
        }
        items.push(hash_range_page_index_complete_item(
            molecule_uuid,
            header.version,
            header.updated_at,
            repaired,
            storage_prefix,
        ));
        self.main_store.batch_put_items(items).await.map_err(|e| {
            SchemaError::InvalidData(format!("rebuild HashRange page index {molecule_uuid}: {e}"))
        })
    }

    pub(crate) async fn scan_hash_range_page_index_paged(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        let scan_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_page_index_prefix(molecule_uuid),
        );
        let scanned: Vec<(String, Value)> = self
            .main_store
            .scan_items_with_prefix_paged(&scan_prefix, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "paged scan HashRange page index {scan_prefix}: {e}"
                ))
            })?;
        let strip = build_storage_key(storage_prefix, "");
        scanned
            .into_iter()
            .map(|(k, _)| {
                let base = k.strip_prefix(&strip).unwrap_or(&k);
                molecule_key_codec::decode_hash_range_page_index(base, molecule_uuid).ok_or_else(
                    || SchemaError::InvalidData(format!("malformed HashRange page index key: {k}")),
                )
            })
            .collect()
    }

    /// Page-index entries strictly after a **storage-form** `(hash, range)`.
    ///
    /// The cursor is storage-form rather than API-form because the refill loop
    /// resumes from the last index entry it walked, and a blinded segment must
    /// not be blinded a second time. Callers holding an API cursor map it once
    /// with `storage_hash` / `storage_range` before calling.
    pub(crate) async fn scan_hash_range_page_index_after_storage_paged(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        storage_hash: &str,
        storage_range: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = molecule_key_codec::hash_range_page_index_prefix(molecule_uuid);
        let start = molecule_key_codec::hash_range_page_index_key(
            molecule_uuid,
            storage_hash,
            storage_range,
        );
        let end = crate::schema::types::field::FilterUtils::create_prefix_end(&prefix);
        let start_with_prefix = build_storage_key(storage_prefix, &start);
        let end_with_prefix = build_storage_key(storage_prefix, &end);
        let scanned: Vec<(String, Value)> = self
            .main_store
            .scan_items_in_range_paged(
                &start_with_prefix,
                &end_with_prefix,
                limit.saturating_add(1),
            )
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "paged scan HashRange page index after {start_with_prefix}: {e}"
                ))
            })?;
        let strip = build_storage_key(storage_prefix, "");
        scanned
            .into_iter()
            .filter_map(|(k, _)| {
                let base = k.strip_prefix(&strip).unwrap_or(&k).to_string();
                (base > start).then_some(base)
            })
            .take(limit)
            .map(|base| {
                molecule_key_codec::decode_hash_range_page_index(&base, molecule_uuid).ok_or_else(
                    || {
                        SchemaError::InvalidData(format!(
                            "malformed HashRange page index key: {base}"
                        ))
                    },
                )
            })
            .collect()
    }

    /// Walk the `mhr:` page index forward until `want` rows that `fill` keeps
    /// have been collected, or the index is exhausted.
    ///
    /// `after` is an **exclusive** storage-form `(hash, range)` cursor; `None`
    /// starts at the head. Returns `(records, stale)` — `stale` when an index
    /// entry named a `mk:` record that is no longer there, which the caller
    /// answers with one rebuild.
    ///
    /// Both shortfalls this closes were silent: a tombstoned row and a stale
    /// index entry each cost the page a slot it never asked to give up, and
    /// nothing on the wire distinguished the short page from a complete one.
    pub(crate) async fn load_hash_range_page_window_filled(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        after: Option<(String, String)>,
        want: usize,
        fill: super::PageFill,
    ) -> Result<(Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, bool), SchemaError> {
        if want == 0 {
            return Ok((Vec::new(), false));
        }
        let mut cursor = after;
        let mut kept: Vec<(String, String, AtomEntry, Option<KeyMetadata>)> = Vec::new();
        let mut stale = false;

        loop {
            let ask = fill.chunk_for(want - kept.len());
            let page_keys = match &cursor {
                None => {
                    self.scan_hash_range_page_index_paged(molecule_uuid, storage_prefix, ask)
                        .await?
                }
                Some((hash, range)) => {
                    self.scan_hash_range_page_index_after_storage_paged(
                        molecule_uuid,
                        storage_prefix,
                        hash,
                        range,
                        ask,
                    )
                    .await?
                }
            };
            let exhausted = page_keys.len() < ask;
            let Some(last) = page_keys.last().cloned() else {
                break;
            };
            cursor = Some(last);

            let (records, batch_stale) = self
                .fetch_hash_range_page_records(molecule_uuid, storage_prefix, page_keys)
                .await?;
            stale |= batch_stale;
            kept.extend(
                records
                    .into_iter()
                    .filter(|(_, _, _, meta)| fill.keeps_meta(meta.as_ref())),
            );
            if kept.len() >= want || exhausted {
                break;
            }
        }

        kept.truncate(want);
        Ok((kept, stale))
    }

    /// Read the `mk:` bodies for one page in one batch.
    ///
    /// Keys that share a hash group are copied from one open of that group.
    /// The generation pointer is read once per molecule, and generation slots
    /// and deletes are read only when a pointer exists. An exact Delete
    /// barrier can hide a body without marking the page index stale. A missing
    /// body sets `stale`. Order of visible keys follows `page_keys`.
    pub(crate) async fn fetch_hash_range_page_records(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        page_keys: Vec<(String, String)>,
    ) -> Result<(Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, bool), SchemaError> {
        if page_keys.is_empty() {
            return Ok((Vec::new(), false));
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
        let stale = fetched.iter().any(Option::is_none);
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
        Ok((records, stale))
    }
}
