//! Per-key molecule header probes and load paths.

use crate::atom::{molecule_key_codec, MoleculeHashRange};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

use super::super::helpers::strip_record_prefix;
use super::super::types::{ChangedKey, MoleculeData, MoleculeHeader, PerKeyRecord};
use super::super::AtomStore;
use super::generation::MaterializeReadCost;

type MaterializedRows = Vec<(
    String,
    String,
    crate::atom::AtomEntry,
    Option<crate::atom::KeyMetadata>,
)>;

/// Process-local count of full `mk:{M}:*` prefix scans (`load_all_mk_records`).
/// Test/ops surface for proving zero-yield purge probes no longer pay this cost.
static MK_FULL_SCANS: AtomicU64 = AtomicU64::new(0);

/// How many times this process has fully scanned a field's `mk:` tips.
/// Used by purge zero-yield regression tests; not a product metric.
pub fn mk_full_scans() -> u64 {
    MK_FULL_SCANS.load(Ordering::Relaxed)
}

impl AtomStore {
    /// Load a molecule stored in the per-key layout, reconstructing entries
    /// **verbatim** from the `mk:{M}:` records. Returns `None` when there is no
    /// `mh:{M}` header — i.e. the molecule is absent at this prefix.
    pub(crate) async fn load_molecule_per_key(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        let header_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::header_key(molecule_uuid),
        );
        // The header is `(version, updated_at)`. A legacy inline
        // `hash_range_order` on the same blob is not returned.
        let header_value: Option<Value> =
            self.main_store.get_item(&header_key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read molecule header {molecule_uuid}: {e}"))
            })?;
        let Some(header_value) = header_value else {
            return Ok(None);
        };
        let header: MoleculeHeader = serde_json::from_value(header_value).map_err(|e| {
            SchemaError::InvalidData(format!("parse molecule header {molecule_uuid}: {e}"))
        })?;

        // Authoritative load is `mk:{M}:*` (hash-major tips). The derived `mhr:`
        // page index is retired (HASH_RANGE_PAGE_INDEX_ENABLED=false): product
        // paths must key-restrict; residual full loads scan `mk:` only.
        // SampleN reads those tips. The product load does not return `mord:`,
        // `moc:`, or a legacy inline `hash_range_order`.
        let records = self
            .load_molecule_records_via_page_index(molecule_uuid, &header, storage_prefix)
            .await?;
        let molecule = MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        );
        Ok(Some(molecule))
    }

    /// Full materialize of authoritative `mk:{M}:*` tips (no `mhr:`).
    pub(crate) async fn load_all_mk_records(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<MaterializedRows, SchemaError> {
        Ok(self
            .load_all_mk_records_with_cost(molecule_uuid, storage_prefix)
            .await?
            .0)
    }

    pub(crate) async fn load_all_mk_records_with_cost(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(MaterializedRows, MaterializeReadCost), SchemaError> {
        self.load_all_mk_records_with_page_rows(
            molecule_uuid,
            storage_prefix,
            super::generation::MATERIALIZE_PAGE_ROWS,
        )
        .await
    }

    async fn load_all_mk_records_with_page_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        page_rows: usize,
    ) -> Result<(MaterializedRows, MaterializeReadCost), SchemaError> {
        MK_FULL_SCANS.fetch_add(1, Ordering::Relaxed);
        let scan_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule_uuid),
        );
        let scanned = self
            .scan_exact_prefix_pages_with_limit::<PerKeyRecord>(
                &scan_prefix,
                &format!("per-key molecule {molecule_uuid}"),
                page_rows,
            )
            .await?;
        let mut cost = scanned.cost;
        let scanned = Self::strip_per_key_storage_prefix(storage_prefix, scanned.rows);
        let (scanned, generation_cost) = self
            .merge_generation_rows_with_page_rows(molecule_uuid, storage_prefix, scanned, page_rows)
            .await?;
        cost.merge(generation_cost);
        let scanned = self
            .filter_delete_barriers_for_records(scanned, storage_prefix)
            .await?;
        let mut out = Vec::with_capacity(scanned.len());
        for (k, r) in scanned {
            let suffix = strip_record_prefix(
                &k,
                &molecule_key_codec::molecule_record_prefix(molecule_uuid),
            );
            let Some((hash, range)) = molecule_key_codec::decode_hash_range_suffix(&suffix) else {
                continue;
            };
            out.push((hash, range, r.entry, r.meta));
        }
        Ok((out, cost))
    }

    /// Full materialize of authoritative `mk:{M}:*` tips.
    ///
    /// When the derived page index is enabled this preferred `mhr:` then fell
    /// back to `mk:`. With the page index retired, always scan `mk:`.
    async fn load_molecule_records_via_page_index(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
    ) -> Result<
        Vec<(
            String,
            String,
            crate::atom::AtomEntry,
            Option<crate::atom::KeyMetadata>,
        )>,
        SchemaError,
    > {
        let _ = header;
        if super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
            use super::super::filter::PageFill;

            if !self
                .hash_range_page_index_complete(molecule_uuid, storage_prefix)
                .await?
            {
                self.rebuild_hash_range_page_index(molecule_uuid, storage_prefix, header)
                    .await?;
            }

            let (mut records, stale) = self
                .load_hash_range_page_window_filled(
                    molecule_uuid,
                    storage_prefix,
                    None,
                    usize::MAX,
                    PageFill::StoredRows,
                )
                .await?;
            if stale {
                self.repair_hash_range_page_index(molecule_uuid, storage_prefix, header)
                    .await?;
                records = self
                    .load_hash_range_page_window_filled(
                        molecule_uuid,
                        storage_prefix,
                        None,
                        usize::MAX,
                        PageFill::StoredRows,
                    )
                    .await?
                    .0;
            }

            if self
                .hash_range_page_index_complete(molecule_uuid, storage_prefix)
                .await?
            {
                return Ok(records);
            }
        }

        self.load_all_mk_records(molecule_uuid, storage_prefix)
            .await
    }

    /// Load a molecule for a **keyed write** that will touch only `changed`,
    /// fetching just those keys' existing `mk:` records (O(changed) point gets)
    /// instead of scanning the whole field. The returned molecule carries the
    /// real header `version`/`updated_at` and is marked as a tail. It does not
    /// read the order log.
    ///
    /// This is the write-side analogue of [`Self::load_filtered_molecule_filled`]: the
    /// schema cache evicts materialized molecules after every batch (so keyed
    /// reads stay O(1) — see `FieldVariant::clear_molecule`), which would
    /// otherwise force `restore_missing_molecules` to re-scan and re-deserialize
    /// every `mk:` record (O(field)) on *every* write. Loading only the touched
    /// keys keeps a single keyed update O(changed) as the field grows.
    ///
    /// Returns `Ok(None)` — caller must fall back to the full
    /// [`crate::schema::types::field::FieldVariant::refresh_from_db`] path — when
    /// there is **no `mh:{M}` header at the exact `storage_prefix` prefix** (absent
    /// molecule, or data at a *different* prefix such as a first write under a
    /// share/org prefix). In that case the persist step's full-rewrite path needs
    /// the whole molecule in memory. We therefore probe ONLY the exact prefix —
    /// no dual-read fallback.
    pub(crate) async fn load_molecule_for_write(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        changed: &std::collections::HashSet<ChangedKey>,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        // Probe the header at the EXACT prefix we will persist to — deliberately
        // NO org pre-tag dual-read fallback. If it's absent here the persist step
        // takes the full-rewrite migration path (which needs the whole molecule),
        // so bail to the full load.
        let header_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::header_key(molecule_uuid),
        );
        let header: MoleculeHeader =
            match self.main_store.get_item(&header_key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read molecule header {molecule_uuid}: {e}"))
            })? {
                Some(h) => h,
                None => return Ok(None),
            };
        let mut records = Vec::with_capacity(changed.len());
        for ck in changed {
            let api_hash = ck.disk_hash();
            let api_range = ck.disk_range();
            let storage_hash = self.storage_hash(molecule_uuid, api_hash)?;
            let storage_range = self.storage_range(molecule_uuid, api_range)?;
            let key = molecule_key_codec::hash_range_record_key(
                molecule_uuid,
                &storage_hash,
                &storage_range,
            );
            if let Some((_, rec)) = self.get_per_key(&key, storage_prefix).await? {
                // Keep API-form hash/range in the in-memory molecule (Option I).
                records.push((
                    api_hash.to_string(),
                    api_range.to_string(),
                    rec.entry,
                    rec.meta,
                ));
            }
        }
        Ok(Some(MoleculeHashRange::from_write_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }

    /// Load only exact storage-form slots for a purge plan.
    ///
    /// Unlike [`Self::load_molecule_for_write`], this method does not encode
    /// its inputs. A purge planner already resolved the durable hash/range
    /// segments and must keep them in storage form through deletion.
    pub(crate) async fn load_molecule_for_storage_slot_purge(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        slots: &[(String, String)],
    ) -> Result<Option<MoleculeData>, SchemaError> {
        let header_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::header_key(molecule_uuid),
        );
        let Some(header): Option<MoleculeHeader> =
            self.main_store.get_item(&header_key).await.map_err(|e| {
                SchemaError::InvalidData(format!(
                    "read molecule header {molecule_uuid} for purge: {e}"
                ))
            })?
        else {
            return Ok(None);
        };

        let keys: Vec<String> = slots
            .iter()
            .map(|(hash, range)| {
                molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range)
            })
            .collect();
        let selected = self
            .load_exact_per_key_records(storage_prefix, &keys)
            .await?;
        let mut records = Vec::with_capacity(slots.len());
        for ((storage_hash, storage_range), rec) in slots.iter().zip(selected) {
            if let Some(rec) = rec {
                records.push((
                    storage_hash.clone(),
                    storage_range.clone(),
                    rec.entry,
                    rec.meta,
                ));
            }
        }

        Ok(Some(MoleculeHashRange::from_write_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }

    /// Read `mh:{M}` at the exact storage prefix. `None` means the molecule
    /// is absent at that prefix.
    pub(crate) async fn read_header_exact_prefix<'a>(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&'a str>,
    ) -> Result<Option<(MoleculeHeader, Option<&'a str>)>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::header_key(molecule_uuid),
        );
        let header = self
            .main_store
            .get_item::<MoleculeHeader>(&key)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("read molecule header {molecule_uuid}: {e}"))
            })?;
        Ok(header.map(|h| (h, storage_prefix)))
    }
}
