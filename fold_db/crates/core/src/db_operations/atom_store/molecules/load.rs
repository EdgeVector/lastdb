//! Per-key molecule header probes and load paths.

use crate::atom::{molecule_key_codec, MoleculeHashRange};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;

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

        // Authoritative load is `mk:{M}:*` (hash-major tips). Product paths
        // must key-restrict; residual full loads use those tips directly.
        // SampleN reads those tips. The product load does not return `mord:`,
        // `moc:`, or a legacy inline `hash_range_order`.
        let records = self
            .load_all_mk_records(molecule_uuid, storage_prefix)
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
        let header = self.header_from_memory(&key).await.map_err(|e| {
            SchemaError::InvalidData(format!("read molecule header {molecule_uuid}: {e}"))
        })?;
        Ok(header.map(|h| (h, storage_prefix)))
    }
}
