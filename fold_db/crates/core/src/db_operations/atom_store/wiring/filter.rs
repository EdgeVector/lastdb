//! Filter wiring: the per-key read port and the wrappers of the filter.

use super::super::filter::{Filter, SchemaKeyMembership};
use super::super::ports::{KeyedRecord, PerKeyReads};
use super::super::types::{FilterLayout, MoleculeData, MoleculeRangeForm, PerKeyRecord};
use super::super::AtomStore;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::SchemaError;

/// A keys-only page: `(hash, range)` pairs, the next cursor, and `has_more`.
type KeyPage = (Vec<(String, String)>, Option<String>, bool);

impl PerKeyReads for AtomStore {
    async fn load_exact_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        base_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        AtomStore::load_exact_per_key_records(self, storage_prefix, base_keys).await
    }

    async fn load_visible_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        AtomStore::load_visible_per_key_records(self, storage_prefix, keys).await
    }

    async fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<KeyedRecord>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        AtomStore::filter_delete_barriers_for_records(self, rows, storage_prefix).await
    }
}

impl AtomStore {
    /// Strip the `{storage_prefix}:` part from each scanned per-key row key.
    pub(crate) fn strip_per_key_storage_prefix(
        storage_prefix: Option<&str>,
        rows: Vec<KeyedRecord>,
    ) -> Vec<KeyedRecord> {
        super::super::helpers::strip_per_key_storage_prefix(storage_prefix, rows)
    }

    /// A filtered molecule load for a query read. See `Filter`.
    pub(crate) async fn load_filtered_molecule_for_read(
        &self,
        molecule_uuid: &str,
        layout: FilterLayout,
        storage_prefix: Option<&str>,
        filter: &HashRangeFilter,
        include_tombstones: bool,
    ) -> Result<Option<(MoleculeData, MoleculeRangeForm)>, SchemaError> {
        Filter::new(self)
            .load_filtered_molecule_for_read(
                molecule_uuid,
                layout,
                storage_prefix,
                filter,
                include_tombstones,
            )
            .await
    }

    /// A Hash field always uses the empty range, including every read fallback.
    pub(crate) async fn get_query_hash_tip(
        &self,
        molecule: &str,
        hash: &str,
        scope: Option<&str>,
    ) -> Result<Option<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .get_query_hash_tip(molecule, hash, scope)
            .await
    }

    /// Select a bounded hash/range page from ordered encrypted-key streams.
    pub(crate) async fn scan_hash_tip_window(
        &self,
        molecule: &str,
        filter: &HashRangeFilter,
        storage_prefix: Option<&str>,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        Filter::new(self)
            .scan_hash_tip_window(molecule, filter, storage_prefix, window, include_tombstones)
            .await
    }

    /// One ordered page of logical tips under `prefix`.
    pub(crate) async fn scan_logical_tip_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .scan_logical_tip_page(prefix, after, storage_prefix, limit)
            .await
    }

    /// Batch-read exact `mk:` keys for one page. See `Filter`.
    pub(crate) async fn load_exact_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        base_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        Filter::new(self)
            .load_exact_per_key_records(storage_prefix, base_keys)
            .await
    }

    /// Batch-fetch per-key records for a known set of `(hash, range)` slots.
    pub(crate) async fn load_per_key_records_for_slots(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<Vec<Option<KeyedRecord>>, SchemaError> {
        Filter::new(self)
            .load_per_key_records_for_slots(molecule_uuid, storage_prefix, keys)
            .await
    }

    /// Keys-only page of the live physical membership of one molecule.
    /// The continuation remains an exclusive physical `mk:` key.
    pub async fn list_live_record_keys(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<KeyPage, SchemaError> {
        Filter::new(self)
            .list_live_record_keys(molecule, limit, after, storage_prefix)
            .await
    }

    /// Schema-facing pages require selected bodies and exclude foreign layouts.
    pub(crate) async fn list_schema_record_keys_filtered<'a>(
        &self,
        molecule: &'a str,
        limit: usize,
        after: Option<&'a str>,
        hash_filter: Option<&'a str>,
        membership: SchemaKeyMembership<'a>,
        storage_prefix: Option<&'a str>,
    ) -> Result<KeyPage, SchemaError> {
        Filter::new(self)
            .list_schema_record_keys_filtered(
                molecule,
                limit,
                after,
                hash_filter,
                membership,
                storage_prefix,
            )
            .await
    }

    /// Check one exact Delete barrier for each returned molecule row.
    pub(crate) async fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<KeyedRecord>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .filter_delete_barriers_for_records(rows, storage_prefix)
            .await
    }

    /// Resolve exact keys in request order, retaining duplicate positions.
    pub(crate) async fn load_visible_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        Filter::new(self)
            .load_visible_per_key_records(storage_prefix, keys)
            .await
    }

    /// Read one exact prefix, then apply generation and Delete state.
    pub(crate) async fn scan_per_key_prefix(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .scan_per_key_prefix(base_prefix, storage_prefix)
            .await
    }

    /// Scan at most `limit` per-key records under the molecule record prefix.
    pub(crate) async fn scan_per_key_prefix_paged(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .scan_per_key_prefix_paged(base_prefix, storage_prefix, limit)
            .await
    }

    /// Scan at most `limit` per-key records in `base_start..base_end`.
    pub(crate) async fn scan_per_key_range_paged(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        Filter::new(self)
            .scan_per_key_range_paged(base_start, base_end, storage_prefix, limit)
            .await
    }
}
