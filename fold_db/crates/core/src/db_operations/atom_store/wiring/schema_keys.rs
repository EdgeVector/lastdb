//! Schema keys wiring: ready key membership and exact query tips.

use super::super::hash_selection::HashTipSelection;
use super::super::ports::{KeyedRecord, QueryTips, ReadyKeys};
use super::super::schema_keys::SchemaKeys;
use super::super::types::PerKeyRecord;
use super::super::AtomStore;
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;

impl ReadyKeys for AtomStore {
    async fn preloaded_window(
        &self,
        molecule: &str,
        selection: &HashTipSelection<'_>,
        scope: Option<&str>,
        include_tombstones: bool,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_window(molecule, selection, scope, include_tombstones)
            .await
    }

    async fn preloaded_hash_range_keys_page_window(
        &self,
        molecule: &str,
        scope: Option<&str>,
        take: Option<usize>,
        after: Option<(&str, &str)>,
    ) -> Result<Option<Vec<(String, String)>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_hash_range_keys_page_window(molecule, scope, take, after)
            .await
    }

    async fn preloaded_per_key_prefix(
        &self,
        prefix: &str,
        scope: Option<&str>,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_per_key_prefix(prefix, scope)
            .await
    }

    async fn preloaded_per_key_prefix_for_api(
        &self,
        prefix: &str,
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_per_key_prefix_for_api(prefix, scope, api_hash)
            .await
    }

    async fn preloaded_per_key_range(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_per_key_range(start, end, scope)
            .await
    }

    async fn preloaded_per_key_range_for_api(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> Result<Option<Vec<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .preloaded_per_key_range_for_api(start, end, scope, api_hash)
            .await
    }
}

impl QueryTips for AtomStore {
    async fn load_query_tip_records(
        &self,
        scope: Option<&str>,
        molecule: &str,
        keys: &[KeyValue],
    ) -> Result<Vec<Option<KeyedRecord>>, SchemaError> {
        AtomStore::load_query_tip_records(self, scope, molecule, keys).await
    }

    fn note_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
    ) -> Result<(), SchemaError> {
        AtomStore::note_selected_query_tip(self, scope, molecule, api_key, record, base)
    }
}

impl AtomStore {
    /// Exact semantic reads: each typed key, then one disk miss batch.
    pub(crate) async fn load_query_tip_records(
        &self,
        scope: Option<&str>,
        molecule: &str,
        keys: &[KeyValue],
    ) -> Result<Vec<Option<KeyedRecord>>, SchemaError> {
        SchemaKeys::new(self)
            .load_query_tip_records(scope, molecule, keys)
            .await
    }

    /// The location receipt of one selected query tip. See `SchemaKeys`.
    pub(crate) fn note_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
    ) -> Result<(), SchemaError> {
        SchemaKeys::new(self).note_selected_query_tip(scope, molecule, api_key, record, base)
    }

    /// The absence receipt of one exact query key.
    pub(crate) fn note_missing_query_key(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        base: &str,
    ) -> Result<(), SchemaError> {
        SchemaKeys::new(self).note_missing_query_key(scope, molecule, api_key, base)
    }
}
