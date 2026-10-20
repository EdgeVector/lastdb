use super::{full_span_page, FutureExt, HashRangeQueryProcessor};
use crate::schema::types::field::{FieldVariant, HashRangeFilter, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};

mod keys;

impl HashRangeQueryProcessor {
    /// Count the distinct **live** rows a query over `schema` / `fields` would
    /// return -- the exact `total_count` for the paginated list path.
    ///
    /// **Tombstone-correct.** Resolves each candidate key's current value and
    /// excludes tombstones, matching the page materialization. Prefers the
    /// schema key field so count cost is not multiplied by field count. Also
    /// unions share-namespace keys. After that, applies the same named-layout
    /// retain as the page so a shared-molecule HashKey does not count sibling
    /// layouts the page dropped.
    ///
    /// `filter` scopes the count to the same rows the page path will read.
    /// `None` counts the schema's whole live row set. Passing a key-restricted
    /// filter counts that **partition** — which is what a `HashKey` read's
    /// `total_count` has to mean, or the caller pages a partition against the
    /// schema's row count and `has_more` never goes false. Only key-restricted
    /// filters belong here: `Page` / `PageAfter` bound how many rows come back
    /// rather than which ones exist, so counting under one would report the
    /// page size as the total.
    pub async fn count_rows(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        Self::require_supported_query(schema, filter)?;
        self.current_query_scope(self.count_rows_captured(schema, fields, filter).boxed())
            .await
    }

    async fn count_rows_captured(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        debug_assert!(
            filter.is_none_or(HashRangeFilter::is_key_restricted),
            "count_rows takes a key-restricted filter or none; \
             a scan-bounding filter would report the page size as the total"
        );
        let received_from_namespaces: Vec<String> = self.collect_received_from_namespaces().await?;

        if filter.is_none() {
            let selected = Self::cokey_selected_fields(schema, fields);
            if selected.is_empty() {
                return Ok(0);
            }
            let (_, _, keys, _) = self
                .resolve_inspection_primary_page(
                    schema,
                    &selected,
                    None,
                    false,
                    &received_from_namespaces,
                )
                .await?;
            return Ok(keys.len());
        }

        let selected = Self::cokey_selected_fields(schema, fields);
        if selected.is_empty() {
            return Ok(0);
        }
        let primary = Self::cokey_primary_field(schema, &selected);
        if Self::needs_named_layout_membership(schema, Some(&primary), filter) {
            let (_, _, keys, _) = self
                .resolve_cokey_primary_page(
                    schema,
                    &selected,
                    filter.cloned(),
                    None,
                    false,
                    &received_from_namespaces,
                )
                .await?;
            return Ok(keys.len());
        }
        let count_field_names = Self::count_field_names(schema, fields);
        let mut keys = self
            .gather_count_keys(
                schema,
                &count_field_names,
                filter,
                &received_from_namespaces,
            )
            .await?;
        let used_named_spine = !keys.is_empty();
        if !used_named_spine {
            self.extend_count_keys_from_fallback(
                schema,
                fields,
                filter,
                &received_from_namespaces,
                &mut keys,
            )
            .await?;
        }
        Ok(keys.len())
    }

    /// Live keys for one field under an optional storage prefix, using the
    /// molecule tombstone flag only (no atom body loads).
    async fn live_field_keys_flag_only(
        &self,
        field: &FieldVariant,
        namespace: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) -> Result<Vec<KeyValue>, SchemaError> {
        Ok(self
            .live_field_matches_flag_only(field, namespace, filter)
            .await?
            .into_iter()
            .map(|(kv, _, _, _, _)| kv)
            .collect())
    }

    /// Tombstone-filtered molecule matches for one field (no atom body loads).
    async fn live_field_matches_flag_only(
        &self,
        field: &FieldVariant,
        namespace: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
        let mut working = field.cloned_without_molecule();
        if let Some(ns) = namespace {
            working
                .common_mut()
                .set_storage_prefix(Some(ns.to_string()));
        }
        working
            .collect_matches(
                &self.db_ops,
                Some(filter.cloned().unwrap_or_else(full_span_page)),
                false,
            )
            .await
    }

    /// Pick the field set whose live keys ARE the query's row set.
    ///
    /// This has to name the same field the read uses as its key spine, because
    /// [`count_rows`](Self::count_rows) UNIONS the key sets of whatever it is
    /// given. The read is a co-key plan: `cokey_primary_field` picks the
    /// declared key field whenever it exists; that primary alone establishes
    /// the page of keys, and secondaries only fill values into keys the primary
    /// already produced. So a union over several projected fields counts keys
    /// that can never become rows.
    ///
    /// Returning every selected field is what made `total_count` exceed the
    /// table. Measured on the live primary, fkanban `Milestone` — 13,965
    /// records, neither projection naming the key field:
    ///
    /// ```text
    /// fields = ["slug"]           total_count = 14,191
    /// fields = ["slug","title"]   total_count = 22,216   <- union of two key sets
    /// ```
    ///
    /// A count that rises when you ask for another column, and that can exceed
    /// the number of rows in the table, is not counting rows. Mirroring the
    /// read's primary keeps `total_count` and `has_more` describing one row set.
    fn count_field_names(schema: &Schema, fields: &[String]) -> Vec<String> {
        let selected = Self::cokey_selected_fields(schema, fields);
        if selected.is_empty() {
            return Vec::new();
        }
        vec![Self::cokey_primary_field(schema, &selected)]
    }

    /// Projection fallback for a multi-key sibling whose own key field has no
    /// atoms. This is consulted only after the preferred key spine counted no
    /// rows, matching `query_cokey_with_sources`.
    fn count_fallback_field(schema: &Schema, fields: &[String]) -> Option<String> {
        let primary = Self::count_field_names(schema, fields).into_iter().next()?;
        let selected = Self::cokey_selected_fields(schema, fields);
        Self::cokey_fallback_field(&selected, &primary).map(ToString::to_string)
    }
}
