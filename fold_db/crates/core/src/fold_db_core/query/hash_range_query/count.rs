use futures::FutureExt;

use super::{cokey_primary::CokeyPrimaryPage, field_rules, HashRangeQueryProcessor};
use crate::schema::types::field::HashRangeFilter;
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
        let comparison = self.db_ops.atoms().key_comparison_context(schema).await?;
        let filter = comparison.canonical_filter(filter.cloned())?;
        self.current_query_scope(
            comparison,
            self.count_rows_captured(schema, fields, filter.as_ref())
                .boxed(),
        )
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

        let selected = field_rules::selected_fields(schema, fields);
        if selected.is_empty() {
            return Ok(0);
        }
        if filter.is_none() {
            let CokeyPrimaryPage { page_keys, .. } = self
                .resolve_inspection_primary_page(
                    schema,
                    &selected,
                    None,
                    false,
                    &received_from_namespaces,
                )
                .await?;
            return Ok(page_keys.len());
        }

        let primary = field_rules::primary_field(schema, &selected);
        if Self::needs_named_layout_membership(schema, Some(&primary), filter) {
            let CokeyPrimaryPage { page_keys, .. } = self
                .resolve_cokey_primary_page(
                    schema,
                    &selected,
                    filter.cloned(),
                    None,
                    false,
                    &received_from_namespaces,
                )
                .await?;
            return Ok(page_keys.len());
        }
        // The primary alone defines membership. Never union projected fields
        // into a nonempty primary set; sparse fields cannot add records.
        let keys = self
            .count_field_keys(schema, &primary, filter, &received_from_namespaces)
            .await?;
        if !keys.is_empty() {
            return Ok(keys.len());
        }
        let Some(fallback) = field_rules::fallback_field(&selected, &primary) else {
            return Ok(0);
        };
        Ok(self
            .count_field_keys(schema, fallback, filter, &received_from_namespaces)
            .await?
            .len())
    }
}
