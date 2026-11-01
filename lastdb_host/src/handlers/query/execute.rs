//! Query execution paths. Pure request and keyed-page rules live in `rules`.

use super::*;

/// The request-level inputs every query shape reads.
pub(super) struct QueryRun<'a, H: HostNode> {
    pub(super) host: &'a H,
    pub(super) ctx: &'a AccessContext,
    pub(super) limit: usize,
    pub(super) offset: usize,
    pub(super) cursor: Option<KeyValue>,
    pub(super) comparison: Option<String>,
    /// An unfiltered read of a format 7 home walks its records (`walk.rs`).
    pub(super) walk: bool,
}

impl<H: HostNode> QueryRun<'_, H> {
    /// Choose and execute the existing query paths in their rejection order.
    pub(super) async fn execute(self, query: Query) -> Result<Value, HostError> {
        if QueryResponseFormat::current() == QueryResponseFormat::Count {
            return self.count_only(query).await;
        }
        if self.walk {
            return self.record_walk(query).await;
        }
        if let Some(payload) = self.caller_supplied_page(&query).await? {
            return Ok(payload);
        }
        if rules::can_push_down_key_restricted(&query) {
            return self.key_restricted(query).await;
        }
        if rules::can_push_down(&query) {
            if let Some(payload) = self.unfiltered_pushdown(&query).await? {
                return Ok(payload);
            }
        }
        self.bounded_fallback(query).await
    }

    /// Count under the same keyed scope without a probe row or reply metadata.
    async fn count_only(&self, query: Query) -> Result<Value, HostError> {
        if !rules::can_push_down_key_restricted(&query)
            || query.include_tombstones
            || self.cursor.is_some()
            || self.offset != 0
        {
            return Err(HostError::new(400, "count replies require keyed live reads without conditions, value ordering, or continuation"));
        }
        let _permit = acquire_read_permit(self.host, Lane::Bulk).await?;
        let (count, drops) = fold_db::db_operations::with_query_row_drop_tally(count_rows(
            self.host, &query, self.ctx,
        ))
        .await;
        let count =
            count?.ok_or_else(|| HostError::new(400, "exact keyed count is unavailable"))?;
        if query
            .expected_total_count
            .is_some_and(|expected| expected != count)
        {
            return Err(HostError::new(
                409,
                "expected_total_count differs from the exact keyed count",
            ));
        }
        Ok(serde_json::json!({"total_count": count, "exact": true,
            "unresolved_rows": drops.unresolved, "tombstoned_rows": drops.tombstoned}))
    }

    /// A caller-supplied bare `Page` filter is already the requested bounded
    /// fetch. `Ok(None)` means this shape does not apply.
    async fn caller_supplied_page(&self, query: &Query) -> Result<Option<Value>, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let Some((page_offset, page_limit)) = rules::caller_supplied_page(query) else {
            return Ok(None);
        };
        if matches!(query.sort_order, Some(SortOrder::Desc)) {
            // Across hashes a reversed ascending window reverses hash ties.
            // Sort the complete bounded set before taking the requested page.
            let mut complete = query.clone();
            complete.filter = Some(HashRangeFilter::Page {
                offset: 0,
                limit: INTERNAL_FETCH_CAP,
            });
            let _permit = acquire_read_permit(host, Lane::Bulk).await?;
            let (rows, drops) = execute_query_rows_windowed(host, complete, None, ctx).await?;
            return Ok(Some(materialized_page_payload(
                rows,
                page_limit,
                page_offset,
                drops,
                false,
            )));
        }
        if let Some(total) = count_rows(host, query, ctx).await? {
            let _permit = acquire_read_permit(host, Lane::Bulk).await?;
            let (page, drops) = execute_query_rows_windowed(host, query.clone(), None, ctx).await?;
            return Ok(Some(counted_page_payload(
                page,
                total,
                page_limit,
                page_offset,
                drops,
            )));
        }
        Ok(None)
    }

    /// Count only when required, select the page, admit the read, and render it.
    async fn key_restricted(&self, query: Query) -> Result<Value, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        let total = if rules::key_restricted_count_is_required(&query, self.cursor.as_ref()) {
            count_rows(host, &query, ctx).await?
        } else {
            None
        };
        let Some(window) = rules::keyed_window(&query, limit, offset, self.cursor.as_ref(), total)?
        else {
            return self.bounded_fallback(query).await;
        };

        let overfetch_limit = limit.saturating_add(1);
        // Admit on the page, not the partition — unlike the unfiltered branch
        // below, which admits on `total` because it may hydrate that many rows.
        let _permit = acquire_read_permit(host, Lane::for_read_rows(overfetch_limit)).await?;
        let uses_cursor = !matches!(query.sort_order, Some(SortOrder::Desc));
        let (page, drops) = execute_query_rows_windowed(host, query, Some(window), ctx).await?;
        Ok(match (uses_cursor, total) {
            (false, Some(total)) => overfetched_offset_payload(page, total, limit, offset, drops),
            (_, total) => {
                cursor_payload(page, total, limit, offset, drops, self.comparison.clone())
            }
        })
    }

    /// Unfiltered "list all", no value filters: count rows cheaply, then fetch
    /// ONLY the requested page. `Ok(None)` means a view or unknown schema.
    async fn unfiltered_pushdown(&self, query: &Query) -> Result<Option<Value>, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        let mut query = query.clone();
        if matches!(query.sort_order, Some(SortOrder::Desc)) {
            return self.bounded_fallback(query).await.map(Some);
        }
        // Unfiltered "list all", no value filters → paginated push-down: count rows
        // cheaply for an exact `total_count`, then fetch ONLY the requested page.
        // `count == None` ⇒ a view (or unknown schema): fall through.
        if let Some(total) = count_rows(host, &query, ctx).await? {
            // Full-enumeration consistency: a client listing the WHOLE set
            // (offset 0, limit at the ceiling — fkanban list/search) materializes
            // it in ONE snapshot so a concurrent insert can't shift page
            // boundaries and skip a row. Above the cap, keep the paged push-down.
            let wants_full_enumeration = offset == 0
                && limit >= crate::pagination::MAX_QUERY_LIMIT
                && total <= INTERNAL_FETCH_CAP;
            if wants_full_enumeration {
                query.filter = Some(HashRangeFilter::Page {
                    offset: 0,
                    limit: INTERNAL_FETCH_CAP,
                });
                let _permit = acquire_read_permit(host, Lane::Bulk).await?;
                let (all_results, drops) =
                    execute_query_rows_windowed(host, query, None, ctx).await?;
                // `total_count` is the counted set on every shape, so two limits
                // seconds apart cannot disagree about how big the set is. If the
                // snapshot came up short of the count, `has_more` says so —
                // reporting the short length as the total with `has_more: false`
                // is what let a partial read look like the whole set.
                let has_more = all_results.len() < total;
                return Ok(Some(page_payload(
                    all_results,
                    Some(total),
                    limit,
                    0,
                    has_more,
                    drops,
                )));
            }

            query.filter = Some(rules::page_filter_for(
                query.sort_order.as_ref(),
                total,
                offset,
                limit,
            ));
            let _permit = acquire_read_permit(host, Lane::Bulk).await?;
            let (page, drops) = execute_query_rows_windowed(host, query, None, ctx).await?;
            return Ok(Some(counted_page_payload(
                page, total, limit, offset, drops,
            )));
        }
        Ok(None)
    }

    /// Fallback / filtered / value-filtered path.
    async fn bounded_fallback(&self, mut query: Query) -> Result<Value, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        // Fallback / filtered / value-filtered path: a no-filter query here (a view,
        // or one carrying value_filters) gets a bounded `Page` fetch capped at
        // INTERNAL_FETCH_CAP; `has_more` flags the ceiling. A caller-supplied filter
        // is honored as-is.
        let cap_was_injected = query.filter.is_none();
        let cap_may_truncate = cap_was_injected
            && host
                .fold_db()
                .schema_manager()
                .get_schema_metadata(&query.schema_name)
                .map_err(|error| HostError::internal(error.to_string()))?
                .is_none();
        if cap_was_injected {
            query.filter = Some(HashRangeFilter::Page {
                offset: 0,
                limit: INTERNAL_FETCH_CAP,
            });
        }

        // Value-filtered / view fallback: cardinality is unknown up front (no cheap
        // count). A full-cap Page materialization (injected INTERNAL_FETCH_CAP
        // or an equivalent caller limit) can walk up to 10k rows — admit as Bulk so it
        // cannot monopolise the interactive reservation. Smaller bounded pages stay
        // Interactive.
        let lane = rules::lane_for_unknown_cardinality(&query);
        let _permit = acquire_read_permit(host, lane).await?;
        let (all_results, drops) = execute_query_rows_windowed(host, query, None, ctx).await?;
        // Native inspection proves its bounded set is complete, including an
        // exact ceiling-sized set. Views keep their existing truncation signal.
        Ok(materialized_page_payload(
            all_results,
            limit,
            offset,
            drops,
            cap_may_truncate,
        ))
    }
}
