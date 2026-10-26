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
}

impl<H: HostNode> QueryRun<'_, H> {
    /// A caller-supplied bare `Page` filter is already the requested bounded
    /// fetch. `Ok(None)` means this shape does not apply.
    pub(super) async fn caller_supplied_page(
        &self,
        query: &Query,
    ) -> Result<Option<Value>, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        // A caller-supplied bare Page filter is already the requested bounded fetch;
        // it still needs the cheap exact count so pagination metadata reflects the
        // full live set.
        if let Some((page_offset, page_limit)) = rules::caller_supplied_page(query) {
            if matches!(query.sort_order, Some(SortOrder::Desc)) {
                // Across hashes a reversed ascending window reverses hash ties.
                // Inspection certifies at most INTERNAL_FETCH_CAP candidates;
                // sort that complete logical set before taking the window.
                let mut complete = query.clone();
                complete.filter = Some(HashRangeFilter::Page {
                    offset: 0,
                    limit: INTERNAL_FETCH_CAP,
                });
                let _permit = acquire_read_permit(host, Lane::Bulk).await?;
                let (rows, drops) = execute_query_rows_windowed(host, complete, None, ctx).await?;
                let total = rows.len();
                let page: Vec<_> = rows
                    .into_iter()
                    .skip(page_offset)
                    .take(page_limit)
                    .collect();
                let has_more = page_offset.saturating_add(page.len()) < total;
                return Ok(Some(page_payload(
                    page,
                    Some(total),
                    page_limit,
                    page_offset,
                    has_more,
                    drops,
                )));
            }
            if let Some(total) = count_rows(host, query, ctx).await? {
                let _permit = acquire_read_permit(host, Lane::Bulk).await?;
                let (page, drops) =
                    execute_query_rows_windowed(host, query.clone(), None, ctx).await?;
                // `total` is an index count, so it counts rows the materializer will
                // not deliver: a tip whose atom body could not be hydrated, and a
                // row whose body is a content tombstone (the index gates on
                // `KeyMetadata.tombstoned`, which a content tombstone need not set).
                // Add both back before comparing against it, or a page ending on one
                // reports `has_more` forever and the caller walks an offset that
                // never advances.
                let consumed = page_offset
                    .saturating_add(page.len())
                    .saturating_add(drops.total());
                let has_more = consumed < total;
                return Ok(Some(page_payload(
                    page,
                    Some(total),
                    page_limit,
                    page_offset,
                    has_more,
                    drops,
                )));
            }
        }
        Ok(None)
    }

    /// Count only when required, select the page, admit the read, and render it.
    pub(super) async fn key_restricted(&self, query: Query) -> Result<Value, HostError> {
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
        if uses_cursor {
            return Ok(cursor_payload(
                page,
                total,
                limit,
                offset,
                drops,
                self.comparison.clone(),
            ));
        }
        if let Some(total) = total {
            return Ok(counted_offset_payload(page, total, limit, offset, drops));
        }
        Ok(cursor_payload(
            page,
            None,
            limit,
            offset,
            drops,
            self.comparison.clone(),
        ))
    }

    /// Unfiltered "list all", no value filters: count rows cheaply, then fetch
    /// ONLY the requested page. `Ok(None)` means a view or unknown schema.
    pub(super) async fn unfiltered_pushdown(
        &self,
        query: &Query,
    ) -> Result<Option<Value>, HostError> {
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

            return self.offset_paged(query, total).await.map(Some);
        }
        Ok(None)
    }

    /// Cross-hash administrative pages continue by offset, without a key cursor.
    async fn offset_paged(&self, mut query: Query, total: usize) -> Result<Value, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        query.filter = Some(rules::page_filter_for(
            query.sort_order.as_ref(),
            total,
            offset,
            limit,
        ));
        let _permit = acquire_read_permit(host, Lane::Bulk).await?;
        let (page, drops) = execute_query_rows_windowed(host, query, None, ctx).await?;
        // Same index-count reconciliation as the caller-supplied-page shape
        // above: dropped rows were counted by `total`, so they have to be
        // counted as consumed here too.
        let consumed = offset
            .saturating_add(page.len())
            .saturating_add(drops.total());
        let has_more = consumed < total;
        Ok(page_payload(
            page,
            Some(total),
            limit,
            offset,
            has_more,
            drops,
        ))
    }

    /// Fallback / filtered / value-filtered path.
    pub(super) async fn bounded_fallback(&self, mut query: Query) -> Result<Value, HostError> {
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
        let total_count = all_results.len();
        let page: Vec<Value> = all_results.into_iter().skip(offset).take(limit).collect();
        // Native inspection proves its bounded set is complete, including an
        // exact ceiling-sized set. Views keep their existing truncation signal.
        let has_more = compute_has_more(offset, page.len(), total_count, cap_may_truncate);
        Ok(page_payload(
            page,
            Some(total_count),
            limit,
            offset,
            has_more,
            drops,
        ))
    }
}
