//! The execution shapes of `execute_query`, one method per shape.

use super::*;

/// The request-level inputs every query shape reads.
pub(super) struct QueryRun<'a, H: HostNode> {
    pub(super) host: &'a H,
    pub(super) ctx: &'a AccessContext,
    pub(super) limit: usize,
    pub(super) offset: usize,
    pub(super) cursor: Option<KeyValue>,
    pub(super) expected_total_count: Option<usize>,
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
        if let Some((page_offset, page_limit)) = caller_supplied_page(query) {
            if let Some(total) = count_rows(host, query, ctx).await? {
                let _permit = acquire_read_permit(host, Lane::for_read_rows(total)).await?;
                let (page, drops) =
                    execute_query_rows_with_unresolved(host, query.clone(), ctx).await?;
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

    /// Key-restricted push-down: hydrate ONLY the requested window.
    pub(super) async fn key_restricted(&self, query: Query) -> Result<Value, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        let expected_total_count = self.expected_total_count;
        let cursor = self.cursor.clone();
        // Key-restricted push-down: hydrate ONLY the requested window. Count the
        // partition only when the descending page calculation or an explicit COUNT
        // assertion requires it.
        //
        // Without this, every `HashKey` / `HashRange*` read — which is every product
        // read, since unfiltered scans are deprecated — fell to the fallback at the
        // bottom of this function and paid `O(partition)` atom hydrations to return
        // `O(page)` rows, on each page request. The push-down existed only for the
        // unfiltered "list all" shape we tell apps not to use.
        let total =
            if key_restricted_count_is_required(&query, cursor.as_ref(), expected_total_count) {
                count_rows(host, &query, ctx).await?
            } else {
                None
            };
        if let Some(total) = total {
            if let Some(expected) = expected_total_count {
                if expected != total {
                    return Err(reject_wrong_expected_total_count(expected, total));
                }
            }
        } else if expected_total_count.is_some() {
            return Err(HostError::new(
                400,
                "expected_total_count is unavailable for this query".to_string(),
            ));
        }

        // Descending range order retains ascending hashes for equal ranges.
        // A reverse slice of ascending keys cannot preserve that tie order
        // across hashes. Resolve only this explicit K-key batch, then use the
        // normal formatter and offset; single-hash requests keep push-down.
        // A tombstone-inclusive read declines the live count and likewise
        // needs its selected set before a descending offset can be chosen.
        if descending_cross_hash_batch(&query)
            || (matches!(query.sort_order, Some(SortOrder::Desc)) && total.is_none())
        {
            return self.bounded_fallback(query).await;
        }

        let overfetch_limit = limit.saturating_add(1);
        let window = if let Some(after) = cursor.clone() {
            KeyWindow::after(after, overfetch_limit)
        } else if let Some(total) = total {
            // Descending offset windows count from the high end, so this is
            // the key-restricted page shape where the exact count feeds the
            // node's own page selection.
            match page_filter_for(query.sort_order.as_ref(), total, offset, overfetch_limit) {
                HashRangeFilter::Page { offset, limit } => KeyWindow::offset(offset, limit),
                // `page_filter_for` only ever returns `Page`; treating
                // anything else as "no window" degrades to the old
                // full-materialize path rather than silently paging wrong.
                _ => KeyWindow::offset(0, total),
            }
        } else {
            KeyWindow::offset(offset, overfetch_limit)
        };
        // Admit on the page, not the partition — unlike the unfiltered branch
        // below, which admits on `total` because it may hydrate that many rows.
        let _permit = acquire_read_permit(host, Lane::for_read_rows(overfetch_limit)).await?;
        let uses_cursor = !matches!(query.sort_order, Some(SortOrder::Desc));
        let (page, drops) = execute_query_rows_windowed(host, query, Some(window), ctx).await?;
        if uses_cursor {
            return Ok(cursor_payload(page, total, limit, offset, drops));
        }
        if let Some(total) = total {
            return Ok(counted_offset_payload(page, total, limit, offset, drops));
        }
        Ok(cursor_payload(page, None, limit, offset, drops))
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
                let _permit = acquire_read_permit(host, Lane::for_read_rows(total)).await?;
                let (all_results, drops) =
                    execute_query_rows_with_unresolved(host, query, ctx).await?;
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
        query.filter = Some(page_filter_for(
            query.sort_order.as_ref(),
            total,
            offset,
            limit,
        ));
        let _permit = acquire_read_permit(host, Lane::for_read_rows(total)).await?;
        let (page, drops) = execute_query_rows_with_unresolved(host, query, ctx).await?;
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
        let lane = lane_for_unknown_cardinality(&query);
        let _permit = acquire_read_permit(host, lane).await?;
        let (all_results, drops) = execute_query_rows_with_unresolved(host, query, ctx).await?;
        let total_count = all_results.len();
        let page: Vec<Value> = all_results.into_iter().skip(offset).take(limit).collect();
        let has_more = compute_has_more(offset, page.len(), total_count, cap_was_injected);
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
