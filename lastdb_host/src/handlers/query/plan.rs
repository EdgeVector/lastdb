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
        let cursor = self.cursor.clone();
        let mut query = query.clone();
        // Unfiltered "list all", no value filters → paginated push-down: count rows
        // cheaply for an exact `total_count`, then fetch ONLY the requested page.
        // `count == None` ⇒ a view (or unknown schema): fall through.
        if let Some(total) = count_rows(host, &query, ctx).await? {
            if let Some(after) = cursor.clone() {
                query.filter = Some(HashRangeFilter::PageAfter {
                    after,
                    limit: limit.saturating_add(1),
                });
                let _permit = acquire_read_permit(host, Lane::for_read_rows(total)).await?;
                let (page, drops) = execute_query_rows_with_unresolved(host, query, ctx).await?;
                return Ok(Some(cursor_payload(page, Some(total), limit, 0, drops)));
            }

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

            return self.offset_paged_with_cursor(query, total).await.map(Some);
        }
        Ok(None)
    }

    /// The offset-paged push-down: fetch the page, and stamp a cursor on THIS
    /// shape only (never for descending order).
    async fn offset_paged_with_cursor(
        &self,
        mut query: Query,
        total: usize,
    ) -> Result<Value, HostError> {
        let (host, ctx) = (self.host, self.ctx);
        let (limit, offset) = (self.limit, self.offset);
        let sort_order_for_cursor = query.sort_order.clone();
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
        // Stamp a cursor on THIS shape, and only this one.
        //
        // `page_payload`'s contract is "no cursor from an offset-paged
        // shape", written after a key-restricted read advertised a cursor it
        // would then ignore on the way back in. That reasoning does not
        // reach here: this is the unfiltered `can_push_down` branch, and the
        // cursor arm a few lines above (`PageAfter`, same branch, same
        // `query.filter.is_none()` precondition) is exactly where an
        // incoming cursor IS consumed. So this shape both honours a cursor
        // and — until now — refused to hand one out, which left a whole-set
        // drain with no exactly-once option at all: offset addresses the
        // index's key space while the caller advances by rows delivered, and
        // any row dropped between the two makes the next window overlap the
        // last. `PageAfter` filters strictly past the caller's last row in
        // both the storage window and the in-memory apply, so it cannot
        // repeat or skip regardless of how many rows drop.
        //
        // Measured on the primary's `Papercut` partition, 2026-08-09: a
        // limit-500 offset drain served 1037 rows containing 606 distinct,
        // page 2 adding 92 new rows out of 446.
        //
        // Descending is deliberately excluded, and by the same rule that
        // motivated the original prohibition: `usable_cursor` drops a
        // descending caller's cursor on the way in, and `format_rows` has
        // already reversed the page, so `page.last()` names the *smallest*
        // row while `PageAfter` walks strictly upward. Advertising one there
        // would be advertising a cursor this shape will not honour.
        let next_cursor = match sort_order_for_cursor {
            Some(SortOrder::Desc) => None,
            _ => next_cursor_for(&page, has_more),
        };
        let mut payload = page_payload(page, Some(total), limit, offset, has_more, drops);
        payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
        Ok(payload)
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
        // count). A full-cap Page/PageAfter materialization (injected INTERNAL_FETCH_CAP
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
