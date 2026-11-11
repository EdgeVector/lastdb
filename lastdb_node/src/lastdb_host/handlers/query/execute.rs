//! Query execution paths. Pure request and keyed-page rules live in `rules`.

use super::*;
use fold_db::db_operations::PageResume;

/// The request-level inputs every keyed query shape reads.
pub(super) struct QueryRun<'a, H: HostNode> {
    pub(super) host: &'a H,
    pub(super) limit: usize,
    pub(super) offset: usize,
    pub(super) cursor: Option<KeyValue>,
    pub(super) comparison: Option<String>,
}

impl<H: HostNode> QueryRun<'_, H> {
    /// Choose and execute the keyed query paths in their rejection order.
    pub(super) async fn execute(self, query: Query) -> Result<Value, HostError> {
        if QueryResponseFormat::current() == QueryResponseFormat::Count {
            return self.count_only(query).await;
        }
        if rules::can_push_down_key_restricted(&query) {
            return self.key_restricted(query).await;
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
            return Err(HostError::new(
                400,
                "count replies require keyed live reads without continuation",
            ));
        }
        let count = count_rows(self.host, &query)
            .await?
            .ok_or_else(|| HostError::new(400, "exact keyed count is unavailable"))?;
        if query
            .expected_total_count
            .is_some_and(|expected| expected != count)
        {
            return Err(HostError::new(
                409,
                "expected_total_count differs from the exact keyed count",
            ));
        }
        Ok(serde_json::json!({"total_count": count, "exact": true}))
    }

    /// Count only when required, select the page, and render it.
    async fn key_restricted(&self, query: Query) -> Result<Value, HostError> {
        let host = self.host;
        let (limit, offset) = (self.limit, self.offset);
        let total = if rules::key_restricted_count_is_required(&query, self.cursor.as_ref()) {
            count_rows(host, &query).await?
        } else {
            None
        };
        let Some(window) = rules::keyed_window(&query, limit, offset, self.cursor.as_ref(), total)?
        else {
            return self.bounded_fallback(query).await;
        };

        let uses_cursor = !matches!(query.sort_order, Some(SortOrder::Desc));
        let (page, flags, end) = execute_query_rows_windowed(host, query, Some(window)).await?;
        Ok(match (uses_cursor, total) {
            (false, Some(total)) => overfetched_offset_payload(page, total, limit, offset, flags),
            (_, total) => {
                let resume = end.resume.map(|PageResume::Key(key)| key);
                let comparison = self.comparison.clone();
                cursor_payload(page, total, limit, offset, flags, resume, comparison)
            }
        })
    }

    /// Fallback for cross-hash descending keyed batches or unknown schemas.
    async fn bounded_fallback(&self, query: Query) -> Result<Value, HostError> {
        let (all_results, flags, _) = execute_query_rows_windowed(self.host, query, None).await?;
        Ok(materialized_page_payload(
            all_results,
            self.limit,
            self.offset,
            flags,
            false,
        ))
    }
}
