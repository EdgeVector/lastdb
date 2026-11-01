//! An unfiltered ascending read of a format 7 home: one page of the record
//! walk (`fold_db` `atom_store/record_walk.rs`).
//!
//! The walk follows record key order, not page order. A record key starts
//! with the blinded hash, so only a read of every record finds the first
//! records in page order, and one request may examine at most 10,000 keys. A
//! walk page costs its offset and its rows, and a cursor page costs its rows.
//! The reply has no count (`total_count: null`); `has_more` and `next_cursor`
//! continue the walk, and `page_order: "record_key"` names the order of the
//! pages. The rows of one page keep page order.

use super::execute::QueryRun;
use super::*;
use fold_db::db_operations::RECORD_WALK_ROWS;

/// The `page_order` of a walk page.
const RECORD_KEY_ORDER: &str = "record_key";

/// True when an unfiltered read walks the records: an ascending read of the
/// primary key with no filter or a caller `Page`, and no post-key work.
/// Other unfiltered reads keep the complete bounded read.
pub(super) fn walks_records(query: &Query) -> bool {
    query.key_name.is_none()
        && matches!(query.filter, None | Some(HashRangeFilter::Page { .. }))
        && !query.requires_post_key_work()
        && !query.include_tombstones
        && !matches!(query.sort_order, Some(SortOrder::Desc))
}

/// The page size and the offset of a walk. A caller `Page` names them; the
/// walk itself reads with no filter.
pub(super) fn page_request(
    query: &mut Query,
    walk: bool,
    limit: Option<usize>,
    offset: Option<usize>,
) -> (Option<usize>, Option<usize>) {
    if !walk {
        return (limit, offset);
    }
    let Some(HashRangeFilter::Page {
        offset: page_offset,
        limit: page_limit,
    }) = query.filter
    else {
        return (limit, offset);
    };
    query.filter = None;
    (Some(page_limit), Some(page_offset))
}

impl<H: HostNode> QueryRun<'_, H> {
    /// One walk page: `limit` rows after the cursor, or after `offset` rows,
    /// in record key order. One probe row past the page sets `has_more`.
    pub(super) async fn record_walk(&self, query: Query) -> Result<Value, HostError> {
        let (host, ctx, limit, offset) = (self.host, self.ctx, self.limit, self.offset);
        let probe = limit.saturating_add(1);
        let window = match &self.cursor {
            Some(after) => KeyWindow::after(after.clone(), probe),
            None if offset.saturating_add(probe) > RECORD_WALK_ROWS => {
                return Err(HostError::new(
                    400,
                    format!(
                        "an unfiltered page reads at most {RECORD_WALK_ROWS} records from its \
                         start; continue with next_cursor instead of offset {offset}"
                    ),
                ));
            }
            None => KeyWindow::offset(offset, probe),
        };
        let schema_name = query.schema_name.clone();
        let _permit = acquire_read_permit(host, Lane::for_read_rows(probe)).await?;
        let (mut rows, drops) = execute_query_rows_windowed(host, query, Some(window), ctx).await?;
        let keys = rows
            .iter()
            .map(|row| serde_json::from_value::<KeyValue>(row["key"].clone()).ok())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| HostError::internal("a record walk row has no key"))?;
        let order = host
            .fold_db()
            .record_walk_order(&schema_name, &keys)
            .await
            .ok_or_else(|| HostError::internal("the record key order of the page is unknown"))?;
        // The rows past `limit` in record key order are the probe. A row that
        // dropped after the walk took it still used a slot, so it can hide
        // the probe (as in `cursor_payload`). The cursor names the last row
        // that the caller receives; a dropped row after it is read again.
        let has_more = rows.len().saturating_add(drops.total()) > limit;
        let kept: Vec<usize> = order.iter().take(limit).copied().collect();
        let next_cursor = kept
            .last()
            .filter(|_| has_more)
            .map(|last| QueryCursor::new(keys[*last].clone(), self.comparison.clone()));
        let kept: HashSet<usize> = kept.into_iter().collect();
        let mut at = 0;
        rows.retain(|_| {
            at += 1;
            kept.contains(&(at - 1))
        });
        let mut payload = page_payload(rows, None, limit, offset, has_more, drops);
        payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
        payload["page_order"] = Value::from(RECORD_KEY_ORDER);
        Ok(payload)
    }
}
