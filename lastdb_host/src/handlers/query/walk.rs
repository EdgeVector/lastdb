//! An unfiltered ascending read of a format 7 home: one page of the record
//! walk (`fold_db` `record_store/record_walk.rs`).
//!
//! The walk follows record key order, not page order. A record key starts
//! with the blinded hash, so only a read of every record finds the first
//! records in page order, and one request may examine at most 10,000 keys. A
//! walk page costs its offset and its rows, and a cursor page costs its rows.
//! The reply has no count (`total_count: null`); `has_more` and `next_cursor`
//! continue the walk, and `page_order: "record_key"` names the order of the
//! pages. The rows of one page keep page order. The cursor names a record key
//! position (`record`), not an API key: a page that a run of deleted records
//! ends early has no API key to name, and a schema with no hash key field has
//! no API key in its rows (`key_form: "opaque"`).

use super::execute::QueryRun;
use super::*;
use fold_db::db_operations::{PageResume, RECORD_WALK_ROWS};

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
    /// in record key order. One probe row past the page sets `has_more`, as
    /// does a page that a run of deleted records ended early. The cursor is
    /// the record key position of the last row the caller receives, or of
    /// the last key that an early end examined.
    pub(super) async fn record_walk(&self, query: Query) -> Result<Value, HostError> {
        let (host, ctx, limit, offset) = (self.host, self.ctx, self.limit, self.offset);
        let probe = limit.saturating_add(1);
        let window = match (&self.record, &self.cursor) {
            (Some(record), _) => KeyWindow::after_record(record.clone(), probe),
            (None, Some(after)) => KeyWindow::after(after.clone(), probe),
            (None, None) if offset.saturating_add(probe) > RECORD_WALK_ROWS => {
                return Err(HostError::new(
                    400,
                    format!(
                        "an unfiltered page reads at most {RECORD_WALK_ROWS} records from its \
                         start; continue with next_cursor instead of offset {offset}"
                    ),
                ));
            }
            (None, None) => KeyWindow::offset(offset, probe),
        };
        let _permit = acquire_read_permit(host, Lane::for_read_rows(probe)).await?;
        let (rows, drops, end) =
            execute_query_rows_windowed(host, query, Some(window), ctx).await?;
        let positions: HashMap<KeyValue, String> = end.walk.into_iter().collect();
        let placed = rows
            .into_iter()
            .map(|row| {
                let key = serde_json::from_value::<KeyValue>(row["key"].clone()).ok()?;
                Some((positions.get(&key)?.clone(), row))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| HostError::internal("a record walk row has no record key position"))?;
        // The first `limit` rows in record key order make the page, and they
        // keep page order. The rows past them are the probe. A row that
        // dropped after the walk took it still used a slot, so it can hide
        // the probe (as in `cursor_payload`). A dropped row after the last
        // row the caller receives is read again.
        let stopped = match end.resume {
            Some(PageResume::Record(at)) => Some(at),
            _ => None,
        };
        let has_more = placed.len().saturating_add(drops.total()) > limit || stopped.is_some();
        let mut ordered: Vec<&str> = placed.iter().map(|(at, _)| at.as_str()).collect();
        ordered.sort_unstable();
        let last_kept = limit
            .min(ordered.len())
            .checked_sub(1)
            .and_then(|at| ordered.get(at))
            .copied()
            .map(str::to_owned);
        let next_cursor = stopped
            .or_else(|| last_kept.clone().filter(|_| has_more))
            .map(|at| QueryCursor::at_record(&at, self.comparison.clone()));
        let rows = placed
            .into_iter()
            .filter(|(at, _)| last_kept.as_ref().is_some_and(|last| at <= last))
            .map(|(_, row)| row)
            .collect();
        let mut payload = page_payload(rows, None, limit, offset, has_more, drops);
        payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
        payload["page_order"] = Value::from(RECORD_KEY_ORDER);
        Ok(payload)
    }
}
