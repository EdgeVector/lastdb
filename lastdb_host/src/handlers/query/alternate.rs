//! Named alternate pages keep primary mutation keys and alternate page order.

use super::*;
use std::collections::HashMap;

pub(super) async fn execute<H: HostNode>(
    host: &H,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    context: &AccessContext,
) -> Result<Value, HostError> {
    if offset.unwrap_or_default() != 0 {
        return Err(HostError::new(
            400,
            "alternate pages use cursors rather than offsets",
        ));
    }
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.record.is_some())
    {
        return Err(HostError::new(
            400,
            "a record cursor continues only an unfiltered read; continue an alternate page with \
             the next_cursor of that page",
        ));
    }
    if QueryResponseFormat::current() == QueryResponseFormat::Count {
        if cursor.is_some() {
            return Err(HostError::new(400, "alternate counts do not use a cursor"));
        }
        let _permit = acquire_read_permit(host, Lane::Bulk).await?;
        let count = host
            .fold_db()
            .query_executor()
            .count_alternate_rows(&query, context)
            .await
            .map_err(HostError::from)?;
        return Ok(serde_json::json!({"total_count": count, "exact": true}));
    }
    let limit = clamp_request_limit(limit);
    let _permit = acquire_read_permit(host, Lane::for_read_rows(limit)).await?;
    let schema_name = query.schema_name.clone();
    let page = host
        .fold_db()
        .query_executor()
        .query_alternate_page(query, limit, cursor, context)
        .await
        .map_err(HostError::from)?;
    let compact = QueryResponseFormat::current() == QueryResponseFormat::Values;
    let mut rows = HashMap::new();
    for row in format_rows(page.values, compact) {
        let key: KeyValue = serde_json::from_value(row["key"].clone())
            .map_err(|_| HostError::internal("alternate response has an invalid primary key"))?;
        rows.insert(key, row);
    }
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = rows.remove(&entry.primary).unwrap_or_else(|| {
            let mut row = serde_json::json!({ "key": entry.primary, "fields": {} });
            if !compact {
                row["metadata"] = Value::Null;
            }
            row
        });
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let conflict_flags = annotate_conflict_flags(host, &schema_name, &mut results).await;
    let next = if page.has_more {
        page.entries
            .last()
            .map(|entry| QueryCursor::new(entry.matched.clone(), Some(page.comparison)))
    } else {
        None
    };
    let flags = PageFlags {
        conflict_flags,
        ..PageFlags::default()
    };
    let mut payload = page_payload(results, None, limit, 0, page.has_more, flags);
    payload["next_cursor"] =
        serde_json::to_value(next).map_err(|_| HostError::internal("invalid alternate cursor"))?;
    Ok(payload)
}
