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
    if offset.unwrap_or_default() != 0
        || QueryResponseFormat::current() == QueryResponseFormat::Count
    {
        return Err(HostError::new(
            400,
            "alternate pages use cursors and do not support count-only responses",
        ));
    }
    let limit = clamp_request_limit(limit);
    let _permit = acquire_read_permit(host, Lane::Interactive).await?;
    let page = host
        .fold_db()
        .query_executor()
        .query_alternate_page(query, limit, cursor, context)
        .await
        .map_err(HostError::from)?;
    let mut rows = HashMap::new();
    for row in format_rows(page.values, None, false) {
        let key: KeyValue = serde_json::from_value(row["key"].clone())
            .map_err(|_| HostError::internal("alternate response has an invalid primary key"))?;
        rows.insert(key, row);
    }
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = rows
            .remove(&entry.primary)
            .ok_or_else(|| HostError::internal("alternate response has no primary record"))?;
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let conflict_flags = annotate_conflict_flags(host, &mut results).await;
    if QueryResponseFormat::current() == QueryResponseFormat::Values {
        output::compact_rows(&mut results, conflict_flags);
    }
    let next = if page.has_more {
        page.entries
            .last()
            .map(|entry| QueryCursor::new(entry.matched.clone(), Some(page.comparison)))
    } else {
        None
    };
    let mut drops = RowDrops::default();
    drops.conflict_flags = conflict_flags;
    let mut payload = page_payload(results, None, limit, 0, page.has_more, drops);
    payload["next_cursor"] =
        serde_json::to_value(next).map_err(|_| HostError::internal("invalid alternate cursor"))?;
    Ok(payload)
}
