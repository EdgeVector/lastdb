//! Themed module split from the parent.

use super::*;

/// The `has_more`-carrying page shape both sockets serve, before the envelope.
///
/// `next_cursor` is always `null` here. Only [`cursor_payload`] — the
/// `PageAfter` push-down, the one shape whose next request actually consumes a
/// cursor — stamps a real one. Emitting a cursor from an offset-paged shape is
/// what produced the non-terminating drain: `execute_query` only reads the
/// caller's cursor inside the `can_push_down` branch (`query.filter.is_none()`),
/// so a key-restricted read (`HashKey(..)`, i.e. every product read) advertised
/// a cursor, ignored it on the way back in, re-served page 1, and any client
/// preferring the cursor over the offset looped until its own row ceiling.
pub(in crate::handlers) fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    drops: RowDrops,
) -> Value {
    let returned_count = page.len();
    let unresolved_rows = drops.unresolved;
    let total_count = total_count.map_or(Value::Null, Value::from);
    serde_json::json!({
        "results": Value::Array(page),
        "total_count": total_count,
        "returned_count": returned_count,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
        "next_cursor": Value::Null,
        // ALWAYS present, including `0`. This field was previously omitted when
        // zero, which reads as tidy and is the wrong shape for an integrity
        // signal: a client asserting `unresolved_rows == 0` cannot distinguish
        // "this node counted zero drops" from "this node never counts", so the
        // assertion silently passes against every node that does not emit it —
        // exactly the read-back-is-the-only-confirmation failure this field was
        // added to end. A completeness check has to be able to fail closed, so
        // the absence of the key must mean "unsupported", never "clean".
        "unresolved_rows": unresolved_rows,
        // The other half of "counted by the index, not delivered to you": rows
        // whose atom body is a content tombstone. `total_count` gates on
        // `KeyMetadata.tombstoned` only, so these are counted, spend a page
        // slot, and used to vanish without a signal — which is why a drain over
        // a partition with deletions re-served rows and never terminated.
        // Always present, including `0`, for the same fail-closed reason.
        "tombstoned_rows": drops.tombstoned,
        // Whether `key.hash` on these rows can be fed back as a key.
        //
        // `"api"` — addressable, point-read it. `"opaque"` — every key is a
        // one-way storage token; a keyed read at it resolves NOTHING, which is
        // the failure that reads as missing data. `"mixed"` — treat every key
        // as unverified. `null` — this query emitted no classifiable page key
        // (a keyed fast-path read, or an empty result).
        //
        // Absent key means "this node does not classify", never "addressable":
        // the same fail-closed rule `unresolved_rows` documents above.
        "key_form": drops.key_form.map_or(Value::Null, |form| Value::from(form.as_str())),
        // Always present. `"unknown"` is not clean. A client that treats a
        // missing `has_conflicts` as clean must also see this field, or a
        // page with no stamp looks the same as a complete empty stamp.
        "conflict_flags": drops.conflict_flags,
    })
}

pub(in crate::handlers) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    drops: RowDrops,
) -> Value {
    // This shape over-fetches `limit + 1` and infers `has_more` from the extra
    // row. Dropped rows never reach `page`, so a page short by a drop would read
    // as "no more rows" and silently truncate the rest of the partition —
    // trading a loud 400 for quiet data loss. Count them back in: they were
    // fetched, they just were not delivered. Content tombstones count for
    // exactly the same reason dangling rows do, and omitting them was the whole
    // defect: on a partition where two rows in five are tombstoned, every page
    // came back short and the fifth page looked like the end of the set.
    let fetched = page.len().saturating_add(drops.total());
    let has_more = fetched > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    // Derive the cursor from the truncated page: it has to name the last row the
    // caller actually received, not the `limit + 1`th probe row, or the next
    // `PageAfter` skips a row.
    let next_cursor = next_cursor_for(&page, has_more);
    let mut payload = page_payload(page, total_count, limit, offset, has_more, drops);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

pub(in crate::handlers) fn counted_offset_payload(
    mut page: Vec<Value>,
    total_count: usize,
    limit: usize,
    offset: usize,
    drops: RowDrops,
) -> Value {
    let fetched = page.len().saturating_add(drops.total());
    let has_more = fetched > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    page_payload(page, Some(total_count), limit, offset, has_more, drops)
}

// The paginated push-down applies only to a true "list all" query: no
// caller-supplied key filter (so the node owns the fetch) and no value filters
// (those need every candidate materialized to evaluate).

/// Key/partition filters that do **not** require a full-schema product scan.
/// `Page` / `PageAfter` / `SampleN` alone still walk the schema and are treated
/// as unfiltered product scans for the deprecation gate.
///
/// The predicate itself lives on [`HashRangeFilter`] because the count path
/// keys off the same distinction: a key-restricted read counts and pages its
/// partition, an unfiltered one the whole schema. Two copies drifting apart is
/// what let a key-restricted read pass the scan gate but miss the push-down.
pub(in crate::handlers) fn is_key_restricted_filter(filter: Option<&HashRangeFilter>) -> bool {
    filter.is_some_and(HashRangeFilter::is_key_restricted)
}

pub(in crate::handlers) const KEYED_FILTER_VARIANTS: &[&str] = &[
    "HashKey",
    "HashRangeKey",
    "HashRangePrefix",
    "HashRangeRange",
    "HashRangeKeys",
    "RangeKey",
    "RangePrefix",
    "RangeRange",
    "HashRange",
];

pub(in crate::handlers) fn can_push_down(query: &Query) -> bool {
    query.filter.is_none() && has_no_post_key_stage(query)
}

/// Can this key-restricted read be counted and windowed instead of
/// materialized whole?
///
/// The filter has to select a **key set** (so the count describes the same rows
/// the page will), and nothing may decide row membership after the keys are
/// read. `value_filters` drop rows by their hydrated content,
/// `field_predicates` / `order_by` / `predicate_limit` run the two-pass plan
/// over every candidate — each would be answering from an arbitrary slice of
/// its input if a window were taken first. Those shapes keep the
/// materialize-then-slice fallback.
pub(in crate::handlers) fn can_push_down_key_restricted(query: &Query) -> bool {
    is_key_restricted_filter(query.filter.as_ref()) && has_no_post_key_stage(query)
}

/// The exact keyed count is required only for a descending offset page, where
/// it chooses the window from the high end, or when a caller asks the node to
/// verify a keyed COUNT assertion. Ordinary ascending keyed pages use a cursor
/// and must not pay a partition count merely to populate metadata.
pub(in crate::handlers) fn key_restricted_count_is_required(
    query: &Query,
    cursor: Option<&KeyValue>,
    expected_total_count: Option<usize>,
) -> bool {
    expected_total_count.is_some()
        || (cursor.is_none() && matches!(query.sort_order.as_ref(), Some(SortOrder::Desc)))
}

pub(in crate::handlers) fn reject_wrong_expected_total_count(
    expected: usize,
    actual: usize,
) -> HostError {
    let _ = (expected, actual);
    HostError::new(
        409,
        "expected_total_count did not match the keyed count".to_string(),
    )
}

/// No stage that decides row membership after the key set is read.
pub(in crate::handlers) fn has_no_post_key_stage(query: &Query) -> bool {
    query.value_filters.is_none()
        && query.field_predicates.is_none()
        && query.order_by.is_none()
        && query.predicate_limit.is_none()
}

pub(in crate::handlers) fn caller_supplied_page(query: &Query) -> Option<(usize, usize)> {
    match (
        &query.filter,
        &query.value_filters,
        &query.field_predicates,
        &query.order_by,
        &query.predicate_limit,
    ) {
        (Some(HashRangeFilter::Page { offset, limit }), None, None, None, None) => {
            Some((*offset, *limit))
        }
        _ => None,
    }
}

pub(in crate::handlers) fn usable_cursor(
    cursor: Option<KeyValue>,
    sort_order: Option<&SortOrder>,
) -> Option<KeyValue> {
    match sort_order {
        Some(SortOrder::Desc) => None,
        _ => cursor,
    }
}

pub(in crate::handlers) fn next_cursor_for(page: &[Value], has_more: bool) -> Option<KeyValue> {
    if !has_more {
        return None;
    }
    page.last()
        .and_then(|row| row.get("key"))
        .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
}

/// Translate a caller's `(offset, limit)` page request into the ascending
/// [`HashRangeFilter::Page`] that selects it, honouring `sort_order`. Records are
/// stored/sliced in ascending `(range, hash)` order; a descending page counts
/// from the high end, so map it to the matching ascending window (the fetched
/// page is then re-ordered descending by [`format_rows`]).
pub(in crate::handlers) fn page_filter_for(
    sort_order: Option<&SortOrder>,
    total: usize,
    offset: usize,
    limit: usize,
) -> HashRangeFilter {
    match sort_order {
        Some(SortOrder::Desc) => {
            let asc_offset = total.saturating_sub(offset.saturating_add(limit));
            let asc_limit = total.saturating_sub(offset).saturating_sub(asc_offset);
            HashRangeFilter::Page {
                offset: asc_offset,
                limit: asc_limit,
            }
        }
        _ => HashRangeFilter::Page { offset, limit },
    }
}
