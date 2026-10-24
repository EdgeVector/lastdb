//! Query page response assembly and continuation cursors.

use super::*;

/// The `has_more`-carrying page shape both sockets serve, before the envelope.
///
/// `next_cursor` is always `null` for offset pages. Keyed query pages use
/// [`cursor_payload`], whose continuation uses the same API-coordinate window.
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
    // row. A drop that still consumes an output slot can hide the probe row,
    // so count those unfilled slots back in. Internal candidate refills retain
    // their integrity counters but replace their missing rows before this
    // boundary; those drops must not make an exhausted range claim more data.
    let fetched = page.len().saturating_add(drops.total());
    let has_more = fetched > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    // Derive the cursor from the truncated page: it has to name the last row the
    // caller actually received, not the `limit + 1`th probe row, or the next
    // continuation skips a row.
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

fn next_cursor_for(page: &[Value], has_more: bool) -> Option<KeyValue> {
    if !has_more {
        return None;
    }
    page.last()
        .and_then(|row| row.get("key"))
        .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
}
