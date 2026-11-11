//! Query pagination constants and the two-signal `has_more` computation — the
//! wire-compat page contract shared by both socket route executors.

/// Default `limit` applied when the request body omits one. Matches the
/// historical implicit cap of 100 records for unfiltered queries, so existing
/// callers see no behaviour change beyond gaining `total_count`/`has_more`.
/// (fold_db_node's `handlers::query::DEFAULT_QUERY_LIMIT`.)
pub const DEFAULT_QUERY_LIMIT: usize = 100;

/// Hard cap on a single page. Prevents a hostile client from asking for an
/// unbounded response. (fold_db_node's `handlers::query::MAX_QUERY_LIMIT`.)
pub const MAX_QUERY_LIMIT: usize = 1000;

/// Ceiling on a **bounded materialize** fetch (no key filter). Bounds the cases
/// that still materialize a candidate set so they can't bulk-load the whole
/// field; when the set reaches the cap, `has_more` flags the truncation.
/// (fold_db_node's `handlers::query::INTERNAL_FETCH_CAP`.)
pub const INTERNAL_FETCH_CAP: usize = 10_000;

/// Clamp a caller-supplied `limit` to `[_, MAX_QUERY_LIMIT]`, defaulting an
/// absent one to [`DEFAULT_QUERY_LIMIT`]. Mirrors the minimal daemon's
/// `clamp_request_limit` and the full node's
/// `handlers::response::clamp_request_limit(limit, DEFAULT, MAX)`.
#[must_use]
pub fn clamp_request_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(DEFAULT_QUERY_LIMIT).min(MAX_QUERY_LIMIT)
}

/// Decide whether a query response should advertise more records past this page.
///
/// Two independent signals drive `has_more`:
///
/// 1. **Page math** — `offset + returned_count < total_count`.
/// 2. **Cap truncation** — when the executor injected the bounded
///    [`INTERNAL_FETCH_CAP`] fetch (because the caller passed no filter) AND
///    `total_count` reached that ceiling, additional records may exist past it.
///    Without this second signal a caller paginating to the end of a capped
///    fetch (`offset + returned == total_count == cap`) would see
///    `has_more = false` and silently miss every row beyond the cap.
///
/// Mirrors the minimal daemon's `compute_has_more` and the full node's
/// `handlers::query::compute_has_more` (which is parameterized on `fetch_cap`
/// but only ever passed [`INTERNAL_FETCH_CAP`]).
#[must_use]
pub fn compute_has_more(
    offset: usize,
    returned_count: usize,
    total_count: usize,
    cap_was_injected: bool,
) -> bool {
    let beyond_page = offset.saturating_add(returned_count) < total_count;
    let cap_truncated = cap_was_injected && total_count >= INTERNAL_FETCH_CAP;
    beyond_page || cap_truncated
}
