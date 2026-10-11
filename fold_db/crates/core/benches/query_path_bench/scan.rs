use std::collections::HashMap;

use criterion::{black_box, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use super::guard::{configure_guard_group, requested_benchmark_filter_matches, CARDINALITIES};
use super::queries::{page_query, value_filter_query};
use super::seed::{seeded_db, seeded_scored_db, VALUE_FILTER_RESULT_ROWS};

/// Value-filter SCAN: a query with a full-span `Page` key filter (loads the
/// whole field) plus a numeric `value_filters` predicate that matches a FIXED
/// ~100-row subset while the corpus grows. This is the dimension every other
/// group is blind to — every other query here sets `value_filters: None` — and
/// it is the exact shape the 2026-06-20 `SampleN(10_000)` abuse exploited: a
/// value-filter scan must materialize + inspect every candidate record's field
/// value before it can drop the non-matching rows
/// (`QueryExecutor::apply_value_filters` runs AFTER the field load), so its cost
/// tracks the CORPUS, not the (fixed) result.
///
/// With no value index this scan is inherently O(corpus) — like `schema_scan`,
/// this is the UPPER-BOUND case, baselined with the default 2.0x relative guard
/// (NOT the 1.6x flat guard the keyed lookups get). The headline is therefore
/// NOT flatness but a sane, ~linear-in-corpus curve: a super-linear blowup vs
/// corpus (e.g. an N² re-scan, or a per-row clone of the whole result on the
/// return path) is the regression this guards against. The fixed 100-row result
/// also pins the return/allocation path: the answer size never changes, so any
/// growth beyond ~linear-in-corpus is pure scan/return overhead.
pub(crate) fn bench_value_filter_scan(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("value_filter_scan") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("value_filter_scan");
    // The upper-bound scan is O(corpus); at 50K/100K a single iteration is ~1s,
    // so 10 samples keeps the group's wall time bounded (like concurrent_list).
    configure_guard_group(&mut group, 10);
    for n in CARDINALITIES {
        let db = rt.block_on(seeded_scored_db(n));
        // Sanity: the predicate matches exactly the fixed subset, independent of
        // corpus — confirms we're measuring "fixed result, growing scan", the
        // shape that makes the O(corpus) scan cost the thing under test.
        let matched = rt
            .block_on(db.query_executor().query(value_filter_query()))
            .expect("query")
            .get("score")
            .map_or(0, HashMap::len);
        assert_eq!(
            matched, VALUE_FILTER_RESULT_ROWS,
            "value_filter predicate must match a fixed subset (got {matched})"
        );
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let res = db
                    .query_executor()
                    .query(value_filter_query())
                    .await
                    .expect("query");
                black_box(res);
            });
        });
    }
    group.finish();
}

/// HashRange paginated list over a fixed 50-row page while the field grows. This
/// is the residual 2026-06-20 hot path: HashRange primary records are hash-major
/// (`mk:`), but page order is `(range, hash)`. The range-major `mhr:` secondary
/// index should keep the curve flat across corpus sizes; a linear climb means
/// the query is listing/decoding/sorting all HashRange keys again.
pub(crate) fn bench_hashrange_page_list(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("hashrange_page_list_vs_N") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    const PAGE: usize = 50;
    let mut group = c.benchmark_group("hashrange_page_list_vs_N");
    configure_guard_group(&mut group, 10);
    for n in CARDINALITIES {
        let db = rt.block_on(seeded_db(n));
        let page_len = rt
            .block_on(db.query_executor().query(page_query(PAGE)))
            .expect("query")
            .get("email")
            .map_or(0, HashMap::len);
        assert_eq!(page_len, PAGE, "page query must return exactly {PAGE} rows");
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let res = db
                    .query_executor()
                    .query(page_query(PAGE))
                    .await
                    .expect("query");
                black_box(res);
            });
        });
    }
    group.finish();
}
