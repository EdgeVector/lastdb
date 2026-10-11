use criterion::{black_box, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use super::guard::{
    configure_guard_group, requested_benchmark_filter_matches, CARDINALITIES, SCHEMA,
};
use super::queries::{full_query, point_query};
use super::seed::{seeded_db, seeded_db_with_schemas, seeded_messages, SCHEMA_COUNT_CORPUS};

/// Schema-count sweep for the registry-clone dimension. A keyed point lookup
/// against a FIXED small corpus must stay FLAT as the number of loaded schemas
/// (and superseded schema versions) grows — the O(N-schema) per-query registry
/// clone is invisible to every other group, which loads exactly one schema.
pub(crate) const SCHEMA_COUNTS: [usize; 4] = [1, 10, 100, 500];

/// Point lookup by hash key against a FIXED corpus, sweeping the NUMBER OF
/// SCHEMAS loaded in the registry. This is the dimension the rest of the suite
/// never exercises (every other group loads exactly one schema), and it is the
/// blind spot that let the 2026-06-20 wedge through: `get_schema_following_-
/// supersession` deep-cloned the entire schema registry per query, an
/// O(N-schema) cost that scales with the count of loaded/superseded schemas,
/// NOT with corpus size. The headline is that the median stays ~FLAT across
/// schema-count 1/10/100/500 — a curve that climbs with K is exactly that
/// per-query registry clone returning.
pub(crate) fn bench_point_lookup_vs_schema_count(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("point_lookup_vs_schema_count") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("point_lookup_vs_schema_count");
    configure_guard_group(&mut group, 20);
    for extra in SCHEMA_COUNTS {
        let db = rt.block_on(seeded_db_with_schemas(SCHEMA_COUNT_CORPUS, extra));
        let mut nth = 0_usize;
        group.bench_with_input(BenchmarkId::from_parameter(extra), &extra, |b, _| {
            b.to_async(&rt).iter(|| {
                nth = nth.wrapping_add(1);
                let key = format!("Person {}", nth % SCHEMA_COUNT_CORPUS);
                async {
                    let res = db
                        .query_executor()
                        .query(point_query(SCHEMA, "email", key))
                        .await
                        .expect("query");
                    black_box(res);
                }
            });
        });
    }
    group.finish();
}

/// Point lookup by hash key across corpora of increasing size. This is the
/// realistic "fetch one record by its key" read. The headline is that the time
/// stays ~FLAT across `n`: the keyed read re-hydrates only the matched key and
/// the cached schema no longer carries (clone/drop) the whole field's molecule,
/// so a point lookup is O(1), not O(field cardinality).
pub(crate) fn bench_point_lookup(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("indexed_point_lookup") {
        return;
    }

    // Isolate the molecule-backed structured path; skip the semantic index.
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("indexed_point_lookup");
    configure_guard_group(&mut group, 20);
    for n in CARDINALITIES {
        let db = rt.block_on(seeded_db(n));
        let mut nth = 0_usize;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                nth = nth.wrapping_add(1);
                let key = format!("Person {}", nth % n);
                async {
                    let res = db
                        .query_executor()
                        .query(point_query(SCHEMA, "email", key))
                        .await
                        .expect("query");
                    black_box(res);
                }
            });
        });
    }
    group.finish();
}

/// Unfiltered full field read, for comparison: it materializes the whole field
/// and scales O(field cardinality), so the FLAT point lookup should diverge
/// below it as `n` grows.
pub(crate) fn bench_full_read(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("indexed_full_read") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("indexed_full_read");
    configure_guard_group(&mut group, 20);
    for n in CARDINALITIES {
        let db = rt.block_on(seeded_db(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let res = db
                    .query_executor()
                    .query(full_query())
                    .await
                    .expect("query");
                black_box(res);
            });
        });
    }
    group.finish();
}

/// Realistic-shape point lookup: the same O(1) keyed read, but against a
/// high-cardinality `Messages` corpus with heavier records (a ~280-byte body),
/// up to 100k. Confirms the point-lookup curve stays flat under a realistic
/// record shape, not just tiny scalar rows — the step-3 "realistic-shape run".
pub(crate) fn bench_realistic_message_lookup(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("realistic_message_lookup") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("realistic_message_lookup");
    configure_guard_group(&mut group, 20);
    for n in [10_000_usize, 50_000, 100_000] {
        let db = rt.block_on(seeded_messages(n));
        let mut nth = 0_usize;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                nth = nth.wrapping_add(1);
                let key = format!("msg-{}", nth % n);
                async {
                    let res = db
                        .query_executor()
                        .query(point_query("Messages", "body", key))
                        .await
                        .expect("query");
                    black_box(res);
                }
            });
        });
    }
    group.finish();
}
