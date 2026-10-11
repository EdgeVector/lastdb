//! Indexed query-path benchmarks — the path a real app read actually takes.
//!
//! Unlike `db_operations_bench` (raw storage core), this drives the full
//! `FoldDB` query path: a HashRange schema, records written through the
//! mutation manager, then `QueryExecutor::query` — both a **point lookup by
//! hash key** (`HashRangeFilter::HashKey`, the "get one record" path) and an
//! unfiltered **full field read**.
//!
//! Why it matters: a field's index is stored per-key (`mk:`/`mh:` records), so a
//! keyed point lookup re-hydrates only the matched key — `indexed_point_lookup`
//! is FLAT (O(1)) across cardinalities, while an unfiltered `indexed_full_read`
//! materializes the whole field and scales O(field cardinality). These benches
//! make that divergence visible and guard the point lookup against regression.
//!
//! History: the point lookup used to scale O(field) too, because the cached
//! schema (cloned per query by `get_schema_following_supersession`) carried a
//! fully-materialized molecule — every keyed read cloned AND dropped all N keys.
//! Clearing the molecule from the cache (`Schema::clear_runtime_molecules`,
//! lazily re-hydrated from `molecule_uuid`) restored the per-key read's O(1) and
//! is what these benches pin.
//!
//! Second dimension (`point_lookup_vs_schema_count`): the 2026-06-20 :9001 wedge
//! had a SECOND O(N) axis the corpus-size sweeps can't see — a per-query deep
//! clone of the WHOLE schema registry in `get_schema_following_supersession`,
//! whose cost scales with the NUMBER of schemas / superseded versions loaded,
//! not corpus size. Every other group here loads exactly one schema, so that
//! axis was invisible — the exact blind spot that let the incident through. This
//! group holds a fixed small corpus and sweeps the schema count (1/10/100/500),
//! asserting the keyed lookup stays FLAT; a curve that climbs with K is the
//! O(N-schema) registry clone returning.
//!
//! Mixed read/write dimension (`concurrent_mixed_read_write`): the 2026-06-20
//! :9001 wedge happened under MIXED load — routines firing reads while writes
//! landed — but the suite split the two: `concurrent_list_reads` is read-only
//! (no writer contending) and the `storage_stress_test` mixed test asserts
//! correctness with no timing. So the headline production shape, reader latency
//! degrading while writers hammer the same trees, had no regression guard. This
//! group seeds a shared corpus, streams a steady background writer, and times
//! READER point-lookup latency at reader-concurrency 1/8/32 WHILE the writer
//! runs — guarded against a writer-induced convoy (2x relative ceiling).
//!
//! Run: `cargo bench -p fold_db --bench query_path_bench`
//!
//! Tracked regression guard: the committed baseline in
//! `benches/baseline/baseline.json` (see `benches/baseline/README.md`) is the
//! source of truth — the `Bench` CI job (`.github/workflows/bench.yml`, off the
//! merge-queue path) runs this bench and fails on a >2x regression vs that
//! baseline (the FLAT point lookups are guarded tighter at 1.6x). Per-case
//! numbers live there, not in stale inline comments. For local A/B comparison:
//! `cargo bench ... -- --save-baseline main` then `... -- --baseline main`.
//!
//! The vector/embedding native index is disabled here
//! (`FOLD_DISABLE_NATIVE_INDEX=1`) so the numbers isolate the molecule-backed
//! structured-query path (hash/range field resolution never consults the
//! semantic index) and stay fully offline.

#[path = "query_path_bench/concurrent.rs"]
mod concurrent;
#[path = "query_path_bench/count_rows.rs"]
mod count_rows;
#[path = "query_path_bench/guard.rs"]
mod guard;
#[path = "query_path_bench/lookup.rs"]
mod lookup;
#[path = "query_path_bench/queries.rs"]
mod queries;
#[path = "query_path_bench/scan.rs"]
mod scan;
#[path = "query_path_bench/seed.rs"]
mod seed;

use criterion::{criterion_group, criterion_main};

use concurrent::{bench_concurrent_list, bench_concurrent_mixed_read_write};
use count_rows::{bench_count_rows_vs_field_count, bench_count_rows_vs_row_count};
use lookup::{
    bench_full_read, bench_point_lookup, bench_point_lookup_vs_schema_count,
    bench_realistic_message_lookup,
};
use scan::{bench_hashrange_page_list, bench_value_filter_scan};

criterion_group!(
    benches,
    bench_point_lookup,
    bench_point_lookup_vs_schema_count,
    bench_full_read,
    bench_realistic_message_lookup,
    bench_concurrent_list,
    bench_value_filter_scan,
    bench_hashrange_page_list,
    bench_concurrent_mixed_read_write,
    bench_count_rows_vs_field_count,
    bench_count_rows_vs_row_count
);
criterion_main!(benches);
