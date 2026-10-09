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

use std::collections::HashMap;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion,
};
use fold_db::access::AccessContext;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{MutationType, Query, ValueFilter};
use fold_db::schema::types::{KeyValue, Mutation};
use fold_db::test_helpers::TestSchemaBuilder;
use serde_json::json;
use tokio::runtime::Runtime;

const SCHEMA: &str = "Contacts";

/// Cardinalities swept by the point-lookup / full-read groups. Extended to
/// 100k so the O(1)-vs-O(field) divergence is unmistakable at scale: a flat
/// `indexed_point_lookup` here is the headline result of the per-key chain.
const CARDINALITIES: [usize; 4] = [1_000, 10_000, 50_000, 100_000];

/// A representative ~280-byte message body so the realistic-shape group writes
/// records with genuine heft (not trivial scalars), the way a messages/memory
/// schema would.
const MESSAGE_BODY: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis \
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute.";

fn requested_benchmark_filter_matches(group_name: &str) -> bool {
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .take_while(|arg| !arg.starts_with("--"))
        .collect();
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| group_name.contains(filter) || filter.contains(group_name))
}

fn fast_guard_enabled() -> bool {
    env_flag::var_truthy("FOLD_BENCH_FAST_GUARD")
        || env_flag::var_truthy("FOLD_DISABLE_NATIVE_INDEX")
}

fn configure_guard_group(group: &mut BenchmarkGroup<'_, WallTime>, sample_size: usize) {
    group.sample_size(sample_size);
    if fast_guard_enabled() {
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_millis(750));
    }
}

fn point_query(schema: &str, field: &str, hash_key: String) -> Query {
    Query {
        schema_name: schema.to_string(),
        fields: vec![field.to_string()],
        filter: Some(HashRangeFilter::HashKey(hash_key)),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

fn full_query() -> Query {
    Query {
        schema_name: SCHEMA.to_string(),
        fields: vec!["email".to_string()],
        filter: None,
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

fn page_query(limit: usize) -> Query {
    Query {
        schema_name: SCHEMA.to_string(),
        fields: vec!["email".to_string()],
        filter: Some(HashRangeFilter::Page { offset: 0, limit }),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

/// Build a `FoldDB` with a HashRange `Contacts` schema and `n` records written
/// through the real mutation path. The temp dir is leaked so the path stays
/// valid for the lifetime of the bench group.
async fn seeded_db(n: usize) -> FoldDB {
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &TestSchemaBuilder::new(SCHEMA)
            .fields(&["full_name", "email", "content_hash"])
            .hash_key("full_name")
            .range_key("content_hash")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let full_name = format!("Person {i}");
        let content_hash = format!("ch{i}");
        let mut fv = HashMap::new();
        fv.insert("full_name".to_string(), json!(full_name));
        fv.insert("content_hash".to_string(), json!(content_hash));
        fv.insert("email".to_string(), json!(format!("p{i}@example.com")));
        muts.push(Mutation::new(
            SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(full_name), Some(content_hash)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed records");
    db
}

/// Number of rows the `value_filter_scan` predicate is tuned to match,
/// independent of corpus size. The scan loads the WHOLE `score` field, then
/// `apply_value_filters` keeps only the rows whose `score` lands in a fixed
/// window — so the RESULT stays ~constant (`VALUE_FILTER_RESULT_ROWS`) while the
/// corpus grows. That fixed-result-in-a-growing-corpus shape is what makes the
/// O(corpus) scan cost visible and what the baseline guards against.
const VALUE_FILTER_RESULT_ROWS: usize = 100;

/// Build a `FoldDB` whose `Reviews` schema carries a NUMERIC `score` field, with
/// `n` records written through the real mutation path. `score` is assigned so
/// that exactly `VALUE_FILTER_RESULT_ROWS` rows fall in `[0.0, 1.0)` regardless
/// of `n`: the first `VALUE_FILTER_RESULT_ROWS` rows get a fractional score in
/// `[0, 1)`, every later row gets a score `>= 10.0`. A `value_filters` predicate
/// of `LessThan { score, 1.0 }` therefore matches a FIXED ~100-row subset while
/// the corpus (and thus the scanned field) grows — isolating the scan cost from
/// the result size, the shape behind the 2026-06-20 `SampleN(10_000)` abuse.
async fn seeded_scored_db(n: usize) -> FoldDB {
    const REVIEW_SCHEMA: &str = "Reviews";
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &TestSchemaBuilder::new(REVIEW_SCHEMA)
            .fields(&["review_id", "score", "created_at"])
            .hash_key("review_id")
            .range_key("created_at")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let review_id = format!("review-{i}");
        let created_at = format!("2026-06-{:02}T{:02}:00:00Z", i % 28 + 1, i % 24);
        // First VALUE_FILTER_RESULT_ROWS rows score in [0, 1); the rest score
        // >= 10.0, so `score < 1.0` always matches exactly that fixed subset.
        let score = if i < VALUE_FILTER_RESULT_ROWS {
            (i as f64) / (VALUE_FILTER_RESULT_ROWS as f64)
        } else {
            10.0 + (i % 90) as f64
        };
        let mut fv = HashMap::new();
        fv.insert("review_id".to_string(), json!(review_id));
        fv.insert("score".to_string(), json!(score));
        fv.insert("created_at".to_string(), json!(created_at.clone()));
        muts.push(Mutation::new(
            REVIEW_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(review_id), Some(created_at)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed scored records");
    db
}

/// A value-filter SCAN query: a full-span `Page` key filter (so the WHOLE
/// `score` field is materialized, NOT capped at the default unfiltered page of
/// 100) plus a numeric `value_filters` predicate applied post-fetch. This is the
/// "load every candidate record, then drop the rows whose value fails the
/// predicate" path (`QueryExecutor::apply_value_filters` runs AFTER the field
/// load) — and the explicit large page is exactly the `SampleN(10_000)` bulk
/// shape the 2026-06-20 wedge exploited.
///
/// (A bare `filter: None` would silently cap at `DEFAULT_UNFILTERED_PAGE_LIMIT`
/// = 100 rows, so the scan would NOT grow with corpus and the bench would
/// measure nothing — the full-span page is what makes this the O(corpus) scan.)
fn value_filter_query() -> Query {
    Query {
        schema_name: "Reviews".to_string(),
        fields: vec!["score".to_string()],
        filter: Some(HashRangeFilter::Page {
            offset: 0,
            limit: usize::MAX,
        }),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        // Matches the fixed `[0, 1)` subset seeded by `seeded_scored_db`.
        value_filters: Some(vec![ValueFilter::LessThan {
            field: "score".to_string(),
            value: 1.0,
        }]),
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

/// Realistic message-shaped corpus: a high-cardinality `Messages` schema where
/// each record carries a `sender`, a `sent_at` range key, and a ~280-byte
/// `body`. This mirrors a real messages / memory-atom workload (heavier records,
/// distinct keys) so the point-lookup-stays-flat claim is validated against a
/// realistic shape, not just tiny scalar rows.
async fn seeded_messages(n: usize) -> FoldDB {
    const MSG_SCHEMA: &str = "Messages";
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &TestSchemaBuilder::new(MSG_SCHEMA)
            .fields(&["msg_id", "sender", "sent_at", "body"])
            .hash_key("msg_id")
            .range_key("sent_at")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let msg_id = format!("msg-{i}");
        let sent_at = format!(
            "2026-06-{:02}T{:02}:{:02}:{:02}Z",
            i % 28 + 1,
            i % 24,
            i % 60,
            i % 60
        );
        let mut fv = HashMap::new();
        fv.insert("msg_id".to_string(), json!(msg_id));
        fv.insert("sender".to_string(), json!(format!("user{}", i % 5_000)));
        fv.insert("sent_at".to_string(), json!(sent_at.clone()));
        fv.insert("body".to_string(), json!(format!("[{i}] {MESSAGE_BODY}")));
        muts.push(Mutation::new(
            MSG_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(msg_id), Some(sent_at)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed messages");
    db
}

/// Schema-count sweep for the registry-clone dimension. A keyed point lookup
/// against a FIXED small corpus must stay FLAT as the number of loaded schemas
/// (and superseded schema versions) grows — the O(N-schema) per-query registry
/// clone is invisible to every other group, which loads exactly one schema.
const SCHEMA_COUNTS: [usize; 4] = [1, 10, 100, 500];

/// Fixed corpus size for the schema-count sweep. Held small + constant so the
/// only axis that varies is the registry size, not the field cardinality.
const SCHEMA_COUNT_CORPUS: usize = 1_000;

/// Build a `FoldDB` with the target `Contacts` schema + a fixed `corpus`, then
/// load `extra_schemas` ADDITIONAL distinct schemas into the registry — and
/// supersede a fraction of them so the `superseded_by` version chain also grows.
/// This reproduces the production load shape behind the 2026-06-20 :9001 wedge:
/// Tom's brain holds dozens–hundreds of schema versions, and a query that
/// deep-clones the whole registry per call scales with THAT count, not corpus.
///
/// The extra schemas are real, Available, queryable schemas (one hash + one
/// range field each, like the target) so they land in exactly the same registry
/// map `get_schema_following_supersession` clones on the hot path.
async fn seeded_db_with_schemas(corpus: usize, extra_schemas: usize) -> FoldDB {
    let db = seeded_db(corpus).await;

    // Load `extra_schemas` distinct schemas so the registry the query path
    // clones grows from 1 to 1 + extra_schemas entries.
    for k in 0..extra_schemas {
        let name = format!("Extra{k}");
        db.load_schema_from_json(
            &TestSchemaBuilder::new(&name)
                .fields(&["k", "v"])
                .hash_key("k")
                .range_key("v")
                .build_json(),
        )
        .await
        .expect("load extra schema");
    }

    // Grow the superseded_by version chain too: supersede every 5th extra schema
    // onto its predecessor. This exercises the version-count axis (superseded
    // versions still occupy the registry + the superseded_by map the resolver
    // consults) without redirecting the TARGET schema the bench queries.
    let schema_core = db.schema_manager();
    let mut prev: Option<String> = None;
    for k in 0..extra_schemas {
        let name = format!("Extra{k}");
        if k % 5 == 0 {
            if let Some(ref successor) = prev {
                // Block `name`, redirecting it to a still-Available successor.
                schema_core
                    .block_and_supersede(&name, successor)
                    .await
                    .expect("supersede extra schema");
            }
        }
        prev = Some(name);
    }

    db
}

/// Point lookup by hash key against a FIXED corpus, sweeping the NUMBER OF
/// SCHEMAS loaded in the registry. This is the dimension the rest of the suite
/// never exercises (every other group loads exactly one schema), and it is the
/// blind spot that let the 2026-06-20 wedge through: `get_schema_following_-
/// supersession` deep-cloned the entire schema registry per query, an
/// O(N-schema) cost that scales with the count of loaded/superseded schemas,
/// NOT with corpus size. The headline is that the median stays ~FLAT across
/// schema-count 1/10/100/500 — a curve that climbs with K is exactly that
/// per-query registry clone returning.
fn bench_point_lookup_vs_schema_count(c: &mut Criterion) {
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
fn bench_point_lookup(c: &mut Criterion) {
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
fn bench_full_read(c: &mut Criterion) {
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
fn bench_realistic_message_lookup(c: &mut Criterion) {
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

/// Fixed tokio worker-thread count for the concurrent-read bench.
///
/// The headline confound on the original measurement
/// (`folddb-concurrent-list-reads-throughput-plateau`) was that a default
/// multi-thread runtime grabs ALL cores, so on a loaded machine the read
/// throughput plateau could be EITHER a lock convoy OR plain CPU-core
/// saturation — the bench couldn't tell them apart. Pinning the runtime to a
/// known, small worker count removes that ambiguity: with exactly N workers the
/// two hypotheses make different, testable predictions (see
/// `bench_concurrent_list`). 4 is small enough to stay reproducible even on a
/// busy box (e.g. while the live :9001 brain is running) yet large enough that
/// "scales to N then plateaus" is clearly distinguishable from "never scales".
const CONCURRENT_READ_WORKERS: usize = 4;

/// Concurrent list reads against one shared `FoldDB` — the load shape that
/// actually wedged :9001 (many routines firing unfiltered reads at once), which
/// the single-query benches above can't see.
///
/// **Disambiguation harness (convoy vs core-saturation).** The runtime is
/// pinned to exactly `CONCURRENT_READ_WORKERS` worker threads, so offered
/// concurrency (1, 4, 8, 32) sweeps from under to far over the worker ceiling.
/// The two failure modes now read differently:
///
/// * **Lock convoy** (a `Mutex` every read funnels through, the bug this card
///   fixed): throughput stays pinned near the 1-way rate no matter the offered
///   concurrency — it never reaches even the `N`-worker ceiling, because readers
///   serialize on the lock, not on cores. Effective parallelism flat at ~1×.
/// * **Genuine core saturation** (no lock; reads are CPU-bound): throughput
///   climbs ~linearly up to `N` workers (effective parallelism ≈ `N` at
///   concurrency ≥ `N`) and *then* plateaus — a legitimate ceiling, not a bug.
///
/// After moving the schema cache from `Mutex` to `RwLock` (so concurrent reads
/// take a shared read lock + an `&self` `clone_for_read_shared`), this bench
/// should show effective parallelism approaching `CONCURRENT_READ_WORKERS` at
/// concurrency ≥ `N` — i.e. the prior "plateau well below the core count" was
/// the convoy, and what remains is the honest CPU ceiling.
///
/// A regression that reintroduces a per-query full-field materialization or a
/// read-path lock convoy shows up here as effective parallelism collapsing back
/// toward 1× even though `N` workers are available.
fn bench_concurrent_list(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("concurrent_list_reads") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    // Pinned worker count — see CONCURRENT_READ_WORKERS for why this is the
    // disambiguation lever. A plain `Runtime::new()` would grab every core and
    // reintroduce the convoy-vs-saturation confound.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CONCURRENT_READ_WORKERS)
        .enable_all()
        .build()
        .expect("pinned tokio runtime");

    // A mid-size field so each read does real work, shared across all tasks.
    let db = std::sync::Arc::new(rt.block_on(seeded_db(10_000)));

    let mut group = c.benchmark_group("concurrent_list_reads");
    configure_guard_group(&mut group, 10);
    // Sweep below, at, and well above the pinned worker ceiling so the
    // scaling curve (and any plateau) is unambiguous.
    for concurrency in [1_usize, CONCURRENT_READ_WORKERS, 8, 32] {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &concurrency| {
                b.to_async(&rt).iter(|| {
                    let db = std::sync::Arc::clone(&db);
                    async move {
                        let tasks: Vec<_> = (0..concurrency)
                            .map(|_| {
                                let db = std::sync::Arc::clone(&db);
                                tokio::spawn(async move {
                                    black_box(db.query_executor().query(full_query()).await.ok());
                                })
                            })
                            .collect();
                        for t in tasks {
                            let _ = t.await;
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

/// Reader concurrency swept by the mixed read/write group. Same shape as
/// `concurrent_list_reads` (1, 8, 32) so the WHILE-writers-run numbers are
/// directly comparable to the no-writer baseline — a convoy shows up as the
/// mixed-load case ballooning relative to the matching read-only case.
const MIXED_READER_CONCURRENCY: [usize; 3] = [1, 8, 32];

/// Build a small batch of keyed `Create` mutations that overwrite existing
/// `Contacts` records (same hash key → an update of the keyed atom). The
/// background writer stream replays these against the shared corpus so readers
/// are contending with live writes landing on the same trees.
fn writer_batch(start: usize, count: usize) -> Vec<Mutation> {
    let mut muts = Vec::with_capacity(count);
    for j in 0..count {
        let i = start + j;
        let full_name = format!("Person {i}");
        let content_hash = format!("ch{i}");
        let mut fv = HashMap::new();
        fv.insert("full_name".to_string(), json!(full_name));
        fv.insert("content_hash".to_string(), json!(content_hash));
        // Vary the email so each replay is a genuine value change (a real write,
        // not a no-op the write path could short-circuit).
        fv.insert(
            "email".to_string(),
            json!(format!("p{i}+{}@example.com", start)),
        );
        muts.push(Mutation::new(
            SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(full_name), Some(content_hash)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    muts
}

/// **Mixed read/write convoy bench** — the headline production shape that the
/// read-only `concurrent_list_reads` and the timing-free
/// `storage_stress_test::concurrent_reads_during_writes` correctness test each
/// miss: reader latency/throughput WHILE writers actively hammer the same trees.
///
/// The 2026-06-20 :9001 wedge happened under exactly this MIXED load (scheduled
/// routines firing reads while writes landed). Splitting reads and writes into
/// separate benches — as the suite did before this group — cannot catch a
/// reader-vs-writer lock convoy or a throughput collapse that only appears when
/// the two cross.
///
/// **Harness.** One shared `Arc<FoldDB>` seeded with 10K `Contacts` records (the
/// same corpus `concurrent_list_reads` uses, so the numbers are comparable). A
/// background writer task streams keyed `write_mutations_batch_async` batches in
/// a tight loop for the whole measured window; a per-iteration `AtomicBool`
/// gates it so the writer only runs while readers are being timed. Each measured
/// iteration spawns `concurrency` reader tasks doing a keyed point lookup +
/// stops the writer, and we time the READERS.
///
/// **What it guards.** Reader latency under write load must stay within a
/// relative ceiling of the no-writer numbers (baseline `regression_factor` 2.0):
/// writers must not convoy readers. A regression that reintroduces a write-path
/// lock the read path also funnels through (or a per-write full-field
/// materialization that starves readers) shows up here as the mixed-load reader
/// latency blowing past 2x of the read-only path even though the read path
/// itself is unchanged.
fn bench_concurrent_mixed_read_write(c: &mut Criterion) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    if !requested_benchmark_filter_matches("concurrent_mixed_read_write") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    // Pin the runtime to the same small worker count as `concurrent_list_reads`
    // so reader concurrency sweeps from under to over the worker ceiling and the
    // mixed-load numbers are comparable to the read-only ones (same confound
    // control — a default runtime grabbing every core would reintroduce the
    // convoy-vs-core-saturation ambiguity).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CONCURRENT_READ_WORKERS)
        .enable_all()
        .build()
        .expect("pinned tokio runtime");

    const CORPUS: usize = 10_000;
    let db = Arc::new(rt.block_on(seeded_db(CORPUS)));

    // Drives the background writer: true while readers are timed, false between
    // iterations so the writer doesn't run unbounded outside the measured window.
    let writing = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn ONE persistent background writer that streams keyed update batches at
    // the same corpus the readers query. It only does work while `writing` is set
    // (during a measured iteration), yielding otherwise — so it contends with
    // readers exactly during the timed window.
    let writer = {
        let db = Arc::clone(&db);
        let writing = Arc::clone(&writing);
        let shutdown = Arc::clone(&shutdown);
        rt.spawn(async move {
            let mut start = 0_usize;
            while !shutdown.load(Ordering::Relaxed) {
                if writing.load(Ordering::Relaxed) {
                    // A modest batch of keyed updates landing on the same trees
                    // the readers hit. Errors (e.g. on shutdown) end the writer.
                    let batch = writer_batch(start % CORPUS, 64);
                    if db
                        .mutation_manager()
                        .write_mutations_batch_async(batch, None)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    start = start.wrapping_add(64);
                } else {
                    tokio::task::yield_now().await;
                }
            }
        })
    };

    let mut group = c.benchmark_group("concurrent_mixed_read_write");
    configure_guard_group(&mut group, 10);
    for concurrency in MIXED_READER_CONCURRENCY {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &concurrency| {
                b.to_async(&rt).iter(|| {
                    let db = Arc::clone(&db);
                    let writing = Arc::clone(&writing);
                    async move {
                        // Open the write window for the measured reads.
                        writing.store(true, Ordering::Relaxed);
                        let tasks: Vec<_> = (0..concurrency)
                            .map(|n| {
                                let db = Arc::clone(&db);
                                tokio::spawn(async move {
                                    let key = format!("Person {}", n % CORPUS);
                                    black_box(
                                        db.query_executor()
                                            .query(point_query(SCHEMA, "email", key))
                                            .await
                                            .ok(),
                                    );
                                })
                            })
                            .collect();
                        for t in tasks {
                            let _ = t.await;
                        }
                        // Close the window so the writer idles between iterations.
                        writing.store(false, Ordering::Relaxed);
                    }
                });
            },
        );
    }
    group.finish();
    shutdown.store(true, Ordering::Relaxed);
    writer.abort();
    let _ = rt.block_on(writer);
}

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
fn bench_value_filter_scan(c: &mut Criterion) {
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
fn bench_hashrange_page_list(c: &mut Criterion) {
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

/// `count_query_rows` fields swept by `bench_count_rows_vs_field_count` — the
/// `total_count` computation for an unfiltered "list all" query
/// (`HashRangeQueryProcessor::count_rows`). The query includes the schema's
/// key field, so count_rows can resolve that single count-bearing field instead
/// of multiplying a full live-row resolution by every requested data field.
/// This pins the field-count axis from the `/api/query` latency-floor report.
const COUNT_ROWS_FIELD_COUNTS: [usize; 3] = [1, 3, 5];

/// Fixed corpus for the field-count sweep, chosen to keep one bench iteration
/// well under a second post-fix while still being large enough that a
/// regression back to the fully-serial per-row `get_per_key` loop in
/// `load_hash_range_page_from_index` is unmistakable in the timings.
const COUNT_ROWS_CORPUS: usize = 2_000;

/// Corpus sizes swept by `bench_count_rows_vs_row_count`. This covers the
/// separate row-count axis: exact tombstone-correct counts may still inspect
/// the count-bearing field's live values, but they must not reintroduce one
/// async storage round trip per row or multiply by requested field count.
const COUNT_ROWS_CARDINALITIES: [usize; 3] = [100, 1_000, 5_000];

const COUNT_ROWS_SCHEMA: &str = "CountRowsWide";

/// Data field names beyond the hash/range keys, so the schema has enough
/// fields to exercise `COUNT_ROWS_FIELD_COUNTS`.
const COUNT_ROWS_DATA_FIELDS: [&str; 5] = ["d0", "d1", "d2", "d3", "d4"];

/// Build a `FoldDB` with a HashRange schema carrying
/// [`COUNT_ROWS_DATA_FIELDS`] (5 independent data fields, all populated for
/// every row) plus `n` records written through the real mutation path.
async fn seeded_count_rows_db(n: usize) -> FoldDB {
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    let mut fields = vec!["shard".to_string(), "doc_id".to_string()];
    fields.extend(COUNT_ROWS_DATA_FIELDS.iter().map(ToString::to_string));
    db.load_schema_from_json(
        &TestSchemaBuilder::new(COUNT_ROWS_SCHEMA)
            .fields(&fields.iter().map(String::as_str).collect::<Vec<_>>())
            .hash_key("shard")
            .range_key("doc_id")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let shard = format!("shard{}", i % 4);
        let doc_id = format!("doc{i:08}");
        let mut fv = HashMap::new();
        fv.insert("shard".to_string(), json!(shard));
        fv.insert("doc_id".to_string(), json!(doc_id));
        for data_field in COUNT_ROWS_DATA_FIELDS {
            fv.insert(data_field.to_string(), json!(format!("{data_field}-{i}")));
        }
        muts.push(Mutation::new(
            COUNT_ROWS_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(shard), Some(doc_id)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed records");
    db
}

fn count_query(fields: &[&str]) -> Query {
    Query {
        schema_name: COUNT_ROWS_SCHEMA.to_string(),
        fields: fields.iter().map(ToString::to_string).collect(),
        filter: None,
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

fn count_query_fields(field_count: usize) -> Vec<&'static str> {
    let mut fields = vec!["shard"];
    fields.extend(
        COUNT_ROWS_DATA_FIELDS[..field_count.saturating_sub(1)]
            .iter()
            .copied(),
    );
    fields
}

/// `count_query_rows` (the exact `total_count` for an unfiltered list query)
/// against a FIXED corpus, sweeping the NUMBER OF FIELDS requested. Each
/// requested field set includes the key field, so `count_rows` should choose
/// that count-bearing field and stay near-flat as more data fields are
/// requested.
fn bench_count_rows_vs_field_count(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("count_rows_vs_field_count") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let db = rt.block_on(seeded_count_rows_db(COUNT_ROWS_CORPUS));
    let ctx = AccessContext::owner("bench-owner");

    let mut group = c.benchmark_group("count_rows_vs_field_count");
    configure_guard_group(&mut group, 20);
    for field_count in COUNT_ROWS_FIELD_COUNTS {
        let fields = count_query_fields(field_count);
        // Sanity: the count must equal the seeded corpus size exactly.
        let counted = rt
            .block_on(
                db.query_executor()
                    .count_query_rows(&count_query(&fields), &ctx),
            )
            .expect("count_query_rows")
            .expect("schema found");
        assert_eq!(
            counted, COUNT_ROWS_CORPUS,
            "count_rows must report the exact live row count"
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            &field_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let res = db
                        .query_executor()
                        .count_query_rows(&count_query(&fields), &ctx)
                        .await
                        .expect("count_query_rows");
                    black_box(res);
                });
            },
        );
    }
    group.finish();
}

/// `count_query_rows` against a single count-bearing field while the corpus
/// grows. This guards the row-count axis independently from the field-count
/// sweep above.
fn bench_count_rows_vs_row_count(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("count_rows_vs_row_count") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");
    let ctx = AccessContext::owner("bench-owner");

    let mut group = c.benchmark_group("count_rows_vs_row_count");
    configure_guard_group(&mut group, 10);
    for n in COUNT_ROWS_CARDINALITIES {
        let db = rt.block_on(seeded_count_rows_db(n));
        let fields = vec!["shard"];
        let counted = rt
            .block_on(
                db.query_executor()
                    .count_query_rows(&count_query(&fields), &ctx),
            )
            .expect("count_query_rows")
            .expect("schema found");
        assert_eq!(
            counted, n,
            "count_rows must report the exact live row count"
        );
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let res = db
                    .query_executor()
                    .count_query_rows(&count_query(&fields), &ctx)
                    .await
                    .expect("count_query_rows");
                black_box(res);
            });
        });
    }
    group.finish();
}

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
