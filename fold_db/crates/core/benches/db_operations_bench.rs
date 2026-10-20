//! Storage-core micro-benchmarks for `DbOperations`.
//!
//! These measure the write path that every write in FoldDB funnels through —
//! the keyed update and the batch payload — without the transform/embedding
//! machinery on top. That keeps the numbers stable and attributable to Last
//! Store + the key-building / (de)serialization path.
//!
//! Run all:           `cargo bench -p fold_db --bench db_operations_bench`
//! Run one group:     `cargo bench -p fold_db --bench db_operations_bench -- keyed_update_write`
//! Local A/B compare: `cargo bench ... -- --save-baseline main`  then later
//!                    `cargo bench ... -- --baseline main`
//!
//! Tracked regression guard: the committed baseline in
//! `benches/baseline/baseline.json` (see `benches/baseline/README.md`) is the
//! source of truth — the `Bench` CI job (`.github/workflows/bench.yml`, off the
//! merge-queue path) runs these benches and fails on a >2x regression vs that
//! baseline. Per-case numbers live there, not in stale inline comments.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use fold_db::benchmark_support::BenchmarkSchemaBuilder;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::operations::MutationType;
use fold_db::schema::types::{KeyValue, Mutation};
use serde_json::json;
use tokio::runtime::Runtime;

/// A representative ~250-byte body so each atom serializes to something with
/// realistic heft rather than a trivial scalar.
const BODY: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.";

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

fn configure_fast_guard_group(group: &mut BenchmarkGroup<'_, WallTime>) {
    if fast_guard_enabled() {
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_millis(750));
    }
}

fn configure_guard_group(group: &mut BenchmarkGroup<'_, WallTime>, sample_size: usize) {
    group.sample_size(sample_size);
    configure_fast_guard_group(group);
}

/// Keyed update-write cost into an already-populated field — the write-side
/// dimension of the per-key chain (#904 "O(changed) per-key writes instead of
/// full rewrite").
///
/// Pre-chain, writing one record into a field of cardinality `n` re-serialized
/// and rewrote the WHOLE `n`-key molecule, so a single update cost O(field)
/// (and a ~17x whole-molecule re-serialize tax dominated, capping ingest around
/// ~136 frag/s at scale). The per-key store path (#904) writes only the changed
/// `mk:` record, so the target is for a single update to approach O(changed) and
/// stay ~FLAT as the field grows.
///
/// We measure the cost of writing one fresh keyed mutation into a field that
/// already holds `n` records. A flat curve is the write-side O(1) target; a
/// curve that climbs with `n` reflects residual per-write whole-field cost.
/// As measured the curve is sub-linear but not flat — better than a pure
/// O(field) rewrite, but a single keyed update into a large field is still
/// costly. The flush per write and the molecule growing across iterations both
/// inflate the absolute numbers; treat this as a RELATIVE regression guard, not
/// an absolute SLA. The current per-cardinality numbers live in the committed
/// baseline (`benches/baseline/baseline.json`, enforced by the `Bench` CI job),
/// NOT inline here, so they can't go stale silently — the way a hand-typed
/// "~41/77/253 ms" comment did and let a 3-4x regression slip past (2026-06-21).
fn bench_keyed_update_write(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("keyed_update_write") {
        return;
    }
    const SCHEMA: &str = "WriteContacts";
    // Isolate the molecule write path; skip the semantic-index embed step.
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    async fn seeded_write_db(n: usize) -> FoldDB {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
            .await
            .expect("create FoldDB");
        db.load_schema_from_json(
            &BenchmarkSchemaBuilder::new(SCHEMA)
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

    fn update_mutation(seq: u64) -> Mutation {
        // A fresh key each iteration so we measure a genuine per-key write into
        // the populated field, never a dedup no-op.
        let full_name = format!("Writer {seq}");
        let content_hash = format!("wh{seq}");
        let mut fv = HashMap::new();
        fv.insert("full_name".to_string(), json!(full_name));
        fv.insert("content_hash".to_string(), json!(content_hash));
        fv.insert("email".to_string(), json!(format!("w{seq}@example.com")));
        Mutation::new(
            SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(full_name), Some(content_hash)),
            "pk".to_string(),
            MutationType::Create,
        )
    }

    let mut group = c.benchmark_group("keyed_update_write");
    configure_guard_group(&mut group, 20);
    let cardinalities: &[usize] = if fast_guard_enabled() {
        &[1_000, 10_000]
    } else {
        &[1_000, 10_000, 50_000, 100_000]
    };
    for &n in cardinalities {
        let db = rt.block_on(seeded_write_db(n));
        let counter = AtomicU64::new(1_000_000);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                let mutation = update_mutation(seq);
                async {
                    db.mutation_manager()
                        .write_mutations_batch_async(vec![mutation], None)
                        .await
                        .expect("write update");
                }
            });
        });
    }
    group.finish();
}

/// Batched mutation write with a *sizable* content payload, routed through the
/// full `MutationManager::write_mutations_batch_async` path — the universal
/// write funnel that `bench_keyed_update_write` exercises one-mutation-at-a-time
/// (so its per-iter atom-batch is size 1 and the canonical-prefix Vec copy is
/// invisible). Here each iteration writes a whole batch of `size` mutations,
/// each field carrying an ~8 KB JSON content blob, so the per-call work scales
/// with `size × payload`. This is the dimension that surfaces the redundant
/// whole-`Vec<Atom>` deep clone the canonical store used to pay on every write:
/// dropping that clone removes one full atom-batch deep-copy from this path, so
/// the wall-clock here should fall (most at the larger batch sizes).
///
/// Each atom's content is content-addressed, so a per-iteration salt keeps every
/// write a genuine new key rather than a dedup no-op.
fn bench_batch_write_payload(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("batch_write_payload") {
        return;
    }
    const SCHEMA: &str = "BatchPayloadWrite";
    // Isolate the molecule write path; skip the semantic-index embed step.
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    // ~8 KB body so each atom's `content` Value is a meaningful deep-copy unit.
    let big_body: String = BODY.repeat(32);

    async fn fresh_write_db(schema: &str) -> FoldDB {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
            .await
            .expect("create FoldDB");
        db.load_schema_from_json(
            &BenchmarkSchemaBuilder::new(schema)
                .fields(&["full_name", "content_hash", "payload"])
                .hash_key("full_name")
                .range_key("content_hash")
                .build_json(),
        )
        .await
        .expect("load schema");
        db
    }

    fn payload_batch(salt: u64, size: usize, body: &str) -> Vec<Mutation> {
        (0..size)
            .map(|i| {
                let full_name = format!("Person {salt}-{i}");
                let content_hash = format!("ch{salt}-{i}");
                let mut fv = HashMap::new();
                fv.insert("full_name".to_string(), json!(full_name));
                fv.insert("content_hash".to_string(), json!(content_hash));
                fv.insert("payload".to_string(), json!(body));
                Mutation::new(
                    SCHEMA.to_string(),
                    fv,
                    KeyValue::new(Some(full_name), Some(content_hash)),
                    "pk".to_string(),
                    MutationType::Create,
                )
            })
            .collect()
    }

    let mut group = c.benchmark_group("batch_write_payload");
    configure_guard_group(&mut group, 20);
    for size in [1_usize, 32, 256] {
        let db = rt.block_on(fresh_write_db(SCHEMA));
        let counter = AtomicU64::new(0);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&rt).iter(|| {
                let salt = counter.fetch_add(1, Ordering::Relaxed);
                let muts = payload_batch(salt, size, &big_body);
                async {
                    db.mutation_manager()
                        .write_mutations_batch_async(black_box(muts), None)
                        .await
                        .expect("batch write");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_keyed_update_write, bench_batch_write_payload);
criterion_main!(benches);
