//! Storage-core micro-benchmarks for `DbOperations`.
//!
//! These measure the public storage surface that every write and read in
//! FoldDB ultimately funnels through — atom batch-store, point-read, and the
//! schema scan — without the schema/transform/embedding machinery on top. That
//! keeps the numbers stable and attributable to Last Store + the key-building /
//! (de)serialization path, which is what a storage regression would move.
//!
//! Run all:           `cargo bench -p fold_db --bench db_operations_bench`
//! Run one group:     `cargo bench -p fold_db --bench db_operations_bench -- batch_store`
//! Local A/B compare: `cargo bench ... -- --save-baseline main`  then later
//!                    `cargo bench ... -- --baseline main`
//!
//! Tracked regression guard: the committed baseline in
//! `benches/baseline/baseline.json` (see `benches/baseline/README.md`) is the
//! source of truth — the `Bench` CI job (`.github/workflows/bench.yml`, off the
//! merge-queue path) runs these benches and fails on a >2x regression vs that
//! baseline. Per-case numbers live there, not in stale inline comments.
//!
//! Note: `DbOperations::batch_store_atoms` does NOT embed/index (that lives in
//! the `MutationManager` layer), so these benches are fully offline and never
//! touch the fastembed model.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use fold_db::atom::Atom;
use fold_db::db_operations::DbOperations;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::operations::MutationType;
use fold_db::schema::types::{KeyValue, Mutation};
use fold_db::storage::{LastStoreNamespacedStore, NamespacedStore};
use fold_db::test_helpers::TestSchemaBuilder;
use fold_db::testing_utils::TestDatabaseFactory;
use serde_json::json;
use tokio::runtime::{Builder, Runtime};

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

/// Build `n` distinct atoms for `schema`. `salt` keeps content (and therefore
/// the content-addressed UUID) unique across benchmark iterations, so each
/// iteration inserts genuinely new keys instead of re-hitting a dedup no-op.
fn salted_atoms(schema: &str, salt: u64, n: usize) -> Vec<Atom> {
    (0..n)
        .map(|i| {
            Atom::new(
                schema.to_string(),
                json!({
                    "salt": salt,
                    "i": i,
                    "title": format!("post-{salt}-{i}"),
                    "body": BODY,
                }),
            )
        })
        .collect()
}

fn new_db(rt: &Runtime) -> Arc<DbOperations> {
    rt.block_on(async {
        Arc::new(
            TestDatabaseFactory::create_temp_db_ops()
                .await
                .expect("create temp db_ops"),
        )
    })
}

async fn fresh_bench_db_ops() -> DbOperations {
    let dir = tempfile::TempDir::new().unwrap().keep();
    let store = Arc::new(LastStoreNamespacedStore::open(&dir).unwrap()) as Arc<dyn NamespacedStore>;
    DbOperations::from_namespaced_store(store)
        .await
        .expect("db ops")
}

/// Write throughput: batch-store at increasing batch sizes. Each measured
/// iteration uses a fresh Last Store so corpus growth cannot inflate later
/// samples or larger-size cases. Throughput is reported per-atom so the cost
/// of the fixed per-call overhead vs. per-atom cost separates out across sizes.
fn bench_batch_store(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("batch_store") {
        return;
    }
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("batch_store");
    configure_fast_guard_group(&mut group);
    for size in [1_u64, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for salt in 0..iters {
                        let db = fresh_bench_db_ops().await;
                        let atoms = salted_atoms("StoreBench", salt, size as usize);
                        let start = Instant::now();
                        db.atoms()
                            .batch_store_atoms(atoms, None)
                            .await
                            .expect("batch store");
                        elapsed += start.elapsed();
                        drop(db);
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
}

/// Point-read latency against a pre-populated 10k-atom corpus. Reads cycle
/// deterministically through every uuid so the access pattern is stable and
/// the Sled block cache is exercised the same way each run.
fn bench_point_read(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("point_read") {
        return;
    }
    let rt = Runtime::new().expect("tokio runtime");
    let db = new_db(&rt);

    let uuids: Vec<String> = rt.block_on(async {
        let atoms = salted_atoms("ReadBench", 0, 10_000);
        let uuids = atoms.iter().map(|a| a.uuid().to_string()).collect();
        db.atoms()
            .batch_store_atoms(atoms, None)
            .await
            .expect("seed corpus");
        uuids
    });

    let cursor = AtomicU64::new(0);
    let mut group = c.benchmark_group("point_read");
    group.bench_function("get_atom_by_uuid", |b| {
        b.to_async(&rt).iter(|| {
            let db = db.clone();
            let idx = cursor.fetch_add(1, Ordering::Relaxed) as usize % uuids.len();
            let uuid = uuids[idx].clone();
            async move {
                let atom = db
                    .atoms()
                    .get_atom_by_uuid(&uuid, None)
                    .await
                    .expect("get atom")
                    .expect("atom present");
                black_box(atom);
            }
        });
    });
    group.finish();
}

/// Schema scan where EVERY atom matches (result == corpus): the degenerate
/// "list a schema that owns the whole store" case. `list_atoms_by_schema` is
/// now index-backed (one bounded prefix scan over the schema's index records),
/// so even here the cost is O(result); since result == corpus this curve is
/// legitimately linear in corpus — it is the upper bound, the companion to
/// `schema_scan_indexed` below (fixed small result in a large corpus), which
/// stays flat. Together they show the cost tracks the RESULT size, not the
/// total atom count. A 10%-sized "noise" schema is mixed in so the index scan
/// must actually seek to the schema's records rather than returning everything.
/// Kept at ≤10K so CI bench time stays bounded (the result-set IS the corpus).
fn bench_schema_scan(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("schema_scan") {
        return;
    }
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("schema_scan");
    configure_guard_group(&mut group, 20);
    for corpus in [1_000_u64, 10_000] {
        let db = new_db(&rt);
        rt.block_on(async {
            db.atoms()
                .batch_store_atoms(salted_atoms("Scan", 1, corpus as usize), None)
                .await
                .expect("seed scan corpus");
            db.atoms()
                .batch_store_atoms(salted_atoms("Noise", 2, (corpus / 10) as usize), None)
                .await
                .expect("seed noise corpus");
            // Warm the index so the measured iterations take the fast path
            // (a first call back-fills + sets the sentinel during seeding).
            db.atoms()
                .list_atoms_by_schema("Scan", None)
                .await
                .expect("warm index");
        });

        group.throughput(Throughput::Elements(corpus));
        group.bench_with_input(BenchmarkId::from_parameter(corpus), &corpus, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = db.clone();
                async move {
                    let atoms = db
                        .atoms()
                        .list_atoms_by_schema("Scan", None)
                        .await
                        .expect("list atoms");
                    black_box(atoms);
                }
            });
        });
    }
    group.finish();
}

/// Indexed schema scan: the secondary-index path for `list_atoms_by_schema`.
///
/// The legacy `schema_scan` group above walks the WHOLE `atom:` namespace,
/// deserializes every atom, and filters — O(total-atoms), ~13 ms @ 10K rising
/// to ~79 ms @ 50K in the committed baseline. This group exercises the SAME
/// `list_atoms_by_schema` API on a corpus where the secondary index is already
/// warm (a first call back-fills + sets the sentinel during seeding, so the
/// measured calls all take the index fast path), across [1K, 10K, 50K, 100K].
///
/// The index makes the cost track the RESULT size, not the corpus: here the
/// target "Scan" schema is held at a fixed 500 atoms while the surrounding
/// corpus grows 1K→100K, so a correct index keeps this curve ~FLAT. A curve
/// that climbs with the corpus means the scan is still touching non-matching
/// atoms — exactly the O(N) regression this card removes. Guarded tight (1.6x)
/// in `benches/baseline/baseline.json` so an accidental fall-back to the full
/// scan trips the regression gate.
fn bench_schema_scan_indexed(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("schema_scan_indexed") {
        return;
    }
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("schema_scan_indexed");
    configure_guard_group(&mut group, 20);
    // Fixed-size result set; the noise corpus around it is what scales.
    const TARGET_RESULT: usize = 500;
    for corpus in [1_000_u64, 10_000, 50_000, 100_000] {
        let db = new_db(&rt);
        rt.block_on(async {
            db.atoms()
                .batch_store_atoms(salted_atoms("Scan", 1, TARGET_RESULT), None)
                .await
                .expect("seed target corpus");
            db.atoms()
                .batch_store_atoms(
                    salted_atoms("Noise", 2, corpus as usize - TARGET_RESULT),
                    None,
                )
                .await
                .expect("seed noise corpus");
            // Warm the index (first call back-fills + sets the sentinel) so the
            // measured iterations all take the fast index path.
            db.atoms()
                .list_atoms_by_schema("Scan", None)
                .await
                .expect("warm index");
        });

        group.throughput(Throughput::Elements(corpus));
        group.bench_with_input(BenchmarkId::from_parameter(corpus), &corpus, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = db.clone();
                async move {
                    let atoms = db
                        .atoms()
                        .list_atoms_by_schema("Scan", None)
                        .await
                        .expect("list atoms");
                    black_box(atoms);
                }
            });
        });
    }
    group.finish();
}

/// Storage metering/breakdown attribution over a local corpus.
///
/// `AtomStore::storage_breakdown` powers the per-schema logical storage view by
/// summing serialized atom bytes grouped by schema. The result set here is
/// fixed at 500 target atoms while the surrounding corpus grows, so the bench
/// reports the current metering-attribution cost curve explicitly in the
/// db-perf guard. Today that implementation performs a canonical atom scan and
/// filters by requested schema, so the guarded curve is linear in total corpus;
/// if it moves to the schema index later, the baseline should be re-captured and
/// this curve should flatten.
fn bench_storage_breakdown(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("storage_breakdown") {
        return;
    }
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("storage_breakdown");
    configure_guard_group(&mut group, 20);
    const TARGET_RESULT: usize = 500;
    let schema_names = vec!["Metered".to_string()];
    let cardinalities: &[u64] = if fast_guard_enabled() {
        &[1_000, 10_000]
    } else {
        &[1_000, 10_000, 50_000]
    };
    for &corpus in cardinalities {
        let db = new_db(&rt);
        rt.block_on(async {
            db.atoms()
                .batch_store_atoms(salted_atoms("Metered", 1, TARGET_RESULT), None)
                .await
                .expect("seed metered corpus");
            db.atoms()
                .batch_store_atoms(
                    salted_atoms("StorageNoise", 2, corpus as usize - TARGET_RESULT),
                    None,
                )
                .await
                .expect("seed noise corpus");
        });

        group.throughput(Throughput::Elements(corpus));
        group.bench_with_input(BenchmarkId::from_parameter(corpus), &corpus, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = db.clone();
                let schema_names = schema_names.clone();
                async move {
                    let breakdown = db
                        .atoms()
                        .storage_breakdown(&schema_names, None)
                        .await
                        .expect("storage breakdown");
                    black_box(breakdown);
                }
            });
        });
    }
    group.finish();
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

/// Tip-write cost as database-catalog aliases grow.
///
/// The reference weight is an exact durable point read per affected molecule.
/// This sweep keeps the schema and record shape fixed while it varies only the
/// number of catalog paths. A rising curve means the write path returned to a
/// per-molecule catalog-edge scan.
fn bench_atom_refcount_tip_write_vs_catalog_refs(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("atom_refcount_tip_write_vs_catalog_refs") {
        return;
    }
    const SCHEMA: &str = "AtomRefcountWrite";
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    fn mutation(seq: u64) -> Mutation {
        let id = format!("record-{seq}");
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), json!(id));
        fields.insert("payload".to_string(), json!(format!("payload-{seq}")));
        Mutation::new(
            SCHEMA.to_string(),
            fields,
            KeyValue::new(Some(id), None),
            "pk".to_string(),
            MutationType::Create,
        )
    }

    async fn seeded_db(catalog_refs: usize) -> FoldDB {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let db = FoldDB::new_with_molecule_wrap_key(dir.to_str().expect("utf-8 path"), [0x51; 32])
            .await
            .expect("create FoldDB");
        db.load_schema_from_json(
            &TestSchemaBuilder::new(SCHEMA)
                .fields(&["payload"])
                .hash_key("id")
                .build_json(),
        )
        .await
        .expect("load schema");
        db.mutation_manager()
            .write_mutations_batch_async(vec![mutation(0)], None)
            .await
            .expect("seed one record");
        for index in 0..catalog_refs {
            db.share_schema(
                "lastdb://personal",
                &format!("lastdb://org/bench/ref-{index}"),
                SCHEMA,
                "org:bench",
                &[0x52; 32],
            )
            .await
            .expect("add catalog reference");
        }
        db
    }

    let mut group = c.benchmark_group("atom_refcount_tip_write_vs_catalog_refs");
    configure_guard_group(&mut group, 20);
    for catalog_refs in [0_usize, 16, 64] {
        let db = rt.block_on(seeded_db(catalog_refs));
        let counter = AtomicU64::new(1);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(catalog_refs),
            &catalog_refs,
            |b, _| {
                b.to_async(&rt).iter(|| {
                    let next = counter.fetch_add(1, Ordering::Relaxed);
                    let measured = mutation(next);
                    async {
                        db.mutation_manager()
                            .write_mutations_batch_async(vec![black_box(measured)], None)
                            .await
                            .expect("write measured record");
                    }
                });
            },
        );
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
            &TestSchemaBuilder::new(schema)
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

criterion_group!(
    benches,
    bench_batch_store,
    bench_point_read,
    bench_schema_scan,
    bench_schema_scan_indexed,
    bench_storage_breakdown,
    bench_keyed_update_write,
    bench_atom_refcount_tip_write_vs_catalog_refs,
    bench_batch_write_payload
);
criterion_main!(benches);
