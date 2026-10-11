use std::collections::HashMap;

use criterion::{black_box, BenchmarkId, Criterion};
use fold_db::schema::types::operations::MutationType;
use fold_db::schema::types::{KeyValue, Mutation};
use serde_json::json;

use super::guard::{configure_guard_group, requested_benchmark_filter_matches, SCHEMA};
use super::queries::{full_query, point_query};
use super::seed::seeded_db;

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
pub(crate) const CONCURRENT_READ_WORKERS: usize = 4;

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
pub(crate) fn bench_concurrent_list(c: &mut Criterion) {
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
pub(crate) const MIXED_READER_CONCURRENCY: [usize; 3] = [1, 8, 32];

/// Build a small batch of keyed `Create` mutations that overwrite existing
/// `Contacts` records (same hash key → an update of the keyed atom). The
/// background writer stream replays these against the shared corpus so readers
/// are contending with live writes landing on the same trees.
pub(crate) fn writer_batch(start: usize, count: usize) -> Vec<Mutation> {
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
pub(crate) fn bench_concurrent_mixed_read_write(c: &mut Criterion) {
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
