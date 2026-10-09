//! What one small transaction's durability barrier costs as the warm set grows.
//!
//! [`LastStore::transaction`] applies its ops and then calls
//! [`LastStore::flush`], which walks **every resident group handle** — not the
//! groups the transaction touched. `sync_open` short-circuits on a clean group,
//! so the walk does no IO for them, but it still pays a global-mutex acquisition
//! and a `ShardKey` hash lookup per resident handle, per barrier.
//!
//! On the primary that walk is over ~12k resident groups and the node's
//! change-feed append — a fixed 2-key `batch_put` on every mutation — was
//! measured at 409 ms (2026-08-04, `lastdb ops`: `change_record_write` 338 s
//! across 827 kanban mutations, 43% of all mutation wall time).
//!
//! This bench isolates that: it grows the resident set, then times a 2-key
//! transaction against a single group. The write itself is constant, so any
//! slope in the reported per-transaction time is the barrier walking groups the
//! transaction did not touch.
//!
//! Usage:
//!   cargo run --release --example flush_barrier_cost
//!   cargo run --release --example flush_barrier_cost -- 200

use laststore::{HashGroupKey, LastStore, LastStoreOptions, LayoutMode, TxnOp};
use std::process;
use std::time::Instant;
use tempfile::TempDir;

const COLLECTION: &str = "change_feed";
/// Production group count (`hash_group_bits = 10`).
const GROUP_BITS: u8 = 10;
/// Resident-set sizes to sample, in groups.
const RESIDENT_STEPS: &[usize] = &[1, 64, 256, 1024, 4096, 12288];
/// Foreign dirty-group counts to sample.
const DIRTY_STEPS: &[usize] = &[0, 1, 4, 16, 64, 256];

fn main() {
    if let Err(err) = run() {
        eprintln!("flush_barrier_cost FAILED: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let iters: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(200);

    println!("flush_barrier_cost — one 2-key transaction, growing resident set");
    println!("  iterations per step: {iters}");
    println!();
    println!(
        "{:>10}  {:>12}  {:>12}  {:>12}",
        "resident", "per-txn", "vs 1 group", "us/group"
    );

    let mut baseline_us = 0f64;
    for (index, &resident) in RESIDENT_STEPS.iter().enumerate() {
        let dir = TempDir::new().map_err(|e| e.to_string())?;
        let store = open(dir.path(), resident)?;

        // Make `resident` groups resident and dirty-then-clean, so the barrier
        // walk has that many handles to visit and every one of them is clean —
        // the steady state on a node whose last barrier just ran.
        //
        // A collection tops out at `1 << GROUP_BITS` groups, so the resident set
        // spreads across as many collections as it takes — which is also the
        // primary's shape: 28 collections over a 1024-group layout.
        for slot in 0..resident {
            let collection = format!("filler{:03}", slot >> GROUP_BITS);
            store
                .put(&collection, &resident_key(slot), b"x")
                .map_err(|e| e.to_string())?;
        }
        store.flush().map_err(|e| e.to_string())?;

        let handles = store.hash_group_warm_stats().resident_groups;

        // The measured op: exactly what `ChangeFeedStore::append` issues.
        let started = Instant::now();
        for seq in 0..iters {
            let ops = vec![
                TxnOp::put(COLLECTION, &event_key(seq as u64), b"{}".to_vec()),
                TxnOp::put(COLLECTION, "tip", b"1".to_vec()),
            ];
            store.transaction(ops).map_err(|e| e.to_string())?;
        }
        let per_txn_us = started.elapsed().as_secs_f64() * 1e6 / iters as f64;

        if index == 0 {
            baseline_us = per_txn_us;
        }
        println!(
            "{:>10}  {:>10.1}us  {:>11.1}x  {:>12.3}",
            handles,
            per_txn_us,
            per_txn_us / baseline_us,
            (per_txn_us - baseline_us) / handles.max(1) as f64,
        );
    }

    println!();
    println!("A flat column means walking clean handles is not the cost.");
    println!();

    dirty_set_cost(iters)
}

/// What a 2-key transaction's barrier pays for groups it did **not** write.
///
/// `flush` syncs every *dirty* resident group, so a small write's barrier also
/// fsyncs whatever every other in-flight writer dirtied since the last barrier.
/// This is the shape a busy node is in: the change-feed append is 2 keys, but
/// the mutation that precedes it and the mutations running beside it have
/// dirtied groups across the tip, atom, index and order-log planes.
fn dirty_set_cost(iters: usize) -> Result<(), String> {
    println!("dirty_set_cost — same 2-key transaction, growing FOREIGN dirty set");
    println!(
        "{:>10}  {:>12}  {:>12}  {:>12}",
        "foreign", "per-txn", "vs 0 dirty", "us/group"
    );

    let mut baseline_us = 0f64;
    for (index, &dirty) in DIRTY_STEPS.iter().enumerate() {
        let dir = TempDir::new().map_err(|e| e.to_string())?;
        let store = open(dir.path(), dirty.max(1))?;

        let started = Instant::now();
        for seq in 0..iters {
            // Another writer's ops, applied deferred — exactly what a
            // concurrent mutation's `apply` phase leaves behind.
            for slot in 0..dirty {
                let collection = format!("filler{:03}", slot >> GROUP_BITS);
                store
                    .put(&collection, &resident_key(slot), b"x")
                    .map_err(|e| e.to_string())?;
            }
            let ops = vec![
                TxnOp::put(COLLECTION, &event_key(seq as u64), b"{}".to_vec()),
                TxnOp::put(COLLECTION, "tip", b"1".to_vec()),
            ];
            store.transaction(ops).map_err(|e| e.to_string())?;
        }
        let per_txn_us = started.elapsed().as_secs_f64() * 1e6 / iters as f64;

        if index == 0 {
            baseline_us = per_txn_us;
        }
        println!(
            "{:>10}  {:>10.1}us  {:>11.1}x  {:>12.1}",
            dirty,
            per_txn_us,
            per_txn_us / baseline_us,
            (per_txn_us - baseline_us) / dirty.max(1) as f64,
        );
    }
    Ok(())
}

fn open(path: &std::path::Path, resident: usize) -> Result<LastStore, String> {
    let opts = LastStoreOptions {
        layout_mode: LayoutMode::HashGroup,
        hash_group_bits: GROUP_BITS,
        hash_group_key: HashGroupKey::PartitionPrefix,
        shard_bits: 0,
        // Never evict: the point is to hold the whole set resident.
        hash_group_warm_bytes: 1 << 30,
        hash_group_warm_max_handles: resident.max(1) * 4 + 16,
        ..Default::default()
    };
    LastStore::open_with(path, opts).map_err(|e| e.to_string())
}

/// One key per group: `\0`-free ids hash whole, so distinct ids spread.
fn resident_key(slot: usize) -> String {
    format!("resident:{slot:08}")
}

fn event_key(seq: u64) -> String {
    format!("event:{seq:020}")
}
