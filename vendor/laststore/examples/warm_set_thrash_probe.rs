//! Measure what an ordinary write costs a saturated warm set, on real data.
//!
//! Reproduces the primary's shape: a thin navigational plane (`tips`; leftover
//! `field_tips` is residue, not a write target) that a write must revisit
//! constantly, interleaved with the fat
//! `atoms` plane whose groups carry megabytes of sealed segments. Reports the
//! cold group loads each write pays and how far the resident set collapses.
//!
//! ALWAYS run this against a copy. It writes.
//!
//! Usage:
//!   cargo run --release --example warm_set_thrash_probe -- <home> [writes]

use laststore::{LastStore, LastStoreOptions};
use std::env;
use std::process;
use std::time::Instant;

const THIN_COLLECTIONS: [&str; 2] = ["tips", "schemas"];

fn main() {
    if let Err(err) = run() {
        eprintln!("warm_set_thrash_probe FAILED: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let home = env::args().nth(1).ok_or("usage: <home> [writes]")?;
    let writes: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);

    let store = LastStore::open_existing_or_with(&home, LastStoreOptions::default())
        .map_err(|e| e.to_string())?;
    let budget = store.options().hash_group_warm_bytes;
    if budget == 0 {
        return Err("warm budget resolved to 0 — eviction disabled, nothing to measure".into());
    }
    println!("home={home}");
    println!("warm_budget_bytes={budget}");

    // Baseline: bring the thin plane resident, the way a booted node is.
    let mut thin_keys: Vec<(&str, Vec<String>)> = Vec::new();
    for coll in THIN_COLLECTIONS {
        let keys = store
            .list_prefix_keys(coll, "")
            .map_err(|e| e.to_string())?;
        println!("{coll}: keys={}", keys.len());
        thin_keys.push((coll, keys));
    }
    let atom_keys = store
        .list_prefix_keys("atoms", "")
        .map_err(|e| e.to_string())?;
    println!("atoms: keys={}", atom_keys.len());
    if atom_keys.is_empty() {
        return Err("no atoms on this home — wrong path?".into());
    }

    // Touch a spread of the thin plane so its groups are warm before we start.
    for (coll, keys) in &thin_keys {
        for key in keys.iter().step_by((keys.len() / 400).max(1)) {
            let _ = store.get(coll, key).map_err(|e| e.to_string())?;
        }
    }
    let base = report(&store, "baseline");

    // The measured workload: each "write" reads a slice of the fat plane the
    // way a real mutation does, then writes to the thin plane it must revisit.
    let atoms_per_write = 12usize;
    let t0 = Instant::now();
    let mut probe_id = 0u64;
    for w in 0..writes {
        let start = (w * atoms_per_write) % atom_keys.len();
        for k in 0..atoms_per_write {
            let key = &atom_keys[(start + k) % atom_keys.len()];
            let _ = store.get("atoms", key).map_err(|e| e.to_string())?;
        }
        for (coll, keys) in &thin_keys {
            if keys.is_empty() {
                continue;
            }
            for key in keys.iter().step_by((keys.len() / 40).max(1)) {
                let _ = store.get(coll, key).map_err(|e| e.to_string())?;
            }
            probe_id += 1;
            store
                .put(
                    coll,
                    &format!("warm-set-probe-{probe_id:016x}"),
                    format!("probe body {probe_id}").as_bytes(),
                )
                .map_err(|e| e.to_string())?;
        }
    }
    store.flush().map_err(|e| e.to_string())?;
    let elapsed = t0.elapsed();
    let end = report(&store, "after");

    let loads = end.0.saturating_sub(base.0);
    println!("---");
    println!("writes={writes}");
    println!("cold_loads_total={loads}");
    println!(
        "cold_loads_per_write={:.1}",
        loads as f64 / writes.max(1) as f64
    );
    println!(
        "resident_groups {} -> {} ({:+})",
        base.1,
        end.1,
        end.1 as i64 - base.1 as i64
    );
    println!("elapsed_ms={}", elapsed.as_millis());
    println!(
        "ms_per_write={:.1}",
        elapsed.as_millis() as f64 / writes.max(1) as f64
    );
    Ok(())
}

fn report(store: &LastStore, label: &str) -> (u64, usize) {
    let warm = store.hash_group_warm_stats();
    let atoms = store.hash_group_warm_stats_for("atoms");
    println!(
        "{label}: cold_loads={} resident_groups={} resident_bytes={} atoms_groups={} atoms_bytes={}",
        store.shard_loads(),
        warm.resident_groups,
        warm.resident_bytes,
        atoms.resident_groups,
        atoms.resident_bytes
    );
    (store.shard_loads(), warm.resident_groups)
}
