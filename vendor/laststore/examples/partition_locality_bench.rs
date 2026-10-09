//! What partition-prefix placement costs and buys, on a real-shaped store.
//!
//! Builds the same key set under `HashGroupKey::FullKey` and under
//! `HashGroupKey::PartitionPrefix` at several fan-outs, then reports, per
//! layout: partition-read latency, cold group loads per read, and the on-disk
//! group-size distribution.
//!
//! The shape is the one measured on the primary tip plane (2026-07-26, then
//! still named `field_tips`; write target is now `tips`): a long tail of tiny
//! partitions — 176,861 partitions, p50 = 1
//! row — plus a handful of mega-partitions of ~74k rows. That skew is the whole
//! story: it is why locality wins (p50 = 1 row currently sweeps every group to
//! return one row) and simultaneously why locality alone is unsafe (a
//! mega-partition would concentrate into one group, and a group is parsed
//! whole). Fan-out is the knob between those two facts, so the bench reports
//! both the read win and the group-size cost side by side.
//!
//! Usage:
//!   cargo run --release --example partition_locality_bench
//!   cargo run --release --example partition_locality_bench -- 20000 4 5000
//!
//! Args: [small_partitions] [mega_partitions] [mega_rows]

use laststore::{HashGroupKey, LastStore, LastStoreOptions};
use std::fs;
use std::path::Path;
use std::process;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Production hash-group count, so group-size numbers are comparable to the
/// measured tip-plane figures rather than to a toy layout.
const GROUP_BITS: u8 = 10;
const GROUPS: usize = 1 << GROUP_BITS;

/// Warm budget deliberately far below the data size: the regression being fixed
/// only exists on a store whose groups do not all stay resident. With a budget
/// that fits everything, every layout looks identical after the first pass.
const WARM_BYTES: u64 = 4 * 1024 * 1024;

const SEP: char = '\0';
const COLLECTION: &str = "tips";

/// Molecule-codec key shape: `mk:{molecule}:{esc(hash)}\0{range}`.
fn row_id(partition: usize, row: usize) -> String {
    format!("mk:Card:{}{SEP}{row:06}", partition_hash(partition))
}

fn partition_prefix(partition: usize) -> String {
    format!("mk:Card:{}{SEP}", partition_hash(partition))
}

fn partition_hash(partition: usize) -> String {
    format!("{partition:040x}")
}

struct Layout {
    label: &'static str,
    key: HashGroupKey,
    fanout: u32,
}

struct Measured {
    small_loads: u64,
    small_time: Duration,
    mega_loads: u64,
    mega_time: Duration,
    limit1_loads: u64,
    groups_on_disk: usize,
    max_group_bytes: u64,
    avg_group_bytes: u64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("partition_locality_bench FAILED: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let small: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let mega: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let mega_rows: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(5_000);

    println!("shape: {small} single-row partitions + {mega} partitions of {mega_rows} rows");
    println!("groups={GROUPS} warm_bytes={WARM_BYTES}");
    println!();

    let layouts = [
        Layout {
            label: "FullKey (today)",
            key: HashGroupKey::FullKey,
            fanout: 1,
        },
        Layout {
            label: "PartitionPrefix f=1",
            key: HashGroupKey::PartitionPrefix,
            fanout: 1,
        },
        Layout {
            label: "PartitionPrefix f=8",
            key: HashGroupKey::PartitionPrefix,
            fanout: 8,
        },
        Layout {
            label: "PartitionPrefix f=64",
            key: HashGroupKey::PartitionPrefix,
            fanout: 64,
        },
    ];

    let mut results = Vec::new();
    for layout in &layouts {
        let measured = measure(layout, small, mega, mega_rows)?;
        report(layout.label, &measured);
        results.push(measured);
    }

    let baseline = &results[0];
    let local = &results[1];

    println!();
    println!(
        "small-partition read (the p50=1 case): {} loads -> {} loads",
        baseline.small_loads, local.small_loads
    );
    println!(
        "mega-partition group size: max {} B under FullKey -> {} B at f=1, {} B at f=64",
        baseline.max_group_bytes, local.max_group_bytes, results[3].max_group_bytes
    );

    // Guard, so this doubles as a perf regression check rather than only a
    // report: locality has to actually prune the tiny-partition read, and
    // fan-out has to actually cut the worst-case group it creates.
    if local.small_loads >= baseline.small_loads {
        return Err(format!(
            "partition-prefix small read did {} loads vs {} for full-key — no pruning",
            local.small_loads, baseline.small_loads
        ));
    }
    if results[3].max_group_bytes >= local.max_group_bytes {
        return Err(format!(
            "f=64 max group {} B is not below f=1's {} B — fan-out did not spread the \
             mega-partition",
            results[3].max_group_bytes, local.max_group_bytes
        ));
    }

    println!();
    println!("partition_locality_bench PASS");
    Ok(())
}

fn measure(
    layout: &Layout,
    small: usize,
    mega: usize,
    mega_rows: usize,
) -> Result<Measured, String> {
    let dir = TempDir::new().map_err(|e| e.to_string())?;

    // Build with eviction disabled: the write path is not what is being
    // measured, and thrashing it only makes the build slow.
    let build_opts = LastStoreOptions {
        hash_group_bits: GROUP_BITS,
        max_dirty_ops: 65_536,
        max_dirty_bytes: 32 * 1024 * 1024,
        ..LastStoreOptions::hash_group()
    }
    .with_hash_group_key(layout.key)
    .with_hash_group_partition_fanout(layout.fanout)
    .with_hash_group_warm_bytes(0);

    let store = LastStore::open_with(dir.path(), build_opts).map_err(|e| e.to_string())?;
    for p in 0..small {
        let id = row_id(p, 0);
        store
            .put(COLLECTION, &id, b"tip")
            .map_err(|e| e.to_string())?;
    }
    for m in 0..mega {
        let p = small + m;
        for r in 0..mega_rows {
            let id = row_id(p, r);
            store
                .put(COLLECTION, &id, b"tip")
                .map_err(|e| e.to_string())?;
        }
    }
    store.flush().map_err(|e| e.to_string())?;
    let groups_on_disk = store
        .hash_group_disk_group_count(COLLECTION)
        .map_err(|e| e.to_string())?;
    drop(store);

    let (max_group_bytes, avg_group_bytes) = group_size_stats(dir.path())?;

    // Cold reopen with a warm budget too small to hold the collection, so the
    // load counter reflects real segment parses.
    let read_opts = LastStoreOptions {
        hash_group_bits: GROUP_BITS,
        ..LastStoreOptions::hash_group()
    }
    .with_hash_group_key(layout.key)
    .with_hash_group_partition_fanout(layout.fanout)
    .with_hash_group_warm_bytes(WARM_BYTES);
    let store = LastStore::open_with(dir.path(), read_opts).map_err(|e| e.to_string())?;

    // Sample small partitions spread across the key space rather than one, so a
    // lucky group placement cannot flatter the result.
    const SAMPLES: usize = 16;
    let step = (small / SAMPLES).max(1);
    let before = store.shard_loads();
    let t0 = Instant::now();
    let mut rows = 0usize;
    for i in 0..SAMPLES {
        let p = (i * step) % small.max(1);
        rows += store
            .list_prefix_keys(COLLECTION, &partition_prefix(p))
            .map_err(|e| e.to_string())?
            .len();
    }
    let small_time = t0.elapsed() / SAMPLES as u32;
    let small_loads = (store.shard_loads() - before) / SAMPLES as u64;
    if rows != SAMPLES {
        return Err(format!(
            "expected one row per sampled small partition, got {rows} over {SAMPLES}"
        ));
    }

    let mega_prefix = partition_prefix(small);
    let before = store.shard_loads();
    let t0 = Instant::now();
    let mega_found = store
        .list_prefix_keys(COLLECTION, &mega_prefix)
        .map_err(|e| e.to_string())?
        .len();
    let mega_time = t0.elapsed();
    let mega_loads = store.shard_loads() - before;
    if mega_found != mega_rows {
        return Err(format!(
            "mega partition returned {mega_found} rows, expected {mega_rows}"
        ));
    }

    // `limit` used to be applied after the cross-group merge, so this read cost
    // a full sweep no matter how small the page was.
    let before = store.shard_loads();
    let page = store
        .list_prefix_keys_paged(COLLECTION, &partition_prefix(0), None, 1)
        .map_err(|e| e.to_string())?;
    let limit1_loads = store.shard_loads() - before;
    if page.len() != 1 {
        return Err(format!("limit=1 page returned {} ids", page.len()));
    }

    Ok(Measured {
        small_loads,
        small_time,
        mega_loads,
        mega_time,
        limit1_loads,
        groups_on_disk,
        max_group_bytes,
        avg_group_bytes,
    })
}

fn report(label: &str, m: &Measured) {
    println!("{label}");
    println!(
        "  small partition (1 row) : {:>6} loads  {:>9?}",
        m.small_loads, m.small_time
    );
    println!(
        "  mega partition          : {:>6} loads  {:>9?}",
        m.mega_loads, m.mega_time
    );
    println!("  limit=1 read            : {:>6} loads", m.limit1_loads);
    println!(
        "  groups on disk          : {:>6}   max group {} B / avg {} B",
        m.groups_on_disk, m.max_group_bytes, m.avg_group_bytes
    );
}

/// Max and mean bytes across the collection's group directories — the cost side
/// of locality, since `load_shard` parses a group segment whole.
fn group_size_stats(home: &Path) -> Result<(u64, u64), String> {
    let root = home.join("data").join(COLLECTION);
    let mut sizes = Vec::new();
    collect_group_sizes(&root, &mut sizes)?;
    if sizes.is_empty() {
        return Ok((0, 0));
    }
    let total: u64 = sizes.iter().sum();
    let max = sizes.iter().copied().max().unwrap_or(0);
    Ok((max, total / sizes.len() as u64))
}

/// Group dirs are the leaves of `data/<collection>/<shard>/<group>`; recurse so
/// the walk does not depend on how many levels the layout uses.
fn collect_group_sizes(dir: &Path, out: &mut Vec<u64>) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }
    let mut own_bytes = 0u64;
    let mut has_subdir = false;
    for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            has_subdir = true;
            collect_group_sizes(&path, out)?;
        } else {
            own_bytes += entry.metadata().map_err(|e| e.to_string())?.len();
        }
    }
    if !has_subdir {
        out.push(own_bytes);
    }
    Ok(())
}
