//! Offline migrate a LastStore home from segment_log (or any layout) to
//! hash-group with product warm budget.
//!
//! Usage:
//!   cargo run --release --example migrate_to_hash_group -- <src_store_root> <dst_store_root>
//!
//! `src_store_root` is the LastStore root (directory that contains `data/`).
//! For Mini homes that is typically `<home>/data`.

use laststore::{LastStore, LastStoreOptions, LayoutMode};
use std::env;
use std::process;
use std::time::Instant;

fn main() {
    if let Err(e) = run() {
        eprintln!("migrate_to_hash_group FAILED: {e}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let src = args
        .next()
        .ok_or("usage: migrate_to_hash_group <src_store_root> <dst_store_root>")?;
    let dst = args
        .next()
        .ok_or("usage: migrate_to_hash_group <src_store_root> <dst_store_root>")?;

    println!("src={src}");
    println!("dst={dst}");
    let t0 = Instant::now();

    // Open existing layout (segment_log or hash_group) without forcing mismatch.
    let source = LastStore::open_existing_or_with(&src, LastStoreOptions::default())
        .map_err(|e| format!("open source: {e}"))?;
    println!(
        "source_layout={:?} warm_bytes={}",
        source.options().layout_mode,
        source.options().hash_group_warm_bytes
    );

    let mut dest_opts = LastStoreOptions::hash_group();
    // Bulk migrate: disable warm eviction thrash during the write firehose.
    dest_opts.hash_group_warm_bytes = 0;
    dest_opts.max_dirty_ops = 65_536;
    dest_opts.max_dirty_bytes = 64 * 1024 * 1024;

    let report = source
        .migrate_to_hash_group(&dst, dest_opts)
        .map_err(|e| format!("migrate: {e}"))?;

    println!("collections={:?}", report.collections);
    println!("total_documents={}", report.total_documents);
    println!("elapsed_s={}", t0.elapsed().as_secs());

    // Reopen dest with product defaults (warm budget restored).
    let dest = LastStore::open_existing_or_with(&dst, LastStoreOptions::default())
        .map_err(|e| format!("reopen dest: {e}"))?;
    assert_eq!(dest.options().layout_mode, LayoutMode::HashGroup);
    println!(
        "dest_layout={:?} warm_bytes={}",
        dest.options().layout_mode,
        dest.options().hash_group_warm_bytes
    );
    if dest.options().hash_group_warm_bytes == 0 {
        return Err("dest warm budget is 0 after reopen".into());
    }
    println!("migrate_to_hash_group PASS");
    Ok(())
}
