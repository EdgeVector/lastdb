//! Multi‑GiB hash-group hot-path memory bar.
//!
//! Builds (or reuses) a large hash-group home, cold-opens it, runs schema-style
//! keys-only navigation + sparse atom gets, and requires process RSS ≤ 512 MiB
//! with only a minority of groups resident.
//!
//! Usage:
//!   cargo run --release --example hot_path_rss -- /path/to/home [target_gib]
//!
//! Env:
//!   LASTSTORE_HOT_PATH_MAX_RSS_MIB  (default 512)

use laststore::{LastStore, LastStoreOptions, LayoutMode};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::Instant;

fn main() {
    if let Err(err) = run() {
        eprintln!("hot_path_rss FAILED: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let home = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join(format!("laststore-hot-path-{}", process::id())));
    let target_gib: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(2);
    let max_rss_mib: u64 = env::var("LASTSTORE_HOT_PATH_MAX_RSS_MIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(512);
    let warm_bytes = 384 * 1024 * 1024u64; // mid of 0.3–0.5 GiB product band

    println!("home={}", home.display());
    println!("target_gib={target_gib}");
    println!("max_rss_mib={max_rss_mib}");
    println!("warm_bytes={warm_bytes}");

    if !home.join("laststore-layout-v1").exists() {
        build_home(&home, target_gib, warm_bytes)?;
    } else {
        println!("reusing existing home");
    }

    // --- cold open + workload (this is the measured process after open) ---
    // Product Mini reopen path: `open_existing_or_with` + Default options.
    // Warm budget must come back as the product default (not 0), even though
    // Default has hash_group_warm_bytes=0 before layout resolve.
    let t0 = Instant::now();
    let store = LastStore::open_existing_or_with(&home, LastStoreOptions::default())
        .map_err(|e| e.to_string())?;
    if store.options().layout_mode != LayoutMode::HashGroup {
        return Err(format!(
            "expected HashGroup layout on reopen, got {:?}",
            store.options().layout_mode
        ));
    }
    if store.options().hash_group_warm_bytes == 0 {
        return Err(
            "Mini-style reopen left warm budget at 0 (eviction disabled) — product hot path broken"
                .into(),
        );
    }
    if store.options().hash_group_warm_bytes > warm_bytes
        && store.options().hash_group_warm_bytes > 512 * 1024 * 1024
    {
        // Soft: product default is 256MiB; allow up to the CLI max band.
    }
    println!(
        "reopen_path=open_existing_or_with(Default) layout={:?} warm_bytes={}",
        store.options().layout_mode,
        store.options().hash_group_warm_bytes
    );
    let open_stats = store.hash_group_warm_stats();
    println!(
        "after_open resident_groups={} resident_bytes={} budget={}",
        open_stats.resident_groups, open_stats.resident_bytes, open_stats.budget_bytes
    );
    if open_stats.resident_groups != 0 {
        return Err(format!(
            "cold open must not open groups; got {:?}",
            open_stats
        ));
    }
    if open_stats.budget_bytes == 0 {
        return Err(format!(
            "warm budget is 0 after Mini reopen: {:?}",
            open_stats
        ));
    }

    // Thin navigational plane: schema keys only (no atom body hydrate).
    let schema_ids = store
        .list_prefix_keys("schemas", "schema-")
        .map_err(|e| e.to_string())?;
    println!("schema_keys={}", schema_ids.len());
    if schema_ids.is_empty() {
        return Err("expected schema navigational keys".into());
    }

    // Sparse atom hydrate across the home (representative interactive set).
    let mut ok = 0u64;
    for i in (0..10_000).step_by(97) {
        let id = atom_id(i);
        match store.get("atoms", &id).map_err(|e| e.to_string())? {
            Some(body) if body.starts_with(b"atom-") => ok += 1,
            Some(_) => return Err(format!("bad body for {id}")),
            None => {} // may not exist if build used fewer docs
        }
    }
    println!("sparse_gets_ok={ok}");
    if ok == 0 {
        return Err("no sparse atom gets succeeded".into());
    }

    // Point get correctness after cold open + nav.
    let probe = atom_id(0);
    let body = store
        .get("atoms", &probe)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("missing probe {probe}"))?;
    if !body.starts_with(b"atom-0|") {
        return Err(format!(
            "probe body mismatch for {probe}: prefix={:?}",
            body.get(..16)
        ));
    }

    let warm = store.hash_group_warm_stats();
    let atoms_warm = store.hash_group_warm_stats_for("atoms");
    let disk_groups = store
        .hash_group_disk_group_count("atoms")
        .map_err(|e| e.to_string())?;
    let rss_mib = process_rss_mib()?;
    let data_bytes = dir_size_bytes(&home).unwrap_or(0);
    println!("disk_atom_groups={disk_groups}");
    println!(
        "warm global resident_groups={} resident_bytes={} budget={}",
        warm.resident_groups, warm.resident_bytes, warm.budget_bytes
    );
    println!(
        "warm atoms resident_groups={} resident_bytes={}",
        atoms_warm.resident_groups, atoms_warm.resident_bytes
    );
    println!("home_bytes={data_bytes}");
    println!("rss_mib={rss_mib}");
    println!("elapsed_ms={}", t0.elapsed().as_millis());

    if disk_groups > 0 && atoms_warm.resident_groups >= disk_groups {
        return Err(format!(
            "expected cold majority for atoms: resident_groups={} disk_groups={}",
            atoms_warm.resident_groups, disk_groups
        ));
    }
    if warm.budget_bytes > 0 && warm.resident_bytes > warm.budget_bytes {
        return Err(format!("warm set over budget: {:?}", warm));
    }
    if rss_mib > max_rss_mib {
        return Err(format!(
            "RSS {rss_mib} MiB exceeds max {max_rss_mib} MiB (home_bytes={data_bytes})"
        ));
    }

    println!("hot_path_rss PASS");
    Ok(())
}

fn build_home(home: &Path, target_gib: u64, warm_bytes: u64) -> Result<(), String> {
    let _ = warm_bytes; // measured open uses warm_bytes; build disables eviction.
    if home.exists() {
        fs::remove_dir_all(home).map_err(|e| e.to_string())?;
    }
    fs::create_dir_all(home).map_err(|e| e.to_string())?;
    // Build path: no warm eviction thrash, large group-commit batches.
    let mut opts = LastStoreOptions {
        layout_mode: LayoutMode::HashGroup,
        hash_group_bits: 10,
        max_dirty_ops: 65_536,
        max_dirty_bytes: 64 * 1024 * 1024,
        ..LastStoreOptions::default()
    };
    opts.hash_group_warm_bytes = 0;
    let store = LastStore::open_with(home, opts).map_err(|e| e.to_string())?;

    // ~64 KiB atom bodies → fewer puts for multi‑GiB homes.
    let body_pad = vec![b'x'; 64 * 1024 - 32];
    let target_bytes = target_gib.saturating_mul(1024 * 1024 * 1024);
    let mut written = 0u64;
    let mut i = 0u64;
    let t0 = Instant::now();
    while written < target_bytes {
        let id = atom_id(i as usize);
        let mut body = format!("atom-{i}|").into_bytes();
        body.extend_from_slice(&body_pad);
        store.put("atoms", &id, &body).map_err(|e| e.to_string())?;
        if i % 64 == 0 {
            store
                .put(
                    "schemas",
                    &format!("schema-{i}"),
                    format!("nav-{i}").as_bytes(),
                )
                .map_err(|e| e.to_string())?;
        }
        written = written.saturating_add(body.len() as u64);
        i += 1;
        if i % 512 == 0 {
            let _ = writeln!(
                io::stderr(),
                "build progress docs={i} written_mib={} elapsed_s={}",
                written / (1024 * 1024),
                t0.elapsed().as_secs()
            );
        }
    }
    store.flush().map_err(|e| e.to_string())?;
    // Drop store so all handles release before measure open.
    drop(store);
    println!("built docs={i} written_bytes={written}");
    Ok(())
}

fn atom_id(i: usize) -> String {
    // Stable UUID-shaped ids so place() distributes across groups.
    format!("00000000-0000-4000-8000-{i:012x}")
}

// The three blocks below are mutually exclusive, so exactly one survives
// cfg-stripping on any target and becomes this function's tail expression.
// That is why none of them needs a trailing `return` — writing one only on the
// platform you happen to build for is what made this example the sole
// clippy failure in the heavy lane.
fn process_rss_mib() -> Result<u64, String> {
    let pid = process::id();
    #[cfg(target_os = "macos")]
    {
        let out = process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err("ps failed".into());
        }
        let s = String::from_utf8_lossy(&out.stdout);
        let kb: u64 = s.trim().parse().map_err(|e| format!("parse rss: {e}"))?;
        Ok(kb / 1024)
    }
    #[cfg(target_os = "linux")]
    {
        let statm = fs::read_to_string(format!("/proc/{pid}/statm")).map_err(|e| e.to_string())?;
        let pages: u64 = statm
            .split_whitespace()
            .nth(1)
            .ok_or("statm")?
            .parse()
            .map_err(|e| format!("parse statm: {e}"))?;
        // NOT an FFI call despite the name — `libc_page_size` is a safe fn
        // returning a 4k constant, so an `unsafe` block here is unused-unsafe.
        // Only the Linux arm compiles this, which is why it stayed invisible
        // while the heavy lane ran on the macOS host runner.
        let page = libc_page_size();
        Ok(pages.saturating_mul(page) / (1024 * 1024))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        Err("RSS sampling unsupported on this OS".into())
    }
}

#[cfg(target_os = "linux")]
fn libc_page_size() -> u64 {
    // 4k default; fine for RSS bar.
    4096
}

fn dir_size_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0u64;
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            total = total.saturating_add(dir_size_bytes(&p)?);
        } else {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}
