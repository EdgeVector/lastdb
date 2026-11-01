//! Run home proof (design-lastdb-disk-size-20261024, phases V2 and V1c).
//! CI only.
//!
//! Drives `laststore::disk_run::home::RunHome` with deterministic synthetic
//! commits: commit + merge, reads by key, batch and range against a model,
//! a read that holds an old CURRENT across merges, crash leftovers, kill -9
//! of a writing child process, the owner lock, and a wrong key. The bound
//! phases sample the disk size during commits and merges with values that do
//! not compress. The values phase (V1b) checks that each stored value, in the
//! compact form or as given, reads back with its exact bytes. It fails when
//! the disk size is over 2x the live logical bytes. No primary or user data is
//! opened.

mod bound;
mod cleanup;
mod compaction_space;
mod crash;
mod gen;
mod value_gen;
mod values;

use gen::{apply, batch, logical_bytes, overwrite_batch, tip_key, Model, TIP_SCHEMAS};
use laststore::disk_run::home::{HomeMutation, RangeBounds, ReadBudget, RunHome, RunRead};
use laststore::disk_run::{RunOptions, RunWriter};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const BATCHES: u64 = 200;
const BATCH_OPS: usize = 1000;
pub(crate) const MIB: f64 = 1024.0 * 1024.0;

fn main() -> Result<()> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
        return Err("the run home proof runs only in GitHub Actions".into());
    }
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 && args[1] == "child" {
        return crash::child(Path::new(&args[2]));
    }
    let base = std::env::var_os("RUNNER_TEMP").map_or_else(std::env::temp_dir, PathBuf::from);
    let scratch = base.join(format!("run-home-proof-{}", std::process::id()));
    fs::create_dir(&scratch)?;
    let result = run(&scratch);
    let _ = fs::remove_dir_all(&scratch);
    result
}

pub(crate) fn budget() -> ReadBudget {
    ReadBudget {
        pages: u64::MAX,
        blocks: u64::MAX,
        bytes: u64::MAX,
        keys: u64::MAX,
    }
}

fn run(scratch: &Path) -> Result<()> {
    let key = random_key()?;
    let dir = scratch.join("home");
    let home = RunHome::create(&dir, &key)?;
    let mut model = Model::new();
    let mut commit_us = Vec::new();
    let mut merges = 0u64;
    let mut dropped = 0u64;
    let mut held: Option<(RunRead, Model)> = None;
    let started = Instant::now();
    for n in 1..=BATCHES {
        let operations = batch(n, BATCH_OPS);
        apply(&mut model, &operations);
        let at = Instant::now();
        home.commit(operations)?;
        commit_us.push(at.elapsed().as_micros() as u64);
        while let Some(merge) = home.merge_step()? {
            merges += 1;
            dropped += u64::from(merge.dropped_tombstones);
        }
        if n == BATCHES / 2 {
            held = Some((home.read(budget()), model.clone()));
        }
    }
    let write_secs = started.elapsed().as_secs_f64();
    let status = home.status();
    if status.frontier != BATCHES {
        return Err(format!("frontier {} after {BATCHES} commits", status.frontier).into());
    }
    let held_check = check_held(&dir, &home, held.take().ok_or("no held read")?)?;
    // Measured after the held read ends, so merged runs are gone.
    let disk = dir_bytes(&dir)?;
    let logical = logical_bytes(&model);
    let ratio = disk as f64 / logical as f64;
    let checked = check_all(&home, &model)?;
    let ranges = check_ranges(&home, &model)?;
    if dropped == 0 {
        return Err("no base merge step ran".into());
    }
    let (overwrites, overwrite_ratio) = check_overwrites(scratch, &key)?;
    let (values_row, values_ratio) = values::check(scratch, &key)?;
    commit_us.sort_unstable();
    println!("| Check | Result |");
    println!("|---|---|");
    println!(
        "| Commit + merge | {BATCHES} commits of {BATCH_OPS} ops in {write_secs:.1} s; commit p50 {} us, p99 {} us; {merges} merges ({dropped} base steps); tiered levels {:?}, base {} runs |",
        commit_us[commit_us.len() / 2],
        commit_us[commit_us.len() * 99 / 100],
        status.levels.iter().map(|l| l.0).collect::<Vec<_>>(),
        status.base.0
    );
    println!(
        "| Size | {:.1} MiB on disk for {:.1} MiB live logical bytes: {ratio:.2}x |",
        disk as f64 / MIB,
        logical as f64 / MIB
    );
    println!("| Overwrite-heavy | {overwrites} |");
    println!("| Explicit cleanup | {} |", cleanup::run(scratch, &key)?);
    println!(
        "| Compaction space | {} |",
        compaction_space::run(scratch, &key)?
    );
    println!("| Compact values (V1b) | {values_row} |");
    println!("| Read back | {checked} keys equal by key and by batch; {ranges} |");
    println!("| Held read across merges | {held_check} |");
    drop(home);
    println!(
        "| Crash leftovers | {} |",
        check_leftovers(&dir, &key, &model)?
    );
    println!("| kill -9 | {} |", crash::kill_nine(scratch, &key)?);
    println!(
        "| Owner lock and key | {} |",
        check_lock_and_key(&dir, &key)?
    );
    let mut ratios = vec![
        ("mixed", ratio),
        ("overwrite-heavy", overwrite_ratio),
        ("compact values", values_ratio),
    ];
    ratios.extend(bound::phases(scratch, &key)?);
    for (name, ratio) in ratios {
        if ratio > bound::LIMIT {
            return Err(format!("{name} disk size {ratio:.2}x is over the 2x limit").into());
        }
    }
    Ok(())
}

/// 400 commits that rewrite and delete the same 20,000 keys: old versions
/// and tombstones must not pile up past 2x the live bytes.
fn check_overwrites(scratch: &Path, key: &[u8; 32]) -> Result<(String, f64)> {
    let dir = scratch.join("overwrites");
    let home = RunHome::create(&dir, key)?;
    let mut model = Model::new();
    let mut worst = 0f64;
    for n in 1..=400 {
        let operations = overwrite_batch(n, BATCH_OPS);
        apply(&mut model, &operations);
        home.commit(operations)?;
        while home.merge_step()?.is_some() {}
        if n % 50 == 0 {
            worst = worst.max(dir_bytes(&dir)? as f64 / logical_bytes(&model) as f64);
        }
    }
    let checked = check_all(&home, &model)?;
    let disk = dir_bytes(&dir)?;
    let ratio = disk as f64 / logical_bytes(&model) as f64;
    let status = home.status();
    let levels: Vec<usize> = status.levels.iter().map(|l| l.0).collect();
    Ok((
        format!(
            "400 commits over 20,000 keys: {:.1} MiB on disk, {ratio:.2}x of live bytes at the end, worst {worst:.2}x at a 50-commit check; tiered levels {levels:?}, base {} runs; {checked} keys equal",
            disk as f64 / MIB,
            status.base.0
        ),
        ratio.max(worst),
    ))
}

/// Every model key by single get (a sample) and by batch (all of them).
pub(crate) fn check_all(home: &RunHome, model: &Model) -> Result<usize> {
    let mut read = home.read(budget());
    let entries: Vec<_> = model.iter().collect();
    for chunk in entries.chunks(10_000) {
        let coordinates: Vec<(String, String)> = chunk
            .iter()
            .map(|((c, k), _)| (c.clone(), k.clone()))
            .collect();
        let found = read.get_many(&coordinates)?;
        for ((coordinate, expected), got) in chunk.iter().zip(found) {
            if got? != **expected {
                return Err(format!("batch read differs at {coordinate:?}").into());
            }
        }
    }
    for ((collection, key), expected) in entries.iter().step_by(97) {
        if read.get(collection, key)? != **expected {
            return Err(format!("get differs at {collection}/{key:?}").into());
        }
    }
    for absent in ["never-written", "zz\0absent"] {
        if read.get("blob_counts", absent)?.is_some() {
            return Err("an absent key returned a value".into());
        }
    }
    Ok(entries.len())
}

fn check_ranges(home: &RunHome, model: &Model) -> Result<String> {
    let mut read = home.read(budget());
    let mut records = 0usize;
    for schema in 0..TIP_SCHEMAS {
        let prefix = tip_key(schema, 0);
        let prefix = &prefix[..prefix.find('\0').expect("NUL") + 1];
        for (start, end) in [("", None), ("00001000", Some("00002000"))] {
            let bounds = RangeBounds {
                start: start.to_string(),
                end: end.map(str::to_string),
            };
            let mut got = Vec::new();
            let mut cursor = read.hash_range("records", prefix, bounds)?;
            loop {
                let page = cursor.next_page(333)?;
                got.extend(page.records);
                if page.exhausted {
                    break;
                }
            }
            let lower = format!("{prefix}{start}");
            let expected: Vec<(String, Vec<u8>)> = model
                .range(("records".to_string(), lower)..)
                .take_while(|((c, k), _)| {
                    c == "records"
                        && k.starts_with(prefix)
                        && (end.is_none() || *k < format!("{prefix}{}", end.unwrap_or_default()))
                })
                .filter_map(|((_, k), v)| v.clone().map(|v| (k.clone(), v)))
                .collect();
            if got != expected {
                return Err(format!("range differs under {prefix:?} from {start:?}").into());
            }
            records += got.len();
        }
    }
    let mut cursor = read.administrative_range("blob_counts", b"", &[0xff])?;
    let atoms = cursor.next_page(usize::MAX)?;
    let live_atoms = model
        .iter()
        .filter(|((c, _), v)| c == "blob_counts" && v.is_some())
        .count();
    if !atoms.exhausted || atoms.records.len() != live_atoms {
        return Err("administrative range over atoms differs".into());
    }
    Ok(format!(
        "{} ranges, {records} records equal; administrative range {live_atoms} atoms equal",
        TIP_SCHEMAS * 2
    ))
}

/// A read taken at the midpoint must still see the midpoint state after
/// later commits and merges; merged runs stay on disk until it ends.
fn check_held(dir: &Path, home: &RunHome, held: (RunRead, Model)) -> Result<String> {
    let (mut read, then) = held;
    let sample: Vec<_> = then.iter().step_by(53).collect();
    for ((collection, key), expected) in &sample {
        if read.get(collection, key)? != **expected {
            return Err(format!("held read changed at {collection}/{key:?}").into());
        }
    }
    let in_current = home.status().runs();
    let (with_held, _) = run_files(dir)?;
    let generation = read.generation();
    drop(read);
    let (after, _) = run_files(dir)?;
    if with_held <= in_current || after != in_current {
        return Err(format!(
            "run files: {with_held} while held, {after} after, {in_current} in CURRENT"
        )
        .into());
    }
    Ok(format!(
        "{} keys at generation {generation} equal; {with_held} run files while held, {after} after the drop",
        sample.len()
    ))
}

/// Leave what a crash between a run write and CURRENT leaves, then reopen.
fn check_leftovers(dir: &Path, key: &[u8; 32], model: &Model) -> Result<String> {
    let runs = dir.join("runs");
    let mut orphan = RunWriter::create(&runs, key, RunOptions::default())?;
    orphan.push(&[0, 5, b'a', b't', b'o', b'm', b's', b'x'], Some(b"orphan"))?;
    orphan.finish()?;
    fs::write(
        runs.join(".00000000-0000-0000-0000-000000000001.tmp"),
        b"partial",
    )?;
    fs::write(dir.join(".CURRENT.partial.tmp"), b"partial")?;
    let home = RunHome::open(dir, key)?;
    let (named, leftovers) = run_files(dir)?;
    let in_current = home.status().runs();
    if named != in_current || leftovers != 0 || dir.join(".CURRENT.partial.tmp").exists() {
        return Err("reopen kept a leftover file".into());
    }
    let checked = check_all(&home, model)?;
    Ok(format!(
        "orphan run and temporary files removed on reopen; {checked} keys equal"
    ))
}

fn check_lock_and_key(dir: &Path, key: &[u8; 32]) -> Result<String> {
    let home = RunHome::open(dir, key)?;
    if RunHome::open(dir, key).is_ok() {
        return Err("a second owner opened a held home".into());
    }
    drop(home);
    let mut wrong = *key;
    wrong[0] ^= 1;
    if RunHome::open(dir, &wrong).is_ok() {
        return Err("a wrong key opened the home".into());
    }
    let home = RunHome::open(dir, key)?;
    let refused = home
        .commit(vec![HomeMutation {
            collection: "blob_counts".into(),
            key: "x".repeat(70 * 1024),
            body: Some(b"too long".to_vec()),
        }])
        .is_err();
    if !refused {
        return Err("an over-limit key was accepted".into());
    }
    Ok("second owner refused; wrong key refused; over-limit key refused".into())
}

/// Run files and temporary files in the home's run directory.
pub(crate) fn run_files(dir: &Path) -> Result<(usize, usize)> {
    let (mut runs, mut temporary) = (0, 0);
    for entry in fs::read_dir(dir.join("runs"))? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".ldrun") {
            runs += 1;
        } else if name.ends_with(".tmp") {
            temporary += 1;
        }
    }
    Ok((runs, temporary))
}

pub(crate) fn dir_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for path in [dir.to_path_buf(), dir.join("runs")] {
        for entry in fs::read_dir(path)? {
            let meta = entry?.metadata()?;
            if meta.is_file() {
                total += meta.len();
            }
        }
    }
    Ok(total)
}

fn random_key() -> Result<[u8; 32]> {
    let mut key = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut key)?;
    Ok(key)
}
