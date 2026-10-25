//! Disk run proof (design-lastdb-disk-size-20261024, phase V1). CI only.
//!
//! Writes the synthetic brain-shaped seed through the real `disk_run` module,
//! reads every record back by full scan, by key and by range under one hash,
//! replays overwrites and deletes through incremental runs and merges, checks
//! that a changed byte fails authentication, and fails the run when the disk
//! size is over 2x the logical atom bytes. No primary or user data is opened.

mod levels;
mod seed;

use laststore::disk_run::{Found, MergeIter, RunOptions, RunReader, RunWriter};
use levels::Levels;
use seed::{build_stream, v2_records_in_write_order, StreamOptions, MIB};
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::ops::Bound;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
/// Expected visible state: `None` is a deleted key.
pub(crate) type Expected = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

const BATCH_KEYS: usize = 10_000;
/// Class byte, hash group and molecule, then the 16-byte hash of a tip key.
const TIP_HASH_PREFIX: usize = 1 + 2 + 4 + 16;

struct Row {
    name: &'static str,
    bytes: u64,
    peak: Option<u64>,
    written: Option<u64>,
    records: u64,
    secs: f64,
    check: String,
}

fn main() -> Result<()> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
        return Err("the disk run proof runs only in GitHub Actions".into());
    }
    let base = std::env::var_os("RUNNER_TEMP").map_or_else(std::env::temp_dir, PathBuf::from);
    let scratch = base.join(format!("disk-run-proof-{}", std::process::id()));
    fs::create_dir(&scratch)?;
    let result = run(&scratch);
    let _ = fs::remove_dir_all(&scratch);
    result
}

fn run(scratch: &Path) -> Result<()> {
    let key = random_key()?;
    let stream = build_stream(&StreamOptions { proteins: false });
    let logical = stream.logical_atom_bytes;
    let mut rows = Vec::new();
    for (name, ope, dir) in [
        ("V1 one run, today's OPE range", true, "single-ope"),
        ("V1 one run, raw range", false, "single-raw"),
    ] {
        let records = v2_records_in_write_order(&stream, ope);
        rows.push(single_run(&fresh(scratch, dir)?, &key, name, &records)?);
    }
    let records = v2_records_in_write_order(&stream, true);
    rows.push(incremental(
        &fresh(scratch, "incremental")?,
        &key,
        &records,
    )?);
    let tamper = tamper_check(&fresh(scratch, "tamper")?, &key, &records)?;
    println!(
        "seed: {} atoms, {:.1} MiB logical, {} records",
        stream.atoms,
        logical as f64 / MIB,
        records.len()
    );
    println!(
        "| Variant | Disk MiB | Ratio | Peak MiB | Written MiB | Records | Write s | Read-back |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    let mib = |b: Option<u64>| b.map_or("-".to_string(), |b| format!("{:.1}", b as f64 / MIB));
    for row in &rows {
        println!(
            "| {} | {:.1} | {:.2}x | {} | {} | {} | {:.1} | {} |",
            row.name,
            row.bytes as f64 / MIB,
            row.bytes as f64 / logical as f64,
            mib(row.peak),
            mib(row.written),
            row.records,
            row.secs,
            row.check
        );
    }
    println!("Tamper check: {tamper}");
    if rows.iter().any(|r| r.bytes > 2 * logical) {
        return Err("disk size is over 2x the logical atom bytes".into());
    }
    Ok(())
}

fn single_run(
    dir: &Path,
    key: &[u8; 32],
    name: &'static str,
    records: &[(Vec<u8>, Vec<u8>)],
) -> Result<Row> {
    let expected: Expected = records
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();
    let start = Instant::now();
    let mut writer = RunWriter::create(dir, key, RunOptions::default())?;
    for (k, v) in &expected {
        writer.push(k, v.as_deref())?;
    }
    let summary = writer.finish()?;
    let secs = start.elapsed().as_secs_f64();
    let runs = vec![summary.path.clone()];
    let check = check_runs(&runs, key, &expected)?;
    let sample: Vec<&[u8]> = expected.keys().step_by(37).map(Vec::as_slice).collect();
    let reader = RunReader::open(&summary.path, key)?;
    for (k, found) in sample.iter().zip(reader.get_many(&sample)?) {
        if found.as_ref().and_then(Found::value) != expected[*k].as_deref() {
            return Err("get_many differs".into());
        }
    }
    Ok(Row {
        name,
        bytes: dir_bytes(dir)?,
        peak: None,
        written: None,
        records: summary.records,
        secs,
        check: format!("{check}; get_many {} equal", sample.len()),
    })
}

/// Loads the seed in write order through 10,000-key runs, then 5% overwrites
/// and 2% deletes. Eight runs in one level merge into one run of the next
/// level, newest first. A final merge drops the tombstones.
fn incremental(dir: &Path, key: &[u8; 32], records: &[(Vec<u8>, Vec<u8>)]) -> Result<Row> {
    let mut updates: Vec<(Vec<u8>, Option<Vec<u8>>)> = records
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();
    for (n, (k, v)) in records.iter().enumerate() {
        if n % 20 == 0 {
            updates.push((k.clone(), Some([v.as_slice(), b"!v2"].concat())));
        } else if n % 50 == 1 {
            updates.push((k.clone(), None));
        }
    }
    let start = Instant::now();
    let mut state = Levels::default();
    let mut expected = Expected::new();
    for chunk in updates.chunks(BATCH_KEYS) {
        let batch: Expected = chunk.iter().cloned().collect();
        expected.extend(batch.clone());
        state.add(dir, key, &batch)?;
    }
    let before = check_runs(&state.newest_first(), key, &expected)?;
    state.merge_all(dir, key)?;
    let secs = start.elapsed().as_secs_f64();
    let after = check_runs(&state.newest_first(), key, &expected)?;
    Ok(Row {
        name: "V1 incremental, 10,000-key runs, merge at 8, OPE range",
        bytes: dir_bytes(dir)?,
        peak: Some(state.peak),
        written: Some(state.written),
        records: expected.values().filter(|v| v.is_some()).count() as u64,
        secs,
        check: format!("before final merge: {before}; after: {after}"),
    })
}

/// Full scan, point reads with per-call open, and ranges under one hash,
/// across `runs` (newest first), against `expected`.
fn check_runs(runs: &[PathBuf], key: &[u8; 32], expected: &Expected) -> Result<String> {
    let readers: Vec<RunReader> = runs
        .iter()
        .map(|p| RunReader::open(p, key))
        .collect::<laststore::Result<_>>()?;
    let visible = check_visible(
        MergeIter::new(readers.iter().map(RunReader::iter).collect()),
        expected,
        ..,
    )?;
    let mut times = Vec::new();
    let sample: Vec<&Vec<u8>> = expected.keys().step_by(29).take(20_000).collect();
    for k in &sample {
        let start = Instant::now();
        let found = lookup(runs, key, k)?;
        times.push(start.elapsed());
        if found != expected[*k] {
            return Err("point read differs".into());
        }
        let mut absent = (*k).clone();
        absent.extend_from_slice(&[0xfe, 0xfe, 0xfe]);
        if !expected.contains_key(&absent) && lookup(runs, key, &absent)?.is_some() {
            return Err("absent key found".into());
        }
    }
    let prefixes: Vec<Vec<u8>> = expected
        .keys()
        .filter(|k| k.first() == Some(&2) && k.len() > TIP_HASH_PREFIX)
        .step_by(211)
        .take(500)
        .map(|k| k[..TIP_HASH_PREFIX].to_vec())
        .collect();
    let mut ranged = 0;
    for prefix in &prefixes {
        let upper = successor(prefix);
        let lower = Bound::Included(prefix.as_slice());
        let ranges = readers
            .iter()
            .map(|r| r.range(lower, Bound::Excluded(upper.as_slice())))
            .collect();
        ranged += check_visible(
            MergeIter::new(ranges),
            expected,
            prefix.clone()..upper.clone(),
        )?;
    }
    times.sort();
    let pct = |p: usize| times[(times.len() * p / 100).min(times.len() - 1)];
    Ok(format!(
        "{visible} records equal; {} point reads equal (p50 {}, p99 {}); {} ranges, {ranged} records equal",
        sample.len(),
        micros(pct(50)),
        micros(pct(99)),
        prefixes.len()
    ))
}

fn check_visible<I, R>(merged: I, expected: &Expected, range: R) -> Result<u64>
where
    I: Iterator<Item = laststore::Result<(Vec<u8>, Found)>>,
    R: std::ops::RangeBounds<Vec<u8>>,
{
    let mut want = expected
        .range(range)
        .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)));
    let mut equal = 0;
    for record in merged {
        let (k, found) = record?;
        let Found::Value(value) = found else { continue };
        if want.next() != Some((&k, &value)) {
            return Err("scan differs from the expected records".into());
        }
        equal += 1;
    }
    if want.next().is_some() {
        return Err("scan ended before the expected records".into());
    }
    Ok(equal)
}

/// Newest run first; the first run that holds the key decides.
fn lookup(runs: &[PathBuf], key: &[u8; 32], k: &[u8]) -> Result<Option<Vec<u8>>> {
    for run in runs {
        if let Some(found) = RunReader::open(run, key)?.get(k)? {
            return Ok(found.value().map(<[u8]>::to_vec));
        }
    }
    Ok(None)
}

/// A changed data byte must fail authentication; a changed footer byte must
/// fail the open.
fn tamper_check(dir: &Path, key: &[u8; 32], records: &[(Vec<u8>, Vec<u8>)]) -> Result<String> {
    let sorted: BTreeMap<_, _> = records.iter().take(50_000).cloned().collect();
    let mut writer = RunWriter::create(dir, key, RunOptions::default())?;
    for (k, v) in &sorted {
        writer.push(k, Some(v))?;
    }
    let summary = writer.finish()?;
    let first = sorted.keys().next().expect("seed is not empty");
    flip(&summary.path, 100)?;
    let data = RunReader::open(&summary.path, key)?.get(first);
    if !matches!(data, Err(laststore::Error::AeadAuthFail)) {
        return Err("a changed data byte was not detected".into());
    }
    flip(&summary.path, summary.file_bytes - 10)?;
    if RunReader::open(&summary.path, key).is_ok() {
        return Err("a changed footer byte was not detected".into());
    }
    Ok("changed data byte fails authentication; changed footer byte fails the open".into())
}

fn flip(path: &Path, offset: u64) -> Result<()> {
    let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    let mut byte = [0u8];
    file.read_exact_at(&mut byte, offset)?;
    byte[0] ^= 0x40;
    file.write_all_at(&byte, offset)?;
    Ok(())
}

fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xff {
            out.push(last + 1);
            return out;
        }
    }
    vec![0xff; prefix.len() + 1]
}

fn random_key() -> Result<[u8; 32]> {
    let mut key = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut key)?;
    Ok(key)
}

fn fresh(scratch: &Path, name: &str) -> Result<PathBuf> {
    let dir = scratch.join(name);
    fs::create_dir(&dir)?;
    Ok(dir)
}

fn dir_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        total += entry?.metadata()?.blocks() * 512;
    }
    Ok(total)
}

fn micros(d: Duration) -> String {
    format!("{} us", d.as_micros())
}
