//! Size bound phases (phase V1c): the disk size against the live logical
//! bytes at every moment. The values do not compress, so the file bytes
//! follow the record bytes and the bound of the merge plan shows directly.
//!
//! A sampler thread reads the size of the home's files every millisecond
//! while a phase commits and merges. A phase fails when its peak is over 2x
//! the live bytes while they are at least 16 MiB.

use crate::gen::{Model, Rng};
use crate::{check_all, dir_bytes, Result, MIB};
use laststore::disk_pack::home::HomeMutation;
use laststore::disk_run::home::{MergeKind, RunHome};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Below this many live bytes a sample does not count: a small home can
/// hold up to the base merge trigger minimum more.
const MIN_LIVE: u64 = 16 << 20;
/// The disk size limit: 2x the live bytes.
pub const LIMIT: f64 = 2.0;

/// Disk bytes against live bytes, read by a background thread.
struct Sampler {
    live: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<(f64, u64)>,
}

impl Sampler {
    fn start(dir: PathBuf) -> Self {
        let live = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (seen, done) = (Arc::clone(&live), Arc::clone(&stop));
        let handle = std::thread::spawn(move || {
            let (mut worst, mut peak) = (0f64, 0u64);
            while !done.load(Ordering::Acquire) {
                // A file can go between the listing and its size; that
                // sample is skipped.
                if let Ok(disk) = dir_bytes(&dir) {
                    peak = peak.max(disk);
                    let live = seen.load(Ordering::Acquire);
                    if live >= MIN_LIVE {
                        worst = worst.max(disk as f64 / live as f64);
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            (worst, peak)
        });
        Self { live, stop, handle }
    }

    /// The worst ratio and the largest disk size seen.
    fn finish(self) -> (f64, u64) {
        self.stop.store(true, Ordering::Release);
        self.handle.join().unwrap_or((f64::INFINITY, 0))
    }
}

/// Deterministic operations over a list of live keys. Values are random
/// bytes, so they do not compress.
struct Workload {
    rng: Rng,
    live: Vec<(String, String)>,
    big: Vec<String>,
    made: u64,
}

impl Workload {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            live: Vec::new(),
            big: Vec::new(),
            made: 0,
        }
    }

    fn value(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            out.extend_from_slice(&self.rng.next_u64().to_le_bytes());
        }
        out.truncate(len);
        out
    }

    fn small(&mut self) -> usize {
        32 + self.rng.below(480) as usize
    }

    fn put(&mut self, collection: &str, key: String, len: usize) -> HomeMutation {
        HomeMutation {
            collection: collection.to_string(),
            key,
            body: Some(self.value(len)),
        }
    }

    /// A new key, spread over the key space.
    fn insert(&mut self, len: usize) -> HomeMutation {
        self.made += 1;
        let key = format!("{:016x}", self.made.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        self.live.push(("atoms".to_string(), key.clone()));
        self.put("atoms", key, len)
    }

    /// The next key of a range that only grows, as the change feed writes.
    fn append(&mut self, len: usize) -> HomeMutation {
        self.made += 1;
        let key = format!("event:{:020}", self.made);
        self.live.push(("change_feed".to_string(), key.clone()));
        self.put("change_feed", key, len)
    }

    fn overwrite(&mut self, len: usize) -> HomeMutation {
        let at = self.rng.below(self.live.len() as u64) as usize;
        let (collection, key) = self.live[at].clone();
        self.put(&collection, key, len)
    }

    fn delete(&mut self) -> HomeMutation {
        let at = self.rng.below(self.live.len() as u64) as usize;
        let (collection, key) = self.live.swap_remove(at);
        HomeMutation {
            collection,
            key,
            body: None,
        }
    }

    /// A new value of `len` bytes in its own list, apart from the others.
    fn insert_big(&mut self, len: usize) -> HomeMutation {
        self.made += 1;
        let key = format!("big:{:08}", self.made);
        self.big.push(key.clone());
        self.put("blobs", key, len)
    }

    /// Replace a large value with 16 bytes: the old value becomes stale.
    fn shrink_big(&mut self) -> Option<HomeMutation> {
        let key = self.big.pop()?;
        Some(self.put("blobs", key, 16))
    }
}

/// What one phase did and measured.
#[derive(Default)]
struct Phase {
    commits: u64,
    tiered: u64,
    base_steps: u64,
    committed: u64,
    merged: u64,
    worst: f64,
    peak: u64,
    end_disk: u64,
    end_live: u64,
    checked: usize,
}

/// Apply a batch to the model, and keep the live byte count.
fn apply_live(model: &mut Model, operations: &[HomeMutation], live: &mut u64) {
    for op in operations {
        let size = |body: &Option<Vec<u8>>| {
            body.as_ref()
                .map_or(0, |v| (op.collection.len() + op.key.len() + v.len()) as u64)
        };
        let old = model.insert((op.collection.clone(), op.key.clone()), op.body.clone());
        *live -= old.as_ref().map_or(0, size);
        *live += size(&op.body);
    }
}

/// Commit `batches` batches from `next`, merge until settled after each,
/// and sample the size the whole time.
fn run_phase(
    dir: &Path,
    key: &[u8; 32],
    seed: u64,
    batches: u64,
    next: impl FnMut(u64, &mut Workload) -> Vec<HomeMutation>,
) -> Result<Phase> {
    let home = RunHome::create(dir, key)?;
    let sampler = Sampler::start(dir.to_path_buf());
    let driven = drive(&home, &sampler.live, seed, batches, next);
    let (worst, peak) = sampler.finish();
    let (mut phase, model, live) = driven?;
    phase.worst = worst;
    phase.peak = peak;
    phase.end_disk = dir_bytes(dir)?;
    phase.end_live = live;
    phase.checked = check_all(&home, &model)?;
    Ok(phase)
}

/// The batches of one phase. `sampled` holds the live bytes that the
/// sampler compares with the disk size.
fn drive(
    home: &RunHome,
    sampled: &AtomicU64,
    seed: u64,
    batches: u64,
    mut next: impl FnMut(u64, &mut Workload) -> Vec<HomeMutation>,
) -> Result<(Phase, Model, u64)> {
    let mut workload = Workload::new(seed);
    let mut model = Model::new();
    let (mut phase, mut live) = (Phase::default(), 0u64);
    for n in 0..batches {
        let operations = next(n, &mut workload);
        let before = live;
        apply_live(&mut model, &operations, &mut live);
        // Until the merges end, the disk can still hold what the batch
        // removed: count the smaller live size.
        sampled.store(before.min(live), Ordering::Release);
        phase.committed += home.commit(operations)?.run_bytes;
        while let Some(merge) = home.merge_step()? {
            phase.merged += merge.run_bytes;
            match merge.kind {
                MergeKind::Tiered { .. } => phase.tiered += 1,
                MergeKind::Base { .. } => phase.base_steps += 1,
            }
        }
        sampled.store(live, Ordering::Release);
        phase.commits += 1;
    }
    Ok((phase, model, live))
}

fn report(name: &str, phase: &Phase) -> String {
    let end = phase.end_disk as f64 / phase.end_live.max(1) as f64;
    format!(
        "| Bound: {name} | {} commits; {} tiered merges, {} base steps; {:.0} MiB merged for {:.0} MiB committed; peak {:.1} MiB, worst {:.2}x of live bytes while live >= 16 MiB; end {:.1} MiB for {:.1} MiB live ({end:.2}x); {} keys equal |",
        phase.commits,
        phase.tiered,
        phase.base_steps,
        phase.merged as f64 / MIB,
        phase.committed as f64 / MIB,
        phase.peak as f64 / MIB,
        phase.worst,
        phase.end_disk as f64 / MIB,
        phase.end_live as f64 / MIB,
        phase.checked,
    )
}

/// Run the three bound phases. Returns each phase's worst ratio.
pub fn phases(scratch: &Path, key: &[u8; 32]) -> Result<Vec<(&'static str, f64)>> {
    let mut out = Vec::new();
    // New keys, appends, overwrites and deletes: about 30 MiB live.
    let mixed = run_phase(&scratch.join("bound-mixed"), key, 0xb0_0d, 160, |_, w| {
        (0..1500)
            .map(|_| {
                let len = w.small();
                match w.rng.below(100) {
                    0..=44 => w.insert(len),
                    45..=54 => w.append(len),
                    _ if w.live.is_empty() => w.insert(len),
                    55..=89 => w.overwrite(len),
                    _ => w.delete(),
                }
            })
            .collect()
    })?;
    // About 42 MiB of inserts, then batches that delete 90% of their keys:
    // about 19 MiB stays live.
    let deletes = run_phase(
        &scratch.join("bound-deletes"),
        key,
        0xde_1e7e,
        160,
        |n, w| {
            (0..1500)
                .map(|_| {
                    let len = w.small();
                    match (n < 100, w.rng.below(10)) {
                        (true, _) => w.insert(len),
                        (false, 0) => w.overwrite(len),
                        (false, _) => w.delete(),
                    }
                })
                .collect()
        },
    )?;
    // 64 MiB of 256 KiB values and 20 MiB of small ones, then each large
    // value shrinks to 16 bytes. Only exact hidden byte counts see that the
    // shrunk values left 64 MiB stale.
    let large = run_phase(&scratch.join("bound-large"), key, 0x1a_7e, 128, |n, w| {
        let mut batch = Vec::new();
        if n < 64 {
            for _ in 0..4 {
                batch.push(w.insert_big(256 << 10));
            }
            for _ in 0..1100 {
                let len = w.small();
                batch.push(w.insert(len));
            }
        } else {
            batch.extend((0..4).filter_map(|_| w.shrink_big()));
            for _ in 0..100 {
                let len = w.small();
                batch.push(w.overwrite(len));
            }
        }
        batch
    })?;
    for (name, phase) in [
        ("insert, append, overwrite, delete", &mixed),
        ("delete-heavy", &deletes),
        ("large values shrunk", &large),
    ] {
        println!("{}", report(name, phase));
        let end = phase.end_disk as f64 / phase.end_live.max(1) as f64;
        out.push((name, phase.worst.max(end)));
    }
    Ok(out)
}
