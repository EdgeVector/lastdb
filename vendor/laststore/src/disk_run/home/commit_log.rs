//! The commit log: files that each commit appends one frame to, with one ordered flush, and the
//! replay that rebuilds the memory layer when a home opens.
//!
//! A frame's counter is the commit's frontier. Replay keeps the frames above the CURRENT
//! frontier and requires them to be consecutive. A seal publishes them in a run, and the files
//! whose commits CURRENT then holds are removed.

use super::memory::Memory;
use super::unpoison;
use crate::disk_run::codec::{corrupt, decode_block, BlockRecords};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame, encoded_len, MAX_DECOMPRESSED_FRAME_BYTES};
use crate::{Error, Result};
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Directory of the commit log inside a home.
pub const LOG: &str = "commit-log";
const SUFFIX: &str = ".log";
/// Bytes after which appends move to the next log file. A larger commit gets a file of its own.
const FILE_BYTES: u64 = 16 << 20;
const MAX_FILES: usize = 4096;
/// Largest commit payload the log takes. A larger commit is sealed into a run of its own.
pub(super) const MAX_PAYLOAD: usize = MAX_DECOMPRESSED_FRAME_BYTES / 2;

struct LogFile {
    seq: u64,
    file: File,
    used: u64,
    cap: u64,
    last_ctr: u64,
}

#[derive(Default)]
struct LogState {
    active: Option<LogFile>,
    spare: Option<LogFile>,
    /// Files that take no more appends: sequence number and last commit number.
    retired: Vec<(u64, u64)>,
}

/// Log files that a replay found.
pub(super) struct LogFiles {
    next_seq: u64,
    files: Vec<(u64, u64)>,
}

/// The open commit log of one home. Appends and file switches happen under the commit lock.
pub(super) struct CommitLog {
    dir: PathBuf,
    next_seq: AtomicU64,
    state: Mutex<LogState>,
    closed: AtomicBool,
    flushes: AtomicU64,
    flush_nanos: AtomicU64,
    slowest_nanos: AtomicU64,
}

/// Ordered flushes of one home's commit log since the home opened, and its memory layer.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct CommitLogStats {
    /// Commits appended and flushed.
    pub flushes: u64,
    /// Mean time of one append and its ordered flush, in microseconds.
    pub mean_flush_micros: u64,
    /// Slowest append and ordered flush, in microseconds.
    pub slowest_flush_micros: u64,
    /// Logged commits that no run holds yet.
    pub memory_commits: u64,
    /// Log bytes of those commits.
    pub memory_bytes: u64,
}

fn file_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}{SUFFIX}"))
}

/// Create one empty log file. It grows with each append; a frame cut short by a crash is the
/// end of the file at replay. Files are not preallocated: an idle home holds no empty log bytes.
fn create_file(dir: &Path, seq: u64, cap: u64) -> Result<LogFile> {
    let path = file_path(dir, seq);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    sync_directory(dir).inspect_err(|_| drop(fs::remove_file(&path)))?;
    Ok(LogFile {
        seq,
        file,
        used: 0,
        cap,
        last_ctr: 0,
    })
}

impl CommitLog {
    /// Open for appends with a new empty file after the replayed ones.
    pub(super) fn open(dir: PathBuf, found: Option<LogFiles>) -> Result<Self> {
        if found.is_none() {
            fs::DirBuilder::new().mode(0o700).create(&dir)?;
            sync_directory(dir.parent().unwrap_or(Path::new(".")))?;
        }
        let found = found.unwrap_or(LogFiles {
            next_seq: 1,
            files: Vec::new(),
        });
        let log = Self {
            dir,
            next_seq: AtomicU64::new(found.next_seq),
            state: Mutex::new(LogState {
                retired: found.files,
                ..LogState::default()
            }),
            closed: AtomicBool::new(false),
            flushes: AtomicU64::new(0),
            flush_nanos: AtomicU64::new(0),
            slowest_nanos: AtomicU64::new(0),
        };
        let active = log.new_file(FILE_BYTES)?;
        unpoison(&log.state).active = Some(active);
        Ok(log)
    }

    fn new_file(&self, cap: u64) -> Result<LogFile> {
        create_file(
            &self.dir,
            self.next_seq.fetch_add(1, Ordering::Relaxed),
            cap,
        )
    }

    /// True after [`CommitLog::remove`]: later commits write runs.
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Append the frame of commit `ctr` and flush it in order. The caller holds the commit lock.
    pub(super) fn append(&self, frame: &[u8], ctr: u64) -> Result<()> {
        let len = frame.len() as u64;
        let mut st = unpoison(&self.state);
        if st.active.as_ref().is_none_or(|a| a.used + len > a.cap) {
            let fresh = match st.spare.take() {
                Some(spare) if spare.cap >= len => spare,
                other => {
                    st.retired.extend(other.map(|f| (f.seq, 0)));
                    self.new_file(len.max(FILE_BYTES))?
                }
            };
            if let Some(old) = st.active.replace(fresh) {
                st.retired.push((old.seq, old.last_ctr));
            }
        }
        let active = st.active.as_mut().expect("an active log file");
        let start = Instant::now();
        active.file.write_all_at(frame, active.used)?;
        sync_dirty_file(&active.file)?;
        active.used += len;
        active.last_ctr = ctr;
        let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.flush_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.slowest_nanos.fetch_max(nanos, Ordering::Relaxed);
        Ok(())
    }

    /// Create the file that the next seal switches appends to, outside the commit lock.
    pub(super) fn add_spare(&self) -> Result<()> {
        if unpoison(&self.state).spare.is_some() {
            return Ok(());
        }
        let file = self.new_file(FILE_BYTES)?;
        let mut st = unpoison(&self.state);
        match &st.spare {
            None => st.spare = Some(file),
            Some(_) => st.retired.push((file.seq, 0)),
        }
        Ok(())
    }

    /// Send later appends to the spare file. The caller holds the commit lock.
    pub(super) fn switch_to_spare(&self) {
        let mut st = unpoison(&self.state);
        if let Some(fresh) = st.spare.take() {
            if let Some(old) = st.active.replace(fresh) {
                st.retired.push((old.seq, old.last_ctr));
            }
        }
    }

    /// Remove the retired files whose commits a published CURRENT at `frontier` holds.
    pub(super) fn prune(&self, frontier: u64) -> Result<()> {
        let doomed: Vec<u64> = {
            let mut st = unpoison(&self.state);
            let (doomed, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut st.retired)
                .into_iter()
                .partition(|&(_, last)| last <= frontier);
            st.retired = keep;
            doomed.into_iter().map(|(seq, _)| seq).collect()
        };
        for &seq in &doomed {
            if let Err(e) = fs::remove_file(file_path(&self.dir, seq)) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }
        if doomed.is_empty() {
            Ok(())
        } else {
            sync_directory(&self.dir)
        }
    }

    /// Remove the whole log after a seal published every logged commit.
    pub(super) fn remove(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        *unpoison(&self.state) = LogState::default();
        remove_dir(&self.dir)
    }

    /// Flush counts since open, and the memory layer of `mem`.
    pub(super) fn stats(&self, mem: &Memory) -> CommitLogStats {
        let flushes = self.flushes.load(Ordering::Relaxed);
        let micros = |nanos: u64| nanos / 1000;
        CommitLogStats {
            flushes,
            mean_flush_micros: micros(self.flush_nanos.load(Ordering::Relaxed) / flushes.max(1)),
            slowest_flush_micros: micros(self.slowest_nanos.load(Ordering::Relaxed)),
            memory_commits: mem.commits(),
            memory_bytes: mem.bytes(),
        }
    }
}

/// Remove a log directory and its files, then sync the home directory.
pub(super) fn remove_dir(dir: &Path) -> Result<()> {
    match fs::remove_dir_all(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        res => res?,
    }
    sync_directory(dir.parent().unwrap_or(Path::new(".")))
}

/// Rebuild the memory layer from the log of the home at `root`, whose CURRENT holds every
/// commit up to `sealed`. `None` for the files when the home has no log.
pub(super) fn replay(
    root: &Path,
    key: &[u8; 32],
    sealed: u64,
) -> Result<(Memory, Option<LogFiles>)> {
    let dir = root.join(LOG);
    let entries = match fs::read_dir(&dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Memory::default(), None)),
        res => res?,
    };
    let mut seqs = Vec::new();
    for entry in entries {
        if seqs.len() >= MAX_FILES {
            return Err(Error::WriteCapacity("commit log has too many files".into()));
        }
        let name = entry?.file_name();
        let text = name.to_string_lossy();
        seqs.extend(
            text.strip_suffix(SUFFIX)
                .and_then(|s| s.parse::<u64>().ok()),
        );
    }
    seqs.sort_unstable();
    let (mut frames, mut files) = (Vec::new(), Vec::new());
    for &seq in &seqs {
        let bytes = fs::read(file_path(&dir, seq))?;
        files.push((seq, read_frames(key, &bytes, sealed, &mut frames)?));
    }
    frames.sort_unstable_by_key(|(ctr, _, _)| *ctr);
    let mut memory = Memory::default();
    for (n, (ctr, records, cost)) in (1u64..).zip(frames) {
        if Some(ctr) != sealed.checked_add(n) {
            return Err(corrupt("commit log: a logged commit is missing"));
        }
        memory.push(records, cost);
    }
    let next_seq = seqs.last().map_or(1, |s| s + 1);
    Ok((memory, Some(LogFiles { next_seq, files })))
}

/// Decode the frames of one log file and keep those above `sealed`. Returns the last commit
/// number in the file.
fn read_frames(
    key: &[u8; 32],
    bytes: &[u8],
    sealed: u64,
    out: &mut Vec<(u64, BlockRecords, u64)>,
) -> Result<u64> {
    let (mut at, mut last) = (0usize, 0u64);
    while let Some(len) = encoded_len(&bytes[at..]).filter(|&n| n <= bytes.len() - at) {
        match decode_frame(
            key,
            &bytes[at..at + len],
            MAX_DECOMPRESSED_FRAME_BYTES,
            (None, None),
        ) {
            Ok((_, ctr, payload)) => {
                if ctr > sealed {
                    out.push((ctr, decode_block(&payload)?, len as u64));
                }
                last = last.max(ctr);
                at += len;
            }
            // A torn last append ends the file. A whole frame after it would be a lost commit.
            Err(_) if encoded_len(&bytes[at + len..]).is_some() => {
                return Err(corrupt("commit log: an unreadable logged commit"));
            }
            Err(_) => break,
        }
    }
    Ok(last)
}
