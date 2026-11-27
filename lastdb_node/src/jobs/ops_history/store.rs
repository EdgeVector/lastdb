//! The history files: `ops-<unix seconds>.jsonl` in one folder, mode 0600, where the number is
//! the time the file started. A daemon appends to the newest file while it is from the same
//! UTC day and under `FILE_BYTES`; otherwise it starts a new file.
//!
//! Before a file opens, the oldest files go first: each file whose successor started more than
//! `RETENTION_SECS` ago, then the oldest files until the folder holds at most `CAP_BYTES` with
//! room for one more full file. Nothing is flushed to the drive. The writer keeps one open file
//! and its size; it holds no lines.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// Files whose data is all older than this go first: 14 days.
pub(super) const RETENTION_SECS: u64 = 14 * DAY_SECS;
/// The most the folder holds: 64 MiB.
pub(super) const CAP_BYTES: u64 = 64 << 20;
/// The size at which a new file starts: 4 MiB.
pub(super) const FILE_BYTES: u64 = 4 << 20;
pub(super) const DAY_SECS: u64 = 86_400;
const PREFIX: &str = "ops-";
const SUFFIX: &str = ".jsonl";

/// One history file: the time it started, its path and its size.
pub(super) struct Found {
    pub(super) start: u64,
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

struct Open {
    file: File,
    start: u64,
    bytes: u64,
}

pub(super) struct Writer {
    dir: PathBuf,
    open: Option<Open>,
    failed: bool,
}

impl Writer {
    pub(super) const fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            open: None,
            failed: false,
        }
    }

    /// Append one line. A failure is logged once until a line is written again; the next
    /// line opens the file again.
    pub(super) fn append(&mut self, line: &[u8], now: u64) {
        match self.try_append(line, now) {
            Ok(()) => self.failed = false,
            Err(error) => {
                self.open = None;
                if !self.failed {
                    tracing::warn!(target: "lastdbd::ops_history", error = %error, "could not write the ops history");
                }
                self.failed = true;
            }
        }
    }

    fn try_append(&mut self, line: &[u8], now: u64) -> io::Result<()> {
        let len = line.len() as u64;
        let fits =
            |open: &Open| open.start / DAY_SECS == now / DAY_SECS && open.bytes + len <= FILE_BYTES;
        if !self.open.as_ref().is_some_and(fits) {
            self.open = None;
            self.open = Some(self.open_file(now, len)?);
        }
        if let Some(open) = self.open.as_mut() {
            open.file.write_all(line)?;
            open.bytes += len;
        }
        Ok(())
    }

    fn open_file(&self, now: u64, len: u64) -> io::Result<Open> {
        if !check_dir(&self.dir)? {
            if let Err(error) = DirBuilder::new().mode(0o700).create(&self.dir) {
                if error.kind() != io::ErrorKind::AlreadyExists || !check_dir(&self.dir)? {
                    return Err(error);
                }
            }
        }
        prune(&self.dir, now)?;
        let newest = files(&self.dir)?.pop().filter(|found| {
            found.start / DAY_SECS == now / DAY_SECS && found.bytes + len <= FILE_BYTES
        });
        let (start, path) = newest.map_or_else(
            || (now, self.dir.join(format!("{PREFIX}{now}{SUFFIX}"))),
            |found| (found.start, found.path),
        );
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?;
        let mut bytes = file.metadata()?.len();
        // A crash can cut the last line short. End it, so the next line stands alone.
        let mut last = [0u8; 1];
        if bytes > 0 && file.read_at(&mut last, bytes - 1)? == 1 && last[0] != b'\n' {
            (&file).write_all(b"\n")?;
            bytes += 1;
        }
        Ok(Open { file, start, bytes })
    }
}

fn check_dir(dir: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ops-history path is not a directory",
        )),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// The history files in `dir`, oldest first.
pub(super) fn files(dir: &Path) -> io::Result<Vec<Found>> {
    if !check_dir(dir)? {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Some(start) = entry.file_name().to_str().and_then(start_of) else {
            continue;
        };
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.is_file() && !meta.file_type().is_symlink() {
            found.push(Found {
                start,
                path,
                bytes: meta.len(),
            });
        }
    }
    found.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then(left.path.cmp(&right.path))
    });
    Ok(found)
}

fn start_of(name: &str) -> Option<u64> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn prune(dir: &Path, now: u64) -> io::Result<()> {
    let found = files(dir)?;
    let mut total: u64 = found.iter().map(|file| file.bytes).sum();
    let cutoff = now.saturating_sub(RETENTION_SECS);
    for file in &found {
        let day_end = (file.start / DAY_SECS)
            .saturating_add(1)
            .saturating_mul(DAY_SECS);
        if day_end > cutoff && total + FILE_BYTES <= CAP_BYTES {
            break;
        }
        if let Err(error) = fs::remove_file(&file.path) {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
        total = total.saturating_sub(file.bytes);
    }
    Ok(())
}
