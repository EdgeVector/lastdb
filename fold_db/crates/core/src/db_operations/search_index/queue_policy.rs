//! Optional, bounded Search delivery. Overflow requires an explicit rebuild.

use laststore::commit_timing as timing;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub const REBUILD_REQUIRED: &str = ".rebuild-required";
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_FILES: usize = 4096;
static CLI_ENABLED: AtomicBool = AtomicBool::new(false);

/// Set before boot; the default daemon and library write paths keep Search off.
pub fn enable_for_process() {
    CLI_ENABLED.store(true, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    CLI_ENABLED.load(Ordering::Relaxed)
        || std::env::var("LASTDB_SEARCH_ENABLED")
            .is_ok_and(|value| matches!(value.trim(), "1" | "true" | "on"))
}

/// The byte cap counts batch file lengths. Filesystem allocation stays explicit in CI.
pub fn max_bytes() -> u64 {
    std::env::var("LASTDB_SEARCH_QUEUE_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_MAX_BYTES)
        .clamp(64 * 1024, 1024 * 1024 * 1024)
}

pub fn status(inbox: &Path) -> serde_json::Value {
    let marker = inbox.join(REBUILD_REQUIRED).try_exists();
    serde_json::json!({
        "enabled": enabled(), "max_batch_bytes": max_bytes(), "max_batch_files": MAX_FILES,
        "rebuild_required": marker.as_ref().ok(),
        "status_error": marker.err().map(|error| error.to_string()),
    })
}

/// Keep a previous consumer from treating writes made while disabled as a complete feed.
pub fn prepare(inbox: &Path) -> io::Result<()> {
    if !enabled() && inbox.try_exists()? {
        let _lock = producer_lock(inbox)?;
        mark_rebuild(inbox, "search_disabled")?;
    }
    Ok(())
}

fn producer_lock(inbox: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(inbox.join(".producer.lock"))?;
    file.lock()?;
    Ok(file)
}

fn mark_rebuild(inbox: &Path, reason: &str) -> io::Result<()> {
    if inbox.join(REBUILD_REQUIRED).try_exists()? {
        return Ok(());
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "format_version": 1, "rebuild_required": true, "reason": reason,
        "max_batch_bytes": max_bytes(), "max_batch_files": MAX_FILES,
    }))?;
    atomic_write(inbox, REBUILD_REQUIRED, &body)?;
    tracing::warn!(
        reason,
        "Search delivery paused; rebuild Search from LastDB before resuming the queue"
    );
    Ok(())
}

fn fits(inbox: &Path, incoming: u64) -> io::Result<bool> {
    let mut bytes = incoming;
    let mut batches = 1usize;
    let deadline = Instant::now() + Duration::from_secs(1);
    for (entries, entry) in fs::read_dir(inbox)?.enumerate() {
        if entries >= MAX_FILES + 16 || Instant::now() >= deadline {
            return Ok(false);
        }
        let entry = entry?;
        let info = match entry.path().symlink_metadata() {
            Ok(info) => info,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !info.is_file() {
            return Ok(false);
        }
        if entry.file_name() != ".producer.lock" {
            batches += 1;
            bytes = bytes.saturating_add(info.len());
        }
        if batches > MAX_FILES || bytes > max_bytes() {
            return Ok(false);
        }
    }
    Ok(bytes <= max_bytes())
}

/// A cap stops delivery, never database writes. Existing batches stay intact.
pub(super) fn deliver(inbox: &Path, name: &str, body: &[u8]) -> io::Result<bool> {
    timing::during("search_mkdir_us", || fs::create_dir_all(inbox))?;
    let _lock = timing::during("search_lock_wait_us", || producer_lock(inbox))?;
    if timing::during("search_marker_check_us", || {
        inbox.join(REBUILD_REQUIRED).try_exists()
    })? {
        timing::count("search_rebuild_paused", 1);
        return Ok(false);
    }
    if !timing::during("search_capacity_check_us", || {
        fits(inbox, body.len() as u64)
    })? {
        timing::during("search_mark_rebuild_us", || {
            mark_rebuild(inbox, "queue_limit")
        })?;
        timing::count("search_queue_limit", 1);
        return Ok(false);
    }
    atomic_write(inbox, name, body)?;
    timing::count("search_delivered", 1);
    Ok(true)
}

fn atomic_write(inbox: &Path, name: &str, body: &[u8]) -> io::Result<()> {
    let mut temp = timing::during("search_file_create_us", || {
        tempfile::NamedTempFile::new_in(inbox)
    })?;
    timing::during("search_file_write_us", || temp.write_all(body))?;
    timing::during("search_file_flush_us", || temp.as_file().sync_all())?;
    timing::during("search_rename_us", || {
        temp.persist_noclobber(inbox.join(name))
    })
    .map_err(|error| error.error)?;
    timing::during("search_directory_flush_us", || {
        File::open(inbox)?.sync_all()
    })
}
