//! Replay sealed mutation-log segments from a local directory.
//!
//! The counterpart of the local-directory sink in `upload_cycle::directory`.
//! It reads the segment files, and hands them to the same peer-apply function
//! the live cloud cycle uses ([`SyncEngine::apply_peer_segments`]). So the
//! order, frontier, tip-decision and peer-apply-marker code is the real code.
//! Only the download is replaced by a directory read.

use super::upload_cycle::validated_relative_key;
use super::*;
use std::path::{Component, Path};

/// Upper bound on segment files one call will list.
const MAX_LISTED_SEGMENTS: usize = 100_000;
/// Upper bound on segment files one call will read.
const MAX_READ_SEGMENTS: usize = 10_000;
/// Upper bound on the bytes one call will hold: the replay keeps every
/// segment in memory.
const MAX_READ_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
/// Slack over the largest object a cloud download accepts, for the envelope.
const SEGMENT_SIZE_MARGIN_BYTES: u64 = 1024 * 1024;
/// The cloud object cap when the engine config sets none (0 means unlimited).
const DEFAULT_SEGMENT_CAP_BYTES: u64 = 16 * 1024 * 1024;
/// Upper bound on directory depth below `log/`.
const MAX_DIRECTORY_DEPTH: usize = 12;

/// Refuse a path that is a symlink or not a directory.
fn require_plain_dir(path: &Path, what: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => Ok(()),
        Ok(meta) if meta.file_type().is_symlink() => {
            Err(format!("{what} is a symlink: {}", path.display()))
        }
        Ok(_) => Err(format!("{what} is not a directory: {}", path.display())),
        Err(error) => Err(format!("cannot inspect {what} {}: {error}", path.display())),
    }
}

/// Collect the object keys of every `*.enc` file below `dir/log`, sorted.
/// Neither `dir` nor `log` may be a symlink; a missing `log` is an empty sink.
fn list_segment_keys(dir: &Path) -> Result<Vec<String>, String> {
    require_plain_dir(dir, "the segment directory")?;
    let root = dir.join("log");
    let mut keys = Vec::new();
    match std::fs::symlink_metadata(&root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(keys),
        _ => require_plain_dir(&root, "the segment log directory")?,
    }
    let mut stack = vec![(root, 0usize)];
    while let Some((path, depth)) = stack.pop() {
        if depth > MAX_DIRECTORY_DEPTH {
            return Err(format!(
                "segment directory is deeper than {MAX_DIRECTORY_DEPTH} levels: {}",
                path.display()
            ));
        }
        let entries = std::fs::read_dir(&path)
            .map_err(|e| format!("read segment directory {}: {e}", path.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("read segment directory entry: {e}"))?;
            let file_type = entry
                .file_type()
                .map_err(|e| format!("stat {}: {e}", entry.path().display()))?;
            if file_type.is_symlink() {
                return Err(format!(
                    "segment directory holds a symlink: {}",
                    entry.path().display()
                ));
            }
            if file_type.is_dir() {
                stack.push((entry.path(), depth + 1));
            } else if file_type.is_file() && entry.file_name().to_string_lossy().ends_with(".enc") {
                keys.push(object_key_of(dir, &entry.path())?);
                if keys.len() > MAX_LISTED_SEGMENTS {
                    return Err(format!(
                        "segment directory holds more than {MAX_LISTED_SEGMENTS} segments"
                    ));
                }
            }
        }
    }
    keys.sort();
    Ok(keys)
}

/// The `log/...` object key of a segment file, with `/` separators.
fn object_key_of(dir: &Path, path: &Path) -> Result<String, String> {
    let relative = path
        .strip_prefix(dir)
        .map_err(|_| format!("segment file is outside the directory: {}", path.display()))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| format!("segment path is not UTF-8: {}", path.display()))?,
            ),
            _ => return Err(format!("segment path is not plain: {}", path.display())),
        }
    }
    Ok(parts.join("/"))
}

/// Read one segment file without following a symlink, bounded by `max` bytes.
fn read_segment_file(path: &Path, max: u64) -> Result<Vec<u8>, String> {
    use std::io::Read as _;

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|e| format!("open segment file {}: {e}", path.display()))?;
    let len = file
        .metadata()
        .map_err(|e| format!("stat segment file {}: {e}", path.display()))?
        .len();
    if len > max {
        return Err(too_large(path, len, max));
    }
    let mut payload = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    file.take(max.saturating_add(1))
        .read_to_end(&mut payload)
        .map_err(|e| format!("read segment file {}: {e}", path.display()))?;
    if payload.len() as u64 > max {
        return Err(too_large(path, payload.len() as u64, max));
    }
    Ok(payload)
}

fn too_large(path: &Path, len: u64, max: u64) -> String {
    format!(
        "segment file {} is {len} bytes; the largest segment accepted is {max} bytes",
        path.display()
    )
}

/// Read every segment in `dir` that `incorporated` does not already cover.
///
/// Sizes are checked for all candidates before any file is read, so a refused
/// directory costs no memory. `max_file_bytes` is the cloud object cap.
fn read_directory_segments(
    dir: &Path,
    incorporated: &Frontier,
    max_file_bytes: u64,
) -> Result<Vec<MutationLogSegment>, String> {
    let keys = list_segment_keys(dir)?;
    let (candidates, _flat_skipped) =
        select_peer_apply_candidates(keys.iter().map(String::as_str), incorporated);
    if candidates.len() > MAX_READ_SEGMENTS {
        return Err(format!(
            "{} segment files to read; the limit is {MAX_READ_SEGMENTS}",
            candidates.len()
        ));
    }
    let max = max_file_bytes.saturating_add(SEGMENT_SIZE_MARGIN_BYTES);
    let mut total = 0u64;
    for (_writer, _through_id, key) in &candidates {
        let path = dir.join(validated_relative_key(key)?);
        let len = std::fs::symlink_metadata(&path)
            .map_err(|e| format!("stat segment file {}: {e}", path.display()))?
            .len();
        if len > max {
            return Err(too_large(&path, len, max));
        }
        total = total.saturating_add(len);
        if total > MAX_READ_TOTAL_BYTES {
            return Err(format!(
                "segment files hold more than {MAX_READ_TOTAL_BYTES} bytes; the limit is per call"
            ));
        }
    }
    let mut segments = Vec::with_capacity(candidates.len());
    for (_writer, through_id, key) in candidates {
        let segment = mutation_log_segment_id_from_object_key(&key, through_id)?;
        let payload = read_segment_file(&dir.join(validated_relative_key(&key)?), max)?;
        segments.push(MutationLogSegment { segment, payload });
    }
    segments.sort_by(|a, b| {
        a.segment
            .writer_id
            .cmp(&b.segment.writer_id)
            .then(a.segment.through_id.cmp(&b.segment.through_id))
    });
    Ok(segments)
}

impl SyncEngine {
    /// Apply the peer segments in `dir`, as the live peer-apply cycle applies
    /// segments it downloaded: drop this device's own writer, replay in
    /// writer-then-frontier order, then persist the applied frontier.
    ///
    /// Running it twice on one directory applies nothing the second time.
    pub async fn replay_mutation_log_from_directory(
        &self,
        dir: &Path,
    ) -> SyncResult<MutationLogReplayReport> {
        let target = self.targets.lock().await.first().cloned().ok_or_else(|| {
            SyncError::Storage("replay mutation-log directory: no personal sync target".into())
        })?;
        let incorporated = self.pin_log.incorporated_frontier().await;
        let max_file_bytes = match self.config.max_download_entry_bytes {
            0 => DEFAULT_SEGMENT_CAP_BYTES,
            configured => configured as u64,
        };
        let segments = read_directory_segments(dir, &incorporated, max_file_bytes)
            .map_err(SyncError::Storage)?;
        let report = self
            .apply_peer_segments(&target, segments, &incorporated)
            .await?;
        if report.segments_applied > 0 {
            self.mutation_log_peer_segments_applied.fetch_add(
                report.segments_applied as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if report.records_applied > 0 {
            self.mutation_log_peer_records_applied.fetch_add(
                report.records_applied as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if !report.frontier_after.is_empty() {
            self.pin_log
                .incorporate_applied_frontier(&report.frontier_after)
                .await
                .map_err(SyncError::Storage)?;
        }
        Ok(report)
    }
}
