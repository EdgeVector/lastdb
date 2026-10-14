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

/// Upper bound on segment files one call will read.
const MAX_DIRECTORY_SEGMENTS: usize = 100_000;
/// Upper bound on directory depth below `log/`.
const MAX_DIRECTORY_DEPTH: usize = 12;

/// Collect the object keys of every `*.enc` file below `dir/log`, sorted.
fn list_segment_keys(dir: &Path) -> Result<Vec<String>, String> {
    let root = dir.join("log");
    let mut keys = Vec::new();
    if !root.exists() {
        return Ok(keys);
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
                if keys.len() > MAX_DIRECTORY_SEGMENTS {
                    return Err(format!(
                        "segment directory holds more than {MAX_DIRECTORY_SEGMENTS} segments"
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

/// Read every segment in `dir` that `incorporated` does not already cover.
fn read_directory_segments(
    dir: &Path,
    incorporated: &Frontier,
) -> Result<Vec<MutationLogSegment>, String> {
    let keys = list_segment_keys(dir)?;
    let (candidates, _flat_skipped) =
        select_peer_apply_candidates(keys.iter().map(String::as_str), incorporated);
    let mut segments = Vec::with_capacity(candidates.len());
    for (_writer, through_id, key) in candidates {
        let segment = mutation_log_segment_id_from_object_key(&key, through_id)?;
        let path = dir.join(validated_relative_key(&key)?);
        let payload = std::fs::read(&path)
            .map_err(|e| format!("read segment file {}: {e}", path.display()))?;
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
        let segments = read_directory_segments(dir, &incorporated).map_err(SyncError::Storage)?;
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
