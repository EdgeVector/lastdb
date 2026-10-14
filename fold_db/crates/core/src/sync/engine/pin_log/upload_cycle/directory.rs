//! Local-directory sink for sealed mutation-log segments.
//!
//! Runs the same scan, materialize, plan, seal and confirm steps as the cloud
//! upload cycle ([`PinLog::run_mutation_log_segment_upload_cycle`]). Only the
//! transport differs: each sealed segment is written under a local directory
//! at its object key instead of being PUT to a cloud. A second home reads the
//! directory with [`SyncEngine::replay_mutation_log_from_directory`].
//!
//! This exists so two homes of one owner can exchange real mutation-log
//! segments without a cloud engine. It is the transport half of the two-home
//! proof tooling; the node guards every caller (`lastdb_node::proof_replay`).

use super::*;
use std::path::Path;

/// Pending-segment cap for one export page. Matches the order of magnitude of
/// the cloud cycle; a proof workload stays far below it.
const EXPORT_PAGE_SEGMENTS: usize = 256;
/// Hard bound on pages for one export call, so a wedged scan cannot spin.
const EXPORT_MAX_PAGES: usize = 4096;
/// Time allowed for the post-ack capture queue to drain before an export.
const CAPTURE_SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Result of one export of pending mutation-log records into a directory.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationLogDirectoryExport {
    /// Cycles that sealed at least one segment.
    pub cycles: usize,
    /// Pin-log records sealed into segments.
    pub records_exported: usize,
    /// Segment files written.
    pub segments_written: usize,
    /// Bytes written across every segment file.
    pub bytes_written: u64,
    /// Object keys written, relative to the directory.
    pub object_keys: Vec<String>,
    /// Records skipped because an atom could not be loaded.
    pub records_quarantined: usize,
    /// Cumulative per-writer frontier this home has published, after the call.
    pub published_frontier_by_writer: BTreeMap<String, u64>,
}

/// Reject any object key that could leave the sink directory.
pub(in crate::sync::engine) fn validated_relative_key(key: &str) -> Result<&str, String> {
    let body = key
        .strip_prefix("log/")
        .ok_or_else(|| format!("mutation-log object key must be under log/: {key}"))?;
    if key.contains('\\') || key.contains('\0') {
        return Err(format!("mutation-log object key has an unsafe byte: {key}"));
    }
    if body
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!(
            "mutation-log object key is not a plain path: {key}"
        ));
    }
    Ok(key)
}

/// Refuse a path that is not a plain directory. Create a missing one when
/// `create` is set. A symlink is never followed and never accepted.
fn ensure_plain_dir(path: &Path, create: bool) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => Ok(()),
        Ok(meta) if meta.file_type().is_symlink() => Err(format!(
            "refusing a symlink on the sink path: {}",
            path.display()
        )),
        Ok(_) => Err(format!("not a directory: {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => {
            std::fs::create_dir(path)
                .map_err(|e| format!("create segment directory {}: {e}", path.display()))
        }
        Err(error) => Err(format!("cannot inspect {}: {error}", path.display())),
    }
}

/// Write one sealed segment under `dir` at its object key, atomically.
///
/// Every directory below `dir` is checked one component at a time and a
/// symlink anywhere is refused, so a planted link cannot send the file out of
/// the sink. `dir` itself must already be a plain directory.
fn write_segment_file(dir: &Path, segment: &MutationLogSegment) -> Result<u64, String> {
    use std::io::Write as _;

    let key = validated_relative_key(&segment.segment.object_key)?;
    ensure_plain_dir(dir, false)?;
    let mut parts: Vec<&str> = key.split('/').collect();
    let file_name = parts
        .pop()
        .ok_or_else(|| format!("mutation-log object key has no file name: {key}"))?;
    let mut parent = dir.to_path_buf();
    for part in parts {
        parent.push(part);
        ensure_plain_dir(&parent, true)?;
    }
    let path = parent.join(file_name);
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if !meta.file_type().is_file() {
            return Err(format!("not a regular file: {}", path.display()));
        }
    }
    let tmp = parent.join(format!("{file_name}.part"));
    match std::fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("clear stale {}: {error}", tmp.display())),
    }
    // `create_new` fails on any existing entry, a symlink included.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|e| format!("create segment file {}: {e}", tmp.display()))?;
    file.write_all(&segment.payload)
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("write segment file {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .map_err(|e| format!("publish segment file {}: {e}", path.display()))?;
    Ok(segment.payload.len() as u64)
}

impl PinLog {
    /// Seal every pending record of the personal target and write the
    /// segments under `dir`. Loops pages until nothing is pending.
    ///
    /// Confirms each page exactly as a cloud publish does: the per-writer
    /// published frontier is persisted and the confirmed pin-log rows are
    /// truncated. So a second call exports only newer records.
    pub(crate) async fn export_mutation_log_to_directory(
        &self,
        engine: &SyncEngine,
        dir: &Path,
    ) -> Result<MutationLogDirectoryExport, String> {
        if !matches!(self.config.capture_mode, CaptureMode::MutationLog) {
            return Err("mutation-log capture is off; nothing to export".to_string());
        }
        if !engine.cloud_plane_allows_upload().await {
            return Err("the sync engine is paused; export refused".to_string());
        }
        ensure_plain_dir(dir, false)?;
        let mut export = MutationLogDirectoryExport::default();
        for _ in 0..EXPORT_MAX_PAGES {
            let report = self.run_directory_export_cycle(engine, dir).await?;
            export.records_quarantined += report.records_quarantined;
            if report.segments_uploaded == 0 {
                break;
            }
            export.cycles += 1;
            export.records_exported += report.records_considered;
            export.segments_written += report.segments_uploaded;
            export.bytes_written += report.bytes_uploaded;
            export.object_keys.extend(report.object_keys);
        }
        export.published_frontier_by_writer = self.read_published_f_strict("personal").await?;
        Ok(export)
    }

    /// One page: scan, materialize, plan, seal, write files, confirm.
    async fn run_directory_export_cycle(
        &self,
        engine: &SyncEngine,
        dir: &Path,
    ) -> Result<MutationLogUploadReport, String> {
        let target_prefix = "";
        let target = self.sync_target_by_prefix(target_prefix).await?;
        let target_id = target_id_for_prefix(&target.prefix);
        let published_before_max = {
            let state = self.state.lock().await;
            state.get(&target_id).map_or(0, |r| r.published_frontier)
        };
        let ScannedPage { summary, batch, .. } = self
            .scan_and_materialize_page(
                engine,
                &target,
                &target_id,
                target_prefix,
                EXPORT_PAGE_SEGMENTS,
            )
            .await?;
        let page_record_count = batch.len();
        let record_batches = plan_mutation_log_record_batches(
            batch,
            EXPORT_PAGE_SEGMENTS,
            self.segment_byte_target(),
        )?;
        let records_selected: usize = record_batches.iter().map(Vec::len).sum();
        let mut report = Self::new_upload_report(
            &target_id,
            published_before_max,
            &summary,
            records_selected,
            page_record_count,
        );
        if record_batches.is_empty() {
            return Ok(report);
        }
        let (sealed_units, confirmed_frontiers) =
            Self::seal_upload_units(&target, &record_batches, &mut report).await?;
        let mut bytes_written = 0u64;
        for unit in &sealed_units {
            for segment in unit {
                bytes_written += write_segment_file(dir, segment)?;
            }
        }
        // The directory is the durable plane here. Confirm only after every
        // file is on disk, exactly as a cloud publish confirms after its PUTs.
        let mut scratch_plane = MutationLogLocalCloud::new();
        let advanced = self
            .confirm_published_units(
                &mut scratch_plane,
                SealedCycle {
                    units: &sealed_units,
                    record_batches: &record_batches,
                    confirmed_frontiers: &confirmed_frontiers,
                },
                &mut report,
                bytes_written,
            )
            .await?;
        self.refresh_report_from_runtime(&target_id, &advanced, &mut report)
            .await;
        Ok(report)
    }
}

impl SyncEngine {
    /// Export this home's pending mutation-log records as sealed segments
    /// under `dir`. See [`PinLog::export_mutation_log_to_directory`].
    pub async fn export_mutation_log_to_directory(
        &self,
        dir: &Path,
    ) -> SyncResult<MutationLogDirectoryExport> {
        // A write is acked before its record reaches the durable mutation log:
        // a post-ack queue appends it. Drain that queue first, so the export
        // holds every write the caller saw acked.
        let router = self.photograph_mutation_router.lock().await.clone();
        if let Some(router) = router {
            if !router.wait_for_completion(CAPTURE_SETTLE_TIMEOUT).await {
                return Err(SyncError::Storage(
                    "post-ack mutation capture did not settle in time".to_string(),
                ));
            }
        }
        self.pin_log
            .export_mutation_log_to_directory(self, dir)
            .await
            .map_err(SyncError::Storage)
    }
}
