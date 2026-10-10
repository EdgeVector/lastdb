//! Private exact admission/result facts and a numeric-only public projection.

use crate::reap::cloud_gate::CloudGateSummary;
use fold_db::storage::laststore::BackupManifest;
use fold_db::sync::auth::ops::BackupLatestPointer;
use fold_db::sync::engine::LastStoreCloudSnapshotReport;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Artifact {
    pub path: PathBuf,
    pub sha256: String,
}

/// The operator validates these actual receipts before it calls the tool.
/// Their hashes bind the held controls/rollback to the publication artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct OperatorEvidence {
    pub version: u32,
    pub home: PathBuf,
    pub pid: u32,
    pub start_ts: u64,
    pub local_writers_paused: bool,
    pub other_local_clients_absent: bool,
    pub supervisor_unloaded: bool,
    pub rollback_preserved: bool,
    pub stopped_receipt: Artifact,
    pub rollback_receipt: Artifact,
    pub controls: Vec<Artifact>,
}

#[derive(Serialize)]
pub(super) struct Intent<'a> {
    pub version: u32,
    pub requested_at: String,
    pub home: &'a std::path::Path,
    pub store_root: &'a std::path::Path,
    pub expected_pid: u32,
    pub expected_start_ts: u64,
    pub expected_build_version: &'a str,
    pub cloud_config_sha256: &'a str,
    pub identity_sha256: &'a str,
    pub device_file_sha256: &'a str,
    pub device_id: &'a str,
    pub previous_manifest_sha256: &'a str,
    pub previous_cache_sha256: &'a str,
    pub historical_unproved_flush_claim_sha256: &'a Option<String>,
    pub operator_evidence_sha256: &'a str,
    pub operator: &'a OperatorEvidence,
    pub cloud_gate: &'a CloudGateSummary,
    pub cloud_latest: &'a BackupLatestPointer,
    pub normal_mode: bool,
    pub background_workers_started: bool,
}

#[derive(Serialize)]
pub(super) struct ResultReport {
    pub version: u32,
    pub requested_at: String,
    pub completed_at: String,
    pub cloud_before: CloudGateSummary,
    pub cloud_after: CloudGateSummary,
    pub manifest: BackupManifest,
    pub report: LastStoreCloudSnapshotReport,
    pub post_cas_latest: BackupLatestPointer,
    pub marker_sha256: String,
    pub cloud_config_sha256: String,
    pub operator_evidence_sha256: String,
    pub personal_map_unchanged: bool,
    pub normal_marker_matches: bool,
    pub clean_stop_unchanged: bool,
    pub background_workers_started: bool,
}

#[derive(Serialize)]
pub(super) struct PublicReport {
    pub event: &'static str,
    pub counter: u64,
    pub cas_counter: u64,
    pub cut_csn: u64,
    pub chunks_referenced: u64,
    pub chunks_already_present: u64,
    pub writers: u64,
    pub personal_map_unchanged: bool,
    pub normal_marker_matches: bool,
    pub clean_stop_unchanged: bool,
    pub background_workers_started: bool,
}

impl PublicReport {
    pub(super) fn from_result(result: &ResultReport) -> Self {
        Self {
            event: "normal_stopped_snapshot_complete",
            counter: result.report.counter,
            cas_counter: result.report.cas_counter,
            cut_csn: result.report.cut_csn,
            chunks_referenced: result.report.chunks_referenced as u64,
            chunks_already_present: result.report.chunks_already_present as u64,
            writers: result.cloud_after.published_maps["personal"].len() as u64,
            personal_map_unchanged: result.personal_map_unchanged,
            normal_marker_matches: result.normal_marker_matches,
            clean_stop_unchanged: result.clean_stop_unchanged,
            background_workers_started: result.background_workers_started,
        }
    }
}
