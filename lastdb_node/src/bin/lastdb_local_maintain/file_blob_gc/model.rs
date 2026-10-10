//! Private exact plans and public count-only reports.

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub(super) const FORMAT: u32 = 1;
pub(super) const GRACE_SECONDS: i64 = 600;
pub(super) const PLAN_FILE: &str = "file-blob-plan.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct BlobRow {
    pub collection: String,
    pub key_b64: String,
    pub blob_ref: String,
    pub raw_sha256: String,
    pub raw_bytes: u64,
    pub stored_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Counts {
    pub physical_rows: u64,
    pub atoms_read: u64,
    pub atom_scopes: BTreeMap<String, u64>,
    pub journal_roots: u64,
    pub confirmed_personal_roots_omitted: u64,
    pub file_blobs_read: u64,
    pub file_blobs_referenced: u64,
    pub file_blobs_recent: u64,
    pub file_blobs_undated: u64,
    pub candidate_rows: u64,
    pub candidate_stored_bytes: u64,
}

/// Numeric summary only; physical scope identities stay in the private plan.
#[derive(Debug, Clone, Serialize)]
pub(super) struct PublicCounts {
    pub physical_rows: u64,
    pub atoms_read: u64,
    pub atom_scope_count: u64,
    pub journal_roots: u64,
    pub confirmed_personal_roots_omitted: u64,
    pub file_blobs_read: u64,
    pub file_blobs_referenced: u64,
    pub file_blobs_recent: u64,
    pub file_blobs_undated: u64,
    pub candidate_rows: u64,
    pub candidate_stored_bytes: u64,
}

impl From<&Counts> for PublicCounts {
    fn from(counts: &Counts) -> Self {
        Self {
            physical_rows: counts.physical_rows,
            atoms_read: counts.atoms_read,
            atom_scope_count: u64::try_from(counts.atom_scopes.len()).unwrap_or(u64::MAX),
            journal_roots: counts.journal_roots,
            confirmed_personal_roots_omitted: counts.confirmed_personal_roots_omitted,
            file_blobs_read: counts.file_blobs_read,
            file_blobs_referenced: counts.file_blobs_referenced,
            file_blobs_recent: counts.file_blobs_recent,
            file_blobs_undated: counts.file_blobs_undated,
            candidate_rows: counts.candidate_rows,
            candidate_stored_bytes: counts.candidate_stored_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Plan {
    pub format: u32,
    pub home: PathBuf,
    pub store_root: PathBuf,
    pub started_at: String,
    pub namespace_digests: BTreeMap<String, String>,
    pub cloud_gate: crate::reap::cloud_gate::CloudGateSummary,
    pub counts: Counts,
    pub candidates: Vec<BlobRow>,
    pub retirement_state_sha256: Option<String>,
    pub prerequisites: Vec<String>,
    pub pre_blob_snapshot_writer_map: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct Report {
    pub event: &'static str,
    pub execute: bool,
    pub counts: PublicCounts,
    pub ledger_committed: bool,
    pub file_blobs_deleted: u64,
    pub compactions: Vec<fold_db::storage::laststore::CollectionCompactReport>,
    pub atom_retirement_state_unchanged: bool,
    pub fresh_snapshot_required: bool,
    pub pre_blob_snapshot_writer_count: u64,
    pub csn_before: u64,
    pub csn_after: u64,
}

pub(super) fn digest(bytes: &[u8]) -> String {
    fold_db::hex::hex_lower(Sha256::digest(bytes))
}

pub(super) fn retirement_state(root: &Path) -> Result<Option<String>, String> {
    let path = root.join("laststore_pending_purged_atom_retirements.json");
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(digest(&bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("read atom retirement state: {error}")),
    }
}

pub(super) fn create_plan_dir(path: &Path, home: &Path, store: &Path) -> Result<(), String> {
    if path.exists() {
        return Err("the plan directory already exists".into());
    }
    let parent = path.parent().ok_or("the plan directory has no parent")?;
    let parent = std::fs::canonicalize(parent).map_err(|e| e.to_string())?;
    if parent.starts_with(home) || parent.starts_with(store) {
        return Err("the plan directory must be outside the home".into());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|e| e.to_string())?;
    File::open(&parent)
        .and_then(|file| file.sync_all())
        .map_err(|e| e.to_string())
}

pub(super) fn write_private<T: Serialize>(dir: &Path, name: &str, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(name))
        .map_err(|e| e.to_string())?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|e| e.to_string())
}

pub(super) fn validate_plan_location(dir: &Path, home: &Path, store: &Path) -> Result<(), String> {
    let dir = std::fs::canonicalize(dir).map_err(err)?;
    if dir.starts_with(home) || dir.starts_with(store) {
        return Err("the plan directory must be outside the home".into());
    }
    Ok(())
}

pub(super) fn load_plan(dir: &Path) -> Result<Plan, String> {
    let dir_meta = std::fs::symlink_metadata(dir).map_err(|e| e.to_string())?;
    let path = dir.join(PLAN_FILE);
    let file_meta = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
    if !dir_meta.is_dir()
        || !file_meta.is_file()
        || dir_meta.permissions().mode() & 0o077 != 0
        || file_meta.permissions().mode() & 0o077 != 0
    {
        return Err("the plan must be a private directory and a regular private file".into());
    }
    let plan: Plan = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    if plan.format != FORMAT {
        return Err("unsupported file blob plan format".into());
    }
    Ok(plan)
}

pub(super) fn exact_plan_equal(before: &Plan, now: &Plan) -> Result<(), String> {
    let before = serde_json::to_vec(before).map_err(|e| e.to_string())?;
    let now = serde_json::to_vec(now).map_err(|e| e.to_string())?;
    if before != now {
        return Err("the stopped home differs from the saved exact file blob plan".into());
    }
    Ok(())
}
