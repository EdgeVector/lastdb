//! Private exact plans and numeric-only public reports.

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const FORMAT: u32 = 1;
pub(super) const PLAN_FILE: &str = "target-atom-plan.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct BodyCopy {
    pub collection: String,
    pub shard: u16,
    pub group_id: Option<u32>,
    pub key_b64: String,
    pub raw_sha256: String,
    pub raw_bytes: u64,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Candidate {
    pub uuid: String,
    pub copies: Vec<BodyCopy>,
    /// Ordered production keys: blob edges first, then the locator.
    pub derived_keys: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Counts {
    pub requested_uuids: u64,
    pub found_uuids: u64,
    pub target_copies_read: u64,
    pub absent_uuids: u64,
    pub retained_uuids: u64,
    pub held_uuids: u64,
    pub other_scope_uuids: u64,
    pub recent_uuids: u64,
    pub undated_uuids: u64,
    pub recent_copies: u64,
    pub undated_copies: u64,
    pub candidate_uuids: u64,
    pub candidate_copies: u64,
    pub candidate_raw_bytes: u64,
    pub derived_storage_keys: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Plan {
    pub format: u32,
    pub home: PathBuf,
    pub store_root: PathBuf,
    pub started_at: String,
    pub input: input::Input,
    pub proof: crate::atom_source_proof::Facts,
    pub counts: Counts,
    pub found_uuids: BTreeSet<String>,
    pub absent_uuids: BTreeSet<String>,
    pub retained: BTreeMap<String, BTreeSet<String>>,
    pub candidates: Vec<Candidate>,
    pub retirement_state_sha256: Option<String>,
    pub prerequisites: Vec<String>,
}

/// Every public field is numeric, boolean, or the fixed event literal.
#[derive(Debug, Serialize)]
pub(super) struct Report {
    pub event: &'static str,
    pub execute: bool,
    pub counts: Counts,
    pub ledger_committed: bool,
    pub atom_copies_deleted: u64,
    pub derived_storage_keys_deleted: u64,
    pub atom_retirement_state_unchanged: bool,
    pub normal_owner_compaction_required: bool,
    pub fresh_snapshot_required: bool,
    pub csn_before: u64,
    pub csn_after: u64,
}

impl Report {
    pub(super) fn planned(plan: &Plan) -> Self {
        Self {
            event: "target_atom_gc_offline",
            execute: false,
            counts: plan.counts.clone(),
            ledger_committed: false,
            atom_copies_deleted: 0,
            derived_storage_keys_deleted: 0,
            atom_retirement_state_unchanged: true,
            normal_owner_compaction_required: false,
            fresh_snapshot_required: false,
            csn_before: 0,
            csn_after: 0,
        }
    }
}

pub(super) fn digest(bytes: &[u8]) -> String {
    fold_db::hex::hex_lower(Sha256::digest(bytes))
}

pub(super) fn retirement_state(root: &Path) -> Result<Option<String>, String> {
    let high_water = fold_db::storage::laststore::high_water_path_for_store_root(root);
    let parent = high_water.parent().ok_or("high-water path has no parent")?;
    let path = parent.join("laststore_pending_purged_atom_retirements.json");
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(digest(&bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("read atom retirement state: {error}")),
    }
}

pub(super) fn exact_plan_equal(saved: &Plan, current: &Plan) -> Result<(), String> {
    if serde_json::to_vec(saved).map_err(err)? != serde_json::to_vec(current).map_err(err)? {
        return Err("the stopped home or exact inputs differ from the target atom plan".into());
    }
    Ok(())
}
