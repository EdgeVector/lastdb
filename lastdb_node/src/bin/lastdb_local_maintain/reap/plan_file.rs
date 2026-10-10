//! The `plan.json` file: what the planner found and what the rules expect.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::catalog::LiveSources;
use super::count_pass::CollectionCount;
use super::guard::GuardReport;
use super::receipts::ReceiptReport;
use super::tips_pass::ClassStat;
use super::tripwire::TripwireStats;

/// Version of the plan directory contract.
pub(crate) const CONTRACT: u32 = 1;

/// The window of the reap that this plan covers.
pub(crate) const WINDOW: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct IdentitiesFacts {
    pub file: String,
    pub sha256: String,
    pub listed: usize,
    pub spellings: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct CatalogSummary {
    pub schemas: usize,
    pub live_molecules: usize,
    pub sources: LiveSources,
}

/// Molecule counts. Every count is a count of molecule identities, not of
/// spellings.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct MoleculeSummary {
    pub e1: usize,
    pub dead: usize,
    pub dead_tokens: usize,
    pub shared_with_live: usize,
    pub protein_mixed: usize,
    pub e2_candidates: usize,
    pub e2_quarantined: usize,
    pub e3_orphans: usize,
    pub e3_orphan_keys: u64,
    pub receiptless_names: usize,
    pub keep_small_shards_found: usize,
    pub keep_small_shards_undecodable: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct TipsSummary {
    pub raw_keys: u64,
    pub decoded_keys: u64,
    pub keys_only_count: Option<u64>,
    pub unsealed_discarded: u64,
    pub groups: u64,
    pub largest_group_keys: u64,
    pub by_class: BTreeMap<String, ClassStat>,
    pub other_heads: BTreeMap<String, u64>,
    pub doomed_keys: u64,
    pub doomed_bytes: u64,
    pub doomed_mk: u64,
    pub edge_keys: u64,
    pub tips_without_v2_edge: u64,
    pub tv_chain_heads: u64,
    pub tombstoned_doomed: u64,
    pub scoped_dead_hits: u64,
    pub unhandled_needle_hits: u64,
    pub needle_samples: Vec<String>,
    /// What the post-drop tripwire checked, and the unit it read.
    pub tripwire: TripwireStats,
}

/// The rules and the expected counts of one collection.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct CollectionPlan {
    pub rules_file: String,
    pub rule_count: usize,
    pub expect_keys: u64,
    pub expect_bytes: u64,
    pub count: CollectionCount,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct PlanFile {
    pub contract: u32,
    pub window: u32,
    pub created_at_unix_ms: u64,
    pub elapsed_ms: u64,
    pub home: String,
    pub store_root: String,
    pub plan_dir: String,
    pub seam: String,
    /// The commit sequence of the store when the plan was made. A later run
    /// can compare it to find a plan that the store has outgrown.
    pub csn_high_water: Option<u64>,
    pub workers: usize,
    pub env: BTreeMap<String, String>,
    pub guard: GuardReport,
    pub identities: IdentitiesFacts,
    pub receipts: ReceiptReport,
    pub catalog: CatalogSummary,
    pub molecules: MoleculeSummary,
    pub tips: TipsSummary,
    pub sources: super::sources::SourceSummary,
    pub cloud_gate: super::cloud_gate::CloudGateSummary,
    pub collections: BTreeMap<String, CollectionPlan>,
    /// Collections on disk that hold dead-molecule rows in an older layout.
    pub legacy_collections_present: Vec<String>,
    /// The gates that passed, in order.
    pub gates: Vec<String>,
    /// SHA-256 of every file the plan directory holds, by relative path.
    pub files: BTreeMap<String, String>,
    pub warnings: Vec<String>,
}
