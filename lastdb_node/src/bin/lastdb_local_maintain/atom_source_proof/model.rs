//! Private evidence only. Atom body bytes never enter a report.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Hold {
    pub collection: String,
    pub key_b64: String,
    pub scope: String,
    pub kind: String,
    pub molecule: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Facts {
    pub complete: bool,
    pub namespace_digests: BTreeMap<String, String>,
    pub namespace_rows: BTreeMap<String, u64>,
    pub cloud_gate: crate::reap::cloud_gate::CloudGateSummary,
    pub snapshot_writer_map: BTreeMap<String, u64>,
    pub counts: BTreeMap<String, u64>,
    pub found_target_ids: BTreeSet<String>,
    pub missing_target_ids: BTreeSet<String>,
    pub holds: BTreeMap<String, BTreeSet<Hold>>,
    pub global_holds: BTreeSet<Hold>,
    pub retained_database_paths: BTreeMap<String, BTreeSet<String>>,
    pub retained_org_targets: BTreeMap<String, BTreeSet<String>>,
    pub molecule_owners: BTreeMap<String, BTreeSet<String>>,
    pub proteins: BTreeMap<String, BTreeSet<String>>,
    pub org_routes: BTreeMap<String, OrgRoute>,
    pub database_catalog: BTreeMap<String, fold_db::db_operations::DbCatalogEntry>,
    pub source_explained_target_ids: BTreeSet<String>,
    pub reference_only_target_ids: BTreeSet<String>,
    pub retained_source_references_complete: bool,
    pub retained_schema_owners: BTreeMap<String, BTreeSet<String>>,
    pub retained_proteins: BTreeMap<String, BTreeSet<String>>,
    pub personal_completion_markers: BTreeMap<String, String>,
}

impl Facts {
    pub(crate) fn holds_uuid(&self, uuid: &str) -> bool {
        !self.global_holds.is_empty() || self.holds.get(uuid).is_some_and(|holds| !holds.is_empty())
    }
    pub(super) fn count(&mut self, kind: &str) {
        *self.counts.entry(kind.into()).or_default() += 1;
    }
    pub(super) fn hold(&mut self, targets: &BTreeSet<String>, uuid: &str, hold: Hold) {
        if targets.contains(uuid) {
            self.holds.entry(uuid.into()).or_default().insert(hold);
        }
    }
    pub(super) fn hold_all(&mut self, targets: &BTreeSet<String>, hold: Hold) {
        if !targets.is_empty() {
            self.global_holds.insert(hold);
        }
    }
}

pub(crate) struct TargetBody {
    pub collection: String,
    pub shard: u16,
    pub group_id: Option<u32>,
    pub key: Vec<u8>,
    pub scope: String,
    pub uuid: String,
    pub raw: Vec<u8>,
    pub plain: Vec<u8>,
}

pub(crate) struct Collected {
    pub facts: Facts,
    pub bodies: Vec<TargetBody>,
    pub(super) body_keys: BTreeSet<(String, Vec<u8>)>,
}

impl Collected {
    pub(super) fn new(
        cloud_gate: crate::reap::cloud_gate::CloudGateSummary,
        snapshot_writer_map: BTreeMap<String, u64>,
    ) -> Self {
        Self {
            facts: Facts {
                cloud_gate,
                snapshot_writer_map,
                ..Facts::default()
            },
            bodies: Vec::new(),
            body_keys: BTreeSet::new(),
        }
    }
}

pub(super) fn hold(
    collection: &str,
    key: &[u8],
    scope: &str,
    kind: &str,
    molecule: Option<&str>,
) -> Hold {
    Hold {
        collection: collection.into(),
        key_b64: STANDARD.encode(key),
        scope: scope.into(),
        kind: kind.into(),
        molecule: molecule.map(str::to_string),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrgRoute {
    pub active: bool,
    pub storage_prefixes: BTreeSet<String>,
    pub unprefixed_schema_names: BTreeSet<String>,
    pub legacy_all_unprefixed: bool,
}
