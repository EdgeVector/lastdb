//! Exact version/history source retirement within the proven-dead boundary.

mod ownership;
mod spill;
mod versions;

use super::keys::{classify, mol_key, Class};
use super::molset::SpellingMap;
use super::rules::RuleSet;
use super::tripwire::{Tripwire, TripwireStats};
use super::walk::walk_both;
use super::ReapError;
use crate::home::HomeStore;
use fold_db::db_operations::atom_store::{
    reap_history_source_keys, reap_legacy_atom_edge, reap_tip_source_keys, reap_tip_sources,
    ReapSourceEdgeKeys,
};
use serde::{Deserialize, Serialize};
use spill::Spill;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct SourceSummary {
    pub chain_roots: u64,
    pub version_nodes: u64,
    pub version_storage_keys: u64,
    pub legacy_missing_links: u64,
    pub history_rows: u64,
    pub legacy_edges: u64,
    pub ownership_rows: u64,
    pub tripwire: TripwireStats,
}

#[derive(Debug)]
pub(crate) struct SourcePlan {
    pub tips: RuleSet,
    pub v1: RuleSet,
    pub v2: RuleSet,
    pub summary: SourceSummary,
}

#[derive(Debug)]
pub(crate) struct SourceBuilder {
    tips: Spill,
    v1: Spill,
    v2: Spill,
    roots: Vec<ChainRoot>,
    nodes: BTreeMap<String, VersionNode>,
    pub(super) summary: SourceSummary,
}

#[derive(Debug, Clone)]
pub(super) struct ChainRoot {
    molecule_uuid: String,
    disk_hash: String,
    disk_range: String,
    version: String,
}

#[derive(Debug, Clone)]
pub(super) struct VersionNode {
    atom_uuid: String,
    written_at: u64,
    prev_tip_id: String,
    keys: Vec<String>,
}

fn abort(message: impl Into<String>) -> ReapError {
    ReapError::abort("VERSION_SOURCE_OWNERSHIP", message)
}

impl SourceBuilder {
    pub(crate) fn new(plan_dir: &Path) -> Result<Self, ReapError> {
        Ok(Self {
            tips: Spill::new(plan_dir, "exact/tips_version_keys.keys")?,
            v1: Spill::new(plan_dir, "exact/atom_ref_edges.keys")?,
            v2: Spill::new(plan_dir, "exact/history_atom_ref_edges_v2.keys")?,
            roots: Vec::new(),
            nodes: BTreeMap::new(),
            summary: SourceSummary::default(),
        })
    }

    fn edges(&mut self, keys: ReapSourceEdgeKeys) -> Result<(), ReapError> {
        self.v1.add(&keys.v1)?;
        if let Some(key) = keys.v2 {
            self.v2.add(&key)?;
        }
        Ok(())
    }

    pub(crate) fn observe(
        &mut self,
        key: &str,
        plain: &[u8],
        class: Class,
        tripwire: &Tripwire<'_>,
    ) -> Result<(), ReapError> {
        match class {
            Class::Mk | Class::Mgr | Class::Mgd => {
                for source in
                    reap_tip_sources(key, plain).map_err(|error| abort(error.to_string()))?
                {
                    if class != Class::Mk {
                        tripwire.check(
                            &mol_key(&source.molecule_uuid),
                            source.entry.written_at,
                            key,
                            &mut self.summary.tripwire,
                        )?;
                    }
                    self.edges(reap_tip_source_keys(&source))?;
                    if !source.entry.prev_tip_id.is_empty() {
                        self.roots.push(ChainRoot {
                            molecule_uuid: source.molecule_uuid,
                            disk_hash: source.disk_hash,
                            disk_range: source.disk_range,
                            version: source.entry.prev_tip_id,
                        });
                    }
                }
            }
            Class::HistoryColon | Class::HistoryAnchored => {
                let (_, edges) = reap_history_source_keys(key, plain)
                    .map_err(|error| abort(error.to_string()))?;
                for edge in edges {
                    self.edges(edge)?;
                }
                self.summary.history_rows += 1;
            }
            _ => {
                if !ownership::non_source_metadata(key)
                    && ownership::nonempty_links(plain)?.next().is_some()
                {
                    return Err(abort(format!("unsupported chain source {key:?}")));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn finish(
        mut self,
        opened: &HomeStore,
        dead: &SpellingMap,
        tripwire: &Tripwire<'_>,
    ) -> Result<SourcePlan, ReapError> {
        let raw = opened
            .base
            .open_namespace("tips")
            .await
            .map_err(|error| abort(format!("open raw tips: {error}")))?;
        let seam = opened
            .store
            .open_namespace("tips")
            .await
            .map_err(|error| abort(format!("open decoded tips: {error}")))?;
        self.summary.chain_roots = self.roots.len() as u64;
        self.read_versions(&raw, &seam, tripwire).await?;
        ownership::check(
            Arc::clone(&raw),
            Arc::clone(&seam),
            dead,
            &self.nodes,
            &mut self.summary,
            true,
        )
        .await?;
        let present = opened
            .base
            .list_namespaces()
            .await
            .map_err(|error| abort(error.to_string()))?;
        for name in [
            "field_tips",
            "field_tip_headers",
            "field_tip_versions",
            "legacy_blob_refs",
            "main",
            "field_hashrange_hash_index",
            "indexes",
        ] {
            if !self.nodes.is_empty() && present.iter().any(|item| item == name) {
                let raw = opened
                    .base
                    .open_namespace(name)
                    .await
                    .map_err(|error| abort(error.to_string()))?;
                let seam = opened
                    .store
                    .open_namespace(name)
                    .await
                    .map_err(|error| abort(error.to_string()))?;
                ownership::check(raw, seam, dead, &self.nodes, &mut self.summary, false).await?;
            }
        }
        if present.iter().any(|name| name == "atom_ref_edges") {
            self.legacy_edges(opened, dead).await?;
        }
        self.summary.tripwire.slack_ms = tripwire.slack_ms();
        Ok(SourcePlan {
            tips: self.tips.finish()?,
            v1: self.v1.finish()?,
            v2: self.v2.finish()?,
            summary: self.summary,
        })
    }

    async fn legacy_edges(
        &mut self,
        opened: &HomeStore,
        dead: &SpellingMap,
    ) -> Result<(), ReapError> {
        let raw = opened
            .base
            .open_namespace("atom_ref_edges")
            .await
            .map_err(|error| abort(error.to_string()))?;
        let seam = opened
            .store
            .open_namespace("atom_ref_edges")
            .await
            .map_err(|error| abort(error.to_string()))?;
        walk_both(raw, seam, "LEGACY_SOURCE_EDGE_DECODE", |_, page| {
            for (key, plain) in &page.rows {
                let key = std::str::from_utf8(key).map_err(|error| abort(error.to_string()))?;
                if !key.starts_with("aref:v1:e:") && !key.starts_with("aref\0v1:e:") {
                    continue;
                }
                let (molecule, edge) =
                    reap_legacy_atom_edge(key, plain).map_err(|error| abort(error.to_string()))?;
                if classify(key).class == Class::Scoped || !dead.contains_key(&mol_key(&molecule)) {
                    continue;
                }
                self.v1.add(key)?;
                if let Some(v2) = edge.v2 {
                    self.v2.add(&v2)?;
                }
                self.summary.legacy_edges += 1;
            }
            Ok(())
        })
        .await
    }
}
