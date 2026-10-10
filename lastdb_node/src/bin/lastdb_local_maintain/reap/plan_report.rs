//! Summaries, warnings and plan-directory checks of `reap plan`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::catalog::CatalogFacts;
use super::molset::MoleculeSets;
use super::plan_file::*;
use super::receipts::ReceiptReport;
use super::tips_pass::TipsReport;
use super::ReapError;

/// The plan directory must be new or empty, and must not be inside the home.
pub(super) fn check_plan_dir(
    plan_dir: &Path,
    home: &Path,
    store_root: &Path,
) -> Result<(), ReapError> {
    if plan_dir.exists() {
        let mut entries = std::fs::read_dir(plan_dir).map_err(|error| {
            ReapError::Refused(format!("plan dir {}: {error}", plan_dir.display()))
        })?;
        if entries.next().is_some() {
            return Err(ReapError::Refused(format!(
                "plan dir {} is not empty",
                plan_dir.display()
            )));
        }
    }
    let canon = |path: &Path| {
        let mut tail = Vec::new();
        let mut cursor = path;
        loop {
            if let Ok(base) = std::fs::canonicalize(cursor) {
                return tail.iter().rev().fold(base, |acc, part| acc.join(part));
            }
            match (cursor.parent(), cursor.file_name()) {
                (Some(parent), Some(name)) => {
                    tail.push(name.to_os_string());
                    cursor = parent;
                }
                _ => return path.to_path_buf(),
            }
        }
    };
    let plan = canon(plan_dir);
    for root in [home, store_root] {
        if plan.starts_with(canon(root)) {
            return Err(ReapError::Refused(format!(
                "plan dir {} is inside the home. The planner writes nothing to the home.",
                plan_dir.display()
            )));
        }
    }
    Ok(())
}

/// The `LASTDB_` and `FOLDDB_` settings in force, without secrets.
pub(super) fn lastdb_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(name, _)| name.starts_with("LASTDB_") || name.starts_with("FOLDDB_"))
        .filter(|(name, value)| {
            let upper = name.to_uppercase();
            value.len() <= 64
                && !["KEY", "TOKEN", "SECRET", "PASSWORD"]
                    .iter()
                    .any(|word| upper.contains(word))
        })
        .collect()
}

pub(super) fn fill_summaries(
    plan: &mut PlanFile,
    facts: &CatalogFacts,
    receipt_report: &ReceiptReport,
    sets: &MoleculeSets,
    report: &TipsReport,
) {
    plan.receipts = receipt_report.clone();
    plan.catalog = CatalogSummary {
        schemas: facts.schema_count,
        live_molecules: facts.live.len(),
        sources: facts.sources.clone(),
    };
    plan.molecules = MoleculeSummary {
        e1: sets.e1.len(),
        dead: sets.dead.len(),
        dead_tokens: sets.dead.values().map(BTreeSet::len).sum(),
        shared_with_live: sets.shared_with_live.len(),
        protein_mixed: sets.protein_mixed.len(),
        receiptless_names: receipt_report.receiptless.len(),
        ..MoleculeSummary::default()
    };
    plan.tips = TipsSummary {
        raw_keys: report.raw_keys,
        decoded_keys: report.decoded_keys,
        keys_only_count: report.keys_only_count,
        unsealed_discarded: report.unsealed_discarded,
        groups: report.groups,
        largest_group_keys: report.largest_group_keys,
        by_class: report
            .by_class
            .iter()
            .map(|(name, stat)| ((*name).to_string(), stat.clone()))
            .collect(),
        other_heads: report.other_heads.clone(),
        doomed_keys: report.matched_keys,
        doomed_bytes: report.matched_bytes,
        doomed_mk: report.doomed_mk,
        edge_keys: report.edge_keys,
        tips_without_v2_edge: report.tips_without_v2_edge,
        tv_chain_heads: report.tv_chain_heads,
        tombstoned_doomed: report.tombstoned_doomed,
        scoped_dead_hits: report.scoped_dead_hits,
        unhandled_needle_hits: report.unhandled_needle_hits,
        needle_samples: report.needle_samples.clone(),
        tripwire: report.tripwire.clone(),
    };
}

pub(super) fn warnings(plan: &PlanFile, workers: usize) -> Vec<String> {
    let t = &plan.tips;
    let m = &plan.molecules;
    let mut out = Vec::new();
    if workers > 1 {
        out.push(format!(
            "--workers {workers} ignored: the planner reads with one worker"
        ));
    }
    if m.dead == 0 {
        out.push("no dead molecule: the plan drops nothing".to_string());
    }
    if t.tips_without_v2_edge > 0 {
        out.push(format!(
            "{} doomed tip(s) name an atom with no 64-hex id. They have no compact edge.",
            t.tips_without_v2_edge
        ));
    }
    if t.scoped_dead_hits > 0 {
        out.push(format!(
            "{} org or share scoped key(s) hold a dead molecule id. They are kept.",
            t.scoped_dead_hits
        ));
    }
    if t.unhandled_needle_hits > 0 {
        out.push(format!(
            "{} key(s) of an unknown class hold a dead molecule id. They are kept. First: {}",
            t.unhandled_needle_hits,
            t.needle_samples.first().cloned().unwrap_or_default()
        ));
    }
    if m.receiptless_names > 0 {
        out.push(format!(
            "{} dropped name(s) have no receipt. Their field molecules are not in the plan.",
            m.receiptless_names
        ));
    }
    if !plan.legacy_collections_present.is_empty() {
        out.push(format!(
            "legacy collection(s) on disk, not planned: {}",
            plan.legacy_collections_present.join(", ")
        ));
    }
    out
}
