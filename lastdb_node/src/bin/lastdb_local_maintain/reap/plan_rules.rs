//! The rule sets of the plan, built from the dead molecules.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fold_db::db_operations::atom_store::molecule_ref_target_prefix;

use super::keys::{aref_manifest_keys, token_rules, MolKey};
use super::meters;
use super::molset::{Kept, MoleculeSets, SpellingMap};
use super::rules::RuleSet;
use super::rules_out::RetainedRow;
use super::tips_pass::TokenStat;

/// Rules for the `tips` collection: every key class of every dead token.
pub(crate) fn tips_rules(dead: &SpellingMap) -> RuleSet {
    let mut rules = RuleSet::default();
    for spelling in dead.values().flatten() {
        let token = token_rules(spelling);
        for prefix in &token.prefixes {
            rules.add_prefix(prefix);
        }
        for exact in &token.exacts {
            rules.add_exact(exact);
        }
    }
    rules.finish();
    rules
}

/// Rules for the compact edge plane: the molecule manifests and, when the tip
/// pass found tip edges, the exact edge keys in `exact_path`.
pub(crate) fn v2_rules(dead: &SpellingMap, edge_hashes: &[u128], exact_path: &str) -> RuleSet {
    let mut rules = RuleSet::default();
    for spelling in dead.values().flatten() {
        for key in aref_manifest_keys(spelling) {
            rules.add_exact(&key);
        }
    }
    if !edge_hashes.is_empty() {
        rules.add_exact_file(exact_path, edge_hashes.to_vec());
    }
    rules.finish();
    rules
}

/// Rules for the molecule-reference plane: one prefix per dead token.
pub(crate) fn mref_rules(dead: &SpellingMap) -> RuleSet {
    let mut rules = RuleSet::default();
    for spelling in dead.values().flatten() {
        if let Some(prefix) = molecule_ref_target_prefix(spelling) {
            rules.add_prefix(prefix.as_bytes());
        }
    }
    rules.finish();
    rules
}

/// Rules for the keep-small plane: the shard key of each dropped name.
pub(crate) fn keep_small_rules(names: &BTreeSet<String>) -> RuleSet {
    let mut rules = RuleSet::default();
    for key in meters::shard_keys(names) {
        rules.add_exact(key.as_bytes());
    }
    rules.finish();
    rules
}

/// The molecules that the plan keeps, with the keys they hold in the tips.
pub(crate) struct KeptLists<'a> {
    pub sets: &'a MoleculeSets,
    pub tokens: &'a HashMap<MolKey, TokenStat>,
}

fn row(
    reason: &'static str,
    key: &MolKey,
    spellings: Vec<String>,
    detail: String,
    tokens: &HashMap<MolKey, TokenStat>,
) -> RetainedRow {
    let stat = tokens.get(key);
    RetainedRow {
        reason,
        key: *key,
        spellings,
        tip_keys: stat.map_or(0, |s| s.keys),
        tip_bytes: stat.map_or(0, |s| s.bytes),
        detail,
    }
}

fn kept_rows(
    reason: &'static str,
    kept: &BTreeMap<MolKey, Kept>,
    tokens: &HashMap<MolKey, TokenStat>,
    out: &mut Vec<RetainedRow>,
) {
    for (key, item) in kept {
        out.push(row(
            reason,
            key,
            item.spellings.iter().cloned().collect(),
            item.detail.clone(),
            tokens,
        ));
    }
}

/// The E2 candidates that are not already dead or live.
pub(crate) fn e2_quarantine(
    candidates: &BTreeMap<MolKey, (String, String)>,
    sets: &MoleculeSets,
    live: &BTreeSet<MolKey>,
) -> BTreeMap<MolKey, (String, String)> {
    candidates
        .iter()
        .filter(|(key, _)| {
            !live.contains(*key) && !sets.e1.contains_key(*key) && !sets.dead.contains_key(*key)
        })
        .map(|(key, value)| (*key, value.clone()))
        .collect()
}

/// The molecules in the tips that nothing owns: not live, not E1, not E2.
pub(crate) fn e3_orphans(
    tokens: &HashMap<MolKey, TokenStat>,
    sets: &MoleculeSets,
    live: &BTreeSet<MolKey>,
    e2: &BTreeMap<MolKey, (String, String)>,
) -> BTreeMap<MolKey, TokenStat> {
    tokens
        .iter()
        .filter(|(key, stat)| {
            stat.digest_shaped
                && !live.contains(*key)
                && !sets.e1.contains_key(*key)
                && !e2.contains_key(*key)
        })
        .map(|(key, stat)| (*key, stat.clone()))
        .collect::<BTreeMap<_, _>>()
}

/// The text rows of `retained.tsv`.
pub(crate) fn retained_rows(
    lists: &KeptLists<'_>,
    e2_quarantine: &BTreeMap<MolKey, (String, String)>,
    e3: &BTreeMap<MolKey, TokenStat>,
) -> Vec<RetainedRow> {
    let mut out = Vec::new();
    kept_rows(
        "shared_with_live",
        &lists.sets.shared_with_live,
        lists.tokens,
        &mut out,
    );
    kept_rows(
        "protein_mixed",
        &lists.sets.protein_mixed,
        lists.tokens,
        &mut out,
    );
    for (key, (spelling, owner)) in e2_quarantine {
        out.push(row(
            "e2_quarantine",
            key,
            vec![spelling.clone()],
            format!("keep_small_shard:{owner}"),
            lists.tokens,
        ));
    }
    for (key, stat) in e3 {
        out.push(row(
            "e3_orphan",
            key,
            vec![stat.spelling.clone()],
            "no_owner".to_string(),
            lists.tokens,
        ));
    }
    out
}
