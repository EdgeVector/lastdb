//! `reap plan`: the read-only planner flow for window 1.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fold_db::storage::traits::NamespacedStore;
use lastdb_node::offline_home::load_e2e_keys;

use super::catalog::{self, CatalogFacts};
use super::count_pass::{count_matches, CollectionCount};
use super::guard::{self, Flags, GuardReport, ProcessView};
use super::identities::{self, Identities};
use super::molset::{self, MoleculeSets};
use super::plan_file::*;
use super::plan_report::{check_plan_dir, fill_summaries, lastdb_env, warnings};
use super::plan_rules::{self, KeptLists};
use super::receipts::{self, ReceiptReport};
use super::rules::RuleSet;
use super::rules_out;
use super::tips_pass::{self, TipsReport, TipsState};
use super::tripwire::Tripwire;
use super::{proteins, ReapError};
use crate::home::{open_home_for_offline_read, resolve_laststore_root, HomeStore};

/// Path of the exact tip-edge keys, relative to the plan directory.
pub(crate) const EXACT_V2_FILE: &str = "exact/atom_ref_edges_v2.keys";

/// Collections that held dead-molecule rows in an older layout.
const LEGACY_COLLECTIONS: [&str; 9] = [
    "field_update_order_log",
    "field_update_order_count",
    "field_update_order_legacy",
    "mutation_history",
    "legacy_blob_refs",
    "sync_conflicts",
    "field_tips",
    "field_tip_headers",
    "field_tip_versions",
];

/// The inputs of one plan run.
pub(crate) struct PlanRun<'a> {
    pub home: &'a Path,
    pub identities: &'a Path,
    pub plan_dir: &'a Path,
    pub flags: Flags,
    pub workers: usize,
    /// Slack of the post-drop tripwire, in milliseconds.
    pub tripwire_slack_ms: u64,
    /// A fixed process view. `None` reads the live process table.
    pub view: Option<ProcessView>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Run the planner. On an error after the plan directory exists, the
/// directory is marked aborted and holds no rules.
pub(crate) fn run_plan(run: &PlanRun<'_>) -> Result<PlanFile, ReapError> {
    let started = Instant::now();
    if run.workers == 0 {
        return Err(ReapError::Refused(
            "--workers must be at least 1".to_string(),
        ));
    }
    let ids = identities::load(run.identities)?;
    let store_root = resolve_laststore_root(run.home).map_err(ReapError::Refused)?;
    check_plan_dir(run.plan_dir, run.home, &store_root)?;
    let paths = guard::socket_paths(run.home, &store_root);
    let view = run
        .view
        .clone()
        .unwrap_or_else(|| guard::live_process_view(&paths));
    let guard_report = guard::prove(run.home, &store_root, run.flags, &view)?;
    let opened = open_home_for_offline_read(run.home).map_err(ReapError::Refused)?;
    if opened.seam != "at-rest-seam" {
        return Err(ReapError::Refused(format!(
            "home opened as {}, not through the at-rest seam",
            opened.seam
        )));
    }
    std::fs::create_dir_all(run.plan_dir)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| ReapError::Failed(format!("tokio: {error}")))?;
    let result = runtime.block_on(build_plan(run, &ids, &opened, guard_report, started));
    if let Err(error) = &result {
        let gate = match error {
            ReapError::Abort { gate, .. } => gate,
            _ => &"FAILED",
        };
        rules_out::mark_aborted(run.plan_dir, gate, &error.to_string());
    }
    result
}

async fn build_plan(
    run: &PlanRun<'_>,
    ids: &Identities,
    opened: &HomeStore,
    guard_report: GuardReport,
    started: Instant,
) -> Result<PlanFile, ReapError> {
    let mut plan = PlanFile {
        contract: CONTRACT,
        window: WINDOW,
        created_at_unix_ms: now_ms(),
        home: run.home.display().to_string(),
        store_root: opened.store_root.display().to_string(),
        plan_dir: run.plan_dir.display().to_string(),
        seam: opened.seam.to_string(),
        csn_high_water: opened
            .base
            .raw_last_store()
            .map(|last| last.csn_high_water()),
        workers: 1,
        env: lastdb_env(),
        guard: guard_report,
        identities: IdentitiesFacts {
            file: run.identities.display().to_string(),
            sha256: ids.sha256.clone(),
            listed: ids.listed.len(),
            spellings: ids.spellings.len(),
        },
        gates: vec!["guard".to_string(), "seam".to_string()],
        ..PlanFile::default()
    };
    let (e2e, _) = load_e2e_keys(run.home).map_err(ReapError::Refused)?;
    let facts = catalog::load(&*opened.store, Some(e2e.encryption_key())).await?;
    plan.gates.push("strict_catalog".to_string());
    catalog::check_no_database_catalog(&*opened.base).await?;
    plan.gates.push("no_database_catalog".to_string());
    catalog::check_names_absent(ids, &facts)?;
    plan.gates.push("names_absent".to_string());
    let all_receipts = receipts::read_all(&*opened.store).await?;
    let receipt_report = receipts::check_listed(&all_receipts, ids)?;
    plan.gates.push("receipts_listed".to_string());
    let protein_index = proteins::load(&opened.base, &opened.store).await?;
    plan.gates.push("protein_index".to_string());
    let named = receipts::receipts_of_names(&all_receipts, ids);
    let sets = molset::compute(&molset::Inputs {
        receipts: &named,
        ids,
        live: &facts.live,
        live_for_check: &facts.live,
        live_owners: &facts.owners,
        proteins: &protein_index,
    })?;
    plan.gates.push("dead_disjoint_from_live".to_string());

    let tips_rules = plan_rules::tips_rules(&sets.dead);
    let report = scan_tips(run, opened, &tips_rules, &sets).await?;
    plan.gates.extend(
        [
            "tips_decode_count",
            "doomed_tip_decode",
            "rules_agree",
            "post_drop_write",
        ]
        .map(str::to_string),
    );
    if report.protein_rows_in_tips > 0 {
        return Err(ReapError::abort(
            "PROTEIN_RESIDUE_IN_TIPS",
            format!(
                "{} protein row(s) are in the tips collection. The protein index is then \
                 incomplete. Drain them on a copy first.",
                report.protein_rows_in_tips
            ),
        ));
    }
    finish_plan(
        run,
        ids,
        opened,
        plan,
        Findings {
            facts,
            receipt_report,
            sets,
            report,
            tips_rules,
        },
        started,
    )
    .await
}

async fn scan_tips(
    run: &PlanRun<'_>,
    opened: &HomeStore,
    rules: &RuleSet,
    sets: &MoleculeSets,
) -> Result<TipsReport, ReapError> {
    let raw = opened
        .base
        .open_namespace("tips")
        .await
        .map_err(|error| ReapError::Failed(format!("open tips: {error}")))?;
    let seam = opened
        .store
        .open_namespace("tips")
        .await
        .map_err(|error| ReapError::Failed(format!("open tips: {error}")))?;
    let spill = run.plan_dir.join(EXACT_V2_FILE);
    let tripwire = Tripwire::new(&sets.drops, run.tripwire_slack_ms);
    let state = TipsState::new(rules, &sets.dead, tripwire, Some(&spill))?;
    tips_pass::run(raw, seam, &opened.base, state).await
}

/// What the passes found, handed to the writing stage.
struct Findings {
    facts: CatalogFacts,
    receipt_report: ReceiptReport,
    sets: MoleculeSets,
    report: TipsReport,
    tips_rules: RuleSet,
}

async fn count_collection(
    base: &Arc<dyn NamespacedStore>,
    present: &BTreeSet<String>,
    name: &str,
    rules: &RuleSet,
) -> Result<CollectionCount, ReapError> {
    if rules.is_empty() || !present.contains(name) {
        return Ok(CollectionCount {
            present: present.contains(name),
            ..CollectionCount::default()
        });
    }
    let kv = base
        .open_namespace(name)
        .await
        .map_err(|error| ReapError::Failed(format!("open {name}: {error}")))?;
    count_matches(kv, rules).await
}

/// A collection with its rules and the counts of its counting pass.
type Counted = (&'static str, RuleSet, CollectionCount);

/// Count the keys that the rules of each collection match.
///
/// The tips counts come from the tip pass, which used the same rule object.
async fn count_all(
    ids: &Identities,
    opened: &HomeStore,
    present: &BTreeSet<String>,
    sets: &MoleculeSets,
    report: &TipsReport,
    tips_rules: RuleSet,
) -> Result<Vec<Counted>, ReapError> {
    let tips_count = CollectionCount {
        present: present.contains("tips"),
        scanned_keys: report.raw_keys,
        scanned_bytes: report.by_class.values().map(|s| s.bytes).sum(),
        matched_keys: report.matched_keys,
        matched_bytes: report.matched_bytes,
    };
    let mut counted = vec![("tips", tips_rules, tips_count)];
    for (name, rules) in [
        (
            "atom_ref_edges_v2",
            plan_rules::v2_rules(&sets.dead, &report.edge_hashes, EXACT_V2_FILE),
        ),
        ("molecule_ref_edges", plan_rules::mref_rules(&sets.dead)),
        ("keep_small", plan_rules::keep_small_rules(&ids.spellings)),
    ] {
        let count = count_collection(&opened.base, present, name, &rules).await?;
        counted.push((name, rules, count));
    }
    Ok(counted)
}

/// Write the rules file of each collection that has rules.
fn write_collections(
    plan_dir: &Path,
    plan: &mut PlanFile,
    counted: Vec<Counted>,
) -> Result<(), ReapError> {
    for (name, rules, count) in counted {
        if rules.is_empty() {
            continue;
        }
        let (file, _) = rules_out::write_rules(
            plan_dir,
            name,
            &rules,
            count.matched_keys,
            count.matched_bytes,
        )?;
        plan.collections.insert(
            name.to_string(),
            CollectionPlan {
                rules_file: file,
                rule_count: rules.rule_count(),
                expect_keys: count.matched_keys,
                expect_bytes: count.matched_bytes,
                count,
            },
        );
    }
    Ok(())
}

async fn finish_plan(
    run: &PlanRun<'_>,
    ids: &Identities,
    opened: &HomeStore,
    mut plan: PlanFile,
    found: Findings,
    started: Instant,
) -> Result<PlanFile, ReapError> {
    let Findings {
        facts,
        receipt_report,
        sets,
        report,
        tips_rules,
    } = found;
    let present: BTreeSet<String> = opened
        .base
        .list_namespaces()
        .await
        .map_err(|error| ReapError::Failed(format!("list collections: {error}")))?
        .into_iter()
        .collect();
    let counted = count_all(ids, opened, &present, &sets, &report, tips_rules).await?;
    if report.edge_keys == 0 {
        let _ = std::fs::remove_file(run.plan_dir.join(EXACT_V2_FILE));
    }
    plan.csn_high_water = opened
        .base
        .raw_last_store()
        .map(|store| store.csn_high_water());
    write_collections(run.plan_dir, &mut plan, counted)?;
    plan.gates.push("counts_written".to_string());
    let shards = super::meters::read_shards(&opened.store, &ids.spellings).await?;
    let e2q = plan_rules::e2_quarantine(&shards.molecules, &sets, &facts.live);
    let e3 = plan_rules::e3_orphans(&report.tokens, &sets, &facts.live, &shards.molecules);
    let rows = plan_rules::retained_rows(
        &KeptLists {
            sets: &sets,
            tokens: &report.tokens,
        },
        &e2q,
        &e3,
    );
    rules_out::write_text(
        run.plan_dir,
        "retained.tsv",
        &rules_out::retained_text(&rows),
    )?;
    fill_summaries(&mut plan, &facts, &receipt_report, &sets, &report);
    plan.molecules.e2_candidates = shards.molecules.len();
    plan.molecules.e2_quarantined = e2q.len();
    plan.molecules.e3_orphans = e3.len();
    plan.molecules.e3_orphan_keys = e3.values().map(|s| s.keys).sum();
    plan.molecules.keep_small_shards_found = shards.names_with_shard.len();
    plan.molecules.keep_small_shards_undecodable = shards.undecodable;
    plan.legacy_collections_present = LEGACY_COLLECTIONS
        .iter()
        .filter(|name| present.contains(**name))
        .map(|name| (*name).to_string())
        .collect();
    plan.workers = 1;
    plan.warnings = warnings(&plan, run.workers);
    plan.files = rules_out::hash_files(run.plan_dir)?;
    plan.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    rules_out::write_plan_json(run.plan_dir, &plan)?;
    Ok(plan)
}
