//! Metadata at the start of one read-only plan.

use super::guard::GuardReport;
use super::identities::Identities;
use super::plan::PlanRun;
use super::plan_file::{IdentitiesFacts, PlanFile, CONTRACT, WINDOW};
use super::plan_report::lastdb_env;
use crate::home::HomeStore;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

pub(super) fn initial_plan(
    run: &PlanRun<'_>,
    ids: &Identities,
    opened: &HomeStore,
    guard_report: GuardReport,
) -> PlanFile {
    PlanFile {
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
    }
}
