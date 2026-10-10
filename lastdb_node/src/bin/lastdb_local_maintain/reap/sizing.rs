//! The human sizing summary of a plan.

use std::fmt::Write as _;
use std::path::Path;

use super::plan_file::PlanFile;
use super::ReapError;

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
}

/// Render the sizing summary. The text uses short sentences.
pub(crate) fn render(plan: &PlanFile) -> String {
    let mut out = String::new();
    let m = &plan.molecules;
    let _ = writeln!(
        out,
        "Reap plan, window {} (contract {})",
        plan.window, plan.contract
    );
    let _ = writeln!(out, "Home: {}", plan.home);
    let _ = writeln!(
        out,
        "Dropped names: {} listed. {} receipt(s) found. {} name(s) have no receipt.",
        plan.identities.listed, plan.receipts.found, m.receiptless_names
    );
    let _ = writeln!(
        out,
        "Live schemas: {}. Live molecules: {}.",
        plan.catalog.schemas, plan.catalog.live_molecules
    );
    let _ = writeln!(
        out,
        "Dead molecules to drop: {} ({} spellings). Kept: {} shared with live, {} in a mixed \
         protein.",
        m.dead, m.dead_tokens, m.shared_with_live, m.protein_mixed
    );
    let _ = writeln!(
        out,
        "Reported only: {} molecule(s) from meter shards (E2), {} orphan molecule(s) with {} \
         key(s) (E3).",
        m.e2_quarantined, m.e3_orphans, m.e3_orphan_keys
    );
    let t = &plan.tips;
    let _ = writeln!(
        out,
        "Tips pass: {} key(s) read in {} group(s). {} key(s) are doomed, {}.",
        t.decoded_keys,
        t.groups,
        t.doomed_keys,
        mib(t.doomed_bytes)
    );
    let _ = writeln!(
        out,
        "Doomed tips: {} (tip edges planned: {}, without a compact edge: {}).",
        t.doomed_mk, t.edge_keys, t.tips_without_v2_edge
    );
    let _ = writeln!(out, "Rules for the engine (expected counts):");
    for (name, c) in &plan.collections {
        let _ = writeln!(
            out,
            "  {name}: {} key(s), {} ({} rule(s), {} key(s) scanned)",
            c.expect_keys,
            mib(c.expect_bytes),
            c.rule_count,
            c.count.scanned_keys
        );
    }
    for warning in &plan.warnings {
        let _ = writeln!(out, "WARNING: {warning}");
    }
    out
}

/// `reap sizing`: print the sizing of an existing plan directory.
pub(crate) fn run(plan_dir: &Path, json: bool) -> Result<(), ReapError> {
    let path = plan_dir.join("plan.json");
    let text = std::fs::read_to_string(&path).map_err(|error| {
        ReapError::Refused(format!(
            "read {}: {error}. A finished plan has plan.json.",
            path.display()
        ))
    })?;
    let plan: PlanFile = serde_json::from_str(&text).map_err(|error| {
        ReapError::Failed(format!("{} does not parse: {error}", path.display()))
    })?;
    if json {
        println!("{text}");
    } else {
        print!("{}", render(&plan));
    }
    Ok(())
}
