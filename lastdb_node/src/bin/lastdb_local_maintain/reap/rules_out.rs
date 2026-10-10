//! Writing the plan directory.

use std::collections::BTreeMap;
use std::path::Path;

use fold_db::hex::{hex_lower, sha256_hex};

use super::keys::MolKey;
use super::plan_file::PlanFile;
use super::rules::{hashes_of_exact_text, RuleSet};
use super::ReapError;

/// Write `text` to `rel` under the plan directory. Returns its SHA-256.
pub(crate) fn write_text(plan_dir: &Path, rel: &str, text: &str) -> Result<String, ReapError> {
    let path = plan_dir.join(rel);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, text.as_bytes())?;
    Ok(sha256_hex(text.as_bytes()))
}

/// Write one rules file and prove that it reads back as the same rules.
///
/// Returns the relative path of the file and its SHA-256.
pub(crate) fn write_rules(
    plan_dir: &Path,
    collection: &str,
    rules: &RuleSet,
    expect_keys: u64,
    expect_bytes: u64,
) -> Result<(String, String), ReapError> {
    let rel = format!("rules/{collection}.rules");
    let text = rules.render(collection, expect_keys, expect_bytes);
    let sha = write_text(plan_dir, &rel, &text)?;
    let written = std::fs::read_to_string(plan_dir.join(&rel))?;
    let (parsed, header) = RuleSet::parse(&written, |path| {
        let exact = std::fs::read_to_string(plan_dir.join(path))
            .map_err(|error| format!("read {path}: {error}"))?;
        hashes_of_exact_text(&exact)
    })
    .map_err(|error| ReapError::Failed(format!("rules file {rel} does not parse: {error}")))?;
    if &parsed != rules
        || header.collection != collection
        || header.expect_keys != Some(expect_keys)
        || header.expect_bytes != Some(expect_bytes)
    {
        return Err(ReapError::Failed(format!(
            "rules file {rel} reads back differently from the matcher"
        )));
    }
    Ok((rel, sha))
}

/// One molecule that the plan keeps, and why.
#[derive(Debug, Clone)]
pub(crate) struct RetainedRow {
    pub reason: &'static str,
    pub key: MolKey,
    pub spellings: Vec<String>,
    pub tip_keys: u64,
    pub tip_bytes: u64,
    pub detail: String,
}

/// The text of `retained.tsv`.
pub(crate) fn retained_text(rows: &[RetainedRow]) -> String {
    let mut out = String::from("reason\tdigest\tspellings\ttip_keys\ttip_bytes\tdetail\n");
    for row in rows {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            row.reason,
            hex_lower(row.key),
            row.spellings.join(","),
            row.tip_keys,
            row.tip_bytes,
            row.detail.replace(['\t', '\n'], " ")
        ));
    }
    out
}

/// Write `plan.json` last. Its presence marks a finished plan.
pub(crate) fn write_plan_json(plan_dir: &Path, plan: &PlanFile) -> Result<(), ReapError> {
    let text = serde_json::to_string_pretty(plan)
        .map_err(|error| ReapError::Failed(format!("plan.json: {error}")))?;
    write_text(plan_dir, "plan.json", &format!("{text}\n"))?;
    Ok(())
}

/// Hash every file under `rules/`, `exact/` and `retained.tsv`.
pub(crate) fn hash_files(plan_dir: &Path) -> Result<BTreeMap<String, String>, ReapError> {
    let mut out = BTreeMap::new();
    for dir in ["rules", "exact"] {
        let Ok(entries) = std::fs::read_dir(plan_dir.join(dir)) else {
            continue;
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let bytes = std::fs::read(entry.path())?;
            out.insert(format!("{dir}/{name}"), sha256_hex(&bytes));
        }
    }
    if let Ok(bytes) = std::fs::read(plan_dir.join("retained.tsv")) {
        out.insert("retained.tsv".to_string(), sha256_hex(&bytes));
    }
    Ok(out)
}

/// Mark the plan directory as aborted and remove the files an engine would read.
pub(crate) fn mark_aborted(plan_dir: &Path, gate: &str, message: &str) {
    for dir in ["rules", "exact"] {
        let _ = std::fs::remove_dir_all(plan_dir.join(dir));
    }
    let _ = std::fs::remove_file(plan_dir.join("plan.json"));
    let body = serde_json::json!({ "aborted": true, "gate": gate, "message": message });
    let _ = std::fs::write(plan_dir.join("aborted.json"), body.to_string());
}
