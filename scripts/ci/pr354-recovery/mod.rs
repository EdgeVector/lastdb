//! One-time generated-data evidence for PR #354; never part of the product.

mod homes;
mod interrupted;
mod markers;
mod raw_replay;
mod shared_files;

use crate::sync::engine::SyncEngine;
use crate::sync::snapshot::Snapshot;
use serde_json::{json, Value};
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn require(condition: bool, message: &str) -> Result<()> {
    if !condition {
        return Err(std::io::Error::other(message).into());
    }
    Ok(())
}

impl SyncEngine {
    pub async fn pr354_recovery_drill(mode: &str, root: &Path) -> Result<Value> {
        match mode {
            "seed" => homes::seed(root).await,
            "complete" => complete(root).await,
            "reopen-complete" => homes::reopen(root, "complete").await,
            "interrupt" => interrupted::interrupt(root).await,
            "refuse-partial" => interrupted::refuse(root).await,
            "recover" => recover(root).await,
            "reopen-recovered" => homes::reopen(root, "partial").await,
            "raw-replay" => raw_replay::run(root).await,
            "shared-files" => shared_files::run(root).await,
            "marker-entries" => markers::run(root).await,
            "marker-complete" => markers::complete(root).await,
            "cleanup" => cleanup(root),
            _ => Err(std::io::Error::other("unknown drill mode").into()),
        }
    }
}

async fn complete(root: &Path) -> Result<Value> {
    let home = homes::Home::open(&root.join("complete"))?;
    home.initialize().await?;
    let snapshot = homes::snapshot(root)?;
    let report = snapshot.restore(home.logical.as_ref()).await?;
    require(report.batches > 1, "restore did not use multiple batches")?;
    homes::compare(&snapshot, home.logical.as_ref()).await?;
    require(
        !home.runtime().restore_validation_pending(),
        "restore stayed blocked",
    )?;
    Ok(
        json!({"case":"complete", "passed":true, "entries":report.entries,
        "batches":report.batches, "validation_before_unblock":true}),
    )
}

async fn recover(root: &Path) -> Result<Value> {
    let home = homes::Home::open(&root.join("partial"))?;
    require(
        home.runtime().restore_validation_pending(),
        "partial marker disappeared",
    )?;
    let snapshot = homes::snapshot(root)?;
    snapshot.restore(home.logical.as_ref()).await?;
    homes::compare(&snapshot, home.logical.as_ref()).await?;
    require(
        !home.runtime().restore_validation_pending(),
        "complete retry stayed blocked",
    )?;
    Ok(json!({"case":"recover", "passed":true, "complete_retry_cleared_pending":true}))
}

fn canonical(
    snapshot: &Snapshot,
) -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    snapshot
        .namespaces
        .iter()
        .filter(|namespace| {
            matches!(
                namespace.name.as_str(),
                "main" | "schemas" | "schema_states" | "db_catalog"
            )
        })
        .map(|namespace| {
            (
                namespace.name.clone(),
                namespace
                    .entries
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.value.clone()))
                    .collect(),
            )
        })
        .collect()
}

fn cleanup(root: &Path) -> Result<Value> {
    let runner = std::fs::canonicalize(std::env::var("RUNNER_TEMP")?)?;
    let owned = std::fs::canonicalize(root)?;
    require(
        owned.parent() == Some(runner.as_path())
            && owned
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("pr354-generated.")),
        "cleanup path is not this drill's generated home",
    )?;
    std::fs::remove_dir_all(owned)?;
    Ok(json!({"case":"cleanup", "passed":true, "owned_homes_removed":true}))
}
