//! Exercise marker refusal only in this drill's generated, inactive home.

use super::{homes, require, Result};
use crate::storage::disk_pack::PackNamespacedStore;
use serde_json::{json, Value};
use std::path::Path;

pub(super) async fn run(root: &Path) -> Result<Value> {
    let home = root.join("complete");
    let marker = home.join("counter-restore-validation-v1");
    require(!marker.try_exists()?, "complete restore retained a marker")?;

    std::os::unix::fs::symlink("absent-generated-target", &marker)?;
    refused(&home)?;
    std::fs::remove_file(&marker)?;

    std::os::unix::fs::symlink("disk-pack/CURRENT", &marker)?;
    refused(&home)?;
    std::fs::remove_file(&marker)?;

    std::fs::create_dir(&marker)?;
    refused(&home)?;
    std::fs::remove_dir(&marker)?;

    std::fs::write(&marker, b"version=1\nextra")?;
    refused(&home)?;
    std::fs::write(&marker, b"bad bytes")?;
    refused(&home)?;

    // Each successful open gets its own OS process. A flush worker can retain
    // its owner after the API returns; a second in-process open races that owner.
    validate_prefix(&home, b"").await?;
    Ok(json!({"case":"marker-entries", "passed":true,
        "dangling_link_refused":true, "regular_link_refused":true,
        "directory_refused":true, "oversized_refused":true, "wrong_bytes_refused":true,
        "empty_marker_validated":true}))
}

pub(super) async fn complete(root: &Path) -> Result<Value> {
    validate_prefix(&root.join("complete"), b"version=1\n").await?;
    Ok(json!({"case":"marker-complete", "passed":true,
        "complete_marker_validated":true}))
}

async fn validate_prefix(home: &Path, bytes: &[u8]) -> Result<()> {
    let marker = home.join("counter-restore-validation-v1");
    std::fs::write(&marker, bytes)?;
    let target = homes::Home::open(home)?;
    require(
        target.runtime().restore_validation_pending(),
        "valid marker prefix did not block startup",
    )?;
    target.initialize().await?;
    require(
        !target.runtime().restore_validation_pending() && !marker.try_exists()?,
        "valid native marker did not clear after counter validation",
    )
}

fn refused(home: &Path) -> Result<()> {
    let error = match PackNamespacedStore::open(home) {
        Err(error) => error,
        Ok(_) => return Err("invalid native marker allowed startup".into()),
    };
    require(
        error.to_string().contains("restore") || error.to_string().contains("restored"),
        "marker case failed for an unrelated reason",
    )
}
