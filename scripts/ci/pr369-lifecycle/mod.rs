//! One-time CI drill on generated homes; never linked into a shipped program.
mod common;
mod controls;
mod files;
mod replay;

use common::{require, Result};
use serde_json::{json, Value};
use std::path::Path;

pub async fn run(mode: &str, root: &Path, fixed: bool) -> Result<Value> {
    let runner = std::fs::canonicalize(std::env::var("RUNNER_TEMP")?)?;
    let owned = std::fs::canonicalize(root)?;
    require(
        owned.parent() == Some(runner.as_path())
            && owned
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("pr369-generated.")),
        "drill root is not its owned CI temporary directory",
    )?;
    match mode {
        "delete-age" | "cutoff" | "replay-age" | "losing-age" => {
            replay::run(mode, &owned.join(mode), fixed).await
        }
        "concurrency" => controls::concurrency(&owned.join(mode)).await,
        "retry" => controls::retry(&owned.join(mode)).await,
        "conflicts-seed" => controls::conflicts_seed(&owned.join("conflicts")).await,
        "conflicts-drain" => controls::conflicts_drain(&owned.join("conflicts")).await,
        "files-seed" => files::seed(&owned.join("files")).await,
        "files-finish" => files::finish(&owned.join("files")).await,
        "files-dwell" => files::dwell(&owned.join("files")).await,
        "cleanup" => {
            // Every earlier invocation shut its owner down and exited. This
            // exact directory is private to this job and contains generated data.
            std::fs::remove_dir_all(owned)?;
            Ok(json!({"case":"cleanup", "passed":true, "owned_homes_removed":true}))
        }
        _ => Err("unknown lifecycle drill mode".into()),
    }
}
