//! Client side of the two-home proof tooling (hidden `lastdb db` verbs).

use super::*;

/// Dispatch the hidden proof verbs. The daemon enforces every guard; this
/// side only makes the directory path absolute.
pub(super) fn db_proof_command(socket: &Path, action: DbProofCommand) -> Result<(), String> {
    match action {
        DbProofCommand::ExportMutationLog { to, json } => {
            // The daemon checks the path and creates the directory. The CLI
            // must not create anything: a refused path must leave no trace.
            let dir = absolute_dir(&to)?;
            let value = db_post_json(
                socket,
                "/api/db/export-mutation-log",
                &serde_json::json!({ "dir": dir }),
            )?;
            print_proof_report(&value, "mutation_log_export", json)
        }
        DbProofCommand::ReplayMutationLog { from, json } => {
            let dir = absolute_dir(&from)?;
            let value = db_post_json(
                socket,
                "/api/db/replay-mutation-log",
                &serde_json::json!({ "dir": dir }),
            )?;
            print_proof_report(&value, "mutation_log_replay", json)
        }
    }
}

/// Make the path absolute without touching the file system. The daemon
/// refuses a symlink or a `..` component, so the CLI must not resolve them.
fn absolute_dir(path: &Path) -> Result<String, String> {
    std::path::absolute(path)
        .map_err(|e| format!("make {} absolute: {e}", path.display()))?
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| "directory path is not UTF-8".to_string())
}

fn print_proof_report(
    value: &serde_json::Value,
    report_key: &str,
    json_only: bool,
) -> Result<(), String> {
    let report = value
        .get(report_key)
        .ok_or_else(|| format!("response missing {report_key}"))?;
    let text = serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?;
    if !json_only {
        eprintln!("proof tooling: synthetic home, no cloud");
    }
    println!("{text}");
    Ok(())
}
