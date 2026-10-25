//! Local volume isolation command.
//!
//! Split out of `run` in `lastdb.rs`; each function is one `Command` arm.

use super::*;

pub(super) fn isolate_volume_command(
    data_dir: Option<PathBuf>,
    volume_name: &str,
    skip_volume: bool,
    execute: bool,
    json: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let plan = lastdb_node::volume_isolation::plan(&home, volume_name, skip_volume)?;

    if !execute {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "dry_run": true,
                    "home": plan.home,
                    "volume": plan.volume.as_ref().map(|v| serde_json::json!({
                        "container": v.container,
                        "volume_name": v.volume_name,
                        "mount_point": v.mount_point,
                    })),
                }))
                .map_err(|e| format!("failed to serialize plan: {e}"))?
            );
        } else {
            println!("Plan (pass --execute to apply; needs root):");
            match &plan.volume {
                None => println!(
                    "  exclude {} from Time Machine (no volume: --skip-volume)",
                    plan.home.display()
                ),
                Some(v) => {
                    println!("  exclude {} from Time Machine", plan.home.display());
                    println!(
                        "  create APFS volume \"{}\" in container {} at {}",
                        v.volume_name,
                        v.container,
                        v.mount_point.display()
                    );
                    println!(
                        "  disable FSEvents on it ({}/.fseventsd/no_log)",
                        v.mount_point.display()
                    );
                    println!(
                        "  initialize the node home at {} instead of {}",
                        v.mount_point.display(),
                        plan.home.display()
                    );
                }
            }
        }
        return Ok(());
    }

    let report = lastdb_node::volume_isolation::execute(&plan)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "time_machine_excluded": report.time_machine_excluded,
                "effective_home": report.effective_home,
            }))
            .map_err(|e| format!("failed to serialize report: {e}"))?
        );
    } else {
        println!("Excluded {} from Time Machine.", plan.home.display());
        if let Some(v) = &plan.volume {
            println!(
                "Created volume \"{}\" at {} with FSEvents disabled.",
                v.volume_name,
                v.mount_point.display()
            );
            println!(
                "Node home initialized at {}. Use --data-dir {} (or `lastdb service-home set {}`) to serve from it.",
                report.effective_home.display(),
                report.effective_home.display(),
                report.effective_home.display()
            );
        }
    }
    Ok(())
}
