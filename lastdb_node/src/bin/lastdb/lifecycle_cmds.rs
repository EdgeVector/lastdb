//! Node-lifecycle subcommands: volume isolation, cloud connect, and restore.
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

pub(super) struct ConnectArgs {
    pub(super) env: Option<String>,
    pub(super) api_url: Option<String>,
    pub(super) invite_code: Option<String>,
    pub(super) invite_code_stdin: bool,
    pub(super) force: bool,
    pub(super) use_existing_identity: bool,
}

pub(super) fn connect_command(data_dir: Option<PathBuf>, args: ConnectArgs) -> Result<(), String> {
    let ConnectArgs {
        env,
        api_url,
        invite_code,
        invite_code_stdin,
        force,
        use_existing_identity,
    } = args;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    if use_existing_identity {
        let (home, invite_code) = prepare_existing_identity_dev_connect(
            data_dir.as_deref(),
            env.as_deref(),
            api_url.as_deref(),
            invite_code.as_deref(),
            invite_code_stdin,
            force,
            read_invite_code_from_stdin,
        )?;
        let report = runtime.block_on(lastdb_node::cloud::connect_existing_identity_dev(
            &home,
            &invite_code,
        ))?;
        eprintln!(
            "Connected the copied identity to DEV at {}.",
            report.api_url
        );
        eprintln!("account user_hash={}", report.user_hash);
        Ok(())
    } else {
        let home = lastdb_node::host::resolve_home(data_dir)?;
        let url = resolve_exemem_url(api_url, env.as_deref())?;
        let invite_code = if invite_code_stdin {
            Some(read_invite_code_from_stdin()?)
        } else {
            invite_code
        };
        runtime.block_on(lastdb_node::cloud::connect(
            &home,
            &url,
            invite_code.as_deref(),
            force,
        ))
    }
}

pub(super) struct RestoreArgs {
    pub(super) into: PathBuf,
    pub(super) env: Option<String>,
    pub(super) api_url: Option<String>,
    pub(super) json: bool,
    pub(super) progress_json: bool,
    pub(super) reuse_chunks_from: Option<PathBuf>,
    pub(super) remote_s0_only: bool,
    pub(super) remote_latest: bool,
    pub(super) db_hash: Option<String>,
    pub(super) manifest_sha256: Option<String>,
}

pub(super) fn restore_dispatch_command(
    data_dir: Option<PathBuf>,
    args: RestoreArgs,
) -> Result<(), String> {
    let RestoreArgs {
        into,
        env,
        api_url,
        json,
        progress_json,
        reuse_chunks_from,
        remote_s0_only,
        remote_latest,
        db_hash,
        manifest_sha256,
    } = args;
    if (db_hash.is_some() || manifest_sha256.is_some()) && !remote_s0_only && !remote_latest {
        return Err(
            "--db-hash and --manifest-sha256 require --remote-s0-only or --remote-latest".into(),
        );
    }
    if remote_s0_only {
        restore_remote_s0_only_command(
            data_dir,
            &into,
            env.as_deref(),
            api_url,
            json,
            progress_json,
            RemoteRecoverySelector {
                db_hash: db_hash.as_deref(),
                manifest_sha256: manifest_sha256.as_deref(),
            },
        )
    } else if remote_latest {
        restore_remote_latest_command(
            data_dir,
            &into,
            env.as_deref(),
            api_url,
            json,
            progress_json,
            RemoteLatestOptions::new(
                db_hash.as_deref(),
                manifest_sha256.as_deref(),
                reuse_chunks_from.as_deref(),
            ),
        )
    } else if let Some(cache_home) = reuse_chunks_from {
        restore_command_with_chunk_cache(
            data_dir,
            &into,
            env.as_deref(),
            api_url,
            json,
            progress_json,
            &cache_home,
        )
    } else if progress_json {
        restore_command_with_progress(data_dir, &into, env.as_deref(), api_url, json)
    } else {
        restore_command(data_dir, &into, env.as_deref(), api_url, json)
    }
}
