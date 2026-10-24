use super::*;

#[path = "cloud_cmds/backup_gc.rs"]
mod backup_gc;
#[path = "cloud_cmds/billing.rs"]
mod billing;
#[path = "cloud_cmds/intent.rs"]
mod intent;
#[path = "cloud_cmds/inventory.rs"]
mod inventory;
#[path = "cloud_cmds/native_backup.rs"]
mod native_backup;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/resume_primary.rs"]
mod resume_primary;

pub(super) use backup_gc::*;
pub(super) use intent::*;
pub(super) use inventory::*;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    if matches!(
        action,
        CloudCommand::Backup {
            action: NativeBackupCommand::Auto { .. }
        }
    ) && !data_dir.as_ref().is_some_and(|home| home.is_absolute())
    {
        return Err("automatic backup requires an explicit absolute --data-dir home".into());
    }
    let action = match action {
        CloudCommand::Backup { action } if native_backup::is_setup(&action) => {
            return native_backup::setup(data_dir.as_deref(), action);
        }
        action => action,
    };
    if recovery_home::handles(&action) {
        return recovery_home::command(data_dir.as_deref(), action);
    }
    let explicit_data_dir = data_dir.is_some();
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    match action {
        CloudCommand::Backup { action } => {
            drop(runtime);
            native_backup::run(&home, action)
        }
        CloudCommand::PrepareRecoveryHome { .. }
        | CloudCommand::RecoverDevCredentialsHome { .. } => {
            unreachable!("handled before home resolution")
        }
        action @ (CloudCommand::SetupPaid { .. }
        | CloudCommand::Upgrade { .. }
        | CloudCommand::FixBilling { .. }
        | CloudCommand::Account { .. }
        | CloudCommand::Status { .. }) => cloud_billing_command(&home, &runtime, action),
        action @ (CloudCommand::HealStaging { .. }
        | CloudCommand::Snapshot { .. }
        | CloudCommand::BackupGc { .. }
        | CloudCommand::PrefixInventory { .. }
        | CloudCommand::BackupConcurrency { .. }
        | CloudCommand::Off { .. }
        | CloudCommand::On { .. }
        | CloudCommand::PrepareResumePrimary { .. }
        | CloudCommand::QuarantineReplay { .. }) => {
            // These talk to the running daemon over UDS; no runtime needed.
            drop(runtime);
            cloud_daemon_command(&home, action)
        }
        CloudCommand::ResumePrimary { action } => resume_primary::run(&home, runtime, action),
        CloudCommand::BackupWhileOff {
            json,
            inspect_writers,
            execute,
            wait,
        } => {
            if !explicit_data_dir {
                return Err("backup-while-off requires an explicit --data-dir home".into());
            }
            runtime.block_on(cloud_rescue_publish::run(
                &home,
                execute,
                wait,
                inspect_writers,
                json,
            ))
        }
        CloudCommand::CutManifest {
            previous,
            out,
            json,
        } => cloud_cut_manifest(&home, previous, out.as_deref(), json),
    }
}

/// `lastdb cloud` subcommands that call the running daemon over its socket.
fn cloud_daemon_command(home: &Path, action: CloudCommand) -> Result<(), String> {
    match action {
        CloudCommand::HealStaging { json } => cloud_heal_staging(home, json),
        CloudCommand::Snapshot { json } => cloud_laststore_snapshot(home, json),
        CloudCommand::BackupGc {
            execute,
            json,
            job,
            status,
            wait,
            request_id,
        } => cloud_backup_gc(
            home,
            execute,
            json,
            job.as_deref(),
            status,
            wait,
            request_id.as_deref(),
        ),
        CloudCommand::PrefixInventory { json } => cloud_prefix_inventory(home, json),
        CloudCommand::BackupConcurrency { value, clear, json } => {
            cloud_backup_concurrency(home, value, clear, json)
        }
        CloudCommand::Off { json } => cloud_set_intent(home, false, json),
        CloudCommand::On { json } => cloud_set_intent(home, true, json),
        CloudCommand::PrepareResumePrimary { json } => cloud_prepare_resume_primary(home, json),
        CloudCommand::QuarantineReplay { target, seq, json } => {
            cloud_quarantine_replay(home, &target, seq, json)
        }
        _ => unreachable!("cloud_command routes only daemon subcommands here"),
    }
}

/// Billing subcommands of `lastdb cloud`: setup-paid, upgrade, fix-billing,
/// account and status.
fn cloud_billing_command(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    action: CloudCommand,
) -> Result<(), String> {
    match action {
        CloudCommand::SetupPaid {
            env,
            api_url,
            force,
        } => billing::setup_paid(home, runtime, env.as_deref(), api_url, force),
        CloudCommand::Upgrade { env, api_url } => {
            billing::upgrade(home, runtime, env.as_deref(), api_url)
        }
        CloudCommand::FixBilling { env, api_url } => {
            billing::fix_billing(home, runtime, env.as_deref(), api_url)
        }
        CloudCommand::Account {
            env,
            api_url,
            json,
            no_open,
        } => billing::account(home, runtime, env.as_deref(), api_url, json, no_open),
        CloudCommand::Status { env, api_url } => {
            billing::status(home, runtime, env.as_deref(), api_url)
        }
        _ => unreachable!("cloud_command routes only billing subcommands here"),
    }
}

pub(super) fn load_cloud_creds(
    home: &Path,
    api_url: Option<String>,
    env: Option<&str>,
) -> Result<(String, String), String> {
    load_cloud_creds_from_path(
        &home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE),
        api_url,
        env,
    )
}

pub(super) fn load_cloud_creds_from_path(
    path: &Path,
    api_url: Option<String>,
    env: Option<&str>,
) -> Result<(String, String), String> {
    let bytes = std::fs::read(path).map_err(|e| {
        format!(
            "missing {} ({e}) — run `lastdb connect` or `lastdb cloud setup-paid` first",
            path.display()
        )
    })?;
    let cfg: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid {}: {e}", path.display()))?;
    let file_url = cfg
        .get("api_url")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let api_key = cfg
        .get("api_key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{} missing api_key", path.display()))?
        .to_string();
    let url = match api_url {
        Some(u) => u,
        None if env.is_some() => resolve_exemem_url(None, env)?,
        None => file_url.ok_or_else(|| format!("{} missing api_url", path.display()))?,
    };
    Ok((url, api_key))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CheckoutReturnUrls {
    pub(super) success: String,
    pub(super) cancel: String,
}

pub(super) fn checkout_return_urls(api_url: &str) -> CheckoutReturnUrls {
    let base = api_url.trim_end_matches('/');
    CheckoutReturnUrls {
        success: format!("{base}/account/thank-you?source=lastdb-cloud-upgrade"),
        cancel: format!("{base}/account?checkout=cancelled&source=lastdb-cloud-upgrade"),
    }
}

pub(super) fn open_url(url: &str) -> Result<(), String> {
    let (program, args) = platform_open_invocation(url);
    let status = ProcessCommand::new(program)
        .args(args)
        .status()
        .map_err(|e| format!("{program}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{program} exited with {status}"))
    }
}

pub(super) fn platform_open_invocation(url: &str) -> (&'static str, Vec<String>) {
    #[cfg(target_os = "macos")]
    {
        ("open", vec![url.to_string()])
    }
    #[cfg(target_os = "windows")]
    {
        (
            "rundll32",
            vec!["url.dll,FileProtocolHandler".to_string(), url.to_string()],
        )
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        ("xdg-open", vec![url.to_string()])
    }
}
