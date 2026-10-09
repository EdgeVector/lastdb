use super::*;

#[path = "cloud_cmds/backup_gc.rs"]
mod backup_gc;
#[path = "cloud_cmds/intent.rs"]
mod intent;
#[path = "cloud_cmds/inventory.rs"]
mod inventory;
#[path = "cloud_cmds/resume_primary.rs"]
mod resume_primary;

pub(super) use backup_gc::*;
pub(super) use intent::*;
pub(super) use inventory::*;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let explicit_data_dir = data_dir.is_some();
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    match action {
        CloudCommand::RewriteDeletedAtoms {
            plan,
            execute,
            json,
        } => runtime.block_on(backup_atom_rewrite::run(&home, &plan, execute, json)),
        CloudCommand::SetupPaid {
            env,
            api_url,
            force,
        } => {
            let url = resolve_exemem_url(api_url, env.as_deref())?;
            runtime.block_on(lastdb_node::cloud::setup_paid(&home, &url, force))
        }
        CloudCommand::Upgrade { env, api_url } => {
            let (url, api_key) = load_cloud_creds(&home, api_url, env.as_deref())?;
            let return_urls = checkout_return_urls(&url);
            let checkout = runtime.block_on(lastdb_node::cloud::create_upgrade_checkout(
                &url,
                &api_key,
                Some(return_urls.success.as_str()),
                Some(return_urls.cancel.as_str()),
            ))?;
            match open_url(&checkout) {
                Ok(()) => {
                    println!("Opened Stripe Checkout in your browser.");
                    println!("If it did not appear, open this URL:");
                    println!("{checkout}");
                }
                Err(e) => {
                    println!("Could not open Stripe Checkout automatically ({e}).");
                    println!("Open this URL:");
                    println!("{checkout}");
                }
            }
            println!();
            println!("Checkout will return to:");
            println!("  {}", return_urls.success);
            println!("After payment succeeds, wait ~1 minute, then run:");
            println!("  lastdb cloud status");
            println!("You need plan=paid and access_allowed=true.");
            Ok(())
        }
        CloudCommand::FixBilling { env, api_url } => {
            let (url, api_key) = load_cloud_creds(&home, api_url, env.as_deref())?;
            eprintln!("Opening Stripe Billing Portal to fix payment...");
            eprintln!("In the portal: update your card and/or pay any open invoice.");
            let portal = runtime.block_on(lastdb_node::cloud::create_portal_session(
                &url, &api_key, None,
            ))?;
            println!("{portal}");
            println!();
            println!("After you fix billing in the browser, wait ~1 minute, then run:");
            println!("  lastdb cloud status");
            println!("You need plan=paid and access_allowed=true before sync works again.");
            println!();
            println!("If the portal fails (never subscribed), run instead:");
            println!("  lastdb cloud upgrade");
            Ok(())
        }
        CloudCommand::Account {
            env,
            api_url,
            json,
            no_open,
        } => {
            let (url, api_key) = load_cloud_creds(&home, api_url, env.as_deref())?;
            let account = runtime.block_on(lastdb_node::cloud::account_link(&url, &api_key))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&account).unwrap_or_else(|_| account.to_string())
                );
                return Ok(());
            }
            let account_url = account
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "account response missing url".to_string())?;
            if no_open {
                println!("Open this URL:");
                println!("{account_url}");
            } else {
                match open_url(account_url) {
                    Ok(()) => {
                        println!("Opened Exemem account page in your browser.");
                        println!("If it did not appear, open this URL:");
                        println!("{account_url}");
                    }
                    Err(e) => {
                        println!("Could not open Exemem account page automatically ({e}).");
                        println!("Open this URL:");
                        println!("{account_url}");
                    }
                }
            }
            if let Some(status) = account.pointer("/account/status") {
                println!();
                lastdb_node::cloud::print_subscription_status(status);
            }
            Ok(())
        }
        CloudCommand::Status { env, api_url } => {
            let (url, api_key) = load_cloud_creds(&home, api_url, env.as_deref())?;
            let status =
                runtime.block_on(lastdb_node::cloud::subscription_status(&url, &api_key))?;
            lastdb_node::cloud::print_subscription_status(&status);
            println!();
            println!("--- raw ---");
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_else(|_| status.to_string())
            );
            Ok(())
        }
        CloudCommand::HealStaging { json } => {
            // Drop the tokio runtime — heal talks to the running daemon over UDS.
            drop(runtime);
            cloud_heal_staging(&home, json)
        }
        CloudCommand::Snapshot { json } => {
            // Drop the tokio runtime — snapshot talks to the running daemon over UDS.
            drop(runtime);
            cloud_laststore_snapshot(&home, json)
        }
        CloudCommand::BackupGc {
            execute,
            json,
            job,
            status,
            wait,
            request_id,
        } => {
            drop(runtime);
            cloud_backup_gc(
                &home,
                execute,
                json,
                job.as_deref(),
                status,
                wait,
                request_id.as_deref(),
            )
        }
        CloudCommand::PrefixInventory { json } => {
            drop(runtime);
            cloud_prefix_inventory(&home, json)
        }
        CloudCommand::BackupConcurrency { value, clear, json } => {
            drop(runtime);
            cloud_backup_concurrency(&home, value, clear, json)
        }
        CloudCommand::Off { json } => {
            drop(runtime);
            cloud_set_intent(&home, false, json)
        }
        CloudCommand::On { json } => {
            drop(runtime);
            cloud_set_intent(&home, true, json)
        }
        CloudCommand::PrepareResumePrimary { json } => {
            drop(runtime);
            cloud_prepare_resume_primary(&home, json)
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
        CloudCommand::QuarantineReplay { target, seq, json } => {
            drop(runtime);
            cloud_quarantine_replay(&home, &target, seq, json)
        }
        CloudCommand::CutManifest {
            previous,
            out,
            json,
        } => cloud_cut_manifest(&home, previous, out.as_deref(), json),
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
