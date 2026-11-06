//! Managed installs use the same signed interface list as resolve.
use super::*;
use lastdb_node::app_registry_index::{self as registry, managed as host};
use serde_json::json;

fn installed(app: &str) -> Result<host::ManagedReceipt, String> {
    host::receipt(&host::app_root(app)?.join("current"))
}

fn app_ids() -> Result<Vec<String>, String> {
    let root = host::apps_root()?;
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        if entry
            .path()
            .join("current/lastdb-app-install.json")
            .is_file()
        {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

fn resolutions(
    home: &Path,
    source: &RegistrySource,
    channel: &str,
) -> Result<Vec<registry::Resolution>, String> {
    if source.trust_key.is_some() || std::env::var_os(registry::TRUST_KEY_ENV).is_some() {
        return Err("managed installs require the pinned registry key".into());
    }
    let list = public_list(home, Some(channel), source.index.as_deref(), None)?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let (build, _, _) = registry::running_node(&socket);
    let (trust, _) = registry::resolve_list_trust_key(None)?;
    let index = registry::IndexLocation::resolve(source.index.as_deref())?
        .describe_interface(channel, list.api_version);
    Ok(list
        .apps
        .into_iter()
        .map(|entry| registry::Resolution {
            app_id: entry.app_id.clone(),
            channel: entry.channel.clone(),
            source: entry.source.clone(),
            lastdb_version: build.clone(),
            lastdb_version_source: "running node".into(),
            app_version: entry.app_version.clone(),
            sha: entry.sha.clone(),
            artifact: None,
            proved_at: entry.published_at.clone(),
            proof_run: String::new(),
            index: index.clone(),
            trust: format!("pinned:{}", app_identity_crypto::key_id(&trust)),
            list_entry: Some(entry),
        })
        .collect())
}

pub(super) fn install(home: &Path, input: &AppInstall) -> Result<(), String> {
    let channel = input
        .source
        .channel
        .as_deref()
        .unwrap_or(registry::DEFAULT_CHANNEL);
    let rows = resolutions(home, &input.source, channel)?;
    let mut results = Vec::new();
    for app in &input.app_ids {
        let row = rows
            .iter()
            .find(|r| &r.app_id == app)
            .ok_or_else(|| format!("no app '{app}' was proved with lastdb on this interface"))?;
        results.push(host::install(
            row,
            home,
            host::ActivationOptions {
                accept_install: input.accept_install,
                inactive: input.inactive,
            },
        )?);
    }
    print_pretty_json(&results);
    registry::managed_poll::install()?;
    Ok(())
}

pub(super) fn upgrade(home: &Path, input: &AppUpgrade) -> Result<(), String> {
    let names = if input.all {
        app_ids()?
    } else {
        input.app_ids.clone()
    };
    let mut rows = std::collections::HashMap::new();
    let mut results = Vec::new();
    let mut failed = false;
    let polling = std::env::var("LASTDB_APP_REGISTRY_POLL").ok().as_deref() == Some("1");
    for app in names {
        let current = installed(&app)?;
        let channel = input.source.channel.as_deref().unwrap_or(&current.channel);
        let stamp = host::app_root(&app)?.join("last-checked");
        if polling
            && channel == "stable"
            && std::fs::metadata(&stamp)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|d| d.as_secs() < 3600)
        {
            let root = host::app_root(&app)?;
            let _lock = host::lock(&root)?;
            let current = installed(&app)?;
            if let Err(error) = host::verify_or_restore(&root, home, &current) {
                failed = true;
                results.push(json!({"app": app, "state": "red", "error": error}));
            }
            continue;
        }
        let mut source = input.source.clone();
        source.index = source.index.or_else(|| Some(current.index.clone()));
        let cache_key = format!("{}|{channel}", source.index.as_deref().unwrap_or_default());
        if let std::collections::hash_map::Entry::Vacant(entry) = rows.entry(cache_key.clone()) {
            entry.insert(resolutions(home, &source, channel)?);
        }
        std::fs::write(&stamp, chrono::Utc::now().to_rfc3339()).map_err(|e| e.to_string())?;
        let Some(row) = rows
            .get(&cache_key)
            .and_then(|v| v.iter().find(|r| r.app_id == app))
        else {
            if !polling {
                results
                    .push(json!({"app": app, "state": "no_row", "installed_release": current.sha}));
            }
            continue;
        };
        match host::install(
            row,
            home,
            host::ActivationOptions {
                accept_install: input.accept_install,
                inactive: current.inactive,
            },
        ) {
            Ok(receipt) => {
                if !polling || receipt.sha != current.sha {
                    results.push(json!({"app": app, "state": "current", "receipt": receipt}));
                }
            }
            Err(error) => {
                failed = true;
                results.push(json!({"app": app, "state": "red", "error": error}));
            }
        }
    }
    if !polling || !results.is_empty() {
        print_pretty_json(&results);
    }
    if failed {
        Err("one or more app upgrades failed; prior versions retained".into())
    } else {
        Ok(())
    }
}

pub(super) fn command(home: &Path, command: AppManagedCommand) -> Result<(), String> {
    match command {
        AppManagedCommand::Rollback { app_id, json: _ } => {
            print_pretty_json(&host::rollback(&app_id, home)?);
            Ok(())
        }
        AppManagedCommand::Status {
            app_id,
            check,
            json: _,
        } => {
            let names = app_id.map_or_else(app_ids, |app| Ok(vec![app]))?;
            let mut rows = Vec::new();
            let mut failed = false;
            for app in names {
                let root = host::app_root(&app)?;
                match installed(&app) {
                    Ok(receipt) => {
                        let probe = if check {
                            host::verify_active(&root, home, &receipt)
                        } else {
                            host::verify_state(&root, &receipt)
                        };
                        failed |= probe.is_err();
                        rows.push(json!({"app": app, "state": if probe.is_err() {"red"} else if receipt.inactive {"inactive"} else {"current"},
                            "installed_release": receipt.sha, "active_release": std::fs::read_link(root.join("current")).ok(),
                            "prior_release": std::fs::read_link(root.join("previous")).ok(), "probe": probe.err(), "receipt": receipt}));
                    }
                    Err(error) => {
                        failed = true;
                        rows.push(json!({"app": app, "state": "not_installed", "error": error}));
                    }
                }
            }
            print_pretty_json(&rows);
            if failed {
                Err("one or more apps are unhealthy".into())
            } else {
                Ok(())
            }
        }
    }
}
