use super::*;
use lastdb_node::app_registry_index::{self as registry, managed as host};
use serde_json::json;

fn installed(app: &str) -> Result<host::ManagedReceipt, String> {
    host::receipt(&host::app_root(app)?.join("current"))
}

fn installed_app_ids() -> Result<Vec<String>, String> {
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
    let (trust, _) = registry::resolve_list_trust_key(None)?;
    let index = registry::IndexLocation::resolve(source.index.as_deref())?
        .describe_interface(channel, list.api_version);
    let trust_str = format!("pinned:{}", app_identity_crypto::key_id(&trust));
    Ok(list
        .apps
        .into_iter()
        .map(|entry| registry::Resolution {
            entry,
            index: index.clone(),
            trust: trust_str.clone(),
        })
        .collect())
}

pub(super) fn install(
    home: &Path,
    accept_install: bool,
    inactive: bool,
    app_ids: &[String],
    source: &RegistrySource,
) -> Result<(), String> {
    let ch = source
        .channel
        .as_deref()
        .unwrap_or(registry::DEFAULT_CHANNEL);
    let rows = resolutions(home, source, ch)?;
    let opts = host::ActivationOptions {
        accept_install,
        inactive,
    };
    let results: Vec<_> = app_ids
        .iter()
        .map(|app| {
            let row = rows
                .iter()
                .find(|r| &r.entry.app_id == app)
                .ok_or_else(|| {
                    format!("no app '{app}' was proved with lastdb on this interface")
                })?;
            host::install(row, home, opts)
        })
        .collect::<Result<_, _>>()?;
    print_pretty_json(&results);
    install_poll()
}

#[cfg(target_os = "macos")]
fn install_poll() -> Result<(), String> {
    use std::process::Command;
    if std::env::var_os("LASTDB_APPS_ROOT").is_some() {
        return Ok(());
    }
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let agents = home.join("Library/LaunchAgents");
    std::fs::create_dir_all(&agents).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(home.join(".local/state/lastdb-app-registry"))
        .map_err(|e| e.to_string())?;
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let user_home = home
        .display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let apps_off = std::env::var("LAST_STACK_APPS_OFF").unwrap_or_else(|_| "0".into());
    if apps_off != "0" && apps_off != "1" {
        return Err("LAST_STACK_APPS_OFF must be 0 or 1".into());
    }
    let data = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\"><dict>\n\
<key>Label</key><string>{label}</string>\n\
<key>ProgramArguments</key><array><string>{user_home}/.local/bin/lastdb</string><string>app</string><string>upgrade</string><string>--all</string></array>\n\
<key>StartInterval</key><integer>60</integer>\n\
<key>EnvironmentVariables</key><dict><key>HOME</key><string>{user_home}</string><key>PATH</key><string>{user_home}/.local/bin:{user_home}/.bun/bin:{user_home}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>LASTDB_APP_REGISTRY_POLL</key><string>1</string><key>LAST_STACK_APPS_OFF</key><string>{apps_off}</string></dict>\n\
<key>StandardOutPath</key><string>{user_home}/.local/state/lastdb-app-registry/upgrade.log</string>\n\
<key>StandardErrorPath</key><string>{user_home}/.local/state/lastdb-app-registry/upgrade.err</string>\n\
</dict></plist>\n"
    );
    if std::fs::read_to_string(&plist).ok().as_deref() == Some(&data) {
        return Ok(());
    }
    std::fs::write(&plist, data).map_err(|e| e.to_string())?;
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| e.to_string())?;
    let domain = format!("gui/{}", String::from_utf8_lossy(&uid.stdout).trim());
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{domain}/{label}")])
        .output();
    Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .output()
        .map_err(|e| e.to_string())?
        .status
        .success()
        .then_some(())
        .ok_or_else(|| "could not load registry app updater".into())
}

#[cfg(not(target_os = "macos"))]
fn install_poll() -> Result<(), String> {
    Ok(())
}

fn upgrade_one(
    home: &Path,
    accept_install: bool,
    base_source: &RegistrySource,
    polling: bool,
    lists: &mut std::collections::HashMap<
        (String, String),
        Result<Vec<registry::Resolution>, String>,
    >,
    app: &str,
) -> Result<Option<serde_json::Value>, String> {
    let current = installed(app)?;
    let mut source = base_source.clone();
    if source.index.is_none() && current.index.trim().starts_with("lastsecrets://") {
        let error = "saved private registry index is no longer supported";
        return if polling {
            Ok(Some(json!({
                "app": app, "state": "blocked", "reason": "unsupported_saved_index",
                "error": error, "installed_release": current.sha
            })))
        } else {
            Err(error.into())
        };
    }
    let channel = source.channel.as_deref().unwrap_or(&current.channel);
    let root = host::app_root(app)?;
    let stamp = root.join("last-checked");
    if polling
        && channel == "stable"
        && std::fs::metadata(&stamp)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|d| d.as_secs() < 3600)
    {
        let _lock = host::lock(&root)?;
        host::verify_or_restore(&root, home, &installed(app)?)?;
        return Ok(None);
    }
    if source.index.is_none() {
        source.index = Some(current.index);
    }
    let key = (
        source.index.as_deref().unwrap_or_default().to_owned(),
        channel.to_owned(),
    );
    let rows = lists
        .entry(key)
        .or_insert_with(|| resolutions(home, &source, channel))
        .as_ref()
        .map_err(Clone::clone)?;
    std::fs::write(&stamp, chrono::Utc::now().to_rfc3339()).map_err(|e| e.to_string())?;
    let Some(row) = rows.iter().find(|r| r.entry.app_id == app) else {
        return Ok((!polling)
            .then(|| json!({"app": app, "state": "no_row", "installed_release": current.sha})));
    };
    let receipt = host::install(
        row,
        home,
        host::ActivationOptions {
            accept_install,
            inactive: current.inactive,
        },
    )?;
    Ok((!polling || receipt.sha != current.sha)
        .then(|| json!({"app": app, "state": "current", "receipt": receipt})))
}

pub(super) fn upgrade(
    home: &Path,
    all: bool,
    accept_install: bool,
    ids: Vec<String>,
    source: &RegistrySource,
) -> Result<(), String> {
    let names = if all { installed_app_ids()? } else { ids };
    let polling = std::env::var("LASTDB_APP_REGISTRY_POLL").as_deref() == Ok("1");
    let (mut lists, mut results, mut failed) =
        (std::collections::HashMap::new(), Vec::new(), false);
    for app in names {
        match upgrade_one(home, accept_install, source, polling, &mut lists, &app) {
            Ok(Some(res)) => results.push(res),
            Ok(None) => {}
            Err(error) => {
                failed = true;
                results.push(json!({"app": app, "state": "red", "error": error}));
            }
        }
    }
    if !polling || !results.is_empty() {
        print_pretty_json(&results);
    }
    (!failed)
        .then_some(())
        .ok_or_else(|| "one or more app upgrades failed; prior versions retained".into())
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
            let names = app_id.map_or_else(installed_app_ids, |app| Ok(vec![app]))?;
            let (mut rows, mut failed) = (Vec::new(), false);
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
                        let state = if probe.is_err() {
                            "red"
                        } else if receipt.inactive {
                            "inactive"
                        } else {
                            "current"
                        };
                        rows.push(json!({
                            "app": app, "state": state, "installed_release": receipt.sha,
                            "active_release": std::fs::read_link(root.join("current")).ok(),
                            "prior_release": std::fs::read_link(root.join("previous")).ok(),
                            "probe": probe.err(), "receipt": receipt
                        }));
                    }
                    Err(error) => {
                        failed = true;
                        rows.push(json!({"app": app, "state": "not_installed", "error": error}));
                    }
                }
            }
            print_pretty_json(&rows);
            (!failed)
                .then_some(())
                .ok_or_else(|| "one or more apps are unhealthy".into())
        }
    }
}
