use super::*;
use lastdb_node::app_registry_index::{self as registry, managed as host};
use serde_json::json;
use std::collections::HashMap;

#[path = "app_cmds/interface_publish.rs"]
mod interface_publish;

pub(super) fn print_pretty_json<T: serde::Serialize + ?Sized>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

pub(super) fn block_on_app<T>(
    fut: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?
        .block_on(fut)
}

type Catalog = (
    registry::InterfaceList,
    String,
    String,
    (String, &'static str),
);

fn load_catalog(
    home: &Path,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
) -> Result<Catalog, String> {
    let ch = channel.unwrap_or(registry::DEFAULT_CHANNEL);
    let loc = registry::IndexLocation::resolve(index)?;
    let (vk, trust) = registry::resolve_list_trust_key(trust_key)?;
    let (build, src, api, outbox) = registry::running_node(home);
    let mut list = block_on_app(registry::fetch_list(&loc, ch, api, &vk))?;
    list.apps
        .retain(|e| e.outbox_format_version.is_none_or(|v| v == outbox));
    let idx = loc.describe_interface(ch, list.api_version);
    Ok((list, idx, trust, (build, src)))
}

pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::Rollback { app_id, .. } => {
            print_pretty_json(&host::rollback(&app_id, &home)?);
            Ok(())
        }
        AppCommand::Status { app_id, check, .. } => status_apps(&home, app_id, check),
        AppCommand::Publish(args) => interface_publish::publish_interface(&home, &args),
        AppCommand::List {
            app_id,
            catalog_entry,
            source,
            json,
        } => list_apps(&home, app_id.as_deref(), catalog_entry, json, &source),
        AppCommand::Install {
            app_ids,
            source,
            accept_install,
            inactive,
            ..
        } => install_apps(&home, accept_install, inactive, &app_ids, &source),
        AppCommand::Upgrade {
            app_ids,
            all,
            accept_install,
            source,
            ..
        } => upgrade_apps(&home, all, accept_install, app_ids, &source),
    }
}

fn list_apps(
    home: &Path,
    app_id: Option<&str>,
    catalog_entry: bool,
    json: bool,
    src: &CatalogSource,
) -> Result<(), String> {
    let (list, index, trust, (nb, ns)) = load_catalog(
        home,
        src.channel.as_deref(),
        src.index.as_deref(),
        src.trust_key.as_deref(),
    )?;
    let Some(app_id) = app_id else {
        if json {
            print_pretty_json(&list);
        } else {
            println!(
                "# {} API {} · sequence {} · generated {}",
                list.channel, list.api_version, list.seq, list.generated_at
            );
            for e in &list.apps {
                println!("{}\t{}\t{}", e.app_id, e.app_version, e.sha);
            }
        }
        return Ok(());
    };
    let e = list
        .apps
        .iter()
        .find(|e| e.app_id == app_id)
        .ok_or_else(|| {
            format!(
                "no app '{app_id}' was proved with lastdb API {}",
                list.api_version
            )
        })?;
    if catalog_entry || json {
        print_pretty_json(e);
    } else {
        println!(
            "{} {} @ {} proved with lastdb {nb} ({ns}) at {}\n  source: {}\n  index: {index} (trust: {trust})",
            e.app_id, e.app_version, e.sha, e.published_at, e.source
        );
    }
    Ok(())
}

fn installed(app: &str) -> Result<host::ManagedReceipt, String> {
    host::receipt(&host::app_root(app)?.join("current"))
}

fn installed_app_ids() -> Result<Vec<String>, String> {
    let root = host::apps_root()?;
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut names: Vec<_> = std::fs::read_dir(root)
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter_map(|e| {
            e.path()
                .join("current")
                .join(registry::RECEIPT_FILE)
                .is_file()
                .then(|| e.file_name().to_string_lossy().into_owned())
        })
        .collect();
    names.sort();
    Ok(names)
}

fn resolutions(
    home: &Path,
    channel: &str,
    index: Option<&str>,
    trust_key: Option<&str>,
) -> Result<Vec<registry::Resolution>, String> {
    if trust_key.is_some() || std::env::var_os(registry::TRUST_KEY_ENV).is_some() {
        return Err("managed installs require the pinned registry key".into());
    }
    let (list, index, trust, _) = load_catalog(home, Some(channel), index, None)?;
    Ok(list
        .apps
        .into_iter()
        .map(|entry| registry::Resolution {
            entry,
            index: index.clone(),
            trust: trust.clone(),
        })
        .collect())
}

fn install_apps(
    home: &Path,
    accept_install: bool,
    inactive: bool,
    app_ids: &[String],
    src: &CatalogSource,
) -> Result<(), String> {
    let ch = src.channel.as_deref().unwrap_or(registry::DEFAULT_CHANNEL);
    let rows = resolutions(home, ch, src.index.as_deref(), src.trust_key.as_deref())?;
    let results: Vec<_> = app_ids
        .iter()
        .map(|app| {
            let row = rows
                .iter()
                .find(|r| &r.entry.app_id == app)
                .ok_or_else(|| {
                    format!("no app '{app}' was proved with lastdb on this interface")
                })?;
            host::install(row, home, accept_install, inactive)
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
    for d in [&agents, &home.join(".local/state/lastdb-app-registry")] {
        std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
    }
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let h = home
        .display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let off = std::env::var("LAST_STACK_APPS_OFF").unwrap_or_else(|_| "0".into());
    if off != "0" && off != "1" {
        return Err("LAST_STACK_APPS_OFF must be 0 or 1".into());
    }
    let data = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>{h}/.local/bin/lastdb</string><string>app</string><string>upgrade</string><string>--all</string></array>\n<key>StartInterval</key><integer>60</integer>\n<key>EnvironmentVariables</key><dict><key>HOME</key><string>{h}</string><key>PATH</key><string>{h}/.local/bin:{h}/.bun/bin:{h}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>LASTDB_APP_REGISTRY_POLL</key><string>1</string><key>LAST_STACK_APPS_OFF</key><string>{off}</string></dict>\n<key>StandardOutPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.log</string>\n<key>StandardErrorPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.err</string></dict></plist>\n"
    );
    if std::fs::read_to_string(&plist).ok().as_deref() == Some(&data) {
        return Ok(());
    }
    std::fs::write(&plist, data).map_err(|e| e.to_string())?;
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| e.to_string())?;
    let dom = format!("gui/{}", String::from_utf8_lossy(&uid.stdout).trim());
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{dom}/{label}")])
        .output();
    Command::new("launchctl")
        .args(["bootstrap", &dom])
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

type CachedLists = HashMap<(String, String), Result<Vec<registry::Resolution>, String>>;

fn upgrade_one(
    home: &Path,
    accept_install: bool,
    polling: bool,
    src: &CatalogSource,
    lists: &mut CachedLists,
    app: &str,
) -> Result<Option<serde_json::Value>, String> {
    let cur = installed(app)?;
    if src.index.is_none() && cur.index.trim().starts_with("lastsecrets://") {
        let err = "saved private registry index is no longer supported";
        return if polling {
            Ok(Some(json!({
                "app": app, "state": "blocked", "reason": "unsupported_saved_index",
                "error": err, "installed_release": cur.sha
            })))
        } else {
            Err(err.into())
        };
    }
    let (ch, root) = (
        src.channel.as_deref().unwrap_or(&cur.channel),
        host::app_root(app)?,
    );
    let stamp = root.join("last-checked");
    if polling
        && ch == "stable"
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
    let eff = src.index.as_deref().unwrap_or(&cur.index);
    let rows = lists
        .entry((eff.to_owned(), ch.to_owned()))
        .or_insert_with(|| resolutions(home, ch, Some(eff), src.trust_key.as_deref()))
        .as_ref()
        .map_err(Clone::clone)?;
    std::fs::write(&stamp, chrono::Utc::now().to_rfc3339()).map_err(|e| e.to_string())?;
    let Some(row) = rows.iter().find(|r| r.entry.app_id == app) else {
        return Ok((!polling)
            .then(|| json!({"app": app, "state": "no_row", "installed_release": cur.sha})));
    };
    let receipt = host::install(row, home, accept_install, cur.inactive)?;
    Ok((!polling || receipt.sha != cur.sha)
        .then(|| json!({"app": app, "state": "current", "receipt": receipt})))
}

fn upgrade_apps(
    home: &Path,
    all: bool,
    accept_install: bool,
    ids: Vec<String>,
    src: &CatalogSource,
) -> Result<(), String> {
    let names = if all { installed_app_ids()? } else { ids };
    let polling = std::env::var("LASTDB_APP_REGISTRY_POLL").as_deref() == Ok("1");
    let (mut lists, mut results, mut failed) = (HashMap::new(), Vec::new(), false);
    for app in names {
        match upgrade_one(home, accept_install, polling, src, &mut lists, &app) {
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

fn status_apps(home: &Path, app_id: Option<String>, check: bool) -> Result<(), String> {
    let names = app_id.map_or_else(installed_app_ids, |app| Ok(vec![app]))?;
    let (mut rows, mut failed) = (Vec::new(), false);
    for app in names {
        let root = host::app_root(&app)?;
        match installed(&app) {
            Ok(r) => {
                let probe = host::verify(&root, check.then_some(home), &r).err();
                failed |= probe.is_some();
                let state = if probe.is_some() {
                    "red"
                } else if r.inactive {
                    "inactive"
                } else {
                    "current"
                };
                rows.push(json!({
                    "app": app, "state": state, "installed_release": r.sha,
                    "active_release": std::fs::read_link(root.join("current")).ok(),
                    "prior_release": std::fs::read_link(root.join("previous")).ok(),
                    "probe": probe, "receipt": r
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
