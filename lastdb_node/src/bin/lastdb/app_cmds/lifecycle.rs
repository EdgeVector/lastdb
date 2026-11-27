//! Installed app status inspection and fleet/per-app upgrade operations.

use super::{parse_catalog_env, print_pretty_json, resolutions, CatalogSource};
use crate::app_registry_index::{self as registry, managed as host};
use lastdb_node::profile::endpoints::Environment;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

fn installed_app_ids() -> Result<Vec<String>, String> {
    let root = host::apps_root()?;
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| e.to_string())? {
        let e = entry.map_err(|e| e.to_string())?;
        if e.path()
            .join("current")
            .join(registry::RECEIPT_FILE)
            .is_file()
        {
            names.push(e.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

type CachedLists = HashMap<(String, String), Result<Vec<registry::Resolution>, String>>;

fn upgrade_one(
    home: &Path,
    (accept_install, polling): (bool, bool),
    src: &CatalogSource,
    lists: &mut CachedLists,
    app: &str,
) -> Result<Option<serde_json::Value>, String> {
    let root = host::app_root(app)?;
    let cur = host::receipt(&root.join("current"))?;
    let env_override = parse_catalog_env(src.env.as_deref())?;
    let target_env = env_override.unwrap_or_else(Environment::from_env_or_prod);
    let migrated_private = if src.index.is_none() && cur.index.trim().starts_with("lastsecrets://")
    {
        let Some(loc) = registry::private_lists::saved_locator_for_account(
            target_env,
            &cur.listing.publisher_account,
        )?
        else {
            let err = format!(
                "saved private registry index is no longer supported; run 'lastdb app show {app} --save' as the owner to save its private list locator"
            );
            if !polling {
                return Err(err);
            }
            return Ok(Some(json!({
                "app": app, "state": "blocked", "reason": "unsupported_saved_index",
                "error": err, "installed_release": cur.sha
            })));
        };
        Some(loc)
    } else {
        None
    };
    let ch = src.channel.as_deref().unwrap_or(&cur.channel);
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
        host::verify_or_restore(&root, home, &host::receipt(&root.join("current"))?)?;
        return Ok(None);
    }
    let eff = src
        .index
        .as_deref()
        .or(migrated_private.as_deref())
        .or_else(|| env_override.is_none().then_some(cur.index.as_str()));
    let key = eff.unwrap_or_else(|| env_override.map_or("", Environment::as_str));
    let rows = lists
        .entry((key.to_owned(), ch.to_owned()))
        .or_insert_with(|| resolutions(home, ch, eff, env_override, src.trust_key.as_deref()))
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

pub(super) fn upgrade_apps(
    home: &Path,
    all: bool,
    accept_install: bool,
    ids: &[String],
    src: &CatalogSource,
) -> Result<(), String> {
    let names = if all {
        installed_app_ids()?
    } else {
        ids.to_vec()
    };
    let polling = std::env::var("LASTDB_APP_REGISTRY_POLL").as_deref() == Ok("1");
    let (mut lists, mut results, mut failed) = (HashMap::new(), Vec::new(), false);
    for app in &names {
        match upgrade_one(home, (accept_install, polling), src, &mut lists, app) {
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

pub(super) fn status_apps(home: &Path, app_id: Option<&str>, check: bool) -> Result<(), String> {
    let names = app_id.map_or_else(installed_app_ids, |app| Ok(vec![app.to_owned()]))?;
    let (mut rows, mut failed) = (Vec::new(), false);
    for app in names {
        let root = host::app_root(&app)?;
        match host::receipt(&root.join("current")) {
            Ok(r) => {
                let probe = host::verify(&root, check.then_some(home), &r).err();
                failed |= probe.is_some();
                let state = match (probe.is_some(), r.inactive) {
                    (true, _) => "red",
                    (false, true) => "inactive",
                    (false, false) => "current",
                };
                let act = std::fs::read_link(root.join("current")).ok();
                let pri = std::fs::read_link(root.join("previous")).ok();
                rows.push(json!({"app": app, "state": state, "installed_release": r.sha, "active_release": act, "prior_release": pri, "probe": probe, "receipt": r}));
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
