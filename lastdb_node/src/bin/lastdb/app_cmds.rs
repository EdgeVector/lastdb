use super::*;
use crate::app_registry_index::{self as registry, managed as host};
use lastdb_node::profile::endpoints::Environment;
use std::path::{Path, PathBuf};

#[path = "app_cmds/interface_publish.rs"]
mod interface_publish;
#[path = "app_cmds/lifecycle.rs"]
mod lifecycle;
#[path = "app_cmds/owner_app.rs"]
mod owner_app;
#[path = "app_cmds/publish_credentials.rs"]
mod publish_credentials;

/// The apps the running node can install, with where and how they were read.
struct Catalog {
    list: registry::InterfaceList,
    location: registry::IndexLocation,
    trust: String,
    /// Build and where it was read from.
    node: (String, &'static str),
    /// The node's minimum supported and current app API versions.
    range: (u32, u32),
}

fn parse_catalog_env(raw: Option<&str>) -> Result<Option<Environment>, String> {
    raw.map(|v| Environment::try_from_str(v).map_err(|_| "--env must be dev or prod".to_owned()))
        .transpose()
}

fn pin_app_env(home: &Path, env_flag: Option<&str>) -> Result<(), String> {
    if let Some(env) = parse_catalog_env(env_flag)? {
        lastdb_node::profile::endpoints::set_home_environment(env);
    }
    lastdb_node::cloud::pin_schema_service_environment_from_home(home);
    Ok(())
}

fn load_catalog(
    home: &Path,
    ch: Option<&str>,
    index: Option<&str>,
    env_override: Option<Environment>,
    trust_key: Option<&str>,
) -> Result<Catalog, String> {
    let ch = ch.unwrap_or(registry::DEFAULT_CHANNEL);
    let loc = registry::IndexLocation::resolve(index, env_override)?;
    let (vk, trust) = registry::resolve_list_trust_key(trust_key, &loc, env_override)?;
    let (build, node_src, api, outbox, min_api) = registry::running_node(home)?;
    let range = (min_api, api);
    let mut list = crate::block_on(
        "app catalog",
        registry::fetch_supported(&loc, ch, range, &vk),
    )?;
    list.apps
        .retain(|e| e.outbox_format_version.is_none_or(|v| v == outbox));
    Ok(Catalog {
        list,
        location: loc,
        trust,
        node: (build, node_src),
        range,
    })
}

fn saved_private_locators(
    home: &Path,
    env_override: Option<Environment>,
) -> Result<Vec<String>, String> {
    let env = env_override.unwrap_or_else(Environment::from_env_or_prod);
    let preferred = lastdb_node::cloud::load_verified_cloud_credentials(home)
        .ok()
        .flatten()
        .and_then(|c| c.expected_account_id);
    registry::private_lists::saved_locators_for_env(env, preferred.as_deref())
}

fn missing_app_error(app: &str, (min_api, api): (u32, u32), explicit_index: bool) -> String {
    if explicit_index {
        return format!("no app '{app}' was proved with lastdb app API {min_api} to {api}");
    }
    format!(
        "no app '{app}' was proved with lastdb app API {min_api} to {api}; if '{app}' is a private app you own, run 'lastdb app show {app} --save' first or pass --index"
    )
}

pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::Run {
            app_id,
            command,
            args,
        } => Err(crate::app_run::run(&home, &app_id, &command, args)),
        AppCommand::Rollback { app_id, .. } => {
            print_pretty_json(&host::rollback(&app_id, &home)?);
            Ok(())
        }
        AppCommand::Status { app_id, check, .. } => {
            lastdb_node::cloud::pin_schema_service_environment_from_home(&home);
            lifecycle::status_apps(&home, app_id.as_deref(), check)
        }
        AppCommand::Publish(args) => interface_publish::publish_interface(&home, &args),
        AppCommand::Show(args) => owner_app::show_owned_app(&home, &args),
        AppCommand::List {
            app_id,
            catalog_entry,
            source,
            json,
        } => {
            pin_app_env(&home, source.env.as_deref())?;
            list_apps(&home, app_id.as_deref(), catalog_entry, json, &source)
        }
        AppCommand::Install {
            app_ids,
            source,
            accept_install,
            inactive,
            ..
        } => {
            pin_app_env(&home, source.env.as_deref())?;
            install_apps(&home, accept_install, inactive, &app_ids, &source)
        }
        AppCommand::Upgrade {
            app_ids,
            all,
            accept_install,
            source,
            ..
        } => {
            pin_app_env(&home, source.env.as_deref())?;
            lifecycle::upgrade_apps(&home, all, accept_install, &app_ids, &source)
        }
        AppCommand::Remove {
            app_id,
            delete_data,
            ..
        } => {
            print_pretty_json(&registry::remove::remove(&app_id, &home, delete_data)?);
            Ok(())
        }
    }
}

fn find_catalog_for_app(
    home: &Path,
    app_id: &str,
    src: &CatalogSource,
    env_override: Option<Environment>,
) -> Result<Catalog, String> {
    let explicit = registry::IndexLocation::has_explicit_source(src.index.as_deref());
    let primary = load_catalog(
        home,
        src.channel.as_deref(),
        src.index.as_deref(),
        env_override,
        src.trust_key.as_deref(),
    );
    if explicit {
        return primary;
    }
    let mut range_hint = None;
    match primary {
        Ok(cat) => {
            if cat.list.apps.iter().any(|e| e.app_id == app_id) {
                return Ok(cat);
            }
            range_hint = Some(cat.range);
        }
        Err(e) if e.starts_with(registry::NO_LIST_PREFIX) => {}
        Err(e) => return Err(e),
    }
    for loc in saved_private_locators(home, env_override)? {
        match load_catalog(
            home,
            src.channel.as_deref(),
            Some(&loc),
            env_override,
            src.trust_key.as_deref(),
        ) {
            Ok(cat) if cat.list.apps.iter().any(|e| e.app_id == app_id) => return Ok(cat),
            Ok(cat) => range_hint = Some(cat.range),
            Err(e) if e.starts_with(registry::NO_LIST_PREFIX) => {}
            Err(e) => return Err(e),
        }
    }
    let range = if let Some(r) = range_hint {
        r
    } else {
        let (_, _, api, _, min_api) = registry::running_node(home)?;
        (min_api, api)
    };
    Err(missing_app_error(app_id, range, false))
}

fn list_apps(
    home: &Path,
    app_id: Option<&str>,
    catalog_entry: bool,
    json: bool,
    src: &CatalogSource,
) -> Result<(), String> {
    let env_override = parse_catalog_env(src.env.as_deref())?;
    let Some(app_id) = app_id else {
        let Catalog {
            list,
            range: (min_api, api),
            ..
        } = load_catalog(
            home,
            src.channel.as_deref(),
            src.index.as_deref(),
            env_override,
            src.trust_key.as_deref(),
        )?;
        if json {
            print_pretty_json(&list);
        } else {
            println!(
                "# {} app API {min_api} to {api} · newest list app API {} sequence {} · generated {}",
                list.channel, list.api_version, list.seq, list.generated_at
            );
            for e in &list.apps {
                println!("{}\t{}\t{}", e.app_id, e.app_version, e.sha);
            }
        }
        return Ok(());
    };
    let explicit = registry::IndexLocation::has_explicit_source(src.index.as_deref());
    let Catalog {
        list,
        location,
        trust,
        node: (nb, ns),
        range,
    } = find_catalog_for_app(home, app_id, src, env_override)?;
    let e = list
        .apps
        .iter()
        .find(|e| e.app_id == app_id)
        .ok_or_else(|| missing_app_error(app_id, range, explicit))?;
    let index = location.describe_interface(&e.channel, e.api_version);
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

pub(super) fn resolutions(
    home: &Path,
    channel: &str,
    index: Option<&str>,
    env_override: Option<Environment>,
    trust_key: Option<&str>,
) -> Result<Vec<registry::Resolution>, String> {
    if trust_key.is_some() || std::env::var_os(registry::TRUST_KEY_ENV).is_some() {
        return Err("managed installs require the pinned registry key".into());
    }
    let Catalog {
        list,
        location,
        trust,
        ..
    } = load_catalog(home, Some(channel), index, env_override, None)?;
    let raw_base = location.raw_url_base().map(str::to_owned);
    Ok(list
        .apps
        .into_iter()
        .map(|entry| registry::Resolution {
            index: location.describe_interface(&entry.channel, entry.api_version),
            raw_base: raw_base.clone(),
            entry,
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
    let env_override = parse_catalog_env(src.env.as_deref())?;
    let ch = src.channel.as_deref().unwrap_or(registry::DEFAULT_CHANNEL);
    let explicit = registry::IndexLocation::has_explicit_source(src.index.as_deref());
    let (_, _, api, _, min_api) = registry::running_node(home)?;
    let range = (min_api, api);
    let mut rows = match resolutions(
        home,
        ch,
        src.index.as_deref(),
        env_override,
        src.trust_key.as_deref(),
    ) {
        Ok(rows) => rows,
        Err(e) if !explicit && e.starts_with(registry::NO_LIST_PREFIX) => Vec::new(),
        Err(e) => return Err(e),
    };
    if !explicit
        && app_ids
            .iter()
            .any(|app| !rows.iter().any(|r| &r.entry.app_id == app))
    {
        for loc in saved_private_locators(home, env_override)? {
            match resolutions(home, ch, Some(&loc), env_override, src.trust_key.as_deref()) {
                Ok(priv_rows) => {
                    for row in priv_rows {
                        if !rows.iter().any(|r| r.entry.app_id == row.entry.app_id) {
                            rows.push(row);
                        }
                    }
                }
                Err(e) if e.starts_with(registry::NO_LIST_PREFIX) => {}
                Err(e) => return Err(e),
            }
            if app_ids
                .iter()
                .all(|app| rows.iter().any(|r| &r.entry.app_id == app))
            {
                break;
            }
        }
    }
    let mut results = Vec::with_capacity(app_ids.len());
    for app in app_ids {
        let row = rows
            .iter()
            .find(|r| &r.entry.app_id == app)
            .ok_or_else(|| missing_app_error(app, range, explicit))?;
        results.push(host::install(row, home, accept_install, inactive)?);
    }
    print_pretty_json(&results);
    host::install_poll()
}
