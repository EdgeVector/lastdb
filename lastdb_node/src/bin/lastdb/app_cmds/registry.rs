//! Themed module split from the parent: reading the signed public index or
//! the live registry service, and installing, upgrading and running apps.

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

use lastdb_node::app_publish;
use lastdb_node::app_registry_index as registry_index;

/// Which registry a command reads. `--env` / `--schema-url` select the live
/// registry service; without either, the signed public index is the source.
fn registry_service_url(
    env: Option<&str>,
    schema_url: Option<String>,
) -> Result<Option<String>, String> {
    if env.is_some() || schema_url.is_some() {
        resolve_schema_service_url(schema_url, env).map(Some)
    } else {
        Ok(None)
    }
}

/// Fetch and verify the signed public index for `channel` (default channel
/// when unset). Returns the channel actually used and where it came from.
fn fetch_signed_index(
    channel: Option<String>,
    index: Option<&str>,
    trust_key: Option<&str>,
) -> Result<
    (
        String,
        registry_index::IndexLocation,
        registry_index::RegistryIndex,
    ),
    String,
> {
    let channel = channel.unwrap_or_else(|| registry_index::DEFAULT_CHANNEL.to_string());
    let location = registry_index::IndexLocation::resolve(index);
    let (trust, trust_source) = registry_index::resolve_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    let index = block_on_app(registry_index::fetch_verified(&location, &channel, &trust))?;
    Ok((channel, location, index))
}

/// `--dir` names one checkout, so it cannot apply to several apps.
fn check_dir_matches_app_count(dir: &Option<PathBuf>, app_ids: &[String]) -> Result<(), String> {
    if dir.is_some() && app_ids.len() != 1 {
        return Err("--dir applies to exactly one app".to_string());
    }
    Ok(())
}

fn short_sha(sha: &str) -> &str {
    &sha[..sha.len().min(12)]
}

pub(crate) fn app_list(
    home: &Path,
    env: Option<String>,
    schema_url: Option<String>,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    if let Some(url) = registry_service_url(env.as_deref(), schema_url)? {
        let apps = block_on_app(app_publish::list_apps(&url))?;
        if json {
            print_pretty_json(&apps);
        } else if apps.is_empty() {
            println!("registry has no published apps");
        } else {
            for app in &apps {
                println!(
                    "{}\t{}\t{:?}\t{}\t{}",
                    app.app_id,
                    app.version,
                    app.tier,
                    app.metadata.display_name,
                    app.metadata.description
                );
            }
        }
        return Ok(());
    }
    let (channel, location, index) =
        fetch_signed_index(channel, index.as_deref(), trust_key.as_deref())?;
    if json {
        print_pretty_json(&index);
    } else if index.apps.is_empty() {
        println!(
            "{} index at {} has no apps (generated {})",
            index.channel,
            location.describe(&channel),
            index.generated_at
        );
    } else {
        let socket = lastdb_uds::uds::socket_path(&home.join("data"));
        let (node, _) = registry_index::running_lastdb_version(None, &socket);
        println!(
            "# {} index · {} · node {node}",
            index.channel,
            location.describe(&channel)
        );
        for app in &index.apps {
            let proved_here = registry_index::pick_row(&index, &app.app_id, &node).map_or_else(
                |_| "no row for this node".to_string(),
                |(_, row)| format!("{} @ {}", row.app_version, short_sha(&row.sha)),
            );
            println!(
                "{}\t{}\t{} rows\t{}",
                app.app_id,
                proved_here,
                app.compat.len(),
                app.description.clone().unwrap_or_default()
            );
        }
    }
    Ok(())
}

pub(crate) fn app_resolve(
    home: &Path,
    app_id: &str,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
    lastdb_version: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let resolution = resolve_by_proof(&socket, app_id, channel, index, trust_key, lastdb_version)?;
    if json {
        print_pretty_json(&resolution);
    } else {
        println!(
            "{} {} @ {} proved with lastdb {} ({}) by {} at {}",
            resolution.app_id,
            resolution.app_version,
            resolution.sha,
            resolution.lastdb_version,
            resolution.lastdb_version_source,
            resolution.proof_run,
            resolution.proved_at
        );
        println!("  source: {}", resolution.source);
        println!(
            "  index: {} (trust: {})",
            resolution.index, resolution.trust
        );
    }
    Ok(())
}

pub(crate) fn app_info(
    app_id: &str,
    env: Option<String>,
    schema_url: Option<String>,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
) -> Result<(), String> {
    if let Some(url) = registry_service_url(env.as_deref(), schema_url)? {
        let record = block_on_app(app_publish::app_info(&url, app_id))?;
        print_pretty_json(&record);
        return Ok(());
    }
    let (channel, _location, index) =
        fetch_signed_index(channel, index.as_deref(), trust_key.as_deref())?;
    let app = index
        .apps
        .iter()
        .find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' is not in the {channel} index"))?;
    print_pretty_json(app);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_install(
    home: &Path,
    app_ids: Vec<String>,
    env: Option<String>,
    schema_url: Option<String>,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
    lastdb_version: Option<String>,
    dir: Option<PathBuf>,
    allow_sandbox: bool,
    force: bool,
    json: bool,
) -> Result<(), String> {
    check_dir_matches_app_count(&dir, &app_ids)?;
    if env.is_none() && schema_url.is_none() {
        let socket = lastdb_uds::uds::socket_path(&home.join("data"));
        let mut outcomes = Vec::new();
        for app_id in &app_ids {
            let resolution = resolve_by_proof(
                &socket,
                app_id,
                channel.as_deref(),
                index.as_deref(),
                trust_key.as_deref(),
                lastdb_version.as_deref(),
            )?;
            let install_dir = dir
                .clone()
                .unwrap_or_else(|| home.join("apps").join(app_id));
            let outcome = registry_index::install_pinned(&resolution, &install_dir, force)?;
            if !json {
                println!(
                    "installed: {} {} @ {} (proved with lastdb {} by {})",
                    outcome.app_id,
                    outcome.app_version,
                    short_sha(&outcome.sha),
                    outcome.lastdb_version,
                    outcome.proof_run
                );
                println!("  source: {}", outcome.source);
                println!("  checkout: {}", outcome.checkout_path);
            }
            outcomes.push(outcome);
        }
        if json {
            print_pretty_json(&outcomes);
        }
        return Ok(());
    }
    let app_id = single_app(&app_ids)?;
    let url = resolve_schema_service_url(schema_url, env.as_deref())?;
    let install_dir = dir.unwrap_or_else(|| home.join("apps").join(&app_id));
    let outcome = block_on_app(app_publish::install_app(
        &url,
        &app_id,
        &install_dir,
        allow_sandbox,
        force,
    ))?;
    if json {
        print_pretty_json(&outcome);
    } else {
        println!("installed: {}", outcome.app_id);
        println!("  version: {}", outcome.version);
        println!("  source: {}", outcome.source);
        println!("  checkout: {}", outcome.checkout_path);
        println!("  install: {}", outcome.install_dir);
        println!("  tier: {}", outcome.tier);
        println!("  publisher: {}", outcome.owner_dev_pubkey);
        if outcome.code_signature_declared {
            println!("  code_signature: declared");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_upgrade(
    home: &Path,
    app_ids: Vec<String>,
    env: Option<String>,
    schema_url: Option<String>,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
    lastdb_version: Option<String>,
    dir: Option<PathBuf>,
    allow_sandbox: bool,
    json: bool,
) -> Result<(), String> {
    check_dir_matches_app_count(&dir, &app_ids)?;
    if env.is_none() && schema_url.is_none() {
        let socket = lastdb_uds::uds::socket_path(&home.join("data"));
        let mut outcomes = Vec::new();
        for app_id in &app_ids {
            let resolution = resolve_by_proof(
                &socket,
                app_id,
                channel.as_deref(),
                index.as_deref(),
                trust_key.as_deref(),
                lastdb_version.as_deref(),
            )?;
            let install_dir = dir
                .clone()
                .unwrap_or_else(|| home.join("apps").join(app_id));
            let outcome = registry_index::upgrade_pinned(&resolution, &install_dir)?;
            if !json {
                if outcome.upgraded {
                    println!(
                        "upgraded: {} {} @ {} -> {} @ {}",
                        outcome.app_id,
                        outcome.installed_version,
                        short_sha(&outcome.installed_sha),
                        outcome.resolved_version,
                        short_sha(&outcome.resolved_sha)
                    );
                } else {
                    println!(
                        "already current: {} {} @ {}",
                        outcome.app_id,
                        outcome.installed_version,
                        short_sha(&outcome.installed_sha)
                    );
                }
            }
            outcomes.push(outcome);
        }
        if json {
            print_pretty_json(&outcomes);
        }
        return Ok(());
    }
    let app_id = single_app(&app_ids)?;
    let url = resolve_schema_service_url(schema_url, env.as_deref())?;
    let install_dir = dir.unwrap_or_else(|| home.join("apps").join(&app_id));
    let outcome = block_on_app(app_publish::upgrade_app(
        &url,
        &app_id,
        &install_dir,
        allow_sandbox,
    ))?;
    if json {
        print_pretty_json(&outcome);
    } else if outcome.upgraded {
        println!(
            "upgraded: {} {} -> {}",
            outcome.app_id, outcome.installed_version, outcome.registry_version
        );
    } else {
        println!(
            "already current: {} {}",
            outcome.app_id, outcome.installed_version
        );
    }
    Ok(())
}

pub(crate) fn app_run(
    home: &Path,
    app_id: String,
    dir: Option<PathBuf>,
    args: Vec<String>,
) -> Result<(), String> {
    let install_dir = dir.unwrap_or_else(|| home.join("apps").join(&app_id));
    let outcome = app_publish::run_installed_app(home, &app_id, &install_dir, &args)?;
    println!(
        "app run completed: {} (runtime={}, entrypoint={}, exit={})",
        outcome.app_id, outcome.runtime, outcome.entrypoint, outcome.exit_code
    );
    Ok(())
}
