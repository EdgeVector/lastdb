//! Themed module split from the parent: reading the signed public index,
//! and installing and upgrading apps.

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

use lastdb_node::app_registry_index as registry_index;

/// `--dir` names one checkout, so it cannot apply to several apps.
pub(super) fn check_dir_matches_app_count(
    dir: &Option<PathBuf>,
    app_ids: &[String],
) -> Result<(), String> {
    if dir.is_some() && app_ids.len() != 1 {
        return Err("--dir applies to exactly one app".to_string());
    }
    Ok(())
}

pub(super) fn short_sha(sha: &str) -> &str {
    observability::truncate::truncate_on_char_boundary(sha, 12)
}

pub(crate) fn app_list(
    home: &Path,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    let list = public_list(
        home,
        channel.as_deref(),
        index.as_deref(),
        trust_key.as_deref(),
    )?;
    print_public_list(&list, json);
    Ok(())
}

pub(crate) fn app_resolve(home: &Path, query: &AppResolveQuery<'_>) -> Result<(), String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let resolution = resolve_by_proof(
        &socket,
        query.app_id,
        query.channel,
        query.index,
        query.trust_key,
    )?;
    if query.json {
        if let Some(entry) = &resolution.list_entry {
            print_pretty_json(entry);
        } else {
            print_pretty_json(&resolution);
        }
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
    home: &Path,
    app_id: &str,
    channel: Option<String>,
    index: Option<String>,
    trust_key: Option<String>,
) -> Result<(), String> {
    let list = public_list(
        home,
        channel.as_deref(),
        index.as_deref(),
        trust_key.as_deref(),
    )?;
    let entry = list
        .apps
        .iter()
        .find(|entry| entry.app_id == app_id)
        .ok_or_else(|| {
            format!(
                "no app '{app_id}' was proved with lastdb API {}",
                list.api_version
            )
        })?;
    print_pretty_json(entry);
    Ok(())
}

pub(crate) fn app_install(home: &Path, install: AppInstall) -> Result<(), String> {
    if install.from.is_some() || install.rev.is_some() || install.dev {
        return app_install_local(home, &install);
    }

    if install.dir.is_none() {
        return super::managed::install(home, &install);
    }
    let AppInstall {
        accept_install: _,
        inactive: _,
        app_ids,
        source:
            RegistrySource {
                channel,
                index,
                trust_key,
            },
        dir,
        force,
        json,
        ..
    } = install;
    check_dir_matches_app_count(&dir, &app_ids)?;
    if !json {
        eprintln!("Apps run with your user permissions. Registry signatures do not sandbox them.");
    }
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let mut outcomes = Vec::new();
    for app_id in &app_ids {
        let resolution = resolve_by_proof(
            &socket,
            app_id,
            channel.as_deref(),
            index.as_deref(),
            trust_key.as_deref(),
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
    Ok(())
}

pub(crate) fn app_upgrade(home: &Path, upgrade: AppUpgrade) -> Result<(), String> {
    if upgrade.dir.is_none() {
        return super::managed::upgrade(home, &upgrade);
    }
    let AppUpgrade {
        all: _,
        accept_install: _,
        app_ids,
        source:
            RegistrySource {
                channel,
                index,
                trust_key,
            },
        dir,
        json,
    } = upgrade;
    check_dir_matches_app_count(&dir, &app_ids)?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let mut outcomes = Vec::new();
    for app_id in &app_ids {
        let resolution = resolve_by_proof(
            &socket,
            app_id,
            channel.as_deref(),
            index.as_deref(),
            trust_key.as_deref(),
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
    Ok(())
}
