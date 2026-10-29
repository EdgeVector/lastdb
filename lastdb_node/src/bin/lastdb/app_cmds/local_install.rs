//! Local install source: install from a git repository at a specific revision.

use super::*;

pub(crate) fn app_install_local(
    home: &std::path::Path,
    install: &AppInstall,
) -> Result<(), String> {
    use lastdb_node::app_registry_index as registry_index;
    use lastdb_node::version;

    let Some(from_path) = install.from.as_deref() else {
        if install.rev.is_some() {
            return Err("--rev requires --from".to_string());
        }
        return Err("--dev requires --from".to_string());
    };
    if !install.dev {
        return Err("--from requires --dev".to_string());
    }
    let Some(rev) = install.rev.as_deref() else {
        return Err("--from requires --rev".to_string());
    };

    check_dir_matches_app_count(&install.dir, &install.app_ids)?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let (node, node_source) = registry_index::running_lastdb_version(None, &socket);
    let versions = registry_index::ContractVersions {
        api_version: version::CURRENT_API_VERSION,
        min_supported_api_version: version::MIN_SUPPORTED_API_VERSION,
        outbox_format_version: version::CURRENT_OUTBOX_FORMAT_VERSION,
        min_supported_outbox_format_version: version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    };
    let node_version = registry_index::NodeVersion {
        version: &node,
        source: node_source,
    };

    let path = std::path::PathBuf::from(from_path);
    let canonical_path = path
        .canonicalize()
        .map_err(|e| format!("failed to resolve --from path '{}': {e}", path.display()))?;

    let mut outcomes = Vec::new();
    for app_id in &install.app_ids {
        let location = registry_index::IndexLocation::Local {
            path: canonical_path.clone(),
            rev: rev.to_string(),
        };

        let resolution = registry_index::resolve_local(
            &location,
            app_id,
            &node_version,
            &versions,
            install.dev,
        )?;

        let install_dir = install
            .dir
            .clone()
            .unwrap_or_else(|| home.join("apps").join(app_id));

        let outcome = registry_index::install_pinned(&resolution, &install_dir, install.force)?;

        if !install.json {
            println!(
                "installed: {} {} @ {} (local, dev: {}, rev: {})",
                outcome.app_id,
                outcome.app_version,
                short_sha(&outcome.sha),
                install.dev,
                rev
            );
            println!("  source: {}", outcome.source);
            println!("  checkout: {}", outcome.checkout_path);
        }
        outcomes.push(outcome);
    }

    if install.json {
        print_pretty_json(&outcomes);
    }

    Ok(())
}
