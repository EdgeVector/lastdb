//! Local install source: install from a git repository at a specific revision.

use super::*;

// lint:allow-ok extracted from app_catalog_command to satisfy file size limits
#[allow(clippy::too_many_arguments)]
pub(crate) fn app_install_local(
    home: &std::path::Path,
    app_ids: &[String],
    from_path: &str,
    rev: &str,
    dir: &Option<std::path::PathBuf>,
    force: bool,
    json: bool,
    is_dev: bool,
) -> Result<(), String> {
    use lastdb_node::app_registry_index as registry_index;
    use lastdb_node::version;

    check_dir_matches_app_count(dir, app_ids)?;
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
    for app_id in app_ids {
        let location = registry_index::IndexLocation::Local {
            path: canonical_path.clone(),
            rev: rev.to_string(),
        };

        let resolution =
            registry_index::resolve_local(&location, app_id, &node_version, &versions, is_dev)?;

        let install_dir = dir
            .clone()
            .unwrap_or_else(|| home.join("apps").join(app_id));

        let outcome = registry_index::install_pinned(&resolution, &install_dir, force)?;

        if !json {
            println!(
                "installed: {} {} @ {} (local, dev: {}, rev: {})",
                outcome.app_id,
                outcome.app_version,
                short_sha(&outcome.sha),
                is_dev,
                rev
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
