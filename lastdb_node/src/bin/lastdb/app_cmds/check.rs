//! Themed module split from the parent.

use super::*;

/// Declare manifest schemas against the local Mini (owner socket must be up).
/// `CheckMode::ReadOnly` writes nothing; `CheckMode::Sync` can register.
pub(crate) fn app_check(
    home: &Path,
    manifest: &lastdb_node::app_publish::AppManifest,
    mode: lastdb_node::app_publish::CheckMode,
    confirm_expire_existing: bool,
) -> Result<Vec<lastdb_node::app_publish::CheckOutcome>, String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — schema checks run against your local Mini; start the daemon first",
            socket.display()
        ));
    }
    lastdb_node::app_publish::check_schemas_with(&socket, manifest, mode, confirm_expire_existing)
}

pub(crate) fn ensure_app_schemas_catalog_ready(
    home: &Path,
    _schema_service: &str,
    manifest: &lastdb_node::app_publish::AppManifest,
    _locked: &HashMap<String, String>,
) -> Result<(), String> {
    // THE PROMOTE GATE: Mini must return an audited, bind-eligible catalog
    // identity for every schema. No direct catalog fallback can supply the
    // missing bind proof.
    let outcomes = app_check(
        home,
        manifest,
        lastdb_node::app_publish::CheckMode::Sync,
        false,
    )?;
    // A promote applies a manifest retention. Say what it did.
    print_retention_changes(&outcomes);
    let novel = lastdb_node::app_publish::novel_schema_names(&outcomes);
    if !novel.is_empty() {
        return Err(format!(
            "promote rejected: schemas lack Mini bind proof ({}) — run `lastdb app check --sync` after the reported dependency or quota recovers",
            novel.join(", ")
        ));
    }
    Ok(())
}

/// One JSON row per schema: how its coverage resolves and the detail to show.
fn check_outcome_rows(
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Vec<serde_json::Value> {
    use lastdb_node::app_publish::SchemaCoverage;
    outcomes
        .iter()
        .map(|o| {
            let (resolution, detail) = match &o.coverage {
                SchemaCoverage::Reuse { identity_hash } => ("reuse", identity_hash.clone()),
                SchemaCoverage::Compose { components } => ("compose", components.join(",")),
                SchemaCoverage::Register {
                    identity_hash,
                    expanded,
                    ..
                } => (
                    if *expanded { "expand" } else { "register" },
                    identity_hash.clone(),
                ),
                SchemaCoverage::WouldRegister {
                    identity_hash,
                    expanded,
                    ..
                } => (
                    if *expanded {
                        "would-expand"
                    } else {
                        "would-register"
                    },
                    identity_hash.clone(),
                ),
                SchemaCoverage::Novel { reason } => ("novel", reason.clone()),
            };
            let mut row = serde_json::json!({
                "schema": o.schema_name,
                "resolution": resolution,
                "detail": detail,
                "covered": o.coverage.is_covered(),
            });
            if let Some(retention) = &o.retention {
                row["retention"] = retention.clone();
            }
            row
        })
        .collect()
}

fn print_check_outcomes_json(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "app_id": manifest.app_id,
            "schemas": check_outcome_rows(outcomes),
            "catalog_collisions": lastdb_node::app_publish::catalog_collisions(outcomes)
                .into_iter()
                .map(|(catalog, schemas)| serde_json::json!({ "catalog": catalog, "schemas": schemas }))
                .collect::<Vec<_>>(),
        }))
        .unwrap_or_default()
    );
}

fn print_check_outcomes_text(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) {
    use lastdb_node::app_publish::SchemaCoverage;
    println!("app: {}", manifest.app_id);
    for o in outcomes {
        match &o.coverage {
            SchemaCoverage::Reuse { identity_hash } => {
                println!("  {}\treuse\t{identity_hash}", o.schema_name);
            }
            SchemaCoverage::Compose { components } => println!(
                "  {}\tcompose\t{} component(s)",
                o.schema_name,
                components.len()
            ),
            SchemaCoverage::Register {
                identity_hash,
                audit_event_id,
                expanded,
            } => println!(
                "  {}\t{}\t{} audit={}",
                o.schema_name,
                if *expanded { "expand" } else { "register" },
                identity_hash,
                audit_event_id
            ),
            SchemaCoverage::WouldRegister {
                identity_hash,
                expanded,
                reason,
            } => println!(
                "  {}\t{}\t{} (read-only check; nothing written{})",
                o.schema_name,
                if *expanded {
                    "would-expand"
                } else {
                    "would-register"
                },
                identity_hash,
                reason
                    .as_deref()
                    .map(|r| format!("; reason={r}"))
                    .unwrap_or_default()
            ),
            SchemaCoverage::Novel { reason } => println!("  {}\tNOVEL\t{reason}", o.schema_name),
        }
        if let Some(retention) = &o.retention {
            print_retention_text(retention);
        }
    }
    for (catalog, names) in lastdb_node::app_publish::catalog_collisions(outcomes) {
        println!(
            "WARNING: [{}] resolve to one catalog schema ({catalog}). They share one dataset: a record written through one name is a record of the others.",
            names.join(", ")
        );
    }
}

pub(crate) fn print_check_outcomes(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
    json: bool,
) {
    if json {
        print_check_outcomes_json(manifest, outcomes);
    } else {
        print_check_outcomes_text(manifest, outcomes);
    }
}

/// The one app id a legacy service-path command accepts.
pub(crate) fn single_app(app_ids: &[String]) -> Result<String, String> {
    match app_ids {
        [one] => Ok(one.clone()),
        _ => Err("the --env / --schema-url service path takes exactly one app id".to_string()),
    }
}

/// Say so when the trusted index key is not the pinned release key. A test
/// index is fine; a silent override is not.
pub(crate) fn warn_trust_override(source: &lastdb_node::app_registry_index::TrustSource) {
    use lastdb_node::app_registry_index::TrustSource;
    match source {
        TrustSource::Pinned => {}
        TrustSource::Flag => eprintln!("note: index trust key overridden by --trust-key"),
        TrustSource::Env => eprintln!(
            "note: index trust key overridden by ${}",
            lastdb_node::app_registry_index::TRUST_KEY_ENV
        ),
    }
}

/// Fetch the signed index and pick the row proved with the running node.
pub(crate) fn resolve_by_proof(
    socket: &Path,
    app_id: &str,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
    lastdb_version: Option<&str>,
) -> Result<lastdb_node::app_registry_index::Resolution, String> {
    use lastdb_node::app_registry_index as registry_index;
    use lastdb_node::version;
    let channel = channel.unwrap_or(registry_index::DEFAULT_CHANNEL);
    let location = registry_index::IndexLocation::resolve(index);
    let (trust, trust_source) = registry_index::resolve_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    let (node, node_source) = registry_index::running_lastdb_version(lastdb_version, socket);
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
    block_on_app(registry_index::resolve(
        &location,
        channel,
        &trust,
        &trust_source,
        app_id,
        &node_version,
        &versions,
    ))
}

/// `lastdb app check`: report what a catalog sync would do, and fail when a
/// manifest schema would not come back bind-eligible.
pub(crate) fn app_check_command(
    home: &Path,
    manifest_path: &Path,
    json: bool,
    sync: bool,
    confirm_expire_existing: bool,
) -> Result<(), String> {
    use lastdb_node::app_publish;
    let manifest = app_publish::load_manifest(manifest_path)?;
    let mode = if sync {
        app_publish::CheckMode::Sync
    } else {
        app_publish::CheckMode::ReadOnly
    };
    let outcomes = app_check(home, &manifest, mode, confirm_expire_existing)?;
    let node_novel = app_publish::novel_schema_names(&outcomes);
    print_check_outcomes(&manifest, &outcomes, json);
    for schema in &manifest.schemas {
        let name = schema
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("<unnamed>");
        if node_novel.iter().any(|n| n == name) {
            let missing = app_publish::missing_field_descriptions(schema);
            if !missing.is_empty() {
                eprintln!(
                    "note: '{name}' is novel and will need field_descriptions to register — missing: {}",
                    missing.join(", ")
                );
            }
        }
    }
    if node_novel.is_empty() {
        collision_error(&manifest, &outcomes).map_or(Ok(()), Err)
    } else if let Some(refused) = retention_refusals(&outcomes) {
        Err(format!(
            "the node did not apply the manifest retention for: {refused}. No retention policy was changed for those schemas. Fix the cause named above (docs/app-manifest-retention.md), then run `lastdb app check` again: a declare is idempotent."
        ))
    } else if sync {
        Err(format!(
            "Mini did not return bind-eligible catalog identities for: {}. Retry `lastdb app check --sync` after the reported dependency or quota recovers; do not call Schema Service directly.",
            node_novel.join(", ")
        ))
    } else {
        Err(format!(
            "a catalog sync would fail for: {}. Fix the reported error, then re-run `lastdb app check` (read-only) before `lastdb app check --sync`.",
            node_novel.join(", ")
        ))
    }
}

/// `lastdb app register-schemas`: a compatibility alias for `check --sync`.
pub(crate) fn app_register_schemas(home: &Path, manifest_path: &Path) -> Result<(), String> {
    use lastdb_node::app_publish;
    let manifest = app_publish::load_manifest(manifest_path)?;
    let outcomes = app_check(home, &manifest, app_publish::CheckMode::Sync, false)?;
    print_retention_changes(&outcomes);
    let novel = app_publish::novel_schema_names(&outcomes);
    if novel.is_empty() {
        println!(
            "all {} manifest schemas synchronized through Mini — nothing to register",
            outcomes.len()
        );
        return Ok(());
    }
    Err(format!(
        "Mini did not return bind-eligible catalog identities for: {}. Retry this command after the reported dependency or quota recovers; do not call Schema Service directly.",
        novel.join(", ")
    ))
}
