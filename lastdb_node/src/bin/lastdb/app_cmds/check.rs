//! Themed module split from the parent.

use super::*;

/// Declare manifest schemas against the local Mini (owner socket must be up).
/// `CheckMode::ReadOnly` writes nothing; `CheckMode::Sync` can register.
pub(crate) fn app_check(
    home: &Path,
    manifest: &lastdb_node::app_publish::AppManifest,
    mode: lastdb_node::app_publish::CheckMode,
) -> Result<Vec<lastdb_node::app_publish::CheckOutcome>, String> {
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — schema checks run against your local Mini; start the daemon first",
            socket.display()
        ));
    }
    lastdb_node::app_publish::check_schemas(&socket, manifest, mode)
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
    let outcomes = app_check(home, manifest, lastdb_node::app_publish::CheckMode::Sync)?;
    let novel = lastdb_node::app_publish::novel_schema_names(&outcomes);
    if !novel.is_empty() {
        return Err(format!(
            "promote rejected: schemas lack Mini bind proof ({}) — run `lastdb app check --sync` after the reported dependency or quota recovers",
            novel.join(", ")
        ));
    }
    Ok(())
}

pub(crate) fn print_check_outcomes(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
    json: bool,
) {
    use lastdb_node::app_publish::SchemaCoverage;
    if json {
        let rows: Vec<serde_json::Value> = outcomes
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
                serde_json::json!({
                    "schema": o.schema_name,
                    "resolution": resolution,
                    "detail": detail,
                    "covered": o.coverage.is_covered(),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "app_id": manifest.app_id,
                "schemas": rows,
            }))
            .unwrap_or_default()
        );
        return;
    }
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
    let channel = channel.unwrap_or(registry_index::DEFAULT_CHANNEL);
    let location = registry_index::IndexLocation::resolve(index);
    let (trust, trust_source) = registry_index::resolve_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    let (node, node_source) = registry_index::running_lastdb_version(lastdb_version, socket);
    block_on_app(registry_index::resolve(
        &location,
        channel,
        &trust,
        &trust_source,
        app_id,
        &node,
        node_source,
    ))
}

/// `lastdb app check`: report what a catalog sync would do, and fail when a
/// manifest schema would not come back bind-eligible.
pub(crate) fn app_check_command(
    home: &Path,
    manifest_path: &Path,
    json: bool,
    sync: bool,
) -> Result<(), String> {
    use lastdb_node::app_publish;
    let manifest = app_publish::load_manifest(manifest_path)?;
    let mode = if sync {
        app_publish::CheckMode::Sync
    } else {
        app_publish::CheckMode::ReadOnly
    };
    let outcomes = app_check(home, &manifest, mode)?;
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
        Ok(())
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
    let outcomes = app_check(home, &manifest, app_publish::CheckMode::Sync)?;
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
