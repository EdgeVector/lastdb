//! Owner-socket route that inspects and repairs one schema's physical field-to-molecule map.

use super::*;

/// Highest number of distinct field molecules one inspect call probes.
pub(in crate::exec) const MOLECULE_MAP_PROBE_LIMIT: usize = 64;

/// Highest number of candidate maps one inspect call reports.
pub(in crate::exec) const MOLECULE_MAP_CANDIDATE_LIMIT: usize = 8;

/// Bounded read that answers whether one molecule still holds a live row.
pub(in crate::exec) async fn molecule_has_live_rows(
    host: &Host,
    molecule: &str,
) -> Result<bool, String> {
    host.db
        .db_ops()
        .atoms()
        .list_live_record_keys(molecule, 1, None, None)
        .await
        .map(|(keys, _, _)| !keys.is_empty())
        .map_err(|error| error.to_string())
}

/// `POST /api/db/repair-schema-molecule-map` — owner-only, compare-and-set
/// repair of one schema's physical field map. The default is a dry run.
///
/// `field_molecule_uuids` is optional. When it is absent the route runs in
/// inspect mode: it reads the installed map, probes every field molecule for
/// live rows, and reports candidate maps taken from other installed schemas
/// that declare the same fields. An operator needs that report to write a
/// repair map at all, because the repair itself demands one molecule UUID per
/// runtime field.
pub(in crate::exec) async fn execute_db_repair_schema_molecule_map_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // lint:fn-size-ok moved verbatim from exec/db_maintenance.rs; one linear parse/inspect/execute flow
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        #[serde(default)]
        field_molecule_uuids: Option<HashMap<String, String>>,
        #[serde(default)]
        execute: bool,
        #[serde(default)]
        expected_current_fingerprint: Option<String>,
    }
    if !ctx.is_owner {
        return error_response(403, "schema molecule-map repair is owner-only", ctx);
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid /api/db/repair-schema-molecule-map body: {error}"),
                ctx,
            )
        }
    };
    if body.schema.trim().is_empty() {
        return error_response(400, "schema is required", ctx);
    }
    let requested_map = match body.field_molecule_uuids.as_ref() {
        Some(map) if map.is_empty() => None,
        other => other,
    };
    if body.execute && requested_map.is_none() {
        return error_response(
            400,
            "field_molecule_uuids is required with execute=true",
            ctx,
        );
    }
    if let Some((field, _)) = requested_map.and_then(|map| {
        map.iter()
            .find(|(_, molecule)| molecule.trim().is_empty() || molecule.contains('\0'))
    }) {
        return error_response(
            400,
            &format!("field '{field}' has an invalid molecule UUID"),
            ctx,
        );
    }

    let canonical = match handlers::resolve_schema_name(host, &body.schema) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => return error_response(404, "schema not found", ctx),
        Err(error) => return render(Err(error), ctx),
    };
    let manager = host.db.schema_manager();
    let Some(mut schema) = (match manager.get_schema_metadata(&canonical) {
        Ok(schema) => schema,
        Err(error) => return error_response(500, &format!("schema lookup failed: {error}"), ctx),
    }) else {
        return error_response(404, "schema not found", ctx);
    };
    if schema.runtime_fields.is_empty() {
        if let Err(error) = schema.populate_runtime_fields() {
            return error_response(500, &format!("schema field load failed: {error}"), ctx);
        }
    }
    let mut declared: Vec<_> = schema.runtime_fields.keys().cloned().collect();
    declared.sort();
    if let Some(map) = requested_map {
        let mut proposed: Vec<_> = map.keys().cloned().collect();
        proposed.sort();
        if proposed != declared {
            return error_response(
                400,
                "field_molecule_uuids must contain every runtime field and no unknown fields",
                ctx,
            );
        }
    }

    let current_map = schema.field_molecule_uuids.clone().unwrap_or_default();
    let current_fingerprint =
        fold_db::schema::SchemaCore::field_molecule_map_fingerprint(&current_map);
    let proposed_fingerprint =
        requested_map.map(fold_db::schema::SchemaCore::field_molecule_map_fingerprint);
    let mut changed_fields: Vec<String> = match requested_map {
        Some(map) => declared
            .iter()
            .filter(|field| current_map.get(*field) != map.get(*field))
            .cloned()
            .collect(),
        None => Vec::new(),
    };
    changed_fields.sort();

    let key_field = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
        .cloned();
    let key_molecule_of = |map: &HashMap<String, String>| {
        key_field.as_ref().and_then(|field| map.get(field)).cloned()
    };
    let current_key_molecule = key_molecule_of(&current_map);
    let proposed_key_molecule = requested_map.and_then(key_molecule_of);

    let mut probes: HashMap<String, bool> = HashMap::new();
    let probe_targets: Vec<String> = current_map
        .values()
        .take(MOLECULE_MAP_PROBE_LIMIT)
        .cloned()
        .chain(proposed_key_molecule.clone())
        .collect();
    for molecule in probe_targets {
        if probes.contains_key(&molecule) {
            continue;
        }
        match molecule_has_live_rows(host, &molecule).await {
            Ok(live) => {
                probes.insert(molecule, live);
            }
            Err(error) => {
                return error_response(
                    500,
                    &format!("molecule live-row probe failed: {error}"),
                    ctx,
                )
            }
        }
    }
    let current_key_has_live_rows = current_key_molecule
        .as_ref()
        .and_then(|molecule| probes.get(molecule).copied());
    let proposed_key_has_live_rows = proposed_key_molecule
        .as_ref()
        .and_then(|molecule| probes.get(molecule).copied());

    let fields_report: Vec<_> = declared
        .iter()
        .map(|field| {
            let molecule = current_map.get(field);
            serde_json::json!({
                "field": field,
                "molecule": molecule,
                "has_live_rows": molecule.and_then(|molecule| probes.get(molecule).copied()),
            })
        })
        .collect();

    // Inspect mode also reports where the rows actually live. Without it an
    // operator cannot write the repair map, because the repair demands one
    // molecule UUID per runtime field and nothing else exposes them.
    let mut candidates = Vec::new();
    let mut suggested_map: Option<HashMap<String, String>> = None;
    if requested_map.is_none() {
        let installed = match manager.get_schemas() {
            Ok(installed) => installed,
            Err(error) => return error_response(500, &format!("schema list failed: {error}"), ctx),
        };
        let mut names: Vec<_> = installed.keys().cloned().collect();
        names.sort();
        for name in names {
            if name == canonical || candidates.len() >= MOLECULE_MAP_CANDIDATE_LIMIT {
                continue;
            }
            let Some(other_map) = installed
                .get(&name)
                .and_then(|other| other.field_molecule_uuids.as_ref())
            else {
                continue;
            };
            let mut mapped = HashMap::with_capacity(declared.len());
            for field in &declared {
                let Some(molecule) = other_map.get(field) else {
                    break;
                };
                mapped.insert(field.clone(), molecule.clone());
            }
            if mapped.len() != declared.len() {
                continue;
            }
            let fingerprint = fold_db::schema::SchemaCore::field_molecule_map_fingerprint(&mapped);
            if fingerprint == current_fingerprint {
                continue;
            }
            let candidate_key_molecule = key_molecule_of(&mapped);
            let candidate_key_has_live_rows = match candidate_key_molecule.as_deref() {
                Some(molecule) => match probes.get(molecule).copied() {
                    Some(live) => Some(live),
                    None => match molecule_has_live_rows(host, molecule).await {
                        Ok(live) => {
                            probes.insert(molecule.to_string(), live);
                            Some(live)
                        }
                        Err(error) => {
                            return error_response(
                                500,
                                &format!("candidate live-row probe failed: {error}"),
                                ctx,
                            )
                        }
                    },
                },
                None => None,
            };
            candidates.push(serde_json::json!({
                "schema": name,
                "fingerprint": fingerprint,
                "key_molecule": candidate_key_molecule,
                "key_has_live_rows": candidate_key_has_live_rows,
                "field_molecule_uuids": mapped.clone(),
            }));
            if suggested_map.is_none()
                && candidate_key_has_live_rows == Some(true)
                && current_key_has_live_rows != Some(true)
            {
                suggested_map = Some(mapped);
            }
        }
    }

    if body.execute {
        let Some(replacement) = requested_map.cloned() else {
            return error_response(
                400,
                "field_molecule_uuids is required with execute=true",
                ctx,
            );
        };
        let Some(expected) = body.expected_current_fingerprint.as_deref() else {
            return error_response(
                400,
                "expected_current_fingerprint is required with execute=true",
                ctx,
            );
        };
        if expected != current_fingerprint {
            return error_response(
                409,
                &format!(
                    "schema molecule map changed: expected {expected}, current {current_fingerprint}"
                ),
                ctx,
            );
        }
        if let Err(error) = manager
            .repair_field_molecule_uuids(&canonical, expected, replacement)
            .await
        {
            return error_response(
                409,
                &format!("schema molecule-map repair failed: {error}"),
                ctx,
            );
        }
    }

    json_ok(&envelope(
        &serde_json::json!({
            "schema": canonical,
            "executed": body.execute,
            "inspect_only": requested_map.is_none(),
            "changed": !changed_fields.is_empty(),
            "changed_fields": changed_fields,
            "current_fingerprint": current_fingerprint,
            "proposed_fingerprint": proposed_fingerprint,
            "key_field": key_field,
            "current_key_molecule": current_key_molecule,
            "current_key_has_live_rows": current_key_has_live_rows,
            "proposed_key_has_live_rows": proposed_key_has_live_rows,
            "current_field_molecule_uuids": current_map,
            "fields": fields_report,
            "candidates": candidates,
            "suggested_field_molecule_uuids": suggested_map,
        }),
        ctx.user_id.as_str(),
    ))
}
