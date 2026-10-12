//! Read-only inspection phase of the schema molecule-map repair route.

use super::*;

/// Everything the route learns about the installed schema before it looks
/// for candidate maps or writes a repair.
pub(super) struct Inspection {
    pub(super) declared: Vec<String>,
    pub(super) current_map: HashMap<String, String>,
    pub(super) current_fingerprint: String,
    pub(super) proposed_fingerprint: Option<String>,
    pub(super) changed_fields: Vec<String>,
    pub(super) key_field: Option<String>,
    pub(super) current_key_molecule: Option<String>,
    pub(super) current_key_has_live_rows: Option<bool>,
    pub(super) proposed_key_has_live_rows: Option<bool>,
    pub(super) fields_report: Vec<serde_json::Value>,
    pub(super) probes: HashMap<String, bool>,
}

/// Load the schema, fingerprint the current and proposed maps, and probe the
/// molecules for live rows.
pub(super) async fn inspect_schema(
    host: &Host,
    ctx: &AccessContext,
    canonical: &str,
    requested_map: Option<&HashMap<String, String>>,
) -> Result<Inspection, UdsResponse> {
    let (schema, declared) = load_declared_schema(host, ctx, canonical, requested_map)?;

    let current_map = schema.field_molecule_uuids.clone().unwrap_or_default();
    let current_fingerprint =
        fold_db::schema::SchemaCore::field_molecule_map_fingerprint(&current_map);
    let proposed_fingerprint =
        requested_map.map(fold_db::schema::SchemaCore::field_molecule_map_fingerprint);
    let changed_fields = changed_fields(&declared, &current_map, requested_map);

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

    let probes = probe_live_rows(host, ctx, &current_map, proposed_key_molecule.clone()).await?;
    let live_rows_of = |molecule: &Option<String>| {
        molecule
            .as_ref()
            .and_then(|molecule| probes.get(molecule).copied())
    };
    let current_key_has_live_rows = live_rows_of(&current_key_molecule);
    let proposed_key_has_live_rows = live_rows_of(&proposed_key_molecule);
    let fields_report = fields_report(&declared, &current_map, &probes);

    Ok(Inspection {
        declared,
        current_map,
        current_fingerprint,
        proposed_fingerprint,
        changed_fields,
        key_field,
        current_key_molecule,
        current_key_has_live_rows,
        proposed_key_has_live_rows,
        fields_report,
        probes,
    })
}

/// Sorted declared fields whose molecule differs between the installed map
/// and the proposed one. An inspect-only call changes nothing.
fn changed_fields(
    declared: &[String],
    current_map: &HashMap<String, String>,
    requested_map: Option<&HashMap<String, String>>,
) -> Vec<String> {
    let mut changed: Vec<String> = match requested_map {
        Some(map) => declared
            .iter()
            .filter(|field| current_map.get(*field) != map.get(*field))
            .cloned()
            .collect(),
        None => Vec::new(),
    };
    changed.sort();
    changed
}

/// Per-field report of the current molecule and whether it holds live rows.
fn fields_report(
    declared: &[String],
    current_map: &HashMap<String, String>,
    probes: &HashMap<String, bool>,
) -> Vec<serde_json::Value> {
    declared
        .iter()
        .map(|field| {
            let molecule = current_map.get(field);
            serde_json::json!({
                "field": field,
                "molecule": molecule,
                "has_live_rows": molecule.and_then(|molecule| probes.get(molecule).copied()),
            })
        })
        .collect()
}

/// Load the installed schema with its runtime fields, and return the sorted
/// declared field names. A proposed map must name exactly those fields.
fn load_declared_schema(
    host: &Host,
    ctx: &AccessContext,
    canonical: &str,
    requested_map: Option<&HashMap<String, String>>,
) -> Result<(fold_db::schema::types::Schema, Vec<String>), UdsResponse> {
    let manager = host.db.schema_manager();
    let Some(mut schema) = (match manager.get_schema_metadata(canonical) {
        Ok(schema) => schema,
        Err(error) => {
            return Err(error_response(
                500,
                &format!("schema lookup failed: {error}"),
                ctx,
            ))
        }
    }) else {
        return Err(error_response(404, "schema not found", ctx));
    };
    if schema.runtime_fields.is_empty() {
        if let Err(error) = schema.populate_runtime_fields() {
            return Err(error_response(
                500,
                &format!("schema field load failed: {error}"),
                ctx,
            ));
        }
    }
    let mut declared: Vec<_> = schema.runtime_fields.keys().cloned().collect();
    declared.sort();
    if let Some(map) = requested_map {
        let mut proposed: Vec<_> = map.keys().cloned().collect();
        proposed.sort();
        if proposed != declared {
            return Err(error_response(
                400,
                "field_molecule_uuids must contain every runtime field and no unknown fields",
                ctx,
            ));
        }
    }
    Ok((schema, declared))
}

/// Probe the current map's molecules (up to the limit) plus the proposed key
/// molecule for live rows.
async fn probe_live_rows(
    host: &Host,
    ctx: &AccessContext,
    current_map: &HashMap<String, String>,
    proposed_key_molecule: Option<String>,
) -> Result<HashMap<String, bool>, UdsResponse> {
    let mut probes: HashMap<String, bool> = HashMap::new();
    let probe_targets: Vec<String> = current_map
        .values()
        .take(MOLECULE_MAP_PROBE_LIMIT)
        .cloned()
        .chain(proposed_key_molecule)
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
                return Err(error_response(
                    500,
                    &format!("molecule live-row probe failed: {error}"),
                    ctx,
                ))
            }
        }
    }
    Ok(probes)
}

/// Inspect mode also reports where the rows actually live. Without it an
/// operator cannot write the repair map, because the repair demands one
/// molecule UUID per runtime field and nothing else exposes them.
pub(super) async fn inspect_candidates(
    host: &Host,
    ctx: &AccessContext,
    canonical: &str,
    inspection: &mut Inspection,
    requested_map: Option<&HashMap<String, String>>,
) -> Result<CandidateReport, UdsResponse> {
    if requested_map.is_some() {
        return Ok(CandidateReport::default());
    }
    let search = CandidateSearch {
        canonical,
        declared: &inspection.declared,
        current_fingerprint: &inspection.current_fingerprint,
        current_key_has_live_rows: inspection.current_key_has_live_rows,
        key_field: inspection.key_field.as_deref(),
    };
    collect_candidates(host, ctx, &search, &mut inspection.probes).await
}
