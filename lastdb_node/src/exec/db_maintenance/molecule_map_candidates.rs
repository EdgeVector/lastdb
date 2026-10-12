//! Candidate molecule maps for the inspect mode of the schema molecule-map repair route.

use super::*;

/// Inputs that stay fixed while the inspect call searches other schemas.
pub(super) struct CandidateSearch<'a> {
    pub(super) canonical: &'a str,
    pub(super) declared: &'a [String],
    pub(super) current_fingerprint: &'a str,
    pub(super) current_key_has_live_rows: Option<bool>,
    pub(super) key_field: Option<&'a str>,
}

impl CandidateSearch<'_> {
    fn key_molecule_of(&self, map: &HashMap<String, String>) -> Option<String> {
        self.key_field.and_then(|field| map.get(field)).cloned()
    }
}

/// Candidate maps taken from other installed schemas, and the first one that
/// restores a key molecule with live rows.
pub(super) type CandidateReport = (Vec<serde_json::Value>, Option<HashMap<String, String>>);

/// Inspect mode also reports where the rows actually live. Without it an
/// operator cannot write the repair map, because the repair demands one
/// molecule UUID per runtime field and nothing else exposes them.
pub(super) async fn collect_candidates(
    host: &Host,
    ctx: &AccessContext,
    search: &CandidateSearch<'_>,
    probes: &mut HashMap<String, bool>,
) -> Result<CandidateReport, UdsResponse> {
    let manager = host.db.schema_manager();
    let mut candidates = Vec::new();
    let mut suggested_map: Option<HashMap<String, String>> = None;
    let installed = match manager.get_schemas() {
        Ok(installed) => installed,
        Err(error) => {
            return Err(error_response(
                500,
                &format!("schema list failed: {error}"),
                ctx,
            ))
        }
    };
    let mut names: Vec<_> = installed.keys().cloned().collect();
    names.sort();
    for name in names {
        if name == search.canonical || candidates.len() >= MOLECULE_MAP_CANDIDATE_LIMIT {
            continue;
        }
        let Some(other_map) = installed
            .get(&name)
            .and_then(|other| other.field_molecule_uuids.as_ref())
        else {
            continue;
        };
        let mut mapped = HashMap::with_capacity(search.declared.len());
        for field in search.declared {
            let Some(molecule) = other_map.get(field) else {
                break;
            };
            mapped.insert(field.clone(), molecule.clone());
        }
        if mapped.len() != search.declared.len() {
            continue;
        }
        let fingerprint = fold_db::schema::SchemaCore::field_molecule_map_fingerprint(&mapped);
        if fingerprint == search.current_fingerprint {
            continue;
        }
        let candidate_key_molecule = search.key_molecule_of(&mapped);
        let candidate_key_has_live_rows = match candidate_key_molecule.as_deref() {
            Some(molecule) => match probes.get(molecule).copied() {
                Some(live) => Some(live),
                None => match molecule_has_live_rows(host, molecule).await {
                    Ok(live) => {
                        probes.insert(molecule.to_string(), live);
                        Some(live)
                    }
                    Err(error) => {
                        return Err(error_response(
                            500,
                            &format!("candidate live-row probe failed: {error}"),
                            ctx,
                        ))
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
            && search.current_key_has_live_rows != Some(true)
        {
            suggested_map = Some(mapped);
        }
    }
    Ok((candidates, suggested_map))
}
