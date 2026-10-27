//! Pure key-layout and resolver-result decisions. No host or storage access.

use super::*;

/// Why a resolver answer cannot bind. Prose stays diagnostic, never policy.
#[derive(Debug)]
pub(crate) enum BindRefusal {
    FieldCoverage(&'static str),
    KeyLayout(String),
    NoCandidate,
}

impl BindRefusal {
    pub(crate) fn registration_reason(&self) -> &'static str {
        match self {
            Self::FieldCoverage(_) => "insufficient_field_coverage",
            Self::KeyLayout(_) => "key_layout_mismatch",
            Self::NoCandidate => "no_candidate_schema",
        }
    }
}

impl std::fmt::Display for BindRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldCoverage(message) => f.write_str(message),
            Self::KeyLayout(message) => f.write_str(message),
            Self::NoCandidate => f.write_str("proposal is novel to the schema catalog"),
        }
    }
}

/// Render a key layout for an operator-facing message, e.g. `HashRange(board, sk)`.
pub(crate) fn describe_key_layout(schema_type: &SchemaType, key: Option<&KeyConfig>) -> String {
    let hash = key.and_then(|k| k.hash_field.as_deref()).unwrap_or("-");
    let range = key.and_then(|k| k.range_field.as_deref()).unwrap_or("-");
    format!("{schema_type:?}({hash}, {range})")
}

/// Project one proposal field name onto the candidate's own field names.
///
/// `field_rename_map` is `proposal_field -> candidate_field`. Some producers
/// write the candidate side as a full `schema.field` mapper, so take the field
/// half and both spellings compare equal.
pub(crate) fn candidate_field_name(
    field: &str,
    field_rename_map: &HashMap<String, String>,
) -> String {
    match field_rename_map.get(field) {
        Some(mapped) => mapped
            .split_once('.')
            .map_or(mapped.as_str(), |(_source_schema, source_field)| {
                source_field
            })
            .trim()
            .to_string(),
        None => field.to_string(),
    }
}

/// Refuse a catalog candidate whose key layout differs from the proposal's.
///
/// Field coverage is not enough. `HashRange(board, sk)` and
/// `HashRange(milestone, sk)` carry the same fields, so the superset check
/// accepts the swap; every write then lands under a different partition key and
/// every read through the bound identity comes back empty. The proposal's key
/// fields are projected through the candidate's renames first, so a genuine
/// rename of the same key still binds.
pub(crate) fn reject_candidate_key_layout_mismatch(
    proposed: &DeclarativeSchemaDefinition,
    candidate_label: &str,
    candidate_type: &SchemaType,
    candidate_key: Option<&KeyConfig>,
    field_rename_map: &HashMap<String, String>,
) -> Result<(), String> {
    let proposed_key = proposed.key.as_ref();
    let want_hash = proposed_key
        .and_then(|k| k.hash_field.as_deref())
        .map(|field| candidate_field_name(field, field_rename_map));
    let want_range = proposed_key
        .and_then(|k| k.range_field.as_deref())
        .map(|field| candidate_field_name(field, field_rename_map));
    let got_hash = candidate_key.and_then(|k| k.hash_field.clone());
    let got_range = candidate_key.and_then(|k| k.range_field.clone());

    if proposed.schema_type == *candidate_type && want_hash == got_hash && want_range == got_range {
        return Ok(());
    }
    Err(format!(
        "catalog candidate {candidate_label} has key layout {}, but proposal {} declares {}",
        describe_key_layout(candidate_type, candidate_key),
        proposed.name,
        describe_key_layout(&proposed.schema_type, proposed_key),
    ))
}

/// Identify one resolve candidate by its catalog identity hash, falling back to
/// its catalog name when the service did not stamp a hash.
pub(crate) fn candidate_catalog_hash(
    candidate: &schema_service_client::types::SchemaReuseMatch,
) -> String {
    candidate
        .schema
        .schema
        .identity_hash
        .clone()
        .unwrap_or_else(|| candidate.schema.schema.name.clone())
        .trim()
        .to_string()
}

/// Map a schema-service resolve result into a Mini bind decision.
///
/// - `reuse` → single catalog identity (existing behavior)
/// - `candidate_equivalent` with component hashes → load all and bind as
///   reuse (1 hash) or compose (≥2 hashes). This productizes embedding-beam
///   **UseComponents** (multi-schema field cover) instead of 409.
///
/// A candidate must both cover every proposed field and carry the proposal's
/// key layout; see [`reject_candidate_key_layout_mismatch`].
pub(crate) fn resolve_declare_bind(
    proposed: &DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
) -> Result<DirectDeclareResolution, BindRefusal> {
    if result.outcome == SchemaResolveOutcome::Reuse {
        if let Some(resolution) = resolve_reuse_outcome(proposed, result)? {
            return Ok(resolution);
        }
    }
    if result.outcome == SchemaResolveOutcome::CandidateEquivalent {
        if let Some(resolution) = resolve_candidate_equivalent(proposed, result)? {
            return Ok(resolution);
        }
    }
    Err(BindRefusal::NoCandidate)
}

/// Trimmed catalog hash of a candidate; empty when the service stamped none.
fn trimmed_candidate_hash(candidate: &schema_service_client::types::SchemaReuseMatch) -> String {
    candidate_catalog_hash(candidate).trim().to_string()
}

/// `reuse` outcome: validate the match and bind the shared schema hash.
fn resolve_reuse_outcome(
    proposed: &DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
) -> Result<Option<DirectDeclareResolution>, BindRefusal> {
    if let Some(matched) = result.r#match.as_ref() {
        if !matched.is_superset {
            return Err(BindRefusal::FieldCoverage(
                "catalog candidate does not cover every proposed field",
            ));
        }
        let candidate = &matched.schema.schema;
        reject_candidate_key_layout_mismatch(
            proposed,
            &candidate_catalog_hash(matched),
            &candidate.schema_type,
            candidate.key.as_ref(),
            &matched.field_rename_map,
        )
        .map_err(BindRefusal::KeyLayout)?;
    }
    let Some(hash) = result.matched_shared_schema_hash.as_deref() else {
        return Ok(None);
    };
    if hash.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(DirectDeclareResolution::Reuse {
        catalog_hash: hash.to_string(),
        field_mappers: result
            .r#match
            .as_ref()
            .map(|m| m.field_rename_map.clone())
            .unwrap_or_default(),
        confidence: result.confidence,
    }))
}

/// Component hashes of a `candidate_equivalent` result, deduplicated in
/// order. Falls back to the candidates' own hashes when the service sent none.
fn candidate_equivalent_hashes(result: &SchemaResolveResult) -> Vec<String> {
    let mut hashes: Vec<String> = result
        .candidate_shared_schema_hashes
        .iter()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .collect();
    if hashes.is_empty() {
        hashes.extend(
            result
                .candidates
                .iter()
                .map(trimmed_candidate_hash)
                .filter(|h| !h.is_empty()),
        );
    }
    let mut seen = HashSet::new();
    hashes.retain(|h| seen.insert(h.clone()));
    hashes
}

/// A lone component must cover every proposed field and keep the proposal's
/// key layout.
fn check_single_candidate(
    proposed: &DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
    hash: &str,
) -> Result<(), BindRefusal> {
    if result
        .candidates
        .iter()
        .any(|candidate| !candidate.is_superset)
    {
        return Err(BindRefusal::FieldCoverage(
            "single catalog candidate does not cover every proposed field",
        ));
    }
    for candidate in &result.candidates {
        if candidate_catalog_hash(candidate) != hash {
            continue;
        }
        let schema = &candidate.schema.schema;
        reject_candidate_key_layout_mismatch(
            proposed,
            hash,
            &schema.schema_type,
            schema.key.as_ref(),
            &candidate.field_rename_map,
        )
        .map_err(BindRefusal::KeyLayout)?;
    }
    Ok(())
}

/// One bind component per hash, carrying candidate details when known.
fn candidate_components(
    result: &SchemaResolveResult,
    hashes: &[String],
) -> Vec<DirectDeclareComponent> {
    let mut by_hash: HashMap<String, usize> = HashMap::new();
    for (i, cand) in result.candidates.iter().enumerate() {
        let h = trimmed_candidate_hash(cand);
        if !h.is_empty() {
            by_hash.entry(h).or_insert(i);
        }
    }
    hashes
        .iter()
        .map(|h| {
            if let Some(&i) = by_hash.get(h) {
                let m = &result.candidates[i];
                DirectDeclareComponent {
                    catalog_hash: h.clone(),
                    field_mappers: m.field_rename_map.clone(),
                    matched_descriptive_name: Some(m.matched_descriptive_name.clone()),
                    unmapped_fields: m.unmapped_fields.clone(),
                    is_exact_match: Some(m.is_exact_match),
                    is_superset: Some(m.is_superset),
                }
            } else {
                DirectDeclareComponent {
                    catalog_hash: h.clone(),
                    field_mappers: HashMap::new(),
                    matched_descriptive_name: None,
                    unmapped_fields: Vec::new(),
                    is_exact_match: None,
                    is_superset: None,
                }
            }
        })
        .collect()
}

/// `candidate_equivalent` outcome: bind one hash as reuse, several as compose.
fn resolve_candidate_equivalent(
    proposed: &DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
) -> Result<Option<DirectDeclareResolution>, BindRefusal> {
    let hashes = candidate_equivalent_hashes(result);
    if hashes.is_empty() {
        return Ok(None);
    }
    if hashes.len() == 1 {
        check_single_candidate(proposed, result, &hashes[0])?;
    }
    let components = candidate_components(result, &hashes);
    if components.len() == 1 {
        let c = &components[0];
        return Ok(Some(DirectDeclareResolution::Reuse {
            catalog_hash: c.catalog_hash.clone(),
            field_mappers: c.field_mappers.clone(),
            confidence: result.confidence,
        }));
    }
    Ok(Some(DirectDeclareResolution::Compose {
        components,
        confidence: result.confidence,
    }))
}
