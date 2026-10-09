//! Key-layout checks and the declare bind resolver.

use super::*;

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
// lint:fn-size-ok moved verbatim from schema_declare.rs; splitting this function is separate work
pub(crate) fn resolve_declare_bind(
    proposed: &DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
) -> Result<DirectDeclareResolution, String> {
    if result.outcome == SchemaResolveOutcome::Reuse {
        if let Some(matched) = result.r#match.as_ref() {
            if !matched.is_superset {
                return Err("catalog candidate does not cover every proposed field".into());
            }
            let candidate = &matched.schema.schema;
            reject_candidate_key_layout_mismatch(
                proposed,
                &candidate_catalog_hash(matched),
                &candidate.schema_type,
                candidate.key.as_ref(),
                &matched.field_rename_map,
            )?;
        }
        if let Some(hash) = result.matched_shared_schema_hash.as_deref() {
            if !hash.trim().is_empty() {
                return Ok(DirectDeclareResolution::Reuse {
                    catalog_hash: hash.to_string(),
                    field_mappers: result
                        .r#match
                        .as_ref()
                        .map(|m| m.field_rename_map.clone())
                        .unwrap_or_default(),
                    confidence: result.confidence,
                });
            }
        }
    }

    if result.outcome == SchemaResolveOutcome::CandidateEquivalent {
        let mut hashes: Vec<String> = result
            .candidate_shared_schema_hashes
            .iter()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .collect();

        if hashes.is_empty() {
            for cand in &result.candidates {
                let h = cand
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| cand.schema.schema.name.clone());
                let h = h.trim().to_string();
                if !h.is_empty() {
                    hashes.push(h);
                }
            }
        }

        let mut seen = HashSet::new();
        hashes.retain(|h| seen.insert(h.clone()));

        if !hashes.is_empty() {
            if hashes.len() == 1 {
                if result
                    .candidates
                    .iter()
                    .any(|candidate| !candidate.is_superset)
                {
                    return Err(
                        "single catalog candidate does not cover every proposed field".into(),
                    );
                }
                for candidate in &result.candidates {
                    if candidate_catalog_hash(candidate) != hashes[0] {
                        continue;
                    }
                    let schema = &candidate.schema.schema;
                    reject_candidate_key_layout_mismatch(
                        proposed,
                        &hashes[0],
                        &schema.schema_type,
                        schema.key.as_ref(),
                        &candidate.field_rename_map,
                    )?;
                }
            }
            let mut by_hash: HashMap<String, usize> = HashMap::new();
            for (i, cand) in result.candidates.iter().enumerate() {
                let h = cand
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| cand.schema.schema.name.clone());
                let h = h.trim().to_string();
                if !h.is_empty() {
                    by_hash.entry(h).or_insert(i);
                }
            }

            let components: Vec<DirectDeclareComponent> = hashes
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
                .collect();

            if components.len() == 1 {
                let c = &components[0];
                return Ok(DirectDeclareResolution::Reuse {
                    catalog_hash: c.catalog_hash.clone(),
                    field_mappers: c.field_mappers.clone(),
                    confidence: result.confidence,
                });
            }
            return Ok(DirectDeclareResolution::Compose {
                components,
                confidence: result.confidence,
            });
        }
    }

    Err("proposal is novel to the schema catalog".into())
}

pub(crate) async fn load_catalog_hashes_on_host(
    host: &Host,
    client: &schema_service_client::SchemaServiceClient,
    hashes: &[String],
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for hash in hashes {
        let hash = hash.trim();
        if hash.is_empty() || !seen.insert(hash.to_string()) {
            continue;
        }
        let envelope = client
            .get_schema(hash)
            .await
            .map_err(|e| format!("failed to fetch catalog schema {hash}: {e}"))?;
        host.db
            .schema_manager()
            .load_schema_internal(envelope.schema.into())
            .await
            .map_err(|e| format!("failed to load catalog schema {hash}: {e}"))?;
    }
    Ok(())
}

/// Spell a binding mapper as `schema.field`.
///
/// Producers disagree on the right-hand side: a service rename
/// (`mutation_mappers`, some `field_rename_map`s) names only the catalog
/// field, while inherited mappers carry `source_schema.field`. A bare name
/// has no source schema, and `FieldMapper::try_from` refuses it, so it is
/// read as a field of `catalog_hash` — the schema the binding targets.
pub(crate) fn qualify_catalog_mapper(catalog_hash: &str, mapper: &str) -> String {
    let mapper = mapper.trim();
    if mapper.contains('.') {
        mapper.to_string()
    } else {
        format!("{catalog_hash}.{mapper}")
    }
}

/// Field mappers for an app binding onto a schema Schema Service just
/// registered or expanded.
///
/// The two inputs are keyed in different name spaces:
/// - `catalog_mappers` are the registered schema's own mappers, keyed by its
///   canonical field names (an expansion inherits its predecessor's rows).
/// - `mutation_mappers` are the service's renames, `proposal_field ->
///   canonical_field`, recorded when canonicalization or a semantic expand
///   renamed a proposal field.
///
/// Reading only the first left every renamed proposal field to the
/// same-name default in [`app_single_catalog_binding`], i.e. a field the
/// catalog does not have: lastgit `LastgitRepoIndex.updated_at` was renamed
/// to `open_crs_updated_at` and the declare failed in `apply_field_mappers`
/// (2026-09-22). A renamed field follows the catalog's own mapper for its
/// canonical name, so it lands where that field's rows live.
pub(crate) fn registered_binding_field_mappers(
    catalog_hash: &str,
    catalog_mappers: &HashMap<String, String>,
    mutation_mappers: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut mappers = catalog_mappers.clone();
    for (proposal_field, canonical) in mutation_mappers {
        let canonical = qualify_catalog_mapper(catalog_hash, canonical);
        let Ok(parsed) = FieldMapper::try_from(canonical.as_str()) else {
            continue;
        };
        if parsed.source_schema() == catalog_hash && parsed.source_field() == proposal_field {
            continue;
        }
        let target = if parsed.source_schema() == catalog_hash {
            catalog_mappers
                .get(parsed.source_field())
                .cloned()
                .unwrap_or(canonical)
        } else {
            canonical
        };
        mappers.insert(proposal_field.clone(), target);
    }
    mappers
}

/// Refuse a binding that maps onto a catalog field the catalog does not have.
///
/// Without this the binding loads and `apply_field_mappers` fails later with
/// `source field '<catalog>.<field>' missing from runtime_fields`, which names
/// neither the proposal field nor what the catalog does carry.
pub(crate) fn check_binding_sources_exist(
    binding: &Schema,
    catalog_hash: &str,
    catalog_fields: &[String],
) -> Result<(), String> {
    let Some(mappers) = binding.field_mappers.as_ref() else {
        return Ok(());
    };
    let mut missing: Vec<String> = mappers
        .iter()
        .filter(|(_, mapper)| mapper.source_schema() == catalog_hash)
        .filter(|(_, mapper)| !catalog_fields.iter().any(|f| f == mapper.source_field()))
        .map(|(field, mapper)| format!("{field} -> {}", mapper.source_field()))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort();
    Err(format!(
        "app binding {} maps onto field(s) catalog schema {catalog_hash} does not have: {}; \
         catalog fields: {}. The schema service renamed or dropped these fields and no \
         mapper names their catalog spelling",
        binding.name,
        missing.join(", "),
        catalog_fields.join(", "),
    ))
}
