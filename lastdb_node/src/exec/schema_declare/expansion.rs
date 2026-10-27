//! Catalog expansion anchoring and registration fallback.

use super::*;

pub(crate) fn service_registered_schema_for_identity(
    mut schema: DeclarativeSchemaDefinition,
    catalog_hash: &str,
) -> DeclarativeSchemaDefinition {
    if schema.name != catalog_hash {
        schema.name = catalog_hash.to_string();
    }
    schema
}

/// Add explicit, flattened mapper sources from the best same-name predecessor
/// before registering an expansion. Returns the predecessor hash when found.
pub(crate) fn anchor_catalog_expansion(
    schema: &mut DeclarativeSchemaDefinition,
    result: &SchemaResolveResult,
    descriptive_name: &str,
) -> Option<String> {
    let predecessor = result
        .candidates
        .iter()
        .filter(|candidate| candidate.matched_descriptive_name == descriptive_name)
        .max_by_key(|candidate| candidate.schema.schema.fields.as_ref().map_or(0, Vec::len))?;
    let old: Schema = predecessor.schema.schema.clone().into();
    Some(anchor_catalog_expansion_to(schema, &old))
}

pub(crate) fn anchor_catalog_expansion_to(
    schema: &mut DeclarativeSchemaDefinition,
    old: &DeclarativeSchemaDefinition,
) -> String {
    // `identity_hash` is the app proposal identity on a catalog schema whose
    // storage name can be a different Schema Service hash. FieldMapper source
    // lookups use installed schema names, so always anchor to that name.
    let source_schema = old.name.clone();
    let new_fields: HashSet<String> = schema.fields.iter().flatten().cloned().collect();
    let mappers = schema.field_mappers.get_or_insert_with(HashMap::new);
    for field in old.fields.iter().flatten() {
        if !new_fields.contains(field.as_str()) {
            continue;
        }
        let mapper = old
            .field_mappers
            .as_ref()
            .and_then(|existing| existing.get(field))
            .cloned()
            .unwrap_or_else(|| FieldMapper::new(source_schema.clone(), field.clone()));
        mappers.insert(field.clone(), mapper);
    }
    source_schema
}

pub(crate) fn local_catalog_expansion_predecessor<'a>(
    schemas: &'a HashMap<String, Schema>,
    declare: &DirectDeclareProposal,
) -> Option<&'a Schema> {
    let proposed_hash = declare.proposal.identity_hash.as_deref();
    schemas
        .values()
        .filter(|candidate| {
            // An app alias can have the largest field set, but the next bind
            // replaces that alias. It cannot serve as its own mapper source.
            candidate.name.len() == 64
                && candidate.name.chars().all(|c| c.is_ascii_hexdigit())
                && candidate.owner_app_id.as_deref() == Some(declare.namespace.as_str())
                && candidate.descriptive_name.as_deref()
                    == Some(declare.proposal.descriptive_name.as_str())
                && proposed_hash.is_none_or(|hash| {
                    candidate.identity_hash.as_deref() != Some(hash) && candidate.name != hash
                })
        })
        .max_by_key(|candidate| candidate.fields.as_ref().map_or(0, Vec::len))
}

pub(crate) fn anchor_catalog_expansion_from_local(
    schemas: &HashMap<String, Schema>,
    schema: &mut DeclarativeSchemaDefinition,
    declare: &DirectDeclareProposal,
) -> Option<String> {
    let predecessor = local_catalog_expansion_predecessor(schemas, declare)?;
    Some(anchor_catalog_expansion_to(schema, predecessor))
}

/// The schema catalog sync registers when no catalog schema covers the
/// proposal, plus why. The executor supplies local schemas only if this
/// remote-result decision cannot select a predecessor.
pub(crate) struct CatalogRegistrationFallback {
    pub(crate) schema: DeclarativeSchemaDefinition,
    /// The same-name predecessor the expansion is anchored to, when found.
    pub(crate) predecessor: Option<String>,
    pub(crate) reason: &'static str,
}

pub(crate) fn catalog_registration_fallback(
    declare: &DirectDeclareProposal,
    result: Option<&SchemaResolveResult>,
    resolved: &Option<Result<DirectDeclareResolution, BindRefusal>>,
) -> CatalogRegistrationFallback {
    let reason = resolved
        .as_ref()
        .and_then(|resolution| resolution.as_ref().err())
        .map(BindRefusal::registration_reason);
    let mut schema = declare.catalog_schema.clone();

    // Anchor an expansion to the best same-name predecessor. Inherit
    // its existing mapper source when present (flattening expansion
    // chains); otherwise map to the predecessor's own molecule. This
    // makes the old rows immediately visible through the new identity
    // without copying data.
    let predecessor = result.and_then(|result| {
        anchor_catalog_expansion(&mut schema, result, &declare.proposal.descriptive_name)
    });
    let reason = if result.is_none() {
        "resolver_unavailable"
    } else {
        reason.unwrap_or("no_candidate_schema")
    };
    CatalogRegistrationFallback {
        schema,
        predecessor,
        reason,
    }
}
