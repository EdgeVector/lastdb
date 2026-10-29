//! Pure resolution facts, replies, and audit fields. No host access.

use super::*;

/// Shared facts for the audit, response and name-claim keeper selection.
pub(crate) struct ResolutionFacts<'a> {
    pub(crate) outcome: &'static str,
    pub(crate) canonical: Option<&'a str>,
    catalog_hashes: Vec<String>,
    mapper_count: usize,
    predecessors: Vec<String>,
}

impl<'a> ResolutionFacts<'a> {
    pub(crate) fn new(resolution: &'a DirectDeclareResolution) -> Self {
        let mut predecessors = HashSet::new();
        let (outcome, canonical, catalog_hashes, mapper_count) = match resolution {
            DirectDeclareResolution::Register {
                catalog_hash,
                field_mappers,
                replaced_schema,
            } => {
                predecessors.extend(replaced_schema.iter().cloned());
                predecessors.extend(mapper_sources(field_mappers));
                (
                    if replaced_schema.is_some() {
                        "expand"
                    } else {
                        "register"
                    },
                    Some(catalog_hash.as_str()),
                    vec![catalog_hash.clone()],
                    field_mappers.len(),
                )
            }
            DirectDeclareResolution::Reuse {
                catalog_hash,
                field_mappers,
                ..
            } => {
                predecessors.extend(mapper_sources(field_mappers));
                (
                    "reuse",
                    Some(catalog_hash.as_str()),
                    vec![catalog_hash.clone()],
                    field_mappers.len(),
                )
            }
            DirectDeclareResolution::Compose { components, .. } => {
                for component in components {
                    predecessors.extend(mapper_sources(&component.field_mappers));
                }
                (
                    "compose",
                    None,
                    components.iter().map(|c| c.catalog_hash.clone()).collect(),
                    components.iter().map(|c| c.field_mappers.len()).sum(),
                )
            }
        };
        let mut predecessors: Vec<_> = predecessors.into_iter().collect();
        predecessors.sort();
        Self {
            outcome,
            canonical,
            catalog_hashes,
            mapper_count,
            predecessors,
        }
    }

    pub(crate) fn stamp_audit(&self, event: &mut SchemaSyncAuditEvent) {
        event.outcome = self.outcome.to_string();
        event.catalog_hashes.clone_from(&self.catalog_hashes);
        event.mapper_count = self.mapper_count;
        event.predecessor_hashes.clone_from(&self.predecessors);
        event.bind_eligible = true;
    }
}

fn mapper_sources(mappers: &HashMap<String, String>) -> impl Iterator<Item = String> + '_ {
    mappers
        .values()
        .filter_map(|mapper| mapper.split_once('.').map(|(hash, _)| hash.to_string()))
}

pub(crate) fn declare_resolution_payload(
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
    facts: &ResolutionFacts<'_>,
) -> Value {
    let mut payload = match resolution {
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema,
        } => register_payload(
            declare,
            catalog_hash,
            field_mappers,
            replaced_schema.as_deref(),
        ),
        DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            confidence,
        } => reuse_payload(declare, catalog_hash, field_mappers, *confidence),
        DirectDeclareResolution::Compose {
            components,
            confidence,
        } => compose_payload(declare, components, *confidence),
    };
    payload["resolution"] = serde_json::json!(facts.outcome);
    payload["app_id"] = serde_json::json!(declare.namespace);
    payload["schema"] = serde_json::json!(declare.local_schema_id);
    if let Some(canonical) = facts.canonical {
        payload["canonical"] = serde_json::json!(canonical);
    }
    if declare.catalog_schema.uses_independent_fields() {
        // The definition identity comes from Schema Service. The runtime name
        // owns this app's values; there is no second schema or molecule alias.
        payload["identity_hash"] = serde_json::json!(declare.proposal.identity_hash);
        payload["independent_fields"] = serde_json::json!(true);
    }
    payload
}

fn register_payload(
    declare: &DirectDeclareProposal,
    catalog_hash: &str,
    field_mappers: &HashMap<String, String>,
    replaced_schema: Option<&str>,
) -> Value {
    let adapter = serde_json::json!({
        "kind": "registered_expansion",
        "local_schema": declare.local_schema_id,
        "catalog_schema": catalog_hash,
        "field_mappers": field_mappers,
    });
    serde_json::json!({
        "namespace": declare.namespace,
        "schema_name": declare.local_schema_id.clone(),
        "descriptive_name": declare.proposal.descriptive_name,
        "identity_hash": catalog_hash,
        "canonical": catalog_hash,
        "replaced_schema": replaced_schema,
        "adapter": adapter,
        "adapters": [adapter],
    })
}

fn reuse_payload(
    declare: &DirectDeclareProposal,
    catalog_hash: &str,
    field_mappers: &HashMap<String, String>,
    confidence: Option<f32>,
) -> Value {
    let adapter = serde_json::json!({
        "kind": "edge_only",
        "local_schema": declare.local_schema_id,
        "catalog_schema": catalog_hash,
        "field_mappers": field_mappers,
    });
    let mut payload = serde_json::json!({
        "namespace": declare.namespace,
        "schema_name": declare.local_schema_id.clone(),
        "descriptive_name": declare.proposal.descriptive_name,
        "identity_hash": catalog_hash,
        "adapter": adapter,
        "adapters": [adapter],
    });
    if let Some(c) = confidence {
        payload["confidence"] = serde_json::json!(c);
    }
    payload
}

fn compose_adapter(declare: &DirectDeclareProposal, c: &DirectDeclareComponent) -> Value {
    let mut a = serde_json::json!({
        "kind": "edge_only",
        "local_schema": declare.local_schema_id,
        "catalog_schema": c.catalog_hash,
        "field_mappers": c.field_mappers,
    });
    if let Some(ref n) = c.matched_descriptive_name {
        a["matched_descriptive_name"] = serde_json::json!(n);
    }
    if !c.unmapped_fields.is_empty() {
        a["unmapped_fields"] = serde_json::json!(c.unmapped_fields);
    }
    if let Some(v) = c.is_exact_match {
        a["is_exact_match"] = serde_json::json!(v);
    }
    if let Some(v) = c.is_superset {
        a["is_superset"] = serde_json::json!(v);
    }
    a
}

fn compose_payload(
    declare: &DirectDeclareProposal,
    components: &[DirectDeclareComponent],
    confidence: Option<f32>,
) -> Value {
    let adapters: Vec<Value> = components
        .iter()
        .map(|c| compose_adapter(declare, c))
        .collect();
    let component_hashes: Vec<String> = components.iter().map(|c| c.catalog_hash.clone()).collect();
    let mut payload = serde_json::json!({
        "namespace": declare.namespace,
        "schema_name": declare.local_schema_id.clone(),
        "descriptive_name": declare.proposal.descriptive_name,
        "identity_hash": serde_json::Value::Null,
        "component_catalog_hashes": component_hashes,
        "adapters": adapters,
        "adapter": adapters.first().cloned().unwrap_or(serde_json::json!({})),
    });
    if let Some(c) = confidence {
        payload["confidence"] = serde_json::json!(c);
    }
    payload
}
