//! The installed schema owns values; the service supplies its definition.

use super::*;

pub(crate) struct DirectDeclareResolution {
    pub(crate) runtime_schema: String,
    pub(crate) reused: bool,
}

impl DirectDeclareResolution {
    pub(crate) fn outcome(&self) -> &'static str {
        if self.reused {
            "reuse"
        } else {
            "register"
        }
    }

    pub(crate) fn stamp_audit(&self, event: &mut SchemaSyncAuditEvent) {
        event.outcome = self.outcome().to_string();
        event.catalog_hashes = vec![self.runtime_schema.clone()];
        event.mapper_count = 0;
        event.predecessor_hashes.clear();
        event.bind_eligible = true;
    }
}

pub(crate) fn declare_resolution_payload(
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Value {
    // Keep the adapter metadata for existing clients.
    let adapter = serde_json::json!({
        "kind": "owned_schema",
        "local_schema": declare.local_schema_id,
        "catalog_schema": resolution.runtime_schema,
    });
    serde_json::json!({
        "app_id": declare.namespace,
        "namespace": declare.namespace,
        "schema": declare.local_schema_id,
        "schema_name": declare.local_schema_id,
        "descriptive_name": declare.catalog_schema.descriptive_name,
        "canonical": resolution.runtime_schema,
        "identity_hash": declare.catalog_schema.identity_hash,
        // Apps confirm the owned layout from this key before they write.
        "independent_fields": true,
        "resolution": resolution.outcome(),
        "adapter": adapter,
        "adapters": [adapter],
    })
}
