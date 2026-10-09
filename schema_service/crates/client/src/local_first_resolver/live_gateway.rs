//! Live schema-service gateway trait (injectable for tests).

use super::*;

// ---------------------------------------------------------------------------
// Live gateway (injectable for tests)
// ---------------------------------------------------------------------------

/// Live schema-service operations used by the facade.
#[async_trait]
pub trait LiveSchemaGateway: Send + Sync {
    async fn resolve_schemas(
        &self,
        client_registry_version: Option<u64>,
        proposals: Vec<SchemaResolveProposal>,
    ) -> FoldDbResult<SchemaResolveResponse>;

    async fn add_shared_schema(
        &self,
        schema: &Schema,
        shared_surface: SharedSurfaceMetadata,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
    ) -> FoldDbResult<AddSchemaResponse>;
}

#[async_trait]
#[allow(clippy::use_self)] // fully-qualify inherent methods to avoid trait recursion
impl LiveSchemaGateway for SchemaServiceClient {
    async fn resolve_schemas(
        &self,
        client_registry_version: Option<u64>,
        proposals: Vec<SchemaResolveProposal>,
    ) -> FoldDbResult<SchemaResolveResponse> {
        // Inherent method (not the trait method being defined here).
        SchemaServiceClient::resolve_schemas(self, client_registry_version, proposals).await
    }

    async fn add_shared_schema(
        &self,
        schema: &Schema,
        shared_surface: SharedSurfaceMetadata,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
    ) -> FoldDbResult<AddSchemaResponse> {
        // Inherent method — fully qualify to avoid trait recursion.
        SchemaServiceClient::add_schema_with_shared_surface(
            self,
            schema,
            HashMap::new(),
            shared_surface,
            schema_match_source,
            fallback_reason,
        )
        .await
    }
}
