//! Resolver I/O. Data-only expansion rules consume the returned service result.

use super::*;

/// Read the real resolver through the existing concrete host client.
///
/// `None` means the resolver is unavailable; catalog sync then registers
/// through Schema Service with the locally loaded predecessor. The resolver is
/// a read: it never registers a schema.
pub(crate) async fn resolve_direct_declare_result(
    host: &Host,
    schema_service_url: &str,
    declare: &DirectDeclareProposal,
) -> Option<SchemaResolveResult> {
    match crate::schema_resolver_host::resolve_direct_declare_with_host_config(
        &host.home,
        schema_service_url,
        declare.local_schema_id.clone(),
        declare.proposal.clone(),
    )
    .await
    {
        Ok(result) => Some(result),
        Err(e) => {
            tracing::warn!(
                target: "lastdb_node::apps_declare_schema",
                schema = %declare.local_schema_id,
                error = %e,
                "schema resolver unavailable; registering through Schema Service with the locally loaded catalog predecessor"
            );
            None
        }
    }
}
