use super::super::*;

/// Schema, query, mutation and status routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_core(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::Query => heap_route!(execute_query_route, req, ctx, host).await,
        DataRoute::QueryBatch => heap_route!(execute_query_batch_route, req, ctx, host).await,
        DataRoute::Mutation => heap_route!(execute_mutation_route, req, ctx, host).await,
        DataRoute::MutationBatch => {
            heap_route!(execute_mutations_batch_route, req, ctx, host).await
        }
        DataRoute::ListSchemas => heap_route!(execute_list_schemas_route, req, ctx, host).await,
        DataRoute::ListRecordKeys => {
            heap_route!(execute_list_record_keys_route, req, ctx, host).await
        }
        DataRoute::GetSchema => execute_get_schema_route(req, ctx, host),
        DataRoute::DeclareSchema => heap_route!(execute_declare_schema_route, req, ctx, host).await,
        DataRoute::SeedSystemSchema => {
            heap_route!(execute_seed_system_schema_route, req, ctx, host).await
        }
        DataRoute::RetireSchemaNameClaim => {
            heap_route!(execute_retire_schema_name_claim_route, req, ctx, host).await
        }
        DataRoute::DropSchema => heap_route!(execute_drop_schema_route, req, ctx, host).await,
        DataRoute::Status => execute_status_route(req, ctx, host),
        DataRoute::AdminShed => heap_route_ctx_host!(execute_admin_shed_route, ctx, host).await,
        DataRoute::LogFilterGet => execute_log_filter_get_route(ctx),
        DataRoute::LogFilterSet => execute_log_filter_set_route(req, ctx),
        DataRoute::DbInventory => heap_route_ctx_host!(execute_db_inventory_route, ctx, host).await,
        DataRoute::DbSchemas => heap_route_ctx_host!(execute_db_schemas_route, ctx, host).await,
        _ => return None,
    })
}

/// Identity, search, change feed and app routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_search_app(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::AutoIdentity => execute_auto_identity_route(host),
        DataRoute::BootIdentity => execute_boot_identity_route(host),
        DataRoute::BootLedger => execute_boot_ledger_route(host),
        DataRoute::SearchAppQuery => execute_search_app_query_route(req, ctx, host),
        DataRoute::AppSearch => execute_app_search_route(req, ctx, host),
        DataRoute::NativeIndexAppVectorPut => {
            heap_route!(execute_native_index_app_vector_put_route, req, ctx, host).await
        }
        DataRoute::NativeIndexKnn => {
            heap_route!(execute_native_index_knn_route, req, ctx, host).await
        }
        DataRoute::LocalWatch => execute_local_watch_route(req, ctx, host),
        DataRoute::AppChanges => heap_route!(execute_app_changes_route, req, ctx, host).await,
        _ => return None,
    })
}
