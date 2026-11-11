//! Exhaustive data-route dispatch for the owner control socket.

use super::*;

/// Execute a recognized control-socket data route against the host, with the
/// caller's user id bound as the task-local user context.
pub async fn execute_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    execute_observed_route(ctx, dispatch_data_route(route, req, ctx, host)).await
}

/// Bind the caller's user id as the task-local user context for dispatch and
/// deferred mutation execution.
pub(in crate::exec) async fn execute_observed_route<F>(ctx: &AccessContext, work: F) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    fold_db::user_context::run_with_user(&ctx.user_id, work).await
}

async fn dispatch_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match route {
        DataRoute::Query => heap_route!(query::execute_query_route, req, ctx, host).await,
        DataRoute::Mutation => heap_route!(mutation::execute_mutation_route, req, ctx, host).await,
        DataRoute::MutationBatch => {
            heap_route!(mutation::execute_mutations_batch_route, req, ctx, host).await
        }
        DataRoute::QueryBatch => {
            heap_route!(query::execute_query_batch_route, req, ctx, host).await
        }
        DataRoute::ListSchemas => schema_routes::execute_list_schemas_route(req, ctx, host),
        DataRoute::ListRecordKeys => {
            heap_route!(
                list_keys_route::execute_list_record_keys_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::GetSchema => schema_routes::execute_get_schema_route(req, ctx, host),
        DataRoute::DeclareSchema => {
            heap_route!(schema_declare::execute_declare_schema_route, req, ctx, host).await
        }
        DataRoute::SeedSystemSchema => {
            heap_route!(
                schema_routes::execute_seed_system_schema_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DropSchema => {
            heap_route!(schema_routes::execute_drop_schema_route, req, ctx, host).await
        }
        DataRoute::Status => admin_routes::execute_status_route(ctx, host),
        DataRoute::AdminShed => {
            heap_route_ctx_host!(admin_routes::execute_admin_shed_route, ctx, host).await
        }
        DataRoute::AutoIdentity => admin_routes::execute_auto_identity_route(host),
        DataRoute::BootIdentity => admin_routes::execute_boot_identity_route(host),
        DataRoute::SyncByteBackupV2 => byte_backup_routes::execute(req, ctx, host).await,
        DataRoute::SyncByteBackupV2Connect => byte_backup_routes::connect(req, ctx, host).await,
        DataRoute::SyncSharedSnapshot => heap_route!(shared_snapshot, req, ctx, host).await,
        DataRoute::DbSchemas => {
            heap_route_ctx_host!(admin_routes::execute_db_schemas_route, ctx, host).await
        }
        DataRoute::DbFlush => {
            heap_route!(admin_routes::execute_db_flush_route, req, ctx, host).await
        }
        DataRoute::DbCleanup => {
            heap_route!(admin_routes::execute_db_cleanup_route, req, ctx, host).await
        }
        DataRoute::DbNativeStatus => {
            heap_route!(admin_routes::execute_db_native_status_route, req, ctx, host).await
        }
        DataRoute::DbDeleteLedger => {
            heap_route!(
                retention_routes::execute_db_delete_ledger_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(
                retention_routes::execute_db_schema_retention_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbPutBlob => {
            heap_route!(file_blob_put::execute_db_put_blob_route, req, ctx, host).await
        }
        DataRoute::DbFetchBlob => {
            heap_route!(file_blob::execute_db_fetch_blob_route, req, ctx, host).await
        }
        DataRoute::DbDeleteBlob => {
            heap_route!(file_blob::execute_db_delete_blob_route, req, ctx, host).await
        }
    }
}

async fn shared_snapshot(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "shared snapshot is owner-only", ctx);
    }
    if req.body.len() > 8192 {
        return error_response(413, "shared snapshot request exceeds its byte limit", ctx);
    }
    let Ok(request) = serde_json::from_slice(&req.body) else {
        return error_response(400, "invalid shared snapshot request", ctx);
    };
    let budget = admin_handler_timeout().saturating_sub(Duration::from_secs(1));
    match crate::shared_snapshot::sequence::run_shared_snapshot(host, &request, budget).await {
        Ok(result) => json_ok(&envelope(&result, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}
