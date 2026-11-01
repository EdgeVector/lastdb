use super::super::*;

/// Flush, cleanup and native status routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_maintenance_gc(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbFlush => heap_route!(execute_db_flush_route, req, ctx, host).await,
        DataRoute::DbCleanup => heap_route!(execute_db_cleanup_route, req, ctx, host).await,
        DataRoute::DbNativeStatus => {
            heap_route!(execute_db_native_status_route, req, ctx, host).await
        }
        _ => return None,
    })
}

/// Delete ledger and schema retention routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_maintenance_repair(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbDeleteLedger => {
            heap_route!(execute_db_delete_ledger_route, req, ctx, host).await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(execute_db_schema_retention_route, req, ctx, host).await
        }
        _ => return None,
    })
}

/// File blob and plane residue routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_blob_identity(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbFetchFileBlob => {
            heap_route!(execute_db_fetch_file_blob_route, req, ctx, host).await
        }
        DataRoute::DbPutBlobLocal => {
            heap_route!(execute_db_put_blob_local_route, req, ctx, host).await
        }
        _ => return None,
    })
}
