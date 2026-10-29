use super::super::*;

/// Local database catalog routes.
pub(super) async fn dispatch_org_catalog(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbCatalogGet => heap_route!(execute_db_catalog_get_route, req, ctx, host).await,
        DataRoute::DbCatalogPut => heap_route!(execute_db_catalog_put_route, req, ctx, host).await,
        DataRoute::DbCatalogDelete => {
            heap_route!(execute_db_catalog_delete_route, req, ctx, host).await
        }
        DataRoute::DbCatalogShare => {
            heap_route!(execute_db_catalog_share_route, req, ctx, host).await
        }
        _ => return None,
    })
}

/// Explicit byte backup and local slice export operations.
pub(super) async fn dispatch_sync_deliver(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::SyncByteBackupV2 => {
            super::super::byte_backup_routes::execute(req, ctx, host).await
        }
        DataRoute::SyncByteBackupV2Connect => {
            super::super::byte_backup_routes::connect(req, ctx, host).await
        }
        DataRoute::SyncLogicalV1 => {
            heap_route!(super::super::logical_sync_routes::execute, req, ctx, host).await
        }
        DataRoute::DeliverSnapshot => {
            heap_route!(crate::deliver::execute_publish_snapshot, req, ctx, host).await
        }
        _ => return None,
    })
}
