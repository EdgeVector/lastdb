use super::super::*;

/// Org authority, org sync and database catalog routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_org_catalog(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::OrgAuthorityPublish => {
            heap_route!(execute_org_authority_publish_route, req, ctx, host).await
        }
        DataRoute::OrgAuthorityStatus => {
            heap_route!(execute_org_authority_status_route, req, ctx, host).await
        }
        DataRoute::OrgSyncRegister => {
            execute_org_sync_register_admitted_route(req, ctx, host).await
        }
        DataRoute::DbCatalogGet => heap_route!(execute_db_catalog_get_route, req, ctx, host).await,
        DataRoute::DbCatalogPut => heap_route!(execute_db_catalog_put_route, req, ctx, host).await,
        DataRoute::DbCatalogDelete => {
            heap_route!(execute_db_catalog_delete_route, req, ctx, host).await
        }
        DataRoute::DbCatalogShare => {
            heap_route!(execute_db_catalog_share_route, req, ctx, host).await
        }
        DataRoute::OrgSyncTargets => {
            heap_route_ctx_host!(execute_org_sync_targets_route, ctx, host).await
        }
        DataRoute::OrgSyncDeactivate => {
            heap_route!(execute_org_sync_deactivate_route, req, ctx, host).await
        }
        DataRoute::OrgSyncGrantMember => {
            heap_route!(execute_org_sync_grant_member_route, req, ctx, host).await
        }
        DataRoute::OrgSyncRevokeMember => {
            heap_route!(execute_org_sync_revoke_member_route, req, ctx, host).await
        }
        _ => return None,
    })
}

/// Cloud sync and delivery routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_sync_deliver(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::SyncHealStaging => {
            heap_route_ctx_host!(execute_sync_heal_staging_route, ctx, host).await
        }
        DataRoute::SyncLastStoreSnapshot => execute_sync_laststore_snapshot_route(ctx, host).await,
        DataRoute::SyncBackupGc => execute_sync_backup_gc_route(req, ctx, host),
        DataRoute::SyncPrefixInventory => {
            heap_route_ctx_host!(execute_sync_prefix_inventory_route, ctx, host).await
        }
        DataRoute::SyncCloudOff => {
            heap_route_ctx_host!(execute_sync_cloud_off_route, ctx, host).await
        }
        DataRoute::SyncCloudOn => {
            heap_route_ctx_host!(execute_sync_cloud_on_route, ctx, host).await
        }
        DataRoute::SyncCloudResumePrimary => {
            execute_sync_cloud_resume_primary_route(req, ctx, host)
        }
        DataRoute::SyncQuarantineReplayBlocker => {
            heap_route!(execute_sync_quarantine_replay_blocker_route, req, ctx, host).await
        }
        DataRoute::SyncBackupConcurrency => {
            heap_route!(execute_sync_backup_concurrency_route, req, ctx, host).await
        }
        DataRoute::DeliverStage => {
            heap_route!(crate::deliver::execute_stage_delivery, req, ctx, host).await
        }
        DataRoute::RecipientAddress => {
            heap_route_ctx_host!(crate::deliver::execute_recipient_address, ctx, host).await
        }
        DataRoute::DeliverSnapshot => {
            heap_route!(crate::deliver::execute_publish_snapshot, req, ctx, host).await
        }
        DataRoute::DeliverList => {
            heap_route_ctx_host!(crate::deliver::execute_list_deliveries, ctx, host).await
        }
        DataRoute::DeliverApprove => {
            heap_delivery!(
                "approve",
                crate::deliver::execute_approve_delivery,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DeliverReject => {
            heap_delivery!(
                "reject",
                crate::deliver::execute_reject_delivery,
                req,
                ctx,
                host
            )
            .await
        }
        _ => return None,
    })
}
