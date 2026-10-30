use super::super::*;

/// Garbage collection, reclaim and purge routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_maintenance_gc(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbClearHistory => {
            heap_route!(execute_db_clear_history_route, req, ctx, host).await
        }
        DataRoute::DbFlush => heap_route!(execute_db_flush_route, req, ctx, host).await,
        DataRoute::DbCleanup => heap_route!(execute_db_cleanup_route, req, ctx, host).await,
        DataRoute::DbNativeStatus => {
            heap_route!(execute_db_native_status_route, req, ctx, host).await
        }
        _ => return None,
    })
}

/// Repair, migration and audit routes.
/// Returns `None` when the route belongs to another group.
pub(super) async fn dispatch_maintenance_repair(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::DbUnresolvedAtoms => match host.db.db_ops().unresolved_atom_identity_report() {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "unresolved_atoms": report }),
                ctx.user_id.as_str(),
            )),
            Err(error) => error_response(500, &error, ctx),
        },
        DataRoute::DbDeleteLedger => {
            heap_route!(execute_db_delete_ledger_route, req, ctx, host).await
        }
        DataRoute::DbMoleculeKeys => {
            heap_route!(execute_db_molecule_keys_route, req, ctx, host).await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(execute_db_schema_retention_route, req, ctx, host).await
        }
        DataRoute::DbAlternateKeys => {
            heap_route!(execute_db_alternate_keys_route, req, ctx, host).await
        }
        DataRoute::DbRepairSchemaMoleculeMap => {
            heap_route!(execute_db_repair_schema_molecule_map_route, req, ctx, host).await
        }
        DataRoute::DbRepairHashRangeKeyFields => {
            heap_route!(execute_db_repair_hashrange_key_fields_route, req, ctx, host).await
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
