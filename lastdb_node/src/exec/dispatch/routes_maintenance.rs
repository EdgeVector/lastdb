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
        DataRoute::DbCompact => heap_route!(execute_db_compact_route, req, ctx, host).await,
        DataRoute::DbNativeBackup => {
            heap_route!(execute_db_native_backup_route, req, ctx, host).await
        }
        DataRoute::DbFlush => heap_route!(execute_db_flush_route, req, ctx, host).await,
        DataRoute::DbNativeStatus => {
            heap_route!(execute_db_native_status_route, req, ctx, host).await
        }
        // Retired with the LastStore engine: the native disk store keeps no
        // successor history to stamp. The route stays until an API version
        // change removes it.
        DataRoute::DbStampPurgedAtomRetirements => error_response(
            500,
            "stamp-purged-atom-retirements: the native disk store has no LastStore \
             successor history to stamp",
            ctx,
        ),
        DataRoute::DbGcProteins => heap_route!(execute_db_gc_proteins_route, req, ctx, host).await,
        // Retired with the LastStore engine: the native disk store has no hash
        // group to drop. The route stays until an API version change removes it.
        DataRoute::DbReclaimKeepSmallSnapshot => error_response(
            409,
            "reclaim-keep-small-snapshot refused: the native disk store has no \
             LastStore hash group to drop",
            ctx,
        ),
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
        DataRoute::DbResealAtRest => {
            heap_route!(execute_db_reseal_at_rest_route, req, ctx, host).await
        }
        DataRoute::DbReapUnsealed => {
            heap_route!(execute_db_reap_unsealed_route, req, ctx, host).await
        }
        DataRoute::DbPinLogAudit => {
            heap_route!(execute_db_pin_log_audit_route, req, ctx, host).await
        }
        DataRoute::DbMoleculeKeys => {
            heap_route!(execute_db_molecule_keys_route, req, ctx, host).await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(execute_db_schema_retention_route, req, ctx, host).await
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
