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
        DataRoute::DbStampPurgedAtomRetirements => {
            heap_route!(
                execute_db_stamp_purged_atom_retirements_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbGcAtoms => heap_route!(execute_db_gc_atoms_route, req, ctx, host).await,
        DataRoute::DbReapDroppedSchema => {
            heap_route!(execute_db_reap_dropped_schema_route, req, ctx, host).await
        }
        DataRoute::DbGcFileBlobs => {
            heap_route!(execute_db_gc_file_blobs_route, req, ctx, host).await
        }
        DataRoute::DbGcProteins => heap_route!(execute_db_gc_proteins_route, req, ctx, host).await,
        DataRoute::DbPurgeRefBlobs => {
            heap_route!(execute_db_purge_ref_blobs_route, req, ctx, host).await
        }
        DataRoute::DbReclaimKeepSmallLegacy => {
            heap_route!(execute_db_reclaim_keep_small_legacy_route, req, ctx, host).await
        }
        DataRoute::DbReclaimKeepSmallSnapshot => {
            heap_route!(execute_db_reclaim_keep_small_snapshot_route, req, ctx, host).await
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
        DataRoute::DbRepairDanglingTips => {
            heap_route!(execute_db_repair_dangling_tips_route, req, ctx, host).await
        }
        DataRoute::DbUnresolvedAtoms => match host.db.db_ops().unresolved_atom_identity_report() {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "unresolved_atoms": report }),
                ctx.user_id.as_str(),
            )),
            Err(error) => error_response(500, &error, ctx),
        },
        DataRoute::DbDrainTipHistory => {
            heap_route!(execute_db_drain_tip_history_route, req, ctx, host).await
        }
        DataRoute::DbRetainSupersededVersions => {
            heap_route!(execute_db_retain_superseded_versions_route, req, ctx, host).await
        }
        DataRoute::DbProbeLocatorOnly => {
            heap_route!(execute_db_probe_locator_only_route, req, ctx, host).await
        }
        DataRoute::DbDeleteLedger => {
            heap_route!(execute_db_delete_ledger_route, req, ctx, host).await
        }
        DataRoute::DbMigratePhotoBlobs => {
            heap_route!(execute_db_migrate_photo_blobs_route, req, ctx, host).await
        }
        DataRoute::DbMigrateThinTips => {
            heap_route!(execute_db_migrate_thin_tips_route, req, ctx, host).await
        }
        DataRoute::DbRekeyAtomPartitionPrefix => {
            heap_route!(execute_db_rekey_atom_partition_prefix_route, req, ctx, host).await
        }
        DataRoute::DbResealAtRest => {
            heap_route!(execute_db_reseal_at_rest_route, req, ctx, host).await
        }
        DataRoute::DbReapUnsealed => {
            heap_route!(execute_db_reap_unsealed_route, req, ctx, host).await
        }
        DataRoute::DbTombstoneFlagAudit => {
            heap_route!(execute_db_tombstone_flag_audit_route, req, ctx, host).await
        }
        DataRoute::DbDrainLegacyTombstones => {
            heap_route!(execute_db_drain_legacy_tombstones_route, req, ctx, host).await
        }
        DataRoute::DbLegacyKeyForkAudit => {
            heap_route!(execute_db_legacy_key_fork_audit_route, req, ctx, host).await
        }
        DataRoute::DbOrderLogAudit => {
            heap_route!(execute_db_order_log_audit_route, req, ctx, host).await
        }
        DataRoute::DbPinLogAudit => {
            heap_route!(execute_db_pin_log_audit_route, req, ctx, host).await
        }
        DataRoute::DbExportMutationLog => {
            heap_route!(execute_db_export_mutation_log_route, req, ctx, host).await
        }
        DataRoute::DbReplayMutationLog => {
            heap_route!(execute_db_replay_mutation_log_route, req, ctx, host).await
        }
        DataRoute::DbOrderLogBloatAudit => {
            heap_route!(execute_db_order_log_bloat_audit_route, req, ctx, host).await
        }
        DataRoute::DbCompactOrderLog => {
            heap_route!(execute_db_compact_order_log_route, req, ctx, host).await
        }
        DataRoute::DbRepairOrderLogShortfall => {
            heap_route!(execute_db_repair_order_log_shortfall_route, req, ctx, host).await
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
        DataRoute::DbDrainPlaneResidue => {
            heap_route!(execute_db_drain_plane_residue_route, req, ctx, host).await
        }
        DataRoute::DbFetchFileBlob => {
            heap_route!(execute_db_fetch_file_blob_route, req, ctx, host).await
        }
        DataRoute::DbForkFileBlob => {
            heap_route!(execute_db_fork_file_blob_route, req, ctx, host).await
        }
        DataRoute::DbPutFileBlob => {
            heap_route!(execute_db_put_file_blob_route, req, ctx, host).await
        }
        DataRoute::DbPutBlobLocal => {
            heap_route!(execute_db_put_blob_local_route, req, ctx, host).await
        }
        _ => return None,
    })
}
