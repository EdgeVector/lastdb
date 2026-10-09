use super::*;

/// Execute a recognized control-socket data route against the minimal host,
/// with the caller's user id bound as the task-local user context (parity with
/// the full node's `run_route_with_user`, so background indexing spawned by a
/// mutation is attributed to the owner).
///
/// Every completed route is recorded into [`crate::request_telemetry`] using
/// the self-reported `X-LastDB-Client` header (or `X-App-Id` fallback), then
/// the schema owner, then the UDS peer process — so an unlabeled caller is
/// still nameable in `lastdb ops`.
// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub async fn execute_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let user_hash = ctx.user_id.clone();
    // Read the peer identity BEFORE dispatch. A short-lived caller (a `curl`
    // in a poll loop) can exit while a slow route is still running, and then
    // the process-name lookup returns `None` for exactly the requests worth
    // attributing. The pid itself is kernel-set at accept time and stays valid
    // either way.
    let peer_pid = ctx.peer_pid;
    let peer_comm = peer_pid.and_then(crate::request_telemetry::peer_comm_from_pid);
    let client = crate::request_telemetry::client_from_request(req);
    let request_id = crate::request_telemetry::request_id_from_request(req);
    let kind = op_kind_for_route(route);
    let schema = schema_hint_for_route(route, req);
    let schema_owner = if client == "unknown" {
        schema_owner_for_hint(host, schema.as_deref())
    } else {
        None
    };
    let client = crate::request_telemetry::client_or_schema_owner(client, schema_owner.as_deref());
    // Last rung: name the process on the other end of the socket. Only a
    // request whose peer pid the kernel would not report stays `"unknown"`.
    let client = crate::request_telemetry::client_or_peer(client, peer_pid, peer_comm.as_deref());
    let body_bytes = req.body.len() as u64;
    // Socket-queue wait, for EVERY op kind: this future is polled by
    // `Handle::block_on` on the UDS worker thread that picked the job up, so
    // the pool's thread-local is readable right here. `None` off a worker
    // thread (tests, direct calls) leaves the phase unset.
    let queue_wait_us = lastdb_uds::worker_pool::current_queue_wait()
        .map_or(0, fold_db::request_phases::duration_us);
    let started = std::time::Instant::now();
    // One relaxed atomic load — see NamespacedStore::cold_shard_loads, which is
    // deliberately the lock-free half of the read-cost pair so it can live here.
    let loads_before = host.db.db_ops().cold_shard_loads();

    // Phase accumulator scope: mutation-path recording sites (parse here in
    // exec, resolve/validate/admission/index-wait in `lastdb_host::handlers`,
    // apply/persist/flush lifted from fold_db core's timing breakdown) add
    // into this request's totals. Non-mutation routes add nothing, so their
    // samples keep an absent phase set by construction.
    let (resp, phase_totals, work_counts, tip_keys) =
        fold_db::request_phases::run_with_phases_and_keys(fold_db::user_context::run_with_user(
            &user_hash,
            async move { dispatch_data_route(route, req, ctx, host).await },
        ))
        .await;

    // Skip recording the status probe itself when it is the ops dashboard
    // poll — still record it, but keep schema empty. Always record so
    // lastdb-status callers show up if they hammer the node.
    let duration_ms = started.elapsed().as_millis() as u64;
    let rows = crate::request_telemetry::rows_from_response_body(resp.status, &resp.body);
    // `saturating_sub` rather than a wrap: the counter is monotonic, so a
    // negative delta is impossible unless the store was reopened mid-request,
    // in which case 0 is the honest answer rather than a garbage spike.
    let cold_shard_loads = loads_before.and_then(|before| {
        host.db
            .db_ops()
            .cold_shard_loads()
            .map(|after| after.saturating_sub(before))
    });
    let path = Some(crate::request_telemetry::path_from_request(
        req.method.as_str(),
        req.target.as_str(),
    ));
    let route_label = Some(route_label(route));
    let uds_snapshot = host
        .uds_workers
        .get()
        .map(lastdb_uds::UdsWorkerPool::snapshot);
    host.request_telemetry.record_with_tip_keys(
        crate::request_telemetry::OpSample {
            ts_ms: fold_db::clock::unix_millis(),
            client,
            request_id,
            kind,
            schema,
            duration_ms,
            status: resp.status,
            rows,
            body_bytes,
            cold_shard_loads,
            phases: crate::request_telemetry::PhaseTimings {
                queue_wait_us,
                admission_wait_us: phase_totals.admission_wait_us,
                parse_us: phase_totals.parse_us,
                schema_resolve_us: phase_totals.schema_resolve_us,
                validate_us: phase_totals.validate_us,
                lock_wait_us: phase_totals.lock_wait_us,
                purge_barrier_us: phase_totals.purge_barrier_us,
                purge_plan_us: phase_totals.purge_plan_us,
                purge_commit_us: phase_totals.purge_commit_us,
                purge_materialize_us: phase_totals.purge_materialize_us,
                purge_trace_us: phase_totals.purge_trace_us,
                purge_retention_guard_us: phase_totals.purge_retention_guard_us,
                purge_delete_us: phase_totals.purge_delete_us,
                purge_finalize_us: phase_totals.purge_finalize_us,
                molecule_gate_us: phase_totals.molecule_gate_us,
                cas_precondition_us: phase_totals.cas_precondition_us,
                count_us: phase_totals.count_us,
                hydrate_us: phase_totals.hydrate_us,
                hydrate_atoms_us: phase_totals.hydrate_atoms_us,
                hydrate_format_us: phase_totals.hydrate_format_us,
                hydrate_sort_us: phase_totals.hydrate_sort_us,
                hydrate_filter_us: phase_totals.hydrate_filter_us,
                annotate_us: phase_totals.annotate_us,
                apply_us: phase_totals.apply_us,
                apply_memory_us: phase_totals.apply_memory_us,
                schema_load_us: phase_totals.schema_load_us,
                dedupe_scan_us: phase_totals.dedupe_scan_us,
                idempotency_check_us: phase_totals.idempotency_check_us,
                grouping_us: phase_totals.grouping_us,
                restore_molecules_us: phase_totals.restore_molecules_us,
                protein_sibling_fold_us: phase_totals.protein_sibling_fold_us,
                spawn_indexing_us: phase_totals.spawn_indexing_us,
                sync_uuids_us: phase_totals.sync_uuids_us,
                schema_reload_us: phase_totals.schema_reload_us,
                persist_us: phase_totals.persist_us,
                persist_molecules_us: phase_totals.persist_molecules_us,
                persist_schema_us: phase_totals.persist_schema_us,
                persist_idempotency_us: phase_totals.persist_idempotency_us,
                flush_us: phase_totals.flush_us,
                sync_capture_us: phase_totals.sync_capture_us,
                index_wait_us: phase_totals.index_wait_us,
                change_record_us: phase_totals.change_record_us,
                change_record_lock_wait_us: phase_totals.change_record_lock_wait_us,
                change_record_write_us: phase_totals.change_record_write_us,
                response_envelope_us: phase_totals.response_envelope_us,
                status_sync_us: phase_totals.status_sync_us,
                status_backup_us: phase_totals.status_backup_us,
                status_durability_us: phase_totals.status_durability_us,
                status_data_dir_us: phase_totals.status_data_dir_us,
                status_request_ops_us: phase_totals.status_request_ops_us,
            },
            molecules_persisted: work_counts.molecules_persisted,
            partition_read_rejections: work_counts.partition_read_rejections,
            all_group_walks: work_counts.all_group_walks,
            molecule_store_commits: work_counts.molecule_store_commits,
            resident_commits: work_counts.resident_commits,
            resident_operations: work_counts.resident_operations,
            uds_in_flight: uds_snapshot.map(|snap| snap.in_flight),
            uds_workers: uds_snapshot.map(|snap| snap.workers),
            uds_queue_capacity: uds_snapshot.map(|snap| snap.queue_capacity),
            path,
            route: route_label,
            peer_pid,
            peer_comm,
        },
        &tip_keys,
    );
    resp
}

/// Bounded identity label for the route the router already matched.
///
/// `OpSample::path` is the raw target, so it cannot be an aggregation key:
/// `/api/atom/<id>`, `/api/history/<key>`, `/api/schema/<name>` and
/// `/api/app/blob/cas/sha256/<digest>` put caller data in the path and would
/// mint one telemetry key per atom, per key, per schema and per digest.
/// [`DataRoute`] is the opposite by construction — its own doc says the
/// variant "carries only route identity, with no caller data" — so the set of
/// labels is bounded by the route table and a label is safe in a key.
///
/// The label is the variant name. It is derived rather than hand-written on
/// purpose: a hand-written table next to [`op_kind_for_route`] is a second
/// route table, and a route added without touching it would silently land in
/// whatever bucket the fallback names — which is the defect this label exists
/// to end.
pub(super) fn route_label(route: DataRoute) -> String {
    format!("{route:?}")
}

// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub(super) async fn dispatch_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match route {
        DataRoute::Query => heap_route!(execute_query_route, req, ctx, host).await,
        DataRoute::QueryBatch => heap_route!(execute_query_batch_route, req, ctx, host).await,
        DataRoute::Mutation => heap_route!(execute_mutation_route, req, ctx, host).await,
        DataRoute::AggregateRepair => {
            heap_route!(execute_aggregate_repair_route, req, ctx, host).await
        }
        DataRoute::AggregateFinalize => {
            heap_route!(execute_aggregate_finalize_route, req, ctx, host).await
        }
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
        DataRoute::Status => heap_route!(execute_status_route, req, ctx, host).await,
        DataRoute::AdminShed => heap_route_ctx_host!(execute_admin_shed_route, ctx, host).await,
        DataRoute::LogFilterGet => execute_log_filter_get_route(ctx),
        DataRoute::LogFilterSet => execute_log_filter_set_route(req, ctx),
        DataRoute::DbInventory => heap_route_ctx_host!(execute_db_inventory_route, ctx, host).await,
        DataRoute::DbSchemas => heap_route_ctx_host!(execute_db_schemas_route, ctx, host).await,
        DataRoute::SchemaStorage => execute_schema_storage_route(req, ctx, host),
        DataRoute::SchemaStorageReport => execute_schema_storage_report_route(ctx, host),
        DataRoute::LivenessExplain => {
            heap_route!(execute_liveness_explain_route, req, ctx, host).await
        }
        DataRoute::LivenessBootstrap => {
            heap_route!(execute_liveness_bootstrap_route, req, ctx, host).await
        }
        DataRoute::DbClearHistory => {
            heap_route!(execute_db_clear_history_route, req, ctx, host).await
        }
        DataRoute::DbCompact => heap_route!(execute_db_compact_route, req, ctx, host).await,
        DataRoute::DbStampPurgedAtomRetirements => {
            heap_route!(
                execute_db_stamp_purged_atom_retirements_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::CompactRecord => heap_route!(execute_compact_record_route, req, ctx, host).await,
        DataRoute::DbPurgeSchemaIdx => {
            heap_route_ctx_host!(execute_db_purge_schemaidx_route, ctx, host).await
        }
        DataRoute::AppStorage => heap_route_ctx_host!(execute_app_storage_route, ctx, host).await,
        DataRoute::AppStorageReconcile => {
            heap_route!(execute_app_storage_reconcile_route, req, ctx, host).await
        }
        DataRoute::HomeStorage => heap_route_ctx_host!(execute_home_storage_route, ctx, host).await,
        DataRoute::HomeStorageReconcile => {
            heap_route!(execute_home_storage_reconcile_route, req, ctx, host).await
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
        DataRoute::AutoIdentity => execute_auto_identity_route(host),
        DataRoute::BootIdentity => execute_boot_identity_route(host),
        DataRoute::BootLedger => execute_boot_ledger_route(host),
        DataRoute::NativeIndexSearch => execute_native_index_search_route(req, ctx, host),
        DataRoute::SearchAppQuery => execute_search_app_query_route(req, ctx, host),
        DataRoute::AppSearch => heap_route!(execute_app_search_route, req, ctx, host).await,
        DataRoute::NativeIndexAppVectorPut => {
            heap_route!(execute_native_index_app_vector_put_route, req, ctx, host).await
        }
        DataRoute::NativeIndexKnn => {
            heap_route!(execute_native_index_knn_route, req, ctx, host).await
        }
        DataRoute::MoleculeHistory => {
            heap_route!(execute_molecule_history_route, req, ctx, host).await
        }
        DataRoute::AtomContent => heap_route!(execute_atom_content_route, req, ctx, host).await,
        DataRoute::ProteinGet => heap_route!(execute_protein_get_route, req, ctx, host).await,
        DataRoute::ProteinOfMolecule => {
            heap_route!(execute_protein_of_molecule_route, req, ctx, host).await
        }
        // No upload-storage backend in the minimal daemon; blob routes are
        // structurally absent — content-free 404, exactly like an
        // unrecognized path (I4).
        DataRoute::AppBlobPut
        | DataRoute::AppBlobGet
        | DataRoute::AppOrgBlobPut
        | DataRoute::AppOrgBlobGet
        | DataRoute::AppBlobFootprint => content_free(404, "Not Found"),
        DataRoute::OrgSyncRegister => execute_org_sync_register_route(req, ctx, host).await,
        DataRoute::DbCatalogGet => heap_route!(execute_db_catalog_get_route, req, ctx, host).await,
        DataRoute::DbCatalogPut => heap_route!(execute_db_catalog_put_route, req, ctx, host).await,
        DataRoute::DbCatalogDelete => {
            heap_route!(execute_db_catalog_delete_route, req, ctx, host).await
        }
        DataRoute::DbCatalogShare => {
            heap_route!(execute_db_catalog_share_route, req, ctx, host).await
        }
        DataRoute::DbCatalogReclaim => {
            heap_route!(execute_db_catalog_reclaim_route, req, ctx, host).await
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
        DataRoute::LocalWatch => execute_local_watch_route(req, ctx, host),
        DataRoute::AppChanges => heap_route!(execute_app_changes_route, req, ctx, host).await,
    }
}

pub(super) fn schema_owner_for_hint(host: &Host, schema: Option<&str>) -> Option<String> {
    let hinted = schema?;
    let canonical = handlers::resolve_schema_name(host, hinted)
        .ok()
        .flatten()
        .unwrap_or_else(|| hinted.to_string());
    host.db
        .schema_manager()
        .get_schema_metadata(&canonical)
        .ok()
        .flatten()
        .and_then(|schema| schema.owner_app_id)
        .filter(|owner| !owner.trim().is_empty())
}

pub(super) fn op_kind_for_route(route: DataRoute) -> crate::request_telemetry::OpKind {
    use crate::request_telemetry::OpKind;
    match route {
        // `ProteinGet` / `ProteinOfMolecule` are reads and must rank as such:
        // counting them as mutations inflated `kanban verb=mutation` and made
        // the offender tables name the wrong op class during triage.
        DataRoute::Query
        | DataRoute::ListRecordKeys
        | DataRoute::ProteinGet
        | DataRoute::ProteinOfMolecule => OpKind::Query,
        DataRoute::Mutation | DataRoute::AggregateRepair | DataRoute::AggregateFinalize => {
            OpKind::Mutation
        }
        DataRoute::MutationBatch => OpKind::MutationBatch,
        DataRoute::QueryBatch => OpKind::QueryBatch,
        DataRoute::Status
        | DataRoute::AutoIdentity
        | DataRoute::BootIdentity
        | DataRoute::BootLedger => OpKind::Status,
        DataRoute::ListSchemas
        | DataRoute::GetSchema
        | DataRoute::DeclareSchema
        | DataRoute::SeedSystemSchema
        | DataRoute::RetireSchemaNameClaim
        | DataRoute::DropSchema => OpKind::Schema,
        DataRoute::NativeIndexSearch | DataRoute::SearchAppQuery | DataRoute::AppSearch => {
            OpKind::Search
        }
        DataRoute::MoleculeHistory | DataRoute::AppChanges => OpKind::History,
        DataRoute::AtomContent => OpKind::Atom,
        DataRoute::DeliverStage
        | DataRoute::DeliverSnapshot
        | DataRoute::DeliverList
        | DataRoute::DeliverApprove
        | DataRoute::DeliverReject => OpKind::Deliver,
        // Idle long-polls: wall time is mostly sleep, not DB work. Split so
        // `lastdb ops` does not rank them as opaque lastgit "other" latency.
        DataRoute::LocalWatch => OpKind::LocalWatch,
        DataRoute::DbPutFileBlob
        | DataRoute::DbPutBlobLocal
        | DataRoute::DbFetchFileBlob
        | DataRoute::DbForkFileBlob => OpKind::FileBlob,
        _ => OpKind::Other,
    }
}

pub(super) fn schema_hint_for_route(route: DataRoute, req: &UdsRequest) -> Option<String> {
    match route {
        DataRoute::Query | DataRoute::Mutation => {
            crate::request_telemetry::schema_from_body(&req.body)
        }
        DataRoute::AggregateRepair | DataRoute::AggregateFinalize => {
            serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|body| {
                    body.get("target_schema_name")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        }
        DataRoute::MutationBatch => crate::request_telemetry::schema_from_batch_body(&req.body),
        DataRoute::QueryBatch => query_batch::schema_hint(&req.body),
        DataRoute::DbForkFileBlob | DataRoute::DbPutFileBlob => {
            serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|body| {
                    body.get("schema")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        }
        DataRoute::GetSchema => {
            // /api/schema/{name} — single path segment only.
            path_tail(req.target.as_str(), "/api/schema/")
        }
        DataRoute::ListRecordKeys => query_value(&req.target, "schema"),
        DataRoute::CompactRecord => {
            serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|body| {
                    body.get("schema")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        }
        _ => None,
    }
}
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
