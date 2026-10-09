use super::*;

/// `GET /api/storage/app` — live storage grouped by `owner_app_id`.
///
/// Scan-free by construction: one point read of the in-process keep-small
/// projection, one point read of the reconciliation checkpoint, then one
/// in-memory `schema_manager` metadata lookup per metered schema. No
/// collection walk, no prefix scan — `lastdb db inventory` is the heavy tool
/// and this route is deliberately not it.
pub(super) async fn execute_app_storage_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    use crate::app_storage::{
        build_report_full, owner_from_catalog, resolve_with_overlay, MetersOrigin, OwnerResolution,
        RECONCILE_CHECKPOINT_KEY,
    };

    let meters = host.db.db_ops().atoms().keep_small();
    let snapshot = meters.export();
    let hydrate_missed = meters.hydrate_missed();
    let stale_after_unclean_stop = meters.stale_after_unclean_stop();
    let checkpoint: crate::app_storage::ReconciliationCheckpoint = host
        .db
        .db_ops()
        .metadata()
        .get_typed(RECONCILE_CHECKPOINT_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let schema_manager = host.db.schema_manager();
    // Both already-held in-memory maps, so this stays scan-free: a hydrate
    // miss on a home that has schemas means the totals below are a floor, not
    // a measurement, and the report must not call itself complete.
    let catalog_non_empty = !schema_manager
        .get_schema_list_entries_with_states()
        .unwrap_or_default()
        .is_empty();
    let incomplete_reason = meters
        .trust_incomplete_cause()
        .or_else(|| meters.incomplete_reason().map(str::to_string));
    let mut report = build_report_full(
        &snapshot,
        &|schema: &str| {
            let live = match schema_manager.get_schema_metadata(schema) {
                // A registered schema with no owner is a core plane, not a miss.
                Ok(Some(schema)) => owner_from_catalog(schema.owner_app_id.as_deref()),
                // Unknown or unreadable: overlay may have recovered the owner
                // via a durable point-get on a prior reconcile page.
                Ok(None) | Err(_) => OwnerResolution::Unknown,
            };
            resolve_with_overlay(schema, live, &checkpoint.overlay)
        },
        &checkpoint,
        MetersOrigin::from_trust(
            meters.global_trust_state(),
            hydrate_missed,
            stale_after_unclean_stop,
            catalog_non_empty,
        ),
    );
    report.incomplete_reason = incomplete_reason;
    json_ok(&envelope(
        &serde_json::json!({ "app_storage": report }),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/storage/home` — one persisted snapshot point-read.
///
/// The explicit reconcile route owns filesystem and ownership work. This read
/// validates only in-memory arithmetic on the decoded snapshot, so it cannot
/// turn an operator status request into a filesystem or product-store scan.
pub(super) async fn execute_home_storage_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    use crate::home_storage::{HomeStorageReport, HOME_STORAGE_SNAPSHOT_KEY};

    let report = match host
        .db
        .db_ops()
        .metadata()
        .get_typed::<HomeStorageReport>(HOME_STORAGE_SNAPSHOT_KEY)
        .await
    {
        Ok(Some(report)) => report.validated_for_read(),
        Ok(None) => HomeStorageReport::missing(),
        Err(error) => {
            return error_response(
                500,
                &format!("home storage snapshot read failed: {error}"),
                ctx,
            )
        }
    };
    json_ok(&envelope(
        &serde_json::json!({ "home_storage": report }),
        ctx.user_id.as_str(),
    ))
}

/// Build the logical app ledger from the in-memory schema catalog and
/// write-path molecule counters. This reads no atom, tip, or filesystem plane.
pub(super) fn collect_home_app_attribution(
    host: &Host,
) -> crate::home_storage::HomeStorageInclusiveAppAttribution {
    use crate::home_storage::{
        build_inclusive_app_attribution, AppLogicalBinding, AppLogicalMolecule,
    };

    let meters = host.db.db_ops().atoms().keep_small();
    let counters_complete = meters.molecule_counters_complete();
    let pending_protein_folds = meters.pending_protein_folds();
    let schema_manager = host.db.schema_manager();
    let mut unresolved = Vec::new();
    let entries = if let Ok(entries) = schema_manager.get_schema_list_entries_with_states() {
        entries
    } else {
        unresolved.push("schema_catalog_unavailable".to_string());
        Vec::new()
    };
    let mut bindings = Vec::with_capacity(entries.len());
    for entry in entries {
        let Ok(Some(schema)) = schema_manager.get_schema_metadata(&entry.name) else {
            unresolved.push(format!("schema_metadata_unavailable:{}", entry.name));
            continue;
        };
        let mut molecule_ids: Vec<String> = schema
            .runtime_fields
            .values()
            .filter_map(|field| field.common().molecule_uuid().cloned())
            .collect();
        molecule_ids.sort_unstable();
        molecule_ids.dedup();
        let molecules = molecule_ids
            .into_iter()
            .map(|molecule_id| {
                let logical_bytes = meters.molecule_counter(&molecule_id).map(|counter| {
                    counter
                        .logical_value_bytes()
                        .saturating_add(counter.structure_bytes())
                });
                AppLogicalMolecule {
                    molecule_id,
                    logical_bytes,
                }
            })
            .collect();
        bindings.push(AppLogicalBinding {
            schema_binding: schema.name,
            app_id: schema.owner_app_id.filter(|owner| !owner.trim().is_empty()),
            molecules,
        });
    }
    build_inclusive_app_attribution(
        &bindings,
        counters_complete,
        pending_protein_folds,
        unresolved,
        chrono::Utc::now(),
    )
}

/// `POST /api/storage/home/reconcile` — one bounded filesystem page.
///
/// The durable checkpoint contains the remaining path queue. Each request
/// measures at most the requested number of entries, writes an honest partial
/// snapshot, and returns the opaque next cursor. Symlinks are never followed.
pub(super) async fn execute_home_storage_reconcile_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use crate::home_storage::{
        reconcile_page, HomeStorageReconcileCheckpoint, HOME_STORAGE_RECONCILE_KEY,
        HOME_STORAGE_RECONCILE_WORK_DEFAULT, HOME_STORAGE_RECONCILE_WORK_MAX,
        HOME_STORAGE_SNAPSHOT_KEY,
    };

    #[derive(Deserialize, Default)]
    struct ReconcileBody {
        work_budget: Option<u64>,
        page_size: Option<u64>,
        #[serde(default)]
        reset: bool,
    }

    let body = match serde_json::from_slice::<ReconcileBody>(&req.body) {
        Ok(body) => body,
        Err(_) if req.body.is_empty() => ReconcileBody::default(),
        Err(error) => return error_response(400, &format!("invalid reconcile body: {error}"), ctx),
    };
    if body.work_budget.is_some() && body.page_size.is_some() {
        return error_response(400, "set only one of work_budget or page_size", ctx);
    }
    let work_budget = body
        .work_budget
        .or(body.page_size)
        .unwrap_or(HOME_STORAGE_RECONCILE_WORK_DEFAULT as u64)
        .clamp(1, HOME_STORAGE_RECONCILE_WORK_MAX as u64) as usize;
    let metadata = host.db.db_ops().metadata();
    let stored = if body.reset {
        None
    } else {
        match metadata
            .get_typed::<HomeStorageReconcileCheckpoint>(HOME_STORAGE_RECONCILE_KEY)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                return error_response(
                    500,
                    &format!("home reconcile checkpoint read failed: {error}"),
                    ctx,
                )
            }
        }
    };
    let mut checkpoint = match stored {
        Some(checkpoint) if !checkpoint.finished => checkpoint,
        _ => HomeStorageReconcileCheckpoint::start(chrono::Utc::now()),
    };
    let visited = reconcile_page(&host.home, &mut checkpoint, work_budget);
    let report = checkpoint
        .report(chrono::Utc::now())
        .with_inclusive_app_attribution(collect_home_app_attribution(host));

    if let Err(error) = metadata
        .put_typed_durable(HOME_STORAGE_RECONCILE_KEY, &checkpoint)
        .await
    {
        return error_response(
            500,
            &format!("persist home reconcile checkpoint: {error}"),
            ctx,
        );
    }
    if let Err(error) = metadata
        .put_typed_durable(HOME_STORAGE_SNAPSHOT_KEY, &report)
        .await
    {
        return error_response(500, &format!("persist home storage snapshot: {error}"), ctx);
    }

    json_ok(&envelope(
        &serde_json::json!({
            "reconciliation": {
                "run_id": checkpoint.run_id,
                "visited": visited as u64,
                "visited_total": checkpoint.visited_entries,
                "work_budget": work_budget as u64,
                "pending_entries": checkpoint.pending_work.len() as u64,
                "resume_cursor": checkpoint.resume_cursor(),
                "finished": checkpoint.finished,
                "complete": report.complete,
                "unresolved_scopes": report.unresolved_scopes.clone(),
                "report": report,
            }
        }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/storage/app/reconcile` — one bounded page of declared layouts.
///
/// Pages in-memory catalog names plus keep-small meter names (already held
/// maps). Each name is a cache lookup, then at most one durable catalog
/// point-get. Never `get_all_schemas` / a collection scan.
pub(super) async fn execute_app_storage_reconcile_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use crate::app_storage::{
        apply_page, declared_layout_names, next_page, owner_from_catalog, OwnerResolution,
        ReconciliationCheckpoint, RECONCILE_CHECKPOINT_KEY, RECONCILE_PAGE_SIZE,
        RECONCILE_PAGE_SIZE_MAX,
    };

    #[derive(Deserialize, Default)]
    struct ReconcileBody {
        page_size: Option<u64>,
    }

    let page_size = match serde_json::from_slice::<ReconcileBody>(&req.body) {
        Ok(body) => body
            .page_size
            .unwrap_or(RECONCILE_PAGE_SIZE as u64)
            .clamp(1, RECONCILE_PAGE_SIZE_MAX as u64) as usize,
        Err(_) if req.body.is_empty() => RECONCILE_PAGE_SIZE,
        Err(e) => return error_response(400, &format!("invalid reconcile body: {e}"), ctx),
    };

    let snapshot = host.db.db_ops().atoms().keep_small().export();
    let checkpoint = host
        .db
        .db_ops()
        .metadata()
        .get_typed::<ReconciliationCheckpoint>(RECONCILE_CHECKPOINT_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let schema_manager = host.db.schema_manager();
    let declared: Vec<String> = schema_manager
        .get_schema_list_entries_with_states()
        .unwrap_or_default()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    let layouts = declared_layout_names(declared, snapshot.schemas.keys().cloned());
    let page = next_page(&layouts, checkpoint.resume_after.as_deref(), page_size).to_vec();

    let mut resolutions: std::collections::HashMap<String, OwnerResolution> =
        std::collections::HashMap::new();
    for name in &page {
        let live = match schema_manager.get_schema_metadata(name) {
            Ok(Some(schema)) => owner_from_catalog(schema.owner_app_id.as_deref()),
            Ok(None) | Err(_) => OwnerResolution::Unknown,
        };
        if !matches!(live, OwnerResolution::Unknown) {
            resolutions.insert(name.clone(), live);
            continue;
        }
        // Cache miss: one durable point-get. Absence stays Unknown.
        let durable = match host.db.db_ops().get_schema(name).await {
            Ok(Some(schema)) => owner_from_catalog(schema.owner_app_id.as_deref()),
            Ok(None) | Err(_) => OwnerResolution::Unknown,
        };
        resolutions.insert(name.clone(), durable);
    }

    let updated = apply_page(
        checkpoint,
        &page,
        &layouts,
        &|name: &str| {
            resolutions
                .get(name)
                .cloned()
                .unwrap_or(OwnerResolution::Unknown)
        },
        chrono::Utc::now(),
    );
    if let Err(e) = host
        .db
        .db_ops()
        .metadata()
        .put_typed(RECONCILE_CHECKPOINT_KEY, &updated)
        .await
    {
        return error_response(500, &format!("persist reconcile checkpoint: {e}"), ctx);
    }

    json_ok(&envelope(
        &serde_json::json!({
            "reconciliation": {
                "visited": page.len() as u64,
                "layouts": layouts.len() as u64,
                "page_size": page_size as u64,
                "resume_after": updated.resume_after,
                "last_pass_at": updated.last_pass_at,
                "overlay_entries": updated.overlay.len() as u64,
            }
        }),
        ctx.user_id.as_str(),
    ))
}
