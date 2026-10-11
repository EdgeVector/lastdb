//! Tip and plane residue drain operations.

use super::*;

impl LastStoreNamespacedStore {
    /// Drain one page of legacy tip-residue rows into canonical `tips`.
    ///
    /// Execute mode is crash-ordered per key: copy to `tips` first when needed,
    /// then delete the legacy row. If `tips` already has the id, tips wins and
    /// the legacy duplicate is deleted without overwriting the canonical row.
    pub async fn drain_tip_residue_collection(
        &self,
        options: TipResidueDrainOptions,
    ) -> StorageResult<TipResidueDrainReport> {
        if options.limit == 0 {
            return Err(StorageError::BackendError(
                "tip-residue drain limit must be greater than zero".to_string(),
            ));
        }
        if !TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&options.legacy_collection.as_str()) {
            return Err(StorageError::BackendError(format!(
                "unsupported tip-residue collection {}",
                options.legacy_collection
            )));
        }

        let logical = Arc::clone(&self.logical);
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        LastStoreKvStore::run_blocking(move || {
            let rows = store
                .list_prefix_paged(
                    &options.legacy_collection,
                    "",
                    options.after.as_deref(),
                    options.limit,
                )
                .map_err(LastStoreKvStore::map_error)?;

            let mut report = TipResidueDrainReport {
                legacy_collection: options.legacy_collection.clone(),
                dry_run: !options.execute,
                done: rows.len() < options.limit,
                after: rows.last().map(|(id, _)| id.clone()),
                ..Default::default()
            };

            let mut ops = Vec::new();
            for (id, value) in &rows {
                let key = LastStoreKvStore::decode_key(id)?;
                report.keys_scanned = report.keys_scanned.saturating_add(1);
                let tips_has = store
                    .exists(TIPS_COLLECTION, id)
                    .map_err(LastStoreKvStore::map_error)?;
                match classify_tip_residue_copy(&key, tips_has, true) {
                    TipResidueCopyAction::CopyToTips => {
                        report.copied_to_tips = report.copied_to_tips.saturating_add(1);
                        if options.execute {
                            ops.push(TxnOp::put(TIPS_COLLECTION, id, value.clone()));
                            ops.push(TxnOp::delete(&options.legacy_collection, id));
                            report.deleted_from_legacy =
                                report.deleted_from_legacy.saturating_add(1);
                        }
                    }
                    TipResidueCopyAction::TipsWins => {
                        report.tips_already_won = report.tips_already_won.saturating_add(1);
                        if options.execute {
                            ops.push(TxnOp::delete(&options.legacy_collection, id));
                            report.deleted_from_legacy =
                                report.deleted_from_legacy.saturating_add(1);
                        }
                    }
                    TipResidueCopyAction::AlreadyOnTips
                    | TipResidueCopyAction::SkipNotTipFamily => {
                        report.skipped = report.skipped.saturating_add(1);
                    }
                }
            }

            if options.execute && !ops.is_empty() {
                direct_write_invalidation::after_direct_write(&logical, || {
                    store.transaction(ops).map_err(LastStoreKvStore::map_error)
                })?;
                LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
            }

            if options.execute && options.drop_empty_collection && report.done {
                report.collection_dropped = store
                    .drop_empty_collection(&options.legacy_collection)
                    .map_err(LastStoreKvStore::map_error)?;
                LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
            }

            if report.done {
                report.after = None;
            }
            Ok(report)
        })
        .await
    }

    /// Drain one page of plane residue from `source` into the plane target.
    ///
    /// Same crash order as tip residue: copy to target first when needed, then
    /// delete the source row. Target wins when both locations hold the key.
    /// Protein/index family filters refuse order-log keys so history-adjacent
    /// mass cannot be mistaken for rebuildable index residue.
    pub async fn drain_plane_residue_collection(
        &self,
        options: PlaneResidueDrainOptions,
    ) -> StorageResult<PlaneResidueDrainReport> {
        if options.limit == 0 {
            return Err(StorageError::BackendError(
                "plane-residue drain limit must be greater than zero".to_string(),
            ));
        }
        validate_plane_residue_endpoints(&options)?;

        let logical = Arc::clone(&self.logical);
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        LastStoreKvStore::run_blocking(move || {
            // Default empty prefix = whole collection. Protein/index drains from
            // `tips` should pass a family prefix so we do not walk millions of
            // unrelated tip rows (terminal-proof incident 2026-07-31).
            let prefix = options.key_prefix.as_deref().unwrap_or("");
            if options.source_collection == TIPS_COLLECTION
                && matches!(
                    options.family,
                    PlaneResidueFamily::Protein | PlaneResidueFamily::Index
                )
                && prefix.is_empty()
            {
                return Err(StorageError::BackendError(
                    "protein/index residue drain from tips requires key_prefix \
                     (e.g. protein: or mhr:) — full tips scan is not allowed"
                        .to_string(),
                ));
            }
            let rows = store
                .list_prefix_paged(
                    &options.source_collection,
                    prefix,
                    options.after.as_deref(),
                    options.limit,
                )
                .map_err(LastStoreKvStore::map_error)?;

            let mut report = PlaneResidueDrainReport {
                family: plane_residue_family_label(options.family).into(),
                source_collection: options.source_collection.clone(),
                target_collection: options.target_collection.clone(),
                dry_run: !options.execute,
                done: rows.len() < options.limit,
                after: rows.last().map(|(id, _)| id.clone()),
                ..Default::default()
            };

            let ops = plan_plane_residue_ops(&store, &options, &rows, &mut report)?;

            if options.execute && !ops.is_empty() {
                direct_write_invalidation::after_direct_write(&logical, || {
                    store.transaction(ops).map_err(LastStoreKvStore::map_error)
                })?;
                LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
            }

            // Never drop the tips collection itself (still holds live tips).
            if options.execute
                && options.drop_empty_source
                && report.done
                && options.source_collection != TIPS_COLLECTION
            {
                report.source_dropped = store
                    .drop_empty_collection(&options.source_collection)
                    .map_err(LastStoreKvStore::map_error)?;
                LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
            }

            if report.done {
                report.after = None;
            }
            Ok(report)
        })
        .await
    }
}

fn residue_endpoint_error(message: impl Into<String>) -> StorageError {
    StorageError::BackendError(message.into())
}

/// Refuse a source/target pair that does not belong to the requested family.
fn validate_plane_residue_endpoints(options: &PlaneResidueDrainOptions) -> StorageResult<()> {
    let source = options.source_collection.as_str();
    let target = options.target_collection.as_str();
    match options.family {
        PlaneResidueFamily::Tip => {
            if !TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&source) && source != TIPS_COLLECTION {
                return Err(residue_endpoint_error(format!(
                    "unsupported tip-residue source {source}"
                )));
            }
            if target != TIPS_COLLECTION {
                return Err(residue_endpoint_error("tip residue target must be tips"));
            }
        }
        PlaneResidueFamily::Protein => {
            if source != TIPS_COLLECTION {
                return Err(residue_endpoint_error(
                    "protein residue source must be tips (legacy placement)",
                ));
            }
            if target != "proteins" {
                return Err(residue_endpoint_error(
                    "protein residue target must be proteins",
                ));
            }
        }
        PlaneResidueFamily::Index => {
            if source != TIPS_COLLECTION && !INDEX_RESIDUE_LEGACY_COLLECTIONS.contains(&source) {
                return Err(residue_endpoint_error(format!(
                    "unsupported index-residue source {source}"
                )));
            }
            if target != "indexes" {
                return Err(residue_endpoint_error(
                    "index residue target must be indexes",
                ));
            }
        }
        PlaneResidueFamily::Conflict => {
            if source != SYNC_CONFLICTS_COLLECTION {
                return Err(residue_endpoint_error(format!(
                    "unsupported conflict-residue source {source}"
                )));
            }
            if target != TIPS_COLLECTION {
                return Err(residue_endpoint_error(
                    "conflict residue target must be tips",
                ));
            }
        }
        PlaneResidueFamily::OrderLog => {
            if !ORDER_LOG_LEGACY_COLLECTIONS.contains(&source) {
                return Err(residue_endpoint_error(format!(
                    "unsupported order-log residue source {source}"
                )));
            }
            if target != TIPS_COLLECTION {
                return Err(residue_endpoint_error(
                    "order-log residue target must be tips",
                ));
            }
        }
    }
    Ok(())
}

fn plane_residue_family_label(family: PlaneResidueFamily) -> &'static str {
    match family {
        PlaneResidueFamily::Tip => "tip",
        PlaneResidueFamily::Protein => "protein",
        PlaneResidueFamily::Index => "index",
        PlaneResidueFamily::Conflict => "conflict",
        PlaneResidueFamily::OrderLog => "order_log",
    }
}

fn classify_plane_residue(
    family: PlaneResidueFamily,
    key: &[u8],
    target_has: bool,
) -> PlaneResidueCopyAction {
    match family {
        PlaneResidueFamily::Tip => match classify_tip_residue_copy(key, target_has, true) {
            TipResidueCopyAction::CopyToTips => PlaneResidueCopyAction::CopyToTarget,
            TipResidueCopyAction::TipsWins => PlaneResidueCopyAction::TargetWins,
            TipResidueCopyAction::AlreadyOnTips | TipResidueCopyAction::SkipNotTipFamily => {
                PlaneResidueCopyAction::SkipWrongFamily
            }
        },
        PlaneResidueFamily::Protein => classify_protein_residue_copy(key, target_has, true),
        PlaneResidueFamily::Index => classify_index_residue_copy(key, target_has, true),
        PlaneResidueFamily::Conflict => classify_conflict_residue_copy(key, target_has, true),
        PlaneResidueFamily::OrderLog => classify_order_log_residue_copy(key, target_has, true),
    }
}

/// Classify each row of one page, update the report counters, and return the
/// transaction ops an execute run must apply (empty on dry run).
fn plan_plane_residue_ops(
    store: &LastStore,
    options: &PlaneResidueDrainOptions,
    rows: &[(String, Vec<u8>)],
    report: &mut PlaneResidueDrainReport,
) -> StorageResult<Vec<TxnOp>> {
    let mut ops = Vec::new();
    for (id, value) in rows {
        let key = LastStoreKvStore::decode_key(id)?;
        report.keys_scanned = report.keys_scanned.saturating_add(1);
        let target_has = store
            .exists(&options.target_collection, id)
            .map_err(LastStoreKvStore::map_error)?;
        match classify_plane_residue(options.family, &key, target_has) {
            PlaneResidueCopyAction::CopyToTarget => {
                report.copied_to_target = report.copied_to_target.saturating_add(1);
                if options.execute {
                    ops.push(TxnOp::put(&options.target_collection, id, value.clone()));
                    ops.push(TxnOp::delete(&options.source_collection, id));
                    report.deleted_from_source = report.deleted_from_source.saturating_add(1);
                }
            }
            PlaneResidueCopyAction::TargetWins => {
                report.target_already_won = report.target_already_won.saturating_add(1);
                if options.execute {
                    ops.push(TxnOp::delete(&options.source_collection, id));
                    report.deleted_from_source = report.deleted_from_source.saturating_add(1);
                }
            }
            PlaneResidueCopyAction::AlreadyOnTarget | PlaneResidueCopyAction::SkipWrongFamily => {
                report.skipped = report.skipped.saturating_add(1);
            }
        }
    }
    Ok(ops)
}
