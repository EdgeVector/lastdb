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
    // lint:fn-size-ok moved verbatim from its original module; no logic change
    pub async fn drain_plane_residue_collection(
        &self,
        options: PlaneResidueDrainOptions,
    ) -> StorageResult<PlaneResidueDrainReport> {
        if options.limit == 0 {
            return Err(StorageError::BackendError(
                "plane-residue drain limit must be greater than zero".to_string(),
            ));
        }
        match options.family {
            PlaneResidueFamily::Tip => {
                if !TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&options.source_collection.as_str())
                    && options.source_collection != TIPS_COLLECTION
                {
                    return Err(StorageError::BackendError(format!(
                        "unsupported tip-residue source {}",
                        options.source_collection
                    )));
                }
                if options.target_collection != TIPS_COLLECTION {
                    return Err(StorageError::BackendError(
                        "tip residue target must be tips".to_string(),
                    ));
                }
            }
            PlaneResidueFamily::Protein => {
                if options.source_collection != TIPS_COLLECTION {
                    return Err(StorageError::BackendError(
                        "protein residue source must be tips (legacy placement)".to_string(),
                    ));
                }
                if options.target_collection != "proteins" {
                    return Err(StorageError::BackendError(
                        "protein residue target must be proteins".to_string(),
                    ));
                }
            }
            PlaneResidueFamily::Index => {
                let ok_source = options.source_collection == TIPS_COLLECTION
                    || INDEX_RESIDUE_LEGACY_COLLECTIONS
                        .contains(&options.source_collection.as_str());
                if !ok_source {
                    return Err(StorageError::BackendError(format!(
                        "unsupported index-residue source {}",
                        options.source_collection
                    )));
                }
                if options.target_collection != "indexes" {
                    return Err(StorageError::BackendError(
                        "index residue target must be indexes".to_string(),
                    ));
                }
            }
            PlaneResidueFamily::Conflict => {
                if options.source_collection != SYNC_CONFLICTS_COLLECTION {
                    return Err(StorageError::BackendError(format!(
                        "unsupported conflict-residue source {}",
                        options.source_collection
                    )));
                }
                if options.target_collection != TIPS_COLLECTION {
                    return Err(StorageError::BackendError(
                        "conflict residue target must be tips".to_string(),
                    ));
                }
            }
            PlaneResidueFamily::OrderLog => {
                if !ORDER_LOG_LEGACY_COLLECTIONS.contains(&options.source_collection.as_str()) {
                    return Err(StorageError::BackendError(format!(
                        "unsupported order-log residue source {}",
                        options.source_collection
                    )));
                }
                if options.target_collection != TIPS_COLLECTION {
                    return Err(StorageError::BackendError(
                        "order-log residue target must be tips".to_string(),
                    ));
                }
            }
        }

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
                family: match options.family {
                    PlaneResidueFamily::Tip => "tip".into(),
                    PlaneResidueFamily::Protein => "protein".into(),
                    PlaneResidueFamily::Index => "index".into(),
                    PlaneResidueFamily::Conflict => "conflict".into(),
                    PlaneResidueFamily::OrderLog => "order_log".into(),
                },
                source_collection: options.source_collection.clone(),
                target_collection: options.target_collection.clone(),
                dry_run: !options.execute,
                done: rows.len() < options.limit,
                after: rows.last().map(|(id, _)| id.clone()),
                ..Default::default()
            };

            let mut ops = Vec::new();
            for (id, value) in &rows {
                let key = LastStoreKvStore::decode_key(id)?;
                report.keys_scanned = report.keys_scanned.saturating_add(1);
                let target_has = store
                    .exists(&options.target_collection, id)
                    .map_err(LastStoreKvStore::map_error)?;
                let action = match options.family {
                    PlaneResidueFamily::Tip => {
                        match classify_tip_residue_copy(&key, target_has, true) {
                            TipResidueCopyAction::CopyToTips => {
                                PlaneResidueCopyAction::CopyToTarget
                            }
                            TipResidueCopyAction::TipsWins => PlaneResidueCopyAction::TargetWins,
                            TipResidueCopyAction::AlreadyOnTips
                            | TipResidueCopyAction::SkipNotTipFamily => {
                                PlaneResidueCopyAction::SkipWrongFamily
                            }
                        }
                    }
                    PlaneResidueFamily::Protein => {
                        classify_protein_residue_copy(&key, target_has, true)
                    }
                    PlaneResidueFamily::Index => {
                        classify_index_residue_copy(&key, target_has, true)
                    }
                    PlaneResidueFamily::Conflict => {
                        classify_conflict_residue_copy(&key, target_has, true)
                    }
                    PlaneResidueFamily::OrderLog => {
                        classify_order_log_residue_copy(&key, target_has, true)
                    }
                };
                match action {
                    PlaneResidueCopyAction::CopyToTarget => {
                        report.copied_to_target = report.copied_to_target.saturating_add(1);
                        if options.execute {
                            ops.push(TxnOp::put(&options.target_collection, id, value.clone()));
                            ops.push(TxnOp::delete(&options.source_collection, id));
                            report.deleted_from_source =
                                report.deleted_from_source.saturating_add(1);
                        }
                    }
                    PlaneResidueCopyAction::TargetWins => {
                        report.target_already_won = report.target_already_won.saturating_add(1);
                        if options.execute {
                            ops.push(TxnOp::delete(&options.source_collection, id));
                            report.deleted_from_source =
                                report.deleted_from_source.saturating_add(1);
                        }
                    }
                    PlaneResidueCopyAction::AlreadyOnTarget
                    | PlaneResidueCopyAction::SkipWrongFamily => {
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

            if options.execute && options.drop_empty_source && report.done {
                // Never drop the tips collection itself (still holds live tips).
                if options.source_collection != TIPS_COLLECTION {
                    report.source_dropped = store
                        .drop_empty_collection(&options.source_collection)
                        .map_err(LastStoreKvStore::map_error)?;
                    LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
                }
            }

            if report.done {
                report.after = None;
            }
            Ok(report)
        })
        .await
    }
}
