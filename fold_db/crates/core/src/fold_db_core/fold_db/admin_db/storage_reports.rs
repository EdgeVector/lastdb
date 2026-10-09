//! Per-database and per-schema storage inventory reports.

use super::*;

impl FoldDB {
    /// Collect field-molecule UUIDs for each schema name (for history scans).
    pub(super) fn schema_field_molecules(
        &self,
        schema_names: &[String],
    ) -> Vec<(String, Vec<String>)> {
        let sm = self.schema_manager();
        let mut out = Vec::with_capacity(schema_names.len());
        for name in schema_names {
            let Ok(Some(schema)) = sm.get_schema_metadata(name) else {
                continue;
            };
            let mols = field_molecule_uuids(&schema);
            if !mols.is_empty() {
                out.push((name.clone(), mols));
            }
        }
        out
    }

    /// Live inventory of the `main` tree key classes + per-schema atom/history
    /// sizes, plus a fresh attribution summary.
    ///
    /// The attribution numbers are not a passive read: this route runs the
    /// bounded schema/system-root and retention-root walks (catalog- and
    /// registry-sized, not record-sized — the same cost class as the rest of
    /// this admin scan) before it summarizes the ledger, so the counts
    /// reflect the current catalog rather than whatever a prior caller
    /// happened to leave behind.
    pub async fn db_inventory(&self) -> Result<DbInventory, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.into_keys().collect();
        let field_mols = self.schema_field_molecules(&names);
        let mut inventory = self
            .db_ops()
            .atoms()
            .db_inventory(&names, None, &field_mols)
            .await
            .map_err(|e| FoldDbError::Database(format!("db_inventory: {e}")))?;

        self.db_ops()
            .attribute_schema_root_molecules(LIVE_ATTRIBUTION_EPOCH_ID, 0)
            .await
            .map_err(|e| FoldDbError::Database(format!("attribute schema roots: {e}")))?;
        self.db_ops()
            .attribute_schema_retention_roots(LIVE_ATTRIBUTION_EPOCH_ID, 0)
            .await
            .map_err(|e| FoldDbError::Database(format!("attribute retention roots: {e}")))?;
        inventory.attribution = self
            .db_ops()
            .attribution()
            .summarize()
            .await
            .map_err(|e| FoldDbError::Database(format!("summarize attribution: {e}")))?;
        Ok(inventory)
    }

    /// Atom-only per-schema logical storage (no history / order-log / tip-format).
    ///
    /// Catalog `descriptive_name` is joined as `display_name` when it differs
    /// from the runtime identity. This is the `lastdb db schemas` product path.
    pub async fn db_schema_storage(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let mut breakdown = self
            .db_ops()
            .atoms()
            .storage_breakdown(&[], None)
            .await
            .map_err(|e| FoldDbError::Database(format!("schema storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, schema) in &schemas {
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(schema.name.trim());
            if label.is_empty() {
                continue;
            }
            display.insert(key.clone(), label.to_string());
            display.insert(schema.name.clone(), label.to_string());
            if let Some(hash) = schema.identity_hash.as_ref() {
                display.insert(hash.clone(), label.to_string());
            }
        }
        breakdown.apply_display_names(&display);
        Ok(SchemaStorageReport::from_breakdown(breakdown))
    }

    /// Bounded logical-current storage for one schema catalog.
    ///
    /// This reads the catalog and its declared molecule counters only.  It is
    /// intentionally separate from [`Self::db_schema_storage`], whose legacy
    /// operator report walks the atom plane.  A missing or unseeded counter is
    /// reported as incomplete; this method never fills it by scanning.
    pub fn schema_current_storage(
        &self,
        schema_name: &str,
    ) -> Result<SchemaCurrentStorageReport, FoldDbError> {
        let schema = self
            .schema_manager()
            .get_schema_metadata(schema_name)
            .map_err(|error| admin_op_error("get schema storage catalog", error))?
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::SchemaError::NotFound(format!(
                    "schema {schema_name}"
                )))
            })?;
        Ok(self.schema_current_storage_for(&schema))
    }

    pub(super) fn schema_current_storage_for(&self, schema: &Schema) -> SchemaCurrentStorageReport {
        let molecules = field_molecule_uuids(schema);
        let meters = self.db_ops().atoms().keep_small();
        let mut report =
            SchemaCurrentStorageReport::new(schema.name.clone(), molecules.len() as u64);
        let trust = meters.trust();
        let schema_trust = trust.schemas.get(&schema.name);
        report.complete = meters.molecule_counters_complete()
            && schema_trust.is_some_and(|trust| trust.state.is_complete());
        report.incomplete_reason = meters
            .trust_incomplete_cause()
            .or_else(|| meters.incomplete_reason().map(str::to_string))
            .or_else(|| {
                schema_trust.map_or_else(
                    || Some("schema_meter_domain_absent".to_string()),
                    |trust| {
                        (!trust.state.is_complete()).then(|| {
                            trust
                                .cause
                                .clone()
                                .unwrap_or_else(|| "schema_meter_domain_incomplete".to_string())
                        })
                    },
                )
            });
        report.pending_protein_folds = meters.pending_protein_folds();
        if report.pending_protein_folds > 0 {
            report.complete = false;
            report
                .incomplete_reason
                .get_or_insert_with(|| "pending_protein_folds".to_string());
        }
        for molecule_uuid in molecules {
            let Some(counter) = meters.molecule_counter(&molecule_uuid) else {
                report.complete = false;
                report
                    .incomplete_reason
                    .get_or_insert_with(|| "missing_molecule_counters".to_string());
                report.missing_molecule_counters =
                    report.missing_molecule_counters.saturating_add(1);
                continue;
            };
            report.active_slot_count = report
                .active_slot_count
                .saturating_add(counter.active_slot_count);
            report.logical_value_bytes = report
                .logical_value_bytes
                .saturating_add(counter.logical_value_bytes());
            report.schema_structure_bytes = report
                .schema_structure_bytes
                .saturating_add(counter.structure_bytes());
            report.retained_history_bytes = report
                .retained_history_bytes
                .saturating_add(counter.retained_history_bytes);
            report.counter_epoch = report.counter_epoch.max(counter.counter_epoch);
        }
        report.schema_current_bytes = report
            .logical_value_bytes
            .saturating_add(report.schema_structure_bytes);
        report
    }

    /// Labelled logical-current storage for every installed schema.
    ///
    /// The catalog supplies the schema labels and field molecule identities.
    /// The keep-small projection supplies the stored molecule counters. This
    /// path does not scan atoms, tips, or any filesystem plane.
    pub fn schema_logical_storage_report(&self) -> Result<SchemaLogicalStorageReport, FoldDbError> {
        // The schema manager cache was hydrated from the durable catalog at
        // boot. Read that cache so this report does not turn into a catalog
        // walk, in addition to never scanning atoms.
        let schemas = self
            .schema_manager()
            .get_schemas()
            .map_err(|e| admin_op_error("read schema storage catalog", e))?;
        let mut per_schema = Vec::with_capacity(schemas.len());
        for schema in schemas.values() {
            let current = self.schema_current_storage_for(schema);
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(schema.name.trim())
                .to_string();
            per_schema.push(SchemaLogicalStorageRow {
                schema_binding: schema.name.clone(),
                label,
                bytes: current.schema_current_bytes,
                logical_value_bytes: current.logical_value_bytes,
                schema_structure_bytes: current.schema_structure_bytes,
                active_slot_count: current.active_slot_count,
                complete: current.complete,
                incomplete_reason: current.incomplete_reason,
            });
        }
        per_schema.sort_by(|a, b| {
            b.bytes
                .cmp(&a.bytes)
                .then_with(|| a.label.cmp(&b.label))
                .then_with(|| a.schema_binding.cmp(&b.schema_binding))
        });
        let complete = per_schema.iter().all(|row| row.complete);
        let incomplete_reason = per_schema
            .iter()
            .find_map(|row| row.incomplete_reason.clone());
        let total_bytes = per_schema
            .iter()
            .map(|row| row.bytes)
            .fold(0, u64::saturating_add);
        let total_logical_value_bytes = per_schema
            .iter()
            .map(|row| row.logical_value_bytes)
            .fold(0, u64::saturating_add);
        let total_schema_structure_bytes = per_schema
            .iter()
            .map(|row| row.schema_structure_bytes)
            .fold(0, u64::saturating_add);
        let total_active_slot_count = per_schema
            .iter()
            .map(|row| row.active_slot_count)
            .fold(0, u64::saturating_add);
        Ok(SchemaLogicalStorageReport {
            measured_at: chrono::Utc::now(),
            heavy: false,
            complete,
            incomplete_reason,
            schema_count: per_schema.len() as u64,
            total_bytes,
            total_logical_value_bytes,
            total_schema_structure_bytes,
            total_active_slot_count,
            per_schema,
        })
    }
}
