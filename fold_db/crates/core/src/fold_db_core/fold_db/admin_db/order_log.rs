//! Order-log and molecule-key audits and repairs.

use super::*;

impl FoldDB {
    /// Audit every molecule's `moc:` order-log count against its live `mk:`
    /// record count. Read-only.
    ///
    /// `update_order` only ever grows, so `moc:{M} >= count(mk:{M}:…)` holds for
    /// every molecule that has an order log. A molecule below that bound has
    /// lost log entries, and because a truncation leaves `moc:` and the
    /// surviving `mord:` rows mutually consistent, the key count is the only
    /// witness left. See [`crate::db_operations::OrderLogAudit`].
    /// Findings carry the owning schema name where it resolves. The audit walks
    /// keys and reports raw molecule uuids; attributing them is a schema-manager
    /// lookup, not a store walk, so it happens here rather than pushing the
    /// schema manager down into the key scan.
    pub async fn audit_order_log_counts(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<OrderLogAudit, FoldDbError> {
        let mut audit = self
            .db_ops()
            .atoms()
            .audit_order_log_counts(max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_order_log_counts", e))?;
        if !audit.short_molecules.is_empty() {
            let by_molecule = self.molecule_schema_index().await;
            for row in &mut audit.short_molecules {
                row.schema = by_molecule.get(row.molecule.as_str()).cloned();
            }
        }
        Ok(audit)
    }

    /// Audit or drain one bounded page of legacy/plain HashKey-encoding tips.
    pub async fn audit_legacy_key_forks(
        &self,
        dry_run: bool,
        max_keys: usize,
        after_key: Option<&str>,
    ) -> Result<LegacyKeyForkAudit, FoldDbError> {
        self.db_ops()
            .atoms()
            .audit_legacy_key_forks(dry_run, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_legacy_key_forks", e))
    }

    /// Measure append-only order-log excess (stale entries + zero-live residue).
    /// Read-only. Findings carry schema names via the schema-manager index.
    pub async fn audit_order_log_bloat(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<crate::db_operations::OrderLogBloatAudit, FoldDbError> {
        let mut audit = self
            .db_ops()
            .atoms()
            .audit_order_log_bloat(max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_order_log_bloat", e))?;
        let needs_index =
            !audit.bloated_molecules.is_empty() || !audit.zero_live_molecules.is_empty();
        if needs_index {
            let by_molecule = self.molecule_schema_index().await;
            for row in &mut audit.bloated_molecules {
                row.schema = by_molecule.get(row.molecule.as_str()).cloned();
            }
            for row in &mut audit.zero_live_molecules {
                row.schema = by_molecule.get(row.molecule.as_str()).cloned();
            }
            let mut by_schema: std::collections::BTreeMap<
                String,
                crate::db_operations::OrderLogBloatSchemaStat,
            > = std::collections::BTreeMap::new();
            let mut accumulate = |row: &crate::db_operations::OrderLogBloatRow| {
                let name = row
                    .schema
                    .clone()
                    .unwrap_or_else(|| "<unattributed>".to_string());
                let entry = by_schema.entry(name.clone()).or_insert_with(|| {
                    crate::db_operations::OrderLogBloatSchemaStat {
                        schema_name: name,
                        ..Default::default()
                    }
                });
                entry.molecules += 1;
                if row.zero_live {
                    entry.zero_live_molecules += 1;
                    entry.zero_live_bytes += row.order_log_bytes + row.order_count_bytes;
                }
                entry.order_log_entries += row.order_log_entries;
                entry.order_log_bytes += row.order_log_bytes;
                entry.live_unique_keys += row.live_unique_keys;
                entry.stale_entries += row.stale_entries;
            };
            for row in &audit.bloated_molecules {
                accumulate(row);
            }
            for row in &audit.zero_live_molecules {
                accumulate(row);
            }
            audit.per_schema = by_schema.into_values().collect();
            audit
                .per_schema
                .sort_by_key(|s| std::cmp::Reverse(s.order_log_bytes));
        }
        Ok(audit)
    }

    /// Dry-run writes nothing. Execute deletes a zero-live log, a bloated log,
    /// and a clean log. It does not write a new log. `retention_seconds` does
    /// not keep rows.
    pub async fn compact_order_log_zero_live(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        retention_seconds: Option<u64>,
    ) -> Result<crate::db_operations::OrderLogZeroLiveCompactionReport, FoldDbError> {
        let retention_seconds =
            retention_seconds.unwrap_or(crate::atom::molecule_key_codec::ORDER_LOG_RETENTION_SECS);
        let mut report = self
            .db_ops()
            .atoms()
            .compact_order_log_zero_live_with_retention(
                dry_run,
                max_keys,
                after_key,
                None,
                retention_seconds,
            )
            .await
            .map_err(|error| admin_op_error("compact_order_log_zero_live", error))?;
        if !report.audit.zero_live_molecules.is_empty()
            || !report.audit.bloated_molecules.is_empty()
        {
            let by_molecule = self.molecule_schema_index().await;
            for row in &mut report.audit.zero_live_molecules {
                row.schema = by_molecule.get(row.molecule.as_str()).cloned();
            }
            for row in &mut report.audit.bloated_molecules {
                row.schema = by_molecule.get(row.molecule.as_str()).cloned();
            }
        }
        Ok(report)
    }

    /// The verb writes nothing.
    pub fn repair_order_log_shortfall(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<crate::db_operations::OrderLogRepairReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .repair_order_log_shortfall(dry_run, max_keys, after_key, None)
            .map_err(|e| FoldDbError::Database(format!("repair_order_log_shortfall: {e}")))
    }

    /// Read-only list of one molecule's live `mk:` storage keys (decoded
    /// hash/range + collision flag). See
    /// [`crate::db_operations::AtomStore::list_molecule_keys`].
    pub async fn list_molecule_keys(
        &self,
        molecule: &str,
        max_keys: Option<usize>,
    ) -> Result<crate::db_operations::MoleculeKeysReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .list_molecule_keys(molecule, max_keys, None)
            .await
            .map_err(|e| admin_op_error("list_molecule_keys", e))
    }

    /// Development-only raw `mk:` dump for one API HashKey partition.
    pub async fn debug_molecule_hash_bucket(
        &self,
        molecule: &str,
        api_hash: &str,
        max_keys: usize,
    ) -> Result<crate::db_operations::MoleculeHashBucketReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .debug_molecule_hash_bucket(molecule, api_hash, max_keys, None)
            .await
            .map_err(|e| admin_op_error("debug_molecule_hash_bucket", e))
    }

    /// Keys-only membership walk of one schema's live records.
    ///
    /// Resolves `schema_id` (canonical identity already resolved by the
    /// caller) to the hash_field (else range_field) molecule, then pages
    /// live `mk:` tips. No atom bodies.
    pub async fn list_schema_record_keys(
        &self,
        requested: &str,
        schema_id: &str,
        limit: usize,
        cursor: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        let mgr = self.schema_manager();
        let mut schema = mgr
            .get_schema_following_supersession(schema_id)
            .await
            .map_err(|e| admin_op_error("list schema keys", e))?
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::SchemaError::NotFound(format!(
                    "schema not found: {schema_id}"
                )))
            })?;

        let hash_field = schema
            .key
            .as_ref()
            .and_then(|k| k.hash_field.clone())
            .filter(|s| !s.is_empty());
        let range_field = schema
            .key
            .as_ref()
            .and_then(|k| k.range_field.clone())
            .filter(|s| !s.is_empty());
        let key_field = hash_field
            .clone()
            .or_else(|| range_field.clone())
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::SchemaError::InvalidField(format!(
                    "schema '{requested}' has no hash_field or range_field to list"
                )))
            })?;

        let molecule = schema
            .field_molecule_uuids
            .as_ref()
            .and_then(|m| m.get(&key_field).cloned())
            .or_else(|| {
                schema
                    .runtime_fields
                    .get_mut(&key_field)
                    .and_then(|field| field.inner.molecule_uuid().cloned())
            })
            .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(&schema.name, &key_field));

        let hash_filter = hash_filter
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let (pairs, next_cursor, has_more) = self
            .db_ops()
            .atoms()
            .list_live_record_keys_filtered(&molecule, limit, cursor, None, hash_filter.as_deref())
            .await
            .map_err(|e| admin_op_error("list schema keys", e))?;
        let resident = self.db_ops().resident();
        let schema_name = schema.name.clone();
        let keys = pairs
            .into_iter()
            .filter(|(hash, range)| {
                !resident.is_schema_key_tombstoned(requested, hash, range)
                    && !resident.is_schema_key_tombstoned(&schema_name, hash, range)
            })
            .map(|(hash, range)| SchemaRecordKey { hash, range })
            .collect();
        Ok(SchemaRecordKeysReport {
            schema: requested.to_string(),
            schema_id: schema.name.clone(),
            key_field,
            hash_field,
            range_field,
            hash_filter,
            molecule,
            keys,
            next_cursor,
            has_more,
            truncated: has_more,
        })
    }

    /// `molecule_uuid -> schema name`, built from each loaded schema's
    /// `field_molecule_uuids`.
    ///
    /// Prefers each schema's `descriptive_name` when present so operators see
    /// `BoardCards` rather than a content hash. Best-effort by design: a schema
    /// that fails to list or load simply leaves its molecules unattributed,
    /// which is strictly better than failing an audit that has already done the
    /// expensive work.
    pub(super) async fn molecule_schema_index(&self) -> std::collections::HashMap<String, String> {
        let mut index = std::collections::HashMap::new();
        let Ok(schemas) = self.db_ops().get_all_schemas().await else {
            return index;
        };
        let mut labels: std::collections::HashMap<String, String> =
            std::collections::HashMap::with_capacity(schemas.len());
        let names: Vec<String> = schemas
            .iter()
            .map(|(stored, schema)| {
                let label = schema
                    .descriptive_name
                    .clone()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| stored.clone());
                labels.insert(stored.clone(), label);
                stored.clone()
            })
            .collect();
        for (schema_name, molecules) in self.schema_field_molecules(&names) {
            let label = labels
                .get(&schema_name)
                .cloned()
                .unwrap_or_else(|| schema_name.clone());
            for molecule in molecules {
                index.insert(molecule, label.clone());
            }
        }
        index
    }
}
