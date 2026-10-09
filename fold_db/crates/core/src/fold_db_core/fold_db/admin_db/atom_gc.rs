//! Orphan atom, protein and blob garbage collection plus dropped-schema reaping.

use super::*;

impl FoldDB {
    /// Delete `atom:` rows not referenced by tips / remaining history / conflicts.
    ///
    /// Prunes tip-version chains on **tombstoned** tips only (preserves `as_of`
    /// history on live tips).
    pub async fn gc_orphan_atoms(&self, dry_run: bool) -> Result<AtomGcReport, FoldDbError> {
        self.gc_orphan_atoms_with(dry_run, false).await
    }

    /// Like [`Self::gc_orphan_atoms`]. When `prune_live_history` is true, also
    /// drop tip-version chains on **live** tips (tip becomes the only version;
    /// historical body atoms become freeable). Use for reclaim after
    /// append-heavy rewrites; sacrifices `as_of` depth.
    pub async fn gc_orphan_atoms_with(
        &self,
        dry_run: bool,
        prune_live_history: bool,
    ) -> Result<AtomGcReport, FoldDbError> {
        // A durable pin-log record references atoms by uuid and has had its
        // inline bodies stripped on that basis, but the pin log is the sync
        // engine's own namespace — `AtomStore`'s five reference roots cannot
        // see it. Without this, one ordinary update moves a tip off a captured
        // atom and the next GC pass frees a body an unpublished record still
        // needs; that record is unsealable from then on and the upload path
        // drops it, so a locally acked write silently never reaches the cloud.
        let extra_roots = self
            .mutation_manager()
            .pending_pin_log_atom_roots()
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_atoms: {e}")))?;
        self.db_ops()
            .atoms()
            .gc_orphan_atoms_with_roots(dry_run, None, prune_live_history, &extra_roots)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_atoms: {e}")))
    }

    /// Delete orphan atom bodies from one schema only.
    ///
    /// A retired schema can be absent from the catalog while its atom bodies
    /// still name the old schema. In that case, use the exact requested name
    /// as the atom-header key instead of rejecting the maintenance request.
    pub async fn gc_orphan_atoms_for_schema(
        &self,
        requested_schema: &str,
        dry_run: bool,
        prune_live_history: bool,
    ) -> Result<AtomGcReport, FoldDbError> {
        if prune_live_history {
            return Err(FoldDbError::Database(
                "schema-scoped gc-atoms does not support --prune-live-history".into(),
            ));
        }
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let mut matches = schemas
            .iter()
            .filter(|(stored, schema)| {
                stored.as_str() == requested_schema
                    || schema.name == requested_schema
                    || schema.descriptive_name.as_deref() == Some(requested_schema)
                    || schema.identity_hash.as_deref() == Some(requested_schema)
            })
            .map(|(_, schema)| schema.name.clone());
        let schema_name = match matches.next() {
            Some(schema_name) => {
                if matches.any(|name| name != schema_name) {
                    return Err(FoldDbError::Database(format!(
                        "schema name is ambiguous: {requested_schema}"
                    )));
                }
                schema_name
            }
            None => requested_schema.to_string(),
        };
        let extra_roots = self
            .mutation_manager()
            .pending_pin_log_atom_roots()
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_atoms: {e}")))?;
        self.db_ops()
            .atoms()
            .gc_orphan_atoms_with_roots_for_schema(
                dry_run,
                None,
                false,
                &extra_roots,
                Some(&schema_name),
            )
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_atoms: {e}")))
    }

    /// Reap live tips of a dropped schema identity. Never scans `atom:`.
    pub async fn reap_dropped_schema(
        &self,
        schema_name: &str,
        field_names: &[String],
        dry_run: bool,
        max_ops: usize,
        cursor: Option<DroppedSchemaReapCursor>,
    ) -> Result<crate::db_operations::DroppedSchemaReapReport, FoldDbError> {
        let receipt = self
            .db_ops()
            .get_schema_drop_receipt(schema_name)
            .await
            .map_err(|e| FoldDbError::Database(format!("load drop receipt: {e}")))?;
        if self
            .db_ops()
            .get_schema(schema_name)
            .await
            .map_err(|e| FoldDbError::Database(format!("confirm dropped schema: {e}")))?
            .is_some()
        {
            return Err(FoldDbError::Database(format!(
                "refuse to reap active schema identity: {schema_name}"
            )));
        }
        let drop_receipt_proves_removed = receipt.is_some();
        let molecules = receipt
            .map(|receipt| receipt.field_molecule_uuids)
            .unwrap_or_default();
        self.db_ops()
            .atoms()
            .reap_dropped_schema_tips_bounded(
                schema_name,
                &molecules,
                field_names,
                drop_receipt_proves_removed,
                dry_run,
                max_ops,
                cursor,
            )
            .await
            .map_err(|e| admin_op_error("reap dropped schema", e))
    }

    /// Point-read one receipt-named tip of a dropped identity. The response
    /// contains only a key fingerprint and presence/liveness booleans.
    pub async fn probe_dropped_schema_tip(
        &self,
        schema_name: &str,
        molecule_uuid: &str,
        key_hash: &str,
        key_range: &str,
        expected_key_fingerprint: Option<&str>,
    ) -> Result<crate::db_operations::DroppedSchemaTipProbe, FoldDbError> {
        if self
            .db_ops()
            .get_schema(schema_name)
            .await
            .map_err(|error| admin_op_error("confirm dropped schema", error))?
            .is_some()
        {
            return Err(FoldDbError::Schema(crate::schema::SchemaError::Blocked(
                "refuse to probe an active schema identity".to_string(),
            )));
        }
        let receipt = self
            .db_ops()
            .get_schema_drop_receipt(schema_name)
            .await
            .map_err(|error| admin_op_error("load drop receipt", error))?
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::SchemaError::Blocked(
                    "exact dropped-tip probe requires a durable drop receipt".to_string(),
                ))
            })?;
        if !receipt
            .field_molecule_uuids
            .iter()
            .any(|candidate| candidate == molecule_uuid)
        {
            return Err(FoldDbError::Schema(
                crate::schema::SchemaError::InvalidField(
                    "molecule is absent from this schema drop receipt".to_string(),
                ),
            ));
        }
        self.db_ops()
            .atoms()
            .probe_dropped_schema_tip(
                schema_name,
                molecule_uuid,
                key_hash,
                key_range,
                expected_key_fingerprint,
            )
            .await
            .map_err(|error| admin_op_error("probe dropped tip", error))
    }

    /// Delete local file-blob rows (`cas_blobs` namespace + resident
    /// `cas_blob:` rows) no live atom references. The reclaim path for the
    /// sealed bytes a purge leaves behind — see
    /// [`crate::db_operations::file_blob_gc`].
    #[cfg(feature = "sharing")]
    pub async fn gc_orphan_file_blobs(
        &self,
        dry_run: bool,
    ) -> Result<crate::db_operations::file_blob_gc::FileBlobGcReport, FoldDbError> {
        crate::db_operations::file_blob_gc::gc_orphan_file_blobs(self.db_ops(), dry_run)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_file_blobs: {e}")))
    }

    /// Delete empty `protein:` rows that are not named by any `molprot:` back-ref.
    pub async fn gc_orphan_proteins(&self, dry_run: bool) -> Result<ProteinGcReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .gc_orphan_proteins(dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_proteins: {e}")))
    }
}
