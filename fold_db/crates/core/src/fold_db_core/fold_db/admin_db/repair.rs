//! Current dangling-tip repair, locator probe and delete ledger.

use super::*;

impl FoldDB {
    /// Sample the locator-only tip population (bounded; always read-only).
    ///
    /// See [`crate::db_operations::AtomStore::probe_locator_only_population`].
    pub async fn probe_locator_only_population(
        &self,
        options: crate::db_operations::LocatorOnlyProbeOptions,
    ) -> Result<crate::db_operations::LocatorOnlyPopulationReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .probe_locator_only_population(options)
            .await
            .map_err(|e| admin_op_error("probe_locator_only_population", e))
    }

    /// Remove `mk:` tips whose atom body is unreachable by every read route.
    ///
    /// Dry-run by default. Execute revalidates each tip/body immediately before
    /// rewriting the molecule, and ledgers the repair batch before mutation.
    pub async fn repair_dangling_tips(
        &self,
        options: DanglingTipRepairOptions,
    ) -> Result<DanglingTipRepairReport, FoldDbError> {
        let mut report = self
            .db_ops()
            .atoms()
            .repair_dangling_tips(options)
            .await
            .map_err(|e| admin_op_error("repair_dangling_tips", e))?;
        // Join each unresolved tip's molecule against the live schema catalog
        // so an operator can answer "which schema is producing these" from
        // this report alone, instead of a separate molecule-keys probe per
        // molecule uuid.
        if !report.unresolved.is_empty() {
            let by_molecule = self.molecule_schema_index().await;
            for row in &mut report.unresolved {
                row.schema = by_molecule.get(row.molecule_uuid.as_str()).cloned();
            }
        }
        Ok(report)
    }

    /// [`Self::repair_dangling_tips`] limited to one schema's field molecules,
    /// and optionally to one API HashKey inside them.
    ///
    /// `requested_schema` matches the stored catalog name, `schema.name`, the
    /// `descriptive_name` (e.g. `BoardCards`), or the identity hash. Every
    /// matching schema is included: a descriptive name can span several
    /// identities, and dangling rows of an older identity are still damage.
    /// No match is an error, not an empty pass, so a typo cannot report a
    /// clean `completed` walk.
    ///
    /// The walk is one `mk:{M}:` prefix range per field molecule (see
    /// [`crate::db_operations::DanglingTipRepairScope`]), so its cost is the
    /// schema's rows, not the store's.
    pub async fn repair_dangling_tips_for_schema(
        &self,
        mut options: DanglingTipRepairOptions,
        requested_schema: &str,
        hash_key: Option<String>,
    ) -> Result<DanglingTipRepairReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let mut names: Vec<String> = schemas
            .iter()
            .filter(|(stored, schema)| {
                stored.as_str() == requested_schema
                    || schema.name == requested_schema
                    || schema.descriptive_name.as_deref() == Some(requested_schema)
                    || schema.identity_hash.as_deref() == Some(requested_schema)
            })
            .map(|(stored, _)| stored.clone())
            .collect();
        names.sort();
        names.dedup();
        if names.is_empty() {
            return Err(FoldDbError::Database(format!(
                "repair-dangling-tips: no loaded schema matches {requested_schema:?}"
            )));
        }
        let mut molecule_uuids: Vec<String> = self
            .schema_field_molecules(&names)
            .into_iter()
            .flat_map(|(_, mols)| mols)
            .collect();
        molecule_uuids.sort();
        molecule_uuids.dedup();
        options.key_window = None;
        options.scope = Some(crate::db_operations::DanglingTipRepairScope {
            molecule_uuids,
            hash_key,
        });
        let mut report = self.repair_dangling_tips(options).await?;
        if let Some(scope) = report.scope.as_mut() {
            scope.schemas = names;
        }
        Ok(report)
    }

    /// Read the durable atom hard-delete ledger, oldest first.
    ///
    /// The audit counterpart to [`Self::gc_orphan_atoms`] and the purge verb:
    /// every hard-delete batch either appears here or did not happen. `limit`
    /// of 0 means unbounded. See [`crate::db_operations::delete_ledger`] for
    /// what a row does and does not carry.
    pub async fn list_atom_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<AtomDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .atoms()
            .list_atom_delete_ledger(None, limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_atom_delete_ledger: {e}")))
    }
}

impl FoldDB {
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

impl FoldDB {
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
}
