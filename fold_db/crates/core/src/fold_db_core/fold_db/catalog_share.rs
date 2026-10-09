use crate::access::{parse_db_locator, DbLocator};
use crate::db_operations::{DbCatalogEntry, DbCatalogKeySelection};
use crate::error::{FoldDbError, FoldDbResult};
use crate::schema::types::SchemaError;
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

use super::FoldDB;

/// Durable proof that one schema instance now belongs to a second catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DbSchemaShareResult {
    pub source_db_locator: String,
    pub target_entry: DbCatalogEntry,
    pub access_domain: String,
    pub molecules_wrapped: usize,
    /// Current personal rows appended as self-contained org mutation intents.
    pub republished_rows: usize,
}

impl FoldDB {
    /// Share one whole schema by reference, without copying molecule or atom data.
    ///
    /// Access-domain wraps land before catalog membership. A partial wrap failure
    /// therefore cannot expose data through the target database handle.
    ///
    /// A personal source (`lastdb://personal` / empty) uses the implicit legacy
    /// instance (`instance_id = None`). A named source must already be a member.
    pub async fn share_schema(
        &self,
        source_db_locator: &str,
        target_db_locator: &str,
        schema_name: &str,
        target_access_domain: &str,
        target_domain_wrap_key: &[u8; 32],
    ) -> FoldDbResult<DbSchemaShareResult> {
        let catalog = self.db_ops().db_catalog();
        let source_parsed = parse_db_locator(source_db_locator)
            .map_err(|e| FoldDbError::Config(format!("invalid source db locator: {e}")))?;
        let target_parsed = parse_db_locator(target_db_locator)
            .map_err(|e| FoldDbError::Config(format!("invalid target db locator: {e}")))?;
        if matches!(target_parsed, DbLocator::Personal) {
            return Err(FoldDbError::Config(
                "share target must be a named locator (lastdb://org/…), not personal".to_string(),
            ));
        }
        let source_canonical = source_parsed.canonical();
        let target_canonical = target_parsed.canonical();

        let (source_instance, source_selection) = if source_parsed == DbLocator::Personal {
            (None, DbCatalogKeySelection::WholeSchema)
        } else {
            let source_entry = catalog
                .get(&source_canonical, schema_name)
                .await?
                .ok_or_else(|| {
                    FoldDbError::Schema(SchemaError::CatalogMembershipDenied {
                        db_locator: source_canonical.clone(),
                        schema_name: schema_name.to_string(),
                    })
                })?;
            (source_entry.instance_id, source_entry.key_selection)
        };

        let target_entry = DbCatalogEntry {
            db_locator: target_canonical.clone(),
            schema_name: schema_name.to_string(),
            instance_id: source_instance,
            key_selection: source_selection,
        };
        if let Some(existing) = catalog.get(&target_canonical, schema_name).await? {
            if existing != target_entry {
                return Err(StorageError::InvalidOperation(format!(
                    "database catalog target '{target_canonical}' already references another instance for schema '{schema_name}'"
                ))
                .into());
            }
        }

        let schema = self
            .schema_manager()
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_string()))?;
        let storage_prefix = target_entry.instance_id.clone();
        let mut molecule_uuids: Vec<String> = schema
            .field_molecule_uuids
            .as_ref()
            .into_iter()
            .flat_map(|molecules| molecules.values().cloned())
            .collect();
        molecule_uuids.sort_unstable();
        molecule_uuids.dedup();
        let mut _molecule_guards = Vec::with_capacity(molecule_uuids.len());
        for molecule_uuid in &molecule_uuids {
            _molecule_guards.push(
                self.db_ops()
                    .atoms()
                    .lock_molecule_commit(molecule_uuid, storage_prefix.as_deref())
                    .await,
            );
        }
        let molecules_wrapped = self
            .db_ops()
            .molecule_keys()
            .grant_schema_domain(&schema, target_access_domain, target_domain_wrap_key)
            .await?;

        #[cfg(feature = "cloud-sync")]
        let republished_rows = if source_parsed == DbLocator::Personal {
            self.attach_shared_instance_to_org_targets(
                &target_canonical,
                schema_name,
                target_entry.instance_id.as_deref(),
            )
            .await?;
            self.republish_personal_schema_to_org_targets(schema_name)
                .await?
        } else {
            0
        };
        #[cfg(not(feature = "cloud-sync"))]
        let republished_rows = 0;
        let atom_ref_plan = self
            .db_ops()
            .atoms()
            .prepare_catalog_atom_refs(
                &target_canonical,
                schema_name,
                &schema,
                storage_prefix.as_deref(),
            )
            .await?;
        if let Err(catalog_error) = catalog.put(&target_entry).await {
            let abort_result = self
                .db_ops()
                .atoms()
                .abort_catalog_atom_refs(&atom_ref_plan)
                .await;
            return Err(catalog_put_failure(catalog_error, abort_result));
        }
        self.db_ops()
            .atoms()
            .commit_catalog_atom_refs(atom_ref_plan)
            .await?;
        Ok(DbSchemaShareResult {
            source_db_locator: source_canonical,
            target_entry,
            access_domain: target_access_domain.to_string(),
            molecules_wrapped,
            republished_rows,
        })
    }
}

fn catalog_put_failure(
    catalog_error: StorageError,
    abort_result: Result<(), SchemaError>,
) -> FoldDbError {
    match abort_result {
        Ok(()) => catalog_error.into(),
        Err(abort_error) => FoldDbError::Database(format!(
            "database catalog write failed: {catalog_error}; abort database-catalog atom reference transition also failed: {abort_error}"
        )),
    }
}
