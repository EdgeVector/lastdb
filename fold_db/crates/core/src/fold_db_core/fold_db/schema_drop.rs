//! Drop a schema together with everything that names its molecules.
//!
//! `SchemaCore::drop_schema` removes the schema record, its schema-field bindings
//! and its protein members in one batch. A database catalog row that names the
//! schema is another binding, in the scope of the row. It is not in that batch:
//! the schema manager has no catalog. This call removes the catalog rows first,
//! each row with its bindings in one batch (`delete_catalog_membership`), and then
//! drops the schema. When the last binding of a molecule goes, the same batch
//! writes a candidate row for the molecule drain, which erases the keys of the
//! dropped schema and then the molecule (`atom_store/molecule_drain`).
//!
//! The catalog is read with `list_entries`, the hygiene enumeration of the catalog
//! store. The catalog is small (one row for each database and schema) and the call
//! is not on a data path.

use super::FoldDB;
use crate::error::FoldDbResult;
use crate::schema::SchemaError;

impl FoldDB {
    /// Drop one schema: its catalog rows, then its record, schema-field bindings
    /// and protein members. Returns whether the schema was installed.
    pub async fn drop_schema_and_bindings(&self, schema_name: &str) -> FoldDbResult<bool> {
        let entries = self
            .db_ops()
            .db_catalog()
            .list_entries()
            .await
            .map_err(SchemaError::from)?;
        for entry in entries
            .iter()
            .filter(|entry| entry.schema_name == schema_name)
        {
            self.delete_catalog_membership(&entry.db_locator, &entry.schema_name)
                .await?;
        }
        Ok(self.schema_manager().drop_schema(schema_name).await?)
    }

    /// Drop every schema that `owner_app` owns, one at a time. Returns the names
    /// that were installed and are dropped, sorted.
    pub async fn drop_schemas_owned_by_with_bindings(
        &self,
        owner_app: &str,
    ) -> FoldDbResult<Vec<String>> {
        let mut dropped = Vec::new();
        for name in self.schema_manager().schema_names_owned_by(owner_app)? {
            if self.drop_schema_and_bindings(&name).await? {
                dropped.push(name);
            }
        }
        dropped.sort();
        Ok(dropped)
    }
}
