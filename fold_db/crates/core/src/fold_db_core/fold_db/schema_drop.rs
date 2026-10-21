//! Drop a schema together with everything that names its molecules.
//!
//! `SchemaCore::drop_schema` removes the schema record, its schema-field bindings
//! and its protein members in one batch. A database catalog row that names the
//! schema is another binding, in the scope of the row. It is not in that batch:
//! the schema manager has no catalog. This call removes the catalog rows first,
//! with their bindings and without any instance reclaim
//! (`delete_catalog_rows_for_drop`), and then drops the schema. When the last
//! binding of a molecule goes, the same batch writes a candidate row for the
//! molecule drain, which erases the keys of the dropped schema in small batches and
//! then the molecule (`atom_store/molecule_drain`). The instance reclaim of
//! `DELETE /api/db/catalog` is not used here: it deletes a whole instance in one
//! native request, and an instance of more than a few hundred keys is over the
//! budget of one request.
//!
//! If the call fails or the process stops between the two steps, the catalog rows
//! are gone and the schema is still installed. The call can run again and ends the
//! drop: it removes the rows that are left and then the schema.
//!
//! The catalog is read with `list_entries`, the hygiene enumeration of the catalog
//! store (a scan of the catalog, which holds one row for each database and schema).
//! The call is not on a data path.

use super::FoldDB;
use crate::error::FoldDbResult;

impl FoldDB {
    /// Drop one schema: its catalog rows, then its record, schema-field bindings
    /// and protein members. Returns whether the schema was installed.
    pub async fn drop_schema_and_bindings(&self, schema_name: &str) -> FoldDbResult<bool> {
        self.delete_catalog_rows_for_drop(schema_name).await?;
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
