//! Catalog rows of named databases.
//!
//! A catalog row names a schema instance for a database. The two functions here
//! are writers of a catalog row besides the share route: the put route and the
//! declare of a schema under a named database. Each runs in one native write
//! scope.

use super::FoldDB;
use crate::db_operations::{DbCatalogEntry, DbCatalogKeySelection};
use crate::error::FoldDbResult;
use crate::schema::SchemaError;

impl FoldDB {
    /// Write one catalog row.
    pub async fn put_catalog_membership(&self, entry: &DbCatalogEntry) -> FoldDbResult<()> {
        self.db_ops()
            .records()
            .in_write_scope(async {
                self.db_ops().db_catalog().put(entry).await?;
                Ok::<(), SchemaError>(())
            })
            .await?;
        Ok(())
    }

    /// The row of a named database for a schema, written when it is absent. An
    /// existing row wins, so a re-declare does not mint a second instance. The
    /// read and the optional insert share one native write scope, so another
    /// writer cannot remove or replace the selected row between them.
    pub async fn ensure_catalog_membership(
        &self,
        db_locator: &str,
        schema_name: &str,
        instance_id: Option<String>,
    ) -> FoldDbResult<DbCatalogEntry> {
        Ok(self
            .db_ops()
            .records()
            .in_write_scope(async {
                let catalog = self.db_ops().db_catalog();
                if let Some(existing) = catalog.get(db_locator, schema_name).await? {
                    return Ok(existing);
                }
                let entry = DbCatalogEntry {
                    db_locator: db_locator.to_string(),
                    schema_name: schema_name.to_string(),
                    instance_id,
                    key_selection: DbCatalogKeySelection::WholeSchema,
                };
                catalog.put(&entry).await?;
                Ok(entry)
            })
            .await?)
    }
}
