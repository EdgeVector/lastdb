//! Catalog rows of named databases.
//!
//! A catalog row names a schema instance for a database. The put route writes
//! one catalog row in one native write scope.

use super::FoldDB;
use crate::db_operations::DbCatalogEntry;
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
}
